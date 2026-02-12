use bytes::BytesMut;
use std::cmp::min;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::watch;

use crate::server::Stats;
use crate::server::db::{DBMessage, DBRequest, DBResponse};
use crate::server::parse;

pub struct ConnectionHandler {
    pub stats: Arc<Stats>,

    pub socket: TcpStream,
    pub peer_addr: SocketAddr,

    pub shutdown_rx: watch::Receiver<bool>,
    pub db_request_tx: flume::Sender<DBRequest>,
    pub db_response_tx: flume::Sender<DBResponse>,
    pub db_response_rx: flume::Receiver<DBResponse>,
}

macro_rules! CH_stats_add {
    ($self:expr, $($field:ident).+, $value:expr) => {{
		let field_val = & $self.stats.$($field).+;
		field_val.fetch_add($value, atomic::Ordering::Relaxed);
    }};
}

#[derive(PartialEq)]
enum ShouldDisconnect {
    No,
    Yes,
}

impl ConnectionHandler {
    pub async fn handle_connection(&mut self) {
        let started = Instant::now();
        let sock_fd = self.socket.as_raw_fd();

        println!("connection accepted on fd {:?} from {}", sock_fd, self.peer_addr);

        match self.handle_connection_loop().await {
            Ok(_) => {}
            Err(e) => println!("connection fd {sock_fd}, error: {}", e),
        };

        let elapsed = Instant::now() - started;
        println!("connection closed on fd {} after {:.6} seconds", sock_fd, elapsed.as_secs_f32());
    }

    async fn handle_connection_loop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // FIXME: this buf can grow indefinitely in capacity, should reset it from time to time
        let mut buf = BytesMut::with_capacity(1024);

        loop {
            select! {
                _ = self.shutdown_rx.changed() => {
                    return Ok(())
                },

                res = self.socket.read_buf(&mut buf) => {
                    let n = res?;
                    if n == 0 {
                        return Ok(());
                    }

                    while let Some(idx) = parse::find_crlf(&buf) {
                        let mut cmd_buf = buf.split_to(idx + 2 /*\r\n*/);
                        cmd_buf.truncate(idx);

                        // check if the command is ascii, we don't want multibyte utf-8 shenanigans
                        if !cmd_buf.iter().all(|b| b.is_ascii()) {
                            self.socket
                                .write_all("ERROR: non-ascii characters in request\r\n".as_bytes())
                                .await?;
                            continue;
                        }

                        // safe now, ascii is utf-8
                        let cmdline = unsafe { std::str::from_utf8_unchecked(&cmd_buf) };

                        // handle_command() can read more - from the buffer or even from the network
                        //  it will modify buf as needed to make sure next command is at the top
                        match self.handle_command(cmdline, &mut buf).await? {
                            ShouldDisconnect::Yes => return Ok(()),
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(
        &mut self,
        cmdline: &str,
        read_buf: &mut BytesMut,
    ) -> Result<ShouldDisconnect, io::Error> {
        // println!("command_line = {:?}, read_buf: \"{}\"", cmdline, read_buf.escape_ascii());

        const KEY_MAX_LEN: usize = 250;
        const KEY_ERROR_MSG: &str = "ERROR: format: len(key) must be in [1..250]\r\n";
        const DATA_MAX_LEN: usize = 1 * 1024 * 1024;
        const DATA_ERROR_MSG: &str = "ERROR: format: len(data) must be in <= 1Mb\r\n";

        let (verb, argline) = cmdline.split_once(' ').unwrap_or((cmdline, ""));
        match verb {
            "quit" => {
                CH_stats_add!(self, requests.quit, 1);
                return Ok(ShouldDisconnect::Yes);
            }

            "get" | "GET" => {
                // request: get <key>\r\n
                // response (ok): <key> <data_len>\r\n<data>\r\n
                // response (err): NOT_FOUND\r\n
                // response (internal err): INTERNAL_ERROR: <message>\r\n

                CH_stats_add!(self, requests.get, 1);

                let mut args = argline.split_whitespace();
                let key = match args.next() {
                    Some(k) if (1..=KEY_MAX_LEN).contains(&k.len()) => k,
                    _ => {
                        CH_stats_add!(self, requests._bad, 1);
                        self.socket.write_all(KEY_ERROR_MSG.as_bytes()).await?;
                        return Ok(ShouldDisconnect::No);
                    }
                };

                let msg = DBRequest {
                    msg: DBMessage::Get {
                        // this is safe because we wait for response, keeping the str buffer alive
                        key: unsafe { std::mem::transmute::<&str, &'static str>(key) },
                    },
                    reply: self.db_response_tx.clone(),
                };

                match self.db_request_tx.send(msg) {
                    Ok(_) => {}
                    Err(_e) => {
                        // The only option is "all receivers have dropped" as per flume's code,
                        // but there is no dedicated enum value for it.
                        // So we assume that db threads have exited and we're about to exit as well.
                        self.socket.write_all("ERROR: shutting down\r\n".as_bytes()).await?;
                        return Ok(ShouldDisconnect::Yes);
                    }
                };

                match self.db_response_rx.recv_async().await {
                    Ok(DBResponse::Get { data }) => match data {
                        Some(mut buf) => {
                            CH_stats_add!(self, requests.get_hit, 1);
                            self.socket.write_all_buf(&mut buf).await?;
                        }
                        None => {
                            self.socket.write_all("NOT_FOUND\r\n".as_bytes()).await?;
                        }
                    },
                    Err(flume::RecvError::Disconnected) => {
                        // No senders remain, so we're not going to receive anything back, ever,
                        //  and the next send attempt might block forever as well.
                        // So we assume that db threads have exited and we're about to exit as well.
                        self.socket.write_all("ERROR: shutting down\r\n".as_bytes()).await?;
                        return Ok(ShouldDisconnect::Yes);
                    }
                    _ => {
                        unreachable!()
                    }
                };

                return Ok(ShouldDisconnect::No);
            }

            "set" | "SET" => {
                // set <key> <data_len>\r\n<data>\r\n
                // response (ok): STORED\r\n
                // response (err): ERROR: <helpful text>\r\n

                let mut args = argline.split_whitespace();

                let key = match args.next() {
                    Some(k) if (1..=KEY_MAX_LEN).contains(&k.len()) => k,
                    _ => {
                        CH_stats_add!(self, requests._bad, 1);
                        self.socket.write_all(KEY_ERROR_MSG.as_bytes()).await?;
                        return Ok(ShouldDisconnect::No);
                    }
                };

                let data_len = match args.next().and_then(|s| s.parse::<usize>().ok()) {
                    Some(dl) if (1..=DATA_MAX_LEN).contains(&dl) => dl,
                    _ => {
                        CH_stats_add!(self, requests._bad, 1);
                        self.socket.write_all(DATA_ERROR_MSG.as_bytes()).await?;
                        return Ok(ShouldDisconnect::No);
                    }
                };

                // read all data into a buffer we can store
                let mut data_buf = BytesMut::with_capacity(
                    data_len + argline.len() + 2 /*\r\n after argline*/ + 2, /*\r\n at the end*/
                );

                // header - it is in network response format already
                data_buf.extend_from_slice(argline.as_bytes());
                data_buf.extend_from_slice("\r\n".as_bytes());

                // copy whatever data is in the read buffer first
                let mut remaining_len = data_len + 2;
                let copy_len = min(remaining_len, read_buf.len());
                data_buf.extend_from_slice(&read_buf.split_to(copy_len));

                // read the rest from the network up to data_len
                remaining_len -= copy_len;
                while remaining_len > 0 {
                    // this means `read_buf` has been fully drained
                    // we continue reading into the `data_buf` here

                    let n = self.socket.read_buf(&mut data_buf).await?;
                    match n {
                        0 => return Err(io::ErrorKind::ConnectionReset.into()),
                        n => remaining_len -= n,
                    }
                }

                // actually execute the handler now
                {
                    CH_stats_add!(self, requests.set, 1);

                    let msg = DBRequest {
                        msg: DBMessage::Set {
                            key: unsafe { std::mem::transmute::<&str, &'static str>(key) },
                            data: data_buf.freeze(),
                        },
                        reply: self.db_response_tx.clone(),
                    };

                    match self.db_request_tx.send(msg) {
                        Ok(_) => {}
                        Err(_e) => {
                            self.socket.write_all("ERROR: shutting down\r\n".as_bytes()).await?;
                            return Ok(ShouldDisconnect::Yes);
                        }
                    };

                    match self.db_response_rx.recv_async().await {
                        Ok(_) => {
                            self.socket.write_all("STORED\r\n".as_bytes()).await?;
                        }
                        Err(flume::RecvError::Disconnected) => {
                            self.socket.write_all("ERROR: shutting down\r\n".as_bytes()).await?;
                            return Ok(ShouldDisconnect::Yes); // db_saver has exited or whatnot
                        }
                    };
                }

                return Ok(ShouldDisconnect::No);
            } // set

            _ => {
                CH_stats_add!(self, requests._bad, 1);
                self.socket.write_all("ERROR: unknown command\r\n".as_bytes()).await?;
            }
        };

        return Ok(ShouldDisconnect::No);
    }
}
