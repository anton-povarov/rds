use bytes::Bytes;
use flume;
use rand::seq::IndexedRandom;
use std::{borrow::Cow, cell::Cell, collections::HashMap, sync::Arc, thread::JoinHandle};

use crate::server::lmdbx;

#[derive(Debug)]
pub enum DBMessage {
    Get {
        key: &'static str,
    },
    Set {
        key: &'static str,
        data: bytes::Bytes,
    },
}

#[derive(Debug)]
pub struct DBRequest {
    pub msg: DBMessage,
    pub reply: flume::Sender<DBResponse>,
}

pub enum DBResponse {
    Get { data: Option<bytes::Bytes> },
    Set { ok: bool },
}

struct DBSyncMessage {
    data: HashMap<Arc<str>, Bytes>,
}

pub struct DBEngine {
    workers: Vec<DBWorkerThread>,
    saver: DBSyncThread,
    worker_shutdown_tx: flume::Sender<bool>,
    saver_shutdown_tx: flume::Sender<bool>,
}

impl DBEngine {
    pub fn startup(n_workers: u32, db: Arc<lmdbx::LmdbxStorage>) -> Self {
        let (w_shutdown_tx, w_shutdown_rx) = flume::unbounded::<bool>();
        let (s_shutdown_tx, s_shutdown_rx) = flume::unbounded::<bool>();

        let saver = DBSyncThread::spawn(s_shutdown_rx.clone(), db.clone());
        let workers = (0..n_workers)
            .map(|id| {
                DBWorkerThread::spawn(id, w_shutdown_rx.clone(), db.clone(), saver.get_sender())
            })
            .collect();

        DBEngine {
            workers: workers,
            saver: saver,
            worker_shutdown_tx: w_shutdown_tx,
            saver_shutdown_tx: s_shutdown_tx,
        }
    }

    pub fn shutdown(self) {
        self.workers.iter().for_each(|_| {
            _ = self.worker_shutdown_tx.send(true);
        });
        self.workers.into_iter().for_each(|w| {
            let id = w.id;
            if let Err(e) = w.join() {
                println!("error joining db_worker[{}]: {:?}", id, e);
            };
        });

        _ = self.saver_shutdown_tx.send(true);
        if let Err(e) = self.saver.thr.join() {
            println!("error joining db_saver: {:?}", e);
        }
    }

    pub fn get_worker_channel(&self) -> flume::Sender<DBRequest> {
        let mut rng = rand::rng();
        self.workers.choose(&mut rng).unwrap().get_channel()
    }
}

struct DBSyncThread {
    input_tx: flume::Sender<DBSyncMessage>,
    thr: JoinHandle<()>,
}

impl DBSyncThread {
    const SYNC_THREAD_CHANNEL_BUFFER_SZ: usize = 64;

    fn spawn(shutdown_rx: flume::Receiver<bool>, db: Arc<lmdbx::LmdbxStorage>) -> Self {
        let (tx, rx) //:
            = flume::bounded::<DBSyncMessage>(Self::SYNC_THREAD_CHANNEL_BUFFER_SZ);

        let thr = std::thread::spawn(move || {
            println!("db_sync: started");

            let should_shutdown = Cell::new(false);
            while !should_shutdown.get() {
                flume::Selector::new()
                    .recv(&shutdown_rx, |_| {
                        should_shutdown.set(true);
                    })
                    .recv(&rx, |msg_or_err| {
                        let msg = match msg_or_err {
                            Ok(x) => x,
                            Err(_) => {
                                should_shutdown.set(true);
                                return;
                            }
                        };

                        let tx = db.db.begin_raw_rw_txn().expect("RW transaction start failed");
                        let table = tx.open_table(None).unwrap();

                        for (k, v) in msg.data.into_iter() {
                            tx.put(&table, k.as_bytes(), v, libmdbx::WriteFlags::UPSERT).unwrap();
                        }

                        _ = tx.commit().inspect_err(|e| {
                            println!("db_sync: THIS IS BAD, sync err: {}", e);
                        });
                    })
                    .wait();
            }

            println!("db_sync: exiting");
        });
        DBSyncThread {
            input_tx: tx,
            thr: thr,
        }
    }

    fn get_sender(&self) -> flume::Sender<DBSyncMessage> {
        self.input_tx.clone()
    }
}

struct DBWorkerThread {
    id: u32,
    thr: JoinHandle<()>,
    input_tx: flume::Sender<DBRequest>,
}

struct DBThreadContext<'db> {
    #[expect(unused)]
    id: u32,
    db: &'db lmdbx::LmdbxStorage,
    db_table: libmdbx::Table<'db>,
    transaction: Option<lmdbx::TransactionRO<'db>>,
    write_cache: HashMap<Arc<str>, Bytes>, // sent to syncer thread periodically
    read_cache: HashMap<Arc<str>, Option<Bytes>>, // negative & positive cache
    syncer_tx: flume::Sender<DBSyncMessage>,
}

impl DBWorkerThread {
    const CHANNEL_BUFFER_SZ: usize = 128;
    const WRITE_CACHE_INITIAL_SIZE: usize = 16 * 1024;
    const WRITE_CACHE_HIGH_WATERMARK: usize = 14 * 1024; // send before the first rehash
    const READ_CACHE_INITIAL_SIZE: usize = 16 * 1024;
    const READ_CACHE_HIGH_WATERMARK: usize = 14 * 1024; // clear before the first rehash

    pub fn spawn(
        id: u32,
        shutdown_rx: flume::Receiver<bool>,
        db: Arc<lmdbx::LmdbxStorage>,
        syncer_tx: flume::Sender<DBSyncMessage>,
    ) -> DBWorkerThread {
        let (db_writer_tx, db_writer_rx) = flume::bounded::<DBRequest>(Self::CHANNEL_BUFFER_SZ);

        let thr = std::thread::spawn({
            move || {
                println!("db_saver[{}]: starting", id);

                // permaopen the table we'll need for all transactions ![in this thread]
                let db_table = {
                    let tx = db.db.begin_ro_txn().unwrap();
                    let table = tx.open_table(None).unwrap();
                    tx.prime_for_permaopen(table);
                    let (_, tvec) = tx.commit_and_rebind_open_dbs().unwrap();
                    tvec.into_iter().next().unwrap()
                };

                let mut ctx = DBThreadContext {
                    id: id,
                    db: &db,
                    db_table: db_table,
                    transaction: None,
                    write_cache: HashMap::with_capacity(Self::WRITE_CACHE_INITIAL_SIZE),
                    read_cache: HashMap::with_capacity(Self::READ_CACHE_INITIAL_SIZE),
                    syncer_tx: syncer_tx,
                };

                let mut next_tick = std::time::Instant::now();

                println!("db_saver[{}]: infinite loop begin", id);
                loop {
                    if let Ok(_) = shutdown_rx.try_recv() {
                        break;
                    }

                    match db_writer_rx.recv_deadline(next_tick) {
                        Ok(msg) => {
                            let handle_r = { Self::handle_message(&mut ctx, msg.msg) };
                            match handle_r {
                                Ok(r) => msg.reply.send(r).unwrap(), // sender is expected to wait for result
                                Err(e) => eprintln!("db_saver[{}]: db error: {:?}", id, e),
                            }
                        }
                        Err(flume::RecvTimeoutError::Timeout) => {
                            next_tick += std::time::Duration::from_millis(1_000);

                            let handle_r = { Self::commit_outstanding_writes(&mut ctx) };
                            match handle_r {
                                Ok(_) => {}
                                Err(e) => eprintln!("db_saver[{}]: db error: {:?}", id, e),
                            }
                        }
                        Err(flume::RecvTimeoutError::Disconnected) => {
                            break;
                        }
                    }
                }
                println!("db_saver[{}]: exiting", id);
            }
        });

        DBWorkerThread {
            id: id,
            thr: thr,
            input_tx: db_writer_tx,
        }
    }

    fn handle_message(
        ctx: &mut DBThreadContext<'_>,
        msg: DBMessage,
    ) -> Result<DBResponse, Box<dyn std::error::Error>> {
        match msg {
            DBMessage::Get { key } => {
                if let Some(b) = ctx.write_cache.get(key) {
                    return Ok(DBResponse::Get {
                        data: Some(b.clone()),
                    });
                }

                match ctx.read_cache.get(key) {
                    Some(Some(b)) => {
                        return Ok(DBResponse::Get {
                            data: Some(b.clone()),
                        });
                    }
                    Some(None) => return Ok(DBResponse::Get { data: None }),
                    None => {}
                };

                // create the read transaction if it doesn't yet exist
                if ctx.transaction.is_none() {
                    let tx: lmdbx::TransactionRO =
                        ctx.db.db.begin_ro_txn().expect("transaction start failed");
                    ctx.transaction = Some(tx);
                }
                let tx = ctx.transaction.as_ref().unwrap();

                // can read now, we will need to copy the data
                //  because transaction can be dropped at any point after this (and the calling) function returns
                //  todo(antoxa): could maybe implement something for Bytes::from_owned() to work
                //   it will keep the transaction alive for longer - the same lifetime as the read_cache
                let db_data = tx.get::<Cow<_>>(&ctx.db_table, key.as_bytes())?;
                let data = match db_data {
                    Some(x) => Some(Bytes::copy_from_slice(&x)),
                    None => None,
                };

                ctx.read_cache.insert(Arc::from(key), data.clone());

                Ok(DBResponse::Get { data: data })
            }

            DBMessage::Set { key, data } => {
                let rc_key = Arc::from(key);

                ctx.read_cache.insert(Arc::clone(&rc_key), Some(data.clone()));

                ctx.write_cache.insert(Arc::clone(&rc_key), data.clone());
                if ctx.write_cache.len() > Self::WRITE_CACHE_HIGH_WATERMARK {
                    Self::commit_outstanding_writes(ctx)?;
                }

                Ok(DBResponse::Set { ok: true })
            }
        }
    }

    fn commit_outstanding_writes(
        ctx: &mut DBThreadContext,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !ctx.write_cache.is_empty() {
            //
            // NOTE(antoxa): not needed anymore, since we only have read trx-s here, keeping for posteriority
            //
            // we must avoid overlapping read and read-write transactions in the same thread
            // Results in error from libmdbx MDBX_TXN_OVERLAPPING = -30415
            // can just drop the read transaction and let it be auto-reopened on next incoming message
            // if ctx.ro_transaction.is_some() {
            //     let tx = ctx.ro_transaction.take();
            //     drop(tx);
            // }

            // send the whole write cache to the syncer thread, replacing it locally with an empty cache
            let send_wcache = std::mem::replace(
                &mut ctx.write_cache,
                HashMap::with_capacity(Self::WRITE_CACHE_INITIAL_SIZE),
            );
            ctx.syncer_tx.send(DBSyncMessage { data: send_wcache }).unwrap();
        }

        // FIXME: this should not be here, read_cache lifecycle
        //  should be independent from the write_cache lifecycle
        if ctx.read_cache.len() > Self::READ_CACHE_HIGH_WATERMARK {
            ctx.read_cache.clear();
        }

        Ok(())
    }

    pub fn get_channel(&self) -> flume::Sender<DBRequest> {
        self.input_tx.clone()
    }

    pub fn join(self) -> Result<(), Box<dyn std::any::Any + Send + 'static>> {
        return self.thr.join();
    }
}
