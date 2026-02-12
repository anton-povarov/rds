use clap::Parser;
use std::sync::{Arc, atomic};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::watch;

use rds as lib;

use lib::server::ConnectionHandler;
use lib::server::DBEngine;
use lib::server::Stats;
use lib::server::lmdbx::LmdbxStorage;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'p', long = "port", default_value = "9000")]
    port: u16,

    #[arg(long = "db", default_value = "./db")]
    db_path: String,

    #[arg(short = 't', long = "threads", default_value = "4")]
    n_threads: u16,
}

fn pick_thread_split(total_threads: usize) -> (usize, u32) {
    let total_threads = std::cmp::max(2, total_threads);
    let mut db_workers = total_threads.saturating_div(3).clamp(1, 4);
    if db_workers >= total_threads {
        db_workers = total_threads - 1;
    }

    let io_threads = total_threads - db_workers;
    (io_threads, db_workers as u32)
}

fn main() {
    let args = Args::parse();
    let requested_total_threads = std::cmp::max(args.n_threads, 2);
    let (io_threads, db_workers) = pick_thread_split(args.n_threads as usize);

    println!(
        "thread config: total={} (io_threads={} db_workers={}) (+1 db sync thread)",
        requested_total_threads, io_threads, db_workers
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .enable_all()
        .build()
        .unwrap();

    match rt.block_on(async { server_main(&args, db_workers).await }) {
        Ok(_) => {}
        Err(e) => println!("Error: {}", e),
    }
}

async fn server_main(args: &Args, db_workers: u32) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let _shutdown_watch_task = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("failed to wait for Ctrl+C");
        _ = shutdown_tx.send(true);
    });

    let listen_addr = &format!("0.0.0.0:{}", args.port);
    println!("Listening on {listen_addr}");
    // TODO: SO_REUSEPORT via socket2 crate on std::net::TcpListener
    let listener = TcpListener::bind(listen_addr).await.map_err(|e| {
        return format!("listen error: {} {}", listen_addr, e);
    })?;

    let stats = Arc::new(Stats::new());
    let db = LmdbxStorage::open(&args.db_path).map_err(|e| {
        return format!("lmdbx error: {}", e);
    })?;
    let db_engine = DBEngine::startup(db_workers, db);

    loop {
        select! {
            _ = shutdown_rx.changed() => {
                break;
            },
            res = listener.accept() => {
                match res {
                    Err(e) => {
                        println!("accept error: {e}");
                        const SLEEP_FOR: Duration = Duration::from_millis(1_000);
                        println!("sleeping for {} seconds before continuing", SLEEP_FOR.as_secs_f32(),);
                        tokio::time::sleep(SLEEP_FOR).await;
                        continue;
                    },
                    Ok((socket, peer_addr)) => {
                        let (db_response_tx, db_response_rx) = flume::bounded(1);
                        let mut conn_handler = ConnectionHandler {
                            stats: Arc::clone(&stats),
                            socket: socket,
                            peer_addr: peer_addr,
                            shutdown_rx: shutdown_rx.clone(),
                            db_request_tx: db_engine.get_worker_channel(),
                            db_response_tx: db_response_tx,
                            db_response_rx: db_response_rx,
                        };
                        tokio::spawn(async move {
                            conn_handler.stats.n_connections.fetch_add(1, atomic::Ordering::Relaxed);
                            conn_handler.handle_connection().await;
                            conn_handler.stats.n_connections.fetch_sub(1, atomic::Ordering::Relaxed);
                        });
                    },
                };
            }
        }
    }

    println!("\rmain: shutdown sequence initiated");

    const WAIT_FOR_CONNECTIONS_SECONDS: u32 = 3;
    for i in 0..WAIT_FOR_CONNECTIONS_SECONDS {
        let n_connections = stats.n_connections.load(atomic::Ordering::Relaxed);

        println!(
            "waiting for connections to close; active: {}; time remaining (max): {} sec",
            n_connections,
            WAIT_FOR_CONNECTIONS_SECONDS - i
        );
        if n_connections == 0 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(1_000)).await;
    }

    db_engine.shutdown();

    println!("\rmain: bye");
    return Ok(());
}
