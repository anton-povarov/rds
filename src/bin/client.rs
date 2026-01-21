use std::{sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Barrier,
    task::JoinSet,
    time,
};

use clap::{self, Parser};
use rand::{self, Rng, distr::Alphanumeric, seq::SliceRandom};

#[derive(clap::Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'c', long = "connect", default_value = "localhost:9000")]
    address: String,

    #[arg(short = 't', long = "tasks", default_value = "10")]
    n_tasks: usize,

    #[arg(short = 'k', long = "keys", default_value = "1000")]
    n_keys: usize,

    #[arg(long = "hot-pct", default_value = "10")]
    hot_keys_percentage: usize,

    #[arg(long = "populate", action = clap::ArgAction::SetTrue, default_value_t = false)]
    populate: bool,

    #[arg(short = 'r', long = "requests", default_value = "100")]
    n_requests: usize,
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());

    println!("generating {} random keys", args.n_keys);
    let (context, elapsed) = {
        let started = time::Instant::now();
        let all_keys = generate_random_keys(args.n_keys);
        let hot_keys = all_keys
            .clone()
            .partial_shuffle(&mut rand::rng(), args.n_keys * args.hot_keys_percentage / 100)
            .0
            .to_vec();

        (
            Arc::new(SharedContext {
                args: Arc::clone(&args),
                barrier: Barrier::new(args.n_tasks),
                all_keys: all_keys,
                hot_keys: hot_keys,
            }),
            (time::Instant::now() - started),
        )
    };
    println!("keys generated, elapsed {} sec", elapsed.as_secs_f32());

    let mut tasks = JoinSet::new();
    for conn_id in 0..args.n_tasks {
        let ctx = Arc::clone(&context);
        tasks.spawn(async move {
            match client_connection(conn_id, ctx).await {
                // Ok(_) => println!("connection {} finished", conn_id),
                Ok(_) => {}
                Err(e) => println!("connection {} error: {}", conn_id, e),
            }
        });
    }
    tasks.join_all().await;
}

struct SharedContext {
    args: Arc<Args>,
    barrier: tokio::sync::Barrier,
    all_keys: Vec<String>,
    hot_keys: Vec<String>,
}

async fn client_connection(conn_id: usize, ctx: Arc<SharedContext>) -> Result<(), io::Error> {
    let args = &ctx.args;
    let mut connection =
        tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(&args.address)).await??;

    if ctx.barrier.wait().await.is_leader() {
        println!("prepare() barrier, proceeding");
    }

    // set the keys
    if args.populate {
        let started = time::Instant::now();
        let key_slice_sz = args.n_keys / args.n_tasks;

        let begin = key_slice_sz * conn_id;
        let end = key_slice_sz * (conn_id + 1);
        println!("task {}; set()-ing slice {:?}", conn_id, (begin..end));

        const SET_BATCH_SIZE: usize = 10;
        let mut i = begin;
        while i < end {
            let mut j = i;
            while j < (i + SET_BATCH_SIZE) && j < end {
                let data = b"test \tdata\t";
                // todo: write_vectored or reuse buf and write in batches or construct a buffer precicely with full req
                connection
                    .write_all(format!("set {} {}\r\n", ctx.all_keys[j], data.len()).as_bytes())
                    .await?;
                connection.write_all(data).await?;
                connection.write_all("\r\n".as_bytes()).await?;

                j += 1;
            }
            i = j;

            // just drain reads, don't care
            read_drain_connection(&mut connection).await?;
        }

        println!("task {}; set()-ing done, waiting on barrier", conn_id);
        if ctx.barrier.wait().await.is_leader() {
            let req_total = args.n_keys;
            let elapsed = time::Instant::now() - started;
            println!(
                "{} set() requests done in {:.6} sec; {:.0} rps",
                req_total,
                elapsed.as_secs_f32(),
                req_total as f64 / elapsed.as_secs_f64()
            );
        }
    }

    {
        let started = time::Instant::now();

        const GET_BATCH_SIZE: usize = 25;
        let mut i = 0;
        while i < args.n_requests {
            let mut j = i;
            while j < (i + GET_BATCH_SIZE) && i < args.n_requests {
                connection
                    .write_all(
                        format!(
                            "get {}\r\n",
                            ctx.all_keys[rand::rng().random_range(0..ctx.all_keys.len())]
                        )
                        .as_bytes(),
                    )
                    .await?;

                j += 1;
            }
            i = j;

            // just drain reads, don't care
            read_drain_connection(&mut connection).await?;
        }

        if ctx.barrier.wait().await.is_leader() {
            let req_total = args.n_requests * args.n_tasks;
            let elapsed = time::Instant::now() - started;
            println!(
                "{} get() requests after {:.6} sec; {:.0} rps",
                req_total,
                elapsed.as_secs_f32(),
                req_total as f64 / elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}

fn generate_random_keys(n_keys: usize) -> Vec<String> {
    let mut rng = rand::rng();
    let mut all_keys = Vec::<String>::new();
    for _ in 0..n_keys {
        let range = Rng::random_range(&mut rng, 5..=100);
        let key: String =
            Rng::sample_iter(&mut rng, &Alphanumeric).take(range).map(char::from).collect();
        all_keys.push(key);
    }
    all_keys
}

async fn read_drain_connection(sock: &mut TcpStream) -> Result<(), io::Error> {
    let mut rdbuf = [0u8; 64 * 1024];
    loop {
        match sock.try_read(&mut rdbuf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}
