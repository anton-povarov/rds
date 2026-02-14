use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use clap::{self, Parser};
use memchr::memchr;
use rand::{
    self, Rng,
    distr::{Alphanumeric, SampleString},
    seq::SliceRandom,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Barrier, BarrierWaitResult},
    task::JoinSet,
    time,
};

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

    #[arg(long = "datalen", default_value = "10")]
    data_len: usize,
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
                all_keys,
                hot_keys,
            }),
            time::Instant::now() - started,
        )
    };
    println!("keys generated, elapsed {} sec", elapsed.as_secs_f32());

    let mut tasks = JoinSet::new();
    for conn_id in 0..args.n_tasks {
        let ctx = Arc::clone(&context);
        tasks.spawn(async move {
            match client_connection(conn_id, ctx).await {
                Ok(_) => {}
                Err(e) => println!("connection {} error: {}", conn_id, e),
            }
        });
    }
    tasks.join_all().await;
}

struct SharedContext {
    args: Arc<Args>,
    barrier: Barrier,
    all_keys: Vec<String>,

    #[allow(unused)] // todo: implement hot keys
    hot_keys: Vec<String>,
}

struct ReadProgress {
    responses: AtomicUsize,
}

impl ReadProgress {
    fn new() -> Self {
        Self {
            responses: AtomicUsize::new(0),
        }
    }

    fn increment(&self) {
        self.responses.fetch_add(1, Ordering::Relaxed);
    }

    fn current(&self) -> usize {
        self.responses.load(Ordering::Relaxed)
    }
}

fn key_range_for_task(n_keys: usize, n_tasks: usize, conn_id: usize) -> (usize, usize) {
    let begin = conn_id * n_keys / n_tasks;
    let end = (conn_id + 1) * n_keys / n_tasks;
    (begin, end)
}

async fn client_connection(conn_id: usize, ctx: Arc<SharedContext>) -> Result<(), io::Error> {
    let args = &ctx.args;
    let connection =
        time::timeout(Duration::from_secs(1), TcpStream::connect(&args.address)).await??;
    let (read_half, mut write_half) = connection.into_split();

    let (set_begin, set_end) = key_range_for_task(args.n_keys, args.n_tasks, conn_id);
    let set_count = if args.populate {
        set_end - set_begin
    } else {
        0
    };
    let total_expected = set_count + args.n_requests;

    let read_progress = Arc::new(ReadProgress::new());
    let read_progress_for_task = Arc::clone(&read_progress);
    let reader_task = tokio::spawn(async move {
        read_responses(conn_id, read_half, read_progress_for_task, total_expected).await
    });

    if wait_barrier(&ctx.barrier).await?.is_leader() {
        println!("prepare() barrier, proceeding");
    }

    if args.populate {
        let started = time::Instant::now();
        println!("task {}; set()-ing slice {:?}", conn_id, (set_begin..set_end));

        let data = rand::distr::Alphanumeric.sample_string(&mut rand::rng(), args.data_len);

        send_set_requests(&mut write_half, &ctx.all_keys, set_begin, set_end, data.as_bytes())
            .await?;
        wait_for_responses(&read_progress, set_count).await?;

        if wait_barrier(&ctx.barrier).await?.is_leader() {
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

    let started = time::Instant::now();
    send_get_requests(&mut write_half, &ctx.all_keys, args.n_requests).await?;
    wait_for_responses(&read_progress, total_expected).await?;

    if wait_barrier(&ctx.barrier).await?.is_leader() {
        let req_total = args.n_requests * args.n_tasks;
        let elapsed = time::Instant::now() - started;
        println!(
            "{} get() requests after {:.6} sec; {:.0} rps",
            req_total,
            elapsed.as_secs_f32(),
            req_total as f64 / elapsed.as_secs_f64()
        );
    }

    drop(write_half);
    reader_task.await.map_err(io::Error::other)??;
    Ok(())
}

async fn send_set_requests(
    writer: &mut OwnedWriteHalf,
    all_keys: &[String],
    begin: usize,
    end: usize,
    data: &[u8],
) -> Result<(), io::Error> {
    const SET_MAX_BUFFER: usize = 128 * 1024;
    let set_batch_size = (SET_MAX_BUFFER / data.len()).max(1);
    let data_len_ascii = data.len().to_string();
    let mut write_buf = Vec::with_capacity(SET_MAX_BUFFER);

    let mut i = begin;
    while i < end {
        write_buf.clear();
        let mut j = i;
        while j < (i + set_batch_size) && j < end {
            write_buf.extend_from_slice(b"set ");
            write_buf.extend_from_slice(all_keys[j].as_bytes());
            write_buf.push(b' ');
            write_buf.extend_from_slice(data_len_ascii.as_bytes());
            write_buf.extend_from_slice(b"\r\n");
            write_buf.extend_from_slice(data);
            write_buf.extend_from_slice(b"\r\n");
            j += 1;
        }
        writer.write_all(&write_buf).await?;
        i = j;
    }
    Ok(())
}

async fn send_get_requests(
    writer: &mut OwnedWriteHalf,
    all_keys: &[String],
    n_requests: usize,
) -> Result<(), io::Error> {
    const GET_BATCH_SIZE: usize = 256;
    let mut write_buf = Vec::with_capacity(GET_BATCH_SIZE * 112);

    let mut i = 0;
    while i < n_requests {
        write_buf.clear();
        let mut j = i;
        {
            let mut rng = rand::rng();
            while j < (i + GET_BATCH_SIZE) && j < n_requests {
                let key = &all_keys[rng.random_range(0..all_keys.len())];
                write_buf.extend_from_slice(b"get ");
                write_buf.extend_from_slice(key.as_bytes());
                write_buf.extend_from_slice(b"\r\n");
                j += 1;
            }
        }
        writer.write_all(&write_buf).await?;
        i = j;
    }
    Ok(())
}

async fn read_responses(
    conn_id: usize,
    read_half: OwnedReadHalf,
    progress: Arc<ReadProgress>,
    expected_total: usize,
) -> Result<(), io::Error> {
    if expected_total == 0 {
        return Ok(());
    }

    let mut reader = BufReader::new(read_half);
    let mut line = Vec::with_capacity(128);
    let mut body_scratch = Vec::with_capacity(4096);

    while progress.current() < expected_total {
        line.clear();
        let n = reader.read_until(b'\n', &mut line).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "connection {} closed while waiting for responses ({}/{})",
                    conn_id,
                    progress.current(),
                    expected_total
                ),
            ));
        }
        if line.len() < 2 || !line.ends_with(b"\r\n") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("connection {}: malformed response line", conn_id),
            ));
        }

        line.truncate(line.len() - 2);
        if line == b"STORED" || line == b"NOT_FOUND" || line.starts_with(b"ERROR:") {
            progress.increment();
            continue;
        }

        if let Some(data_len) = parse_get_header(&line) {
            let need = data_len + 2;
            if body_scratch.len() < need {
                body_scratch.resize(need, 0);
            }
            let body = &mut body_scratch[..need];
            reader.read_exact(body).await?;
            if !body.ends_with(b"\r\n") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("connection {}: malformed get body trailer", conn_id),
                ));
            }
            progress.increment();
            continue;
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "connection {}: unrecognized response header: {}",
                conn_id,
                String::from_utf8_lossy(&line)
            ),
        ));
    }

    Ok(())
}

fn parse_get_header(line: &[u8]) -> Option<usize> {
    let sep = memchr(b' ', line)?;
    if sep == 0 || sep + 1 >= line.len() {
        return None;
    }

    let mut v: usize = 0;
    for &b in &line[sep + 1..] {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(v)
}

async fn wait_for_responses(progress: &ReadProgress, target: usize) -> Result<(), io::Error> {
    if target == 0 {
        return Ok(());
    }

    const RESPONSE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
    let started = time::Instant::now();
    loop {
        if progress.current() >= target {
            return Ok(());
        }
        if time::Instant::now().duration_since(started) > RESPONSE_WAIT_TIMEOUT {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "response wait timed out"));
        }
        time::sleep(Duration::from_millis(1)).await;
    }
}

async fn wait_barrier(barrier: &Barrier) -> Result<BarrierWaitResult, io::Error> {
    const BARRIER_TIMEOUT: Duration = Duration::from_secs(60);
    time::timeout(BARRIER_TIMEOUT, barrier.wait())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "barrier wait timed out"))
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
