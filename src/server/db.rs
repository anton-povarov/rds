use bytes::Bytes;
use flume;
use lru::LruCache;
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

struct ReadCache {
    entries: LruCache<Arc<str>, Option<Bytes>>,
    used_bytes: usize,
    max_bytes: usize,
}

impl ReadCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: LruCache::unbounded(),
            used_bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, key: &str) -> Option<Option<Bytes>> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: Arc<str>, value: Option<Bytes>) {
        if let Some(old) = self.entries.pop(key.as_ref()) {
            self.used_bytes =
                self.used_bytes.saturating_sub(Self::entry_charge(key.as_ref(), &old));
        }

        let charge = Self::entry_charge(key.as_ref(), &value);
        if charge > self.max_bytes {
            return;
        }

        self.entries.push(key, value);
        self.used_bytes = self.used_bytes.saturating_add(charge);

        while self.used_bytes > self.max_bytes {
            let Some((evict_key, evict_value)) = self.entries.pop_lru() else {
                self.used_bytes = 0;
                break;
            };
            self.used_bytes = self
                .used_bytes
                .saturating_sub(Self::entry_charge(evict_key.as_ref(), &evict_value));
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.used_bytes = 0;
    }

    fn entry_charge(key: &str, value: &Option<Bytes>) -> usize {
        key.len() + value.as_ref().map_or(0, |v| v.len())
    }
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
                println!("error joining db_worker/{}: {:?}", id, e);
            };
        });

        _ = self.saver_shutdown_tx.send(true);
        if let Err(e) = self.saver.thr.join() {
            println!("error joining db_syncer: {:?}", e);
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

        let thr = std::thread::Builder::new()
            .name("db_syncer".to_string())
            .spawn(move || {
                println!("db_syncer: starting");

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

                            const MAX_TRANSACTION_ITEMS: usize = 16 * 1024;
                            const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

                            let mut tx = db.db.begin_rw_txn().expect("RW transaction start failed");
                            let mut table = tx.open_table(None).unwrap();

                            let mut tx_items: usize = 0;
                            let mut tx_bytes: usize = 0;
                            for (k, v) in msg.data.into_iter() {
                                let record_size = k.len() + v.len();

                                tx.put(&table, k.as_bytes(), v, libmdbx::WriteFlags::UPSERT)
                                    .unwrap();

                                tx_items += 1;
                                tx_bytes += record_size + 128 /* assumed overhead */;

                                if tx_items >= MAX_TRANSACTION_ITEMS
                                    || tx_bytes >= MAX_TRANSACTION_BYTES
                                {
                                    _ = tx.commit().expect("db_syncer: transaction commit failed");
                                    tx = db.db.begin_rw_txn().expect("RW transaction start failed");
                                    table = tx.open_table(None).unwrap();

                                    tx_items = 0;
                                    tx_bytes = 0;
                                }
                            }
                            _ = tx.commit().expect("db_syncer: transaction commit failed");
                        })
                        .wait();
                }

                println!("db_syncer: exiting");
            })
            .expect("failed to spawn db_syncer thread");
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
    read_cache: ReadCache,
    syncer_tx: flume::Sender<DBSyncMessage>,
}

impl DBWorkerThread {
    const CHANNEL_BUFFER_SZ: usize = 128;
    const WRITE_CACHE_INITIAL_SIZE: usize = 16 * 1024;
    const WRITE_CACHE_HIGH_WATERMARK: usize = 14 * 1024; // send before the first rehash
    const READ_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;
    const WORKER_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000);

    pub fn spawn(
        id: u32,
        shutdown_rx: flume::Receiver<bool>,
        db: Arc<lmdbx::LmdbxStorage>,
        syncer_tx: flume::Sender<DBSyncMessage>,
    ) -> DBWorkerThread {
        let (db_writer_tx, db_writer_rx) = flume::bounded::<DBRequest>(Self::CHANNEL_BUFFER_SZ);

        let thr = std::thread::Builder::new()
            .name(format!("db_worker/{}", id))
            .spawn({
                move || {
                    println!("db_worker/{}: starting", id);

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
                        read_cache: ReadCache::new(Self::READ_CACHE_MAX_BYTES),
                        syncer_tx: syncer_tx,
                    };

                    let mut next_tick = std::time::Instant::now() + Self::WORKER_TICK_INTERVAL;

                    loop {
                        if let Ok(_) = shutdown_rx.try_recv() {
                            break;
                        }

                        match db_writer_rx.recv_deadline(next_tick) {
                            Ok(msg) => {
                                let handle_r = { Self::handle_message(&mut ctx, msg.msg) };
                                match handle_r {
                                    Ok(r) => msg.reply.send(r).unwrap(), // sender is expected to wait for result
                                    Err(e) => eprintln!("db_worker/{}: db error: {:?}", id, e),
                                }
                            }
                            Err(flume::RecvTimeoutError::Timeout) => {
                                next_tick += Self::WORKER_TICK_INTERVAL;

                                let handle_r = { Self::tick_maintenance(&mut ctx) };
                                match handle_r {
                                    Ok(_) => {}
                                    Err(e) => eprintln!("db_worker/{}: db error: {:?}", id, e),
                                }
                            }
                            Err(flume::RecvTimeoutError::Disconnected) => {
                                break;
                            }
                        }
                    }
                    println!("db_worker/{}: exiting", id);
                }
            })
            .expect("failed to spawn db_worker thread");

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
                        return Ok(DBResponse::Get { data: Some(b) });
                    }
                    Some(None) => return Ok(DBResponse::Get { data: None }),
                    None => {}
                };

                // create the read transaction if it doesn't yet exist
                if ctx.transaction.is_none() {
                    let tx: lmdbx::TransactionRO =
                        ctx.db.db.begin_ro_txn().expect("RO transaction start failed");
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
                    Self::commit_outstanding_writes(ctx);
                }

                Ok(DBResponse::Set { ok: true })
            }
        }
    }

    fn commit_outstanding_writes(ctx: &mut DBThreadContext) {
        if !ctx.write_cache.is_empty() {
            let send_wcache = std::mem::replace(
                &mut ctx.write_cache,
                HashMap::with_capacity(Self::WRITE_CACHE_INITIAL_SIZE),
            );
            ctx.syncer_tx.send(DBSyncMessage { data: send_wcache }).unwrap();
        }
    }

    fn tick_maintenance(ctx: &mut DBThreadContext) -> Result<(), Box<dyn std::error::Error>> {
        Self::commit_outstanding_writes(ctx);
        _ = ctx.transaction.take(); // reset curr transaction, it will get reopened on 1st request
        ctx.read_cache.clear(); // also clear cache to avoid extra staleness
        Ok(())
    }

    pub fn get_channel(&self) -> flume::Sender<DBRequest> {
        self.input_tx.clone()
    }

    pub fn join(self) -> Result<(), Box<dyn std::any::Any + Send + 'static>> {
        return self.thr.join();
    }
}
