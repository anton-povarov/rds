use std::borrow::Cow;

use bytes::Bytes;
use rand::{Rng, distr::Alphanumeric, seq::SliceRandom};
use tempfile::{self, tempdir_in};

use criterion::{Criterion, criterion_group, criterion_main};
use rds::server::lmdbx;

fn get_random_n_keys(c: &mut Criterion, n_keys: usize, n_get_keys: usize) {
    assert!(n_keys >= n_get_keys);

    let mut rng = rand::rng();
    let mut all_keys = Vec::<String>::new();
    for _ in 0..n_keys {
        let range = rng.random_range(5..=240);
        let key: String =
            (&mut rng).sample_iter(&Alphanumeric).take(range).map(char::from).collect();
        all_keys.push(key);
    }

    let dir = tempdir_in(".").unwrap();
    let storage = lmdbx::LmdbxStorage::open(&dir).unwrap();

    {
        let tx = storage.db.begin_rw_txn().unwrap();
        let table = tx.open_table(None).unwrap();
        for key in &all_keys {
            let data = Bytes::from_static(b"test");
            tx.put(&table, key, data, libmdbx::WriteFlags::UPSERT).unwrap();
        }
        _ = tx.commit();
    }

    let get_keys = all_keys.partial_shuffle(&mut rng, n_get_keys).0;

    let read_trx = storage.db.begin_ro_txn().unwrap();
    let read_table = read_trx.open_table(None).unwrap();

    c.bench_function(format!("get_random__{}_{}", n_keys, n_get_keys).as_str(), |b| {
        b.iter(|| {
            read_trx
                .get::<Cow<_>>(&&read_table, get_keys[rng.random_range(0..n_get_keys)].as_bytes())
        });
    });
}

fn bench_get_random(c: &mut Criterion) {
    get_random_n_keys(c, 1_000, 100);
    get_random_n_keys(c, 10_000, 500);
    get_random_n_keys(c, 100_000, 5000);
    get_random_n_keys(c, 100_000, 50000);
}

criterion_group!(benches, bench_get_random);
criterion_main!(benches);
