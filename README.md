## RDS

RDS is an experimental KV database written in Rust.\
Supports get/set commands.\
Stores data in a [LMDBX](https://github.com/erthink/libmdbx) database.

The purpose of this to experiment with Rust and to make a nice performant persistence mechanism for a cache-like service (think memcached/redis). The idea has currently somewhat drifted towards building a database, but is likely to back to being cache-like and using the database for restart/replication persistence only.

### Benchmark

Latest performance achieved on a macbook pro m3 (both client and server are local, so they compete for resources, which is not ideal.).\

Server - runs with db on a local ssd and 6 io-worker threads (+4 database threads)
```bash
$ cargo run --bin server --release -- --port 9000 --db ./db -t 6
```

Client.

0. generate 200k random string keys
1. 200 threads - run 200k set() requests, writing all the keys to the database (with a small data payload)
2. 200 threads - each executes 30k get() requests for random keys from the keyset (i.e. total 6M requests for random keys from the initial 200k set)
```bash
$ cargo run --bin client --release -- --port 9000 -k 200000 -t 200 -r 30000 --populate
...
200000 set() requests done in 0.369237 sec; 541657 rps
6000000 get() requests after 5.712650 sec; 1050301 rps
```

### TODO
- [ ] Implement TTL (background thread scanning the database) or database scanning (possible with lmdbx cursors).

### FIXME
- Building this project currently requires an augmented version of [libmdbx-rs](https://github.com/vorot93/libmdbx-rs).
  - [ ] fork libmdbx-rs, add support for running write transactions in the current thread (without a hidden one, provided by the library).
  - [ ] contribute changes back
