use crossbeam_utils::CachePadded;
use std::sync::atomic::AtomicU64;

pub struct StatsRequests {
    pub quit: CachePadded<AtomicU64>,
    pub get: CachePadded<AtomicU64>,
    pub get_hit: CachePadded<AtomicU64>,
    pub set: CachePadded<AtomicU64>,
    pub _bad: CachePadded<AtomicU64>,
}

pub struct Stats {
    pub n_connections: CachePadded<AtomicU64>,
    pub requests: StatsRequests,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            n_connections: CachePadded::new(AtomicU64::new(0)),
            requests: StatsRequests {
                quit: CachePadded::new(AtomicU64::new(0)),
                get: CachePadded::new(AtomicU64::new(0)),
                get_hit: CachePadded::new(AtomicU64::new(0)),
                set: CachePadded::new(AtomicU64::new(0)),
                _bad: CachePadded::new(AtomicU64::new(0)),
            },
        }
    }
}
