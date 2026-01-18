use std::{hint::black_box, sync::Arc, thread};

use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;

use carcinusdb::{
    storage::{cache::ShardedLruCache, pager::MemPage},
    utils::concurrency::ShardedClockCache,
};

fn bench_lru(c: &mut Criterion) {
    c.bench_function("lru_mixed_workload", |b| {
        b.iter(|| {
            let cache = Arc::new(ShardedLruCache::new(1000));
            let mut handles = vec![];

            for _tid in 0..4 {
                let cache = Arc::clone(&cache);
                handles.push(thread::spawn(move || {
                    let mut rng = rand::rng();

                    // Each thread does 10k operations
                    for _ in 0..10_000 {
                        let page_id = if rng.random_bool(0.8) {
                            // 80% hot pages (0-200)
                            rng.random_range(0..200)
                        } else {
                            // 20% random pages (0-2000)
                            rng.random_range(0..2000)
                        };

                        if cache.get(&page_id).is_none() {
                            let page = Arc::new(MemPage::new(page_id));
                            let _ = cache.insert(page_id, page);
                        }
                        black_box(page_id);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

fn bench_clock(c: &mut Criterion) {
    c.bench_function("clock_mixed_workload", |b| {
        b.iter(|| {
            let cache = Arc::new(ShardedClockCache::new(1000));
            let mut handles = vec![];

            for _tid in 0..4 {
                let cache = Arc::clone(&cache);
                handles.push(thread::spawn(move || {
                    let mut rng = rand::rng();

                    for _ in 0..10_000 {
                        let page_id = if rng.random_bool(0.8) {
                            rng.random_range(0..200)
                        } else {
                            rng.random_range(0..2000)
                        };

                        if cache.get(&page_id).is_none() {
                            let page = Arc::new(MemPage::new(page_id));
                            let _ = cache.insert(page_id, page);
                        }
                        black_box(page_id);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_lru, bench_clock);
criterion_main!(benches);
