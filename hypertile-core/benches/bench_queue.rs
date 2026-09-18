use hypertile_core::{block_on, Runtime};
use std::time::Instant;

/// Benchmark: spawn a pool of N workers, execute M tasks to completion,
/// and measure throughput and latency.
fn bench_pool_throughput(n_workers: usize, n_tasks: usize) {
    let rt = Runtime::new(n_workers);
    let start = Instant::now();

    let mut handles = Vec::with_capacity(n_tasks);
    for i in 0..n_tasks {
        let h = rt.spawn(async move {
            let mut acc = i as u64;
            for j in 0..50 {
                acc = acc.wrapping_add(j as u64);
            }
            acc
        });
        handles.push(h);
    }

    block_on(async {
        for h in handles {
            let res = h.await;
            assert!(res.is_ok());
        }
    });

    let elapsed = start.elapsed();
    let throughput = (n_tasks as f64) / elapsed.as_secs_f64();
    println!(
        "bench_pool_throughput: {:2} workers, {:6} tasks -> {:>9.2?} ({:>10.0} tasks/sec)",
        n_workers, n_tasks, elapsed, throughput
    );
}

/// Benchmark: measure single-hop continuation handoff latency.
fn bench_single_hop_continuation(chain_depth: usize) {
    let rt = Runtime::new(2);
    let start = Instant::now();

    block_on(async {
        let mut val = 0u64;
        for _ in 0..chain_depth {
            let h = rt.spawn_local(async move { val + 1 });
            val = h.await.unwrap();
        }
        assert_eq!(val, chain_depth as u64);
    });

    let elapsed = start.elapsed();
    let per_hop = elapsed / (chain_depth as u32);
    println!(
        "bench_single_hop_continuation: {:6} chained hops -> {:>9.2?} ({:>8.2?} per hop)",
        chain_depth, elapsed, per_hop
    );
}

fn main() {
    println!("=== Hypertile Queue & Work-Stealing Benchmarks ===");
    bench_single_hop_continuation(10_000);
    for &n in &[1usize, 2, 4, 8] {
        bench_pool_throughput(n, 20_000);
    }
    println!("=== Benchmarks Complete ===");
}
