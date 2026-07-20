//! Criterion benchmarks for HybridScheduler.
//!
//! Run with: cargo bench  (requires Rust >= 1.80)
//!
//! Scenarios:
//!   no_conflicts      — all txs touch distinct accounts, zero contention.
//!   hot_account       — all txs contend on a single AMM-pool-like account.
//!   mixed_10pct_hot   — 10 % hot / 90 % independent.
//!   high_churn        — many passes, tests cleanup_hotspots amortised cost.

use agave_rift_scheduler::{AccountId, HybridScheduler, SchedulerConfig, Transaction};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const BUDGET: u64 = u64::MAX;

fn make_txs(n: usize, account_fn: impl Fn(usize) -> AccountId) -> Vec<Transaction> {
    (0..n)
        .map(|i| Transaction::new(i, 1, vec![account_fn(i)], vec![true]))
        .collect()
}

fn bench_no_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/no_conflicts");
    for batch_size in [64usize, 256, 1024, 4096] {
        let txs = make_txs(batch_size, |i| AccountId(i as u64));
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &txs, |b, txs| {
            b.iter(|| {
                let mut s = HybridScheduler::default();
                black_box(s.schedule(txs, BUDGET))
            });
        });
    }
    group.finish();
}

fn bench_hot_account(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/hot_account");
    for batch_size in [64usize, 256, 1024, 4096] {
        let txs = make_txs(batch_size, |_| AccountId(0));
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &txs, |b, txs| {
            b.iter(|| {
                let mut s = HybridScheduler::with_config(SchedulerConfig {
                    conflict_threshold: 4,
                    ..SchedulerConfig::default()
                });
                black_box(s.schedule(txs, BUDGET))
            });
        });
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/mixed_10pct_hot");
    for batch_size in [64usize, 256, 1024, 4096] {
        let txs = make_txs(batch_size, |i| {
            if i % 10 == 0 {
                AccountId(0)
            } else {
                AccountId(i as u64 + 1_000_000)
            }
        });
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &txs, |b, txs| {
            b.iter(|| {
                let mut s = HybridScheduler::default();
                black_box(s.schedule(txs, BUDGET))
            });
        });
    }
    group.finish();
}

fn bench_high_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/high_churn");
    for passes in [10usize, 50, 200] {
        group.throughput(Throughput::Elements(passes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(passes),
            &passes,
            |b, &passes| {
                b.iter(|| {
                    let mut s = HybridScheduler::default();
                    for p in 0..passes {
                        let txs = make_txs(64, |i| AccountId((p * 64 + i) as u64));
                        black_box(s.schedule(&txs, BUDGET));
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_no_conflicts,
    bench_hot_account,
    bench_mixed_workload,
    bench_high_churn
);
criterion_main!(benches);
