use agave_rift_scheduler::{AccountId, HybridScheduler, SchedulerConfig, Transaction};

fn main() {
    let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
        conflict_threshold: 4,
        ..SchedulerConfig::default()
    });

    let transactions = vec![
        Transaction::new(1, 8, vec![AccountId(1)], vec![true]),
        Transaction::new(2, 6, vec![AccountId(1)], vec![true]),
        Transaction::new(3, 4, vec![AccountId(2)], vec![true]),
    ];

    let summary = scheduler.schedule(&transactions, 12);

    println!(
        "scheduled={} deferred={} dropped={} scanned={} generation={}",
        summary.scheduled, summary.deferred, summary.dropped, summary.scanned, summary.generation
    );
    println!(
        "metrics: passes={} scheduled={} deferred={} dropped={} conflicts={} hot_accounts={}",
        scheduler.metrics().scheduler_passes,
        scheduler.metrics().scheduled_txs,
        scheduler.metrics().deferred_txs,
        scheduler.metrics().dropped_txs,
        scheduler.metrics().lock_conflicts,
        scheduler.metrics().hot_accounts
    );
}
