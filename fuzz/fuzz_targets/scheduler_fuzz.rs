#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use agave_rift_scheduler::{AccountId, HybridScheduler, SchedulerConfig, Transaction};
use libfuzzer_sys::fuzz_target;

/// One account access: which account and whether it's writable.
#[derive(Debug, Arbitrary)]
struct FuzzAccountAccess {
    /// Small id range (0..256) forces hot-account collisions.
    account_id: u8,
    writable: bool,
}

/// Compact fuzz-friendly transaction.
#[derive(Debug, Arbitrary)]
struct FuzzTx {
    cost: u16,
    accesses: Vec<FuzzAccountAccess>,
}

/// One scheduling pass.
#[derive(Debug, Arbitrary)]
struct FuzzPass {
    txs: Vec<FuzzTx>,
    budget: u32,
}

/// Fuzz-controlled scheduler config (bounded ranges).
#[derive(Debug, Arbitrary)]
struct FuzzConfig {
    conflict_threshold: u8,
    max_generation_age: u8,
    hotspot_decay_shift: u8,
    max_retry_count: u8,
    initial_heat: u8,
}

/// Top-level input: config + sequence of scheduling passes.
#[derive(Debug, Arbitrary)]
struct FuzzInput {
    config: FuzzConfig,
    passes: Vec<FuzzPass>,
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let input = match FuzzInput::arbitrary(&mut u) {
        Ok(v) => v,
        Err(_) => return,
    };

    let config = SchedulerConfig {
        conflict_threshold: input.config.conflict_threshold as u32,
        max_generation_age: (input.config.max_generation_age as u32).max(1),
        hotspot_decay_shift: (input.config.hotspot_decay_shift % 16) as u32,
        max_retry_count: input.config.max_retry_count % 16,
        hotspot_capacity: 1024,
        initial_heat: (input.config.initial_heat as u16).max(1),
        max_account_heat: 255,
    };

    let mut scheduler = HybridScheduler::with_config(config);

    for (pass_idx, pass) in input.passes.iter().enumerate() {
        let txs: Vec<Transaction> = pass
            .txs
            .iter()
            .enumerate()
            .filter(|(_, t)| !t.accesses.is_empty())
            .map(|(i, t)| {
                let accounts: Vec<AccountId> =
                    t.accesses.iter().map(|a| AccountId(a.account_id as u64)).collect();
                let writable: Vec<bool> = t.accesses.iter().map(|a| a.writable).collect();
                Transaction::new(pass_idx * 256 + i, t.cost as u64, accounts, writable)
            })
            .collect();

        let summary = scheduler.schedule(&txs, pass.budget as u64);

        // INVARIANT 1: accounting — no more outcomes than inputs scanned
        assert!(
            summary.scheduled + summary.deferred + summary.dropped <= summary.scanned,
            "pass {pass_idx}: accounting violation: \
             scheduled({}) + deferred({}) + dropped({}) > scanned({})",
            summary.scheduled, summary.deferred, summary.dropped, summary.scanned
        );

        // INVARIANT 2: generation is always strictly positive after first pass
        assert!(summary.generation > 0, "pass {pass_idx}: generation must be >= 1");

        // INVARIANT 3: cumulative pass counter never goes backwards
        assert!(
            scheduler.metrics().scheduler_passes >= 1,
            "pass {pass_idx}: scheduler_passes must be >= 1"
        );
    }

    // INVARIANT 4: deferred queue eventually drains completely.
    // Give it 512 drain passes — more than enough for any backoff sequence
    // with max_retry_count <= 15 (max backoff = 2^15 = 32768 generations,
    // but drop threshold enforces a strict upper bound).
    for _ in 0..512 {
        if scheduler.metrics().deferred_txs == 0 { break; }
        scheduler.schedule(&[], u64::MAX);
    }
    assert_eq!(
        scheduler.metrics().deferred_txs, 0,
        "deferred queue never fully drained: {} txs still waiting",
        scheduler.metrics().deferred_txs
    );
});
