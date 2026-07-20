#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use agave_rift_scheduler::{AccountId, HybridScheduler, SchedulerConfig, Transaction};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct FuzzAccountAccess {
    account_id: u8,
    writable: bool,
}

#[derive(Debug, Arbitrary)]
struct FuzzTx {
    cost: u16,
    accesses: Vec<FuzzAccountAccess>,
}

#[derive(Debug, Arbitrary)]
struct FuzzPass {
    txs: Vec<FuzzTx>,
    budget: u32,
}

#[derive(Debug, Arbitrary)]
struct FuzzConfig {
    conflict_threshold: u8,
    max_generation_age: u8,
    hotspot_decay_shift: u8,
    // Bounded to 8 to keep max backoff at 2^8=256 generations.
    // With drain_passes=8192 this is always sufficient.
    max_retry_count: u8,
    initial_heat: u8,
}

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

    // max_retry_count capped at 8: worst-case total backoff = sum(2^0..2^8) = 511
    // drain_passes=8192 >> 511, so drain is always guaranteed to complete.
    let max_retry_count = input.config.max_retry_count % 9;

    let config = SchedulerConfig {
        conflict_threshold: input.config.conflict_threshold as u32,
        max_generation_age: (input.config.max_generation_age as u32).max(1),
        hotspot_decay_shift: (input.config.hotspot_decay_shift % 16) as u32,
        max_retry_count,
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

        // INVARIANT 1: accounting
        assert!(
            summary.scheduled + summary.deferred + summary.dropped <= summary.scanned,
            "pass {pass_idx}: accounting violation: \
             scheduled({}) + deferred({}) + dropped({}) > scanned({})",
            summary.scheduled,
            summary.deferred,
            summary.dropped,
            summary.scanned
        );

        // INVARIANT 2: generation always positive
        assert!(summary.generation > 0, "pass {pass_idx}: generation must be >= 1");

        // INVARIANT 3: pass counter monotonic
        assert!(
            scheduler.metrics().scheduler_passes >= 1,
            "pass {pass_idx}: scheduler_passes must be >= 1"
        );
    }

    // INVARIANT 4: deferred queue drains completely.
    // max_retry_count <= 8, so worst-case total backoff = 1+2+4+...+256 = 511.
    // 8192 drain passes is vastly more than sufficient.
    for _ in 0..8192 {
        if scheduler.metrics().deferred_txs == 0 {
            break;
        }
        scheduler.schedule(&[], u64::MAX);
    }
    assert_eq!(
        scheduler.metrics().deferred_txs,
        0,
        "deferred queue never fully drained: {} txs still waiting",
        scheduler.metrics().deferred_txs
    );
});
