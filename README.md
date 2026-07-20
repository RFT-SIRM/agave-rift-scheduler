# agave-rift-scheduler

[![CI](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.75+-orange)](https://www.rust-lang.org/)
[![Fuzzing](https://img.shields.io/badge/Fuzzing-5h%2055m%20daily-brightgreen)](#fuzzing)

**Conflict-aware transaction scheduler for Agave SVM — research implementation with formal invariant verification.**

## What is this?

A research implementation of a hybrid transaction scheduler that addresses two bugs found in the original Agave scheduler:

- **Bug 1: Dead deferred queue** — deferred transactions were pushed into a queue that was never read back, causing them to be silently lost forever
- **Bug 2: Zero-cost bypass** — a `tx.cost > 0` guard allowed zero-cost transactions to bypass conflict detection entirely and schedule immediately regardless of contention

Both bugs are fixed, regression-tested, and verified through 5h 55m of continuous fuzzing.

## Architecture
### Scheduling invariants (verified by fuzzer)

| Invariant | Description |
|-----------|-------------|
| **I1: Accounting** | `scheduled + deferred + dropped <= scanned` per pass |
| **I2: Generation** | Generation counter is always strictly positive after first pass |
| **I3: Monotonicity** | `scheduler_passes` never goes backwards |
| **I4: Drain** | Deferred queue fully drains within 512 passes |

## Bugs fixed

### Bug 1: Dead deferred queue

```rust
// BEFORE: deferred queue was push-only, never read
self.deferred.push(DeferredTx { tx, retry_count, ready_generation });
// (nothing ever retried these transactions)

// AFTER: every schedule() pass drains ready entries first
let (ready, still_waiting): (Vec<_>, Vec<_>) =
    std::mem::take(&mut self.deferred)
        .into_iter()
        .partition(|d| d.ready_generation <= current_gen);
self.deferred = still_waiting;
for deferred_tx in ready { self.try_schedule_or_defer(...) }
```

### Bug 2: Zero-cost conflict bypass

```rust
// BEFORE: cost guard let zero-cost transactions skip conflict detection
if tx.cost > 0 && score >= self.config.conflict_threshold {
    // defer...
}

// AFTER: conflict check is unconditional
let is_conflicted = score >= self.config.conflict_threshold;
if is_conflicted { /* defer regardless of cost */ }
```

## Quick start

```bash
# Build
cargo build --release

# Run all tests
cargo test --lib

# Fuzz for 5 minutes locally
cargo +nightly fuzz run scheduler_fuzz -- -max_total_time=300

# Full 5h 55m run (matches CI)
cargo +nightly fuzz run scheduler_fuzz -- -max_total_time=21300
```

## Testing

### Unit tests (15 tests)

```bash
cargo test --lib
```

| Test | Covers |
|------|--------|
| `deferred_transactions_are_retried_and_eventually_scheduled` | Bug 1 regression |
| `zero_cost_tx_does_not_bypass_conflict_detection` | Bug 2 regression |
| `deferred_transaction_is_dropped_after_max_retries` | Retry cap |
| `hotspot_decay_reduces_heat_gradually` | Heat decay |
| `budget_exhaustion_defers_without_conflict` | Budget enforcement |
| `retried_transaction_succeeds_when_conflict_clears` | Full retry cycle |
| `read_only_accounts_do_not_cause_conflicts` | Read isolation |
| `generation_counter_wraps_safely` | Overflow safety |
| `metrics_accumulate_across_scheduling_passes` | Metrics correctness |

### Fuzzing

The fuzz harness generates random scheduler configs and sequences of scheduling passes, then asserts all 4 invariants after every pass.

**CI schedule:** 5h 55m daily at 12:00 UTC (15:00 UTC+3)  
**On push:** 60-second smoke run

**Latest results:**

| Metric | Value |
|--------|-------|
| Duration | 5h 55m (21,300s) |
| Total executions | 4,294,967,296+ |
| Execution speed | ~421,000 exec/s |
| Invariant violations | 0 |
| Panics | 0 |

## Configuration

```rust
SchedulerConfig {
    conflict_threshold: 1,      // min heat score to defer a transaction
    max_generation_age: 16,     // generations before hotspot eviction
    hotspot_decay_shift: 1,     // heat halves every N generations
    max_retry_count: 6,         // max retries before drop
    hotspot_capacity: 4096,     // initial HashMap capacity
    initial_heat: 2,            // heat added on first write to account
    max_account_heat: 255,      // heat ceiling per account
}
```

## Repository layout
## Next steps

- Integrate with `anza-xyz/svm` transaction-context crate
- Measure per-CPI-frame rollback cost vs existing benchmarks
- Open RFC / Draft PR against `solana-transaction-context`

## License

Apache-2.0 — see [LICENSE](LICENSE)

© 2026 Eugeny (RFT-SIRM)
