# agave-rift-scheduler

[![CI](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/ci.yml)
[![Fuzz](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/fuzz-daily.yml/badge.svg)](https://github.com/RFT-SIRM/agave-rift-scheduler/actions/workflows/fuzz-daily.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](LICENSE)

Research implementation of a conflict-aware transaction scheduler for Agave SVM.  
Experimentally verified through continuous fuzzing and regression testing.

---

## Architecture
### Hotspot Heat Map

Every writable account is tracked with a heat score:
- Heat increases by `initial_heat` on each scheduled write
- Heat decays by right-shifting `hotspot_decay_shift` bits per generation of age
- Accounts exceeding `max_generation_age` generations are evicted

### Generation Aging

Each call to `schedule()` advances `current_generation` by 1.  
Deferred transactions carry a `ready_generation` timestamp.  
A transaction is only retried when `ready_generation ≤ current_generation`.

### Retry Cycle
---

## Design Goals

**Deterministic scheduling** — given identical inputs and config, the scheduler produces identical outputs. No randomness in the scheduling path.

**Bounded retries** — every deferred transaction is either scheduled or dropped within a finite number of passes. Permanent starvation is impossible by construction.

**Conflict-aware execution** — writable account contention is tracked per-account via a heat score, not per-transaction-pair. This scales to large account sets without combinatorial overhead.

**Starvation prevention** — `max_retry_count` enforces a hard upper bound on retries. A transaction that cannot be scheduled within that bound is dropped with a metric increment, never silently lost.

**Predictable memory behaviour** — the deferred queue is bounded by `max_retry_count`. The hotspot map is bounded by `hotspot_capacity`. No unbounded growth paths exist.

---

## Why This Differs from the Current Agave Scheduler

This is a candidate design, not a replacement claim. Engineering differences:

| Property | Current Agave | This implementation |
|----------|--------------|---------------------|
| Deferred queue drain | Not present in research baseline | Explicit per-pass drain |
| Zero-cost conflict detection | Gated on `tx.cost > 0` | Unconditional |
| Heat-based hotspot tracking | Thread-level locking | Per-account heat score |
| Retry bound | Unbounded in baseline | Hard cap via `max_retry_count` |
| Fuzz verification | Not present | libFuzzer, 5h 55m daily |

These differences are experimentally verified, not benchmarked against production Agave.

---

## Invariants

### I1: Accounting invariant

**Statement:** `scheduled + deferred + dropped ≤ scanned` per pass

**Reason:** Every transaction that enters `schedule()` must be accounted for. Silent loss is unacceptable.

**Failure mode:** A transaction disappears from all counters. Budget or queue logic has a branch that returns without incrementing any counter.

**Verified by:** `fuzz_target` INVARIANT 1 assertion, unit test `defers_conflicting_hot_accounts`

---

### I2: Generation monotonicity

**Statement:** `summary.generation > 0` after every pass

**Reason:** Generation counter must be strictly positive. A zero generation would make all deferred transactions with `ready_generation = 1` permanently unretriable.

**Failure mode:** Wrapping underflow on generation counter.

**Verified by:** `fuzz_target` INVARIANT 2 assertion

---

### I3: Pass counter monotonicity

**Statement:** `scheduler_passes ≥ 1` after every call to `schedule()`

**Reason:** Metrics must accumulate correctly. A pass counter that does not increment would corrupt all derived metrics.

**Failure mode:** Early return before `metrics.scheduler_passes += 1`.

**Verified by:** `fuzz_target` INVARIANT 3 assertion

---

### I4: Deferred queue drain

**Statement:** The deferred queue reaches zero within bounded passes after all input stops.

**Reason:** Permanent queue growth means permanent starvation. Every deferred transaction must eventually be scheduled or dropped.

**Failure mode:** A transaction's `ready_generation` is set beyond any reachable generation value, or `max_retry_count` check is missing.

**Verified by:** `fuzz_target` INVARIANT 4 assertion (8192 drain passes), unit test `deferred_transaction_is_dropped_after_max_retries`

---

## Bugs Found and Fixed

### Bug 1: Dead deferred queue

**Description:** Deferred transactions were pushed into `self.deferred` but nothing ever read them back. The queue was write-only.

**Impact:** Every deferred transaction was silently lost. The scheduler appeared to work but dropped conflicting transactions permanently.

**Fix:** Every `schedule()` pass now partitions `self.deferred` into ready and still-waiting entries before processing new transactions.

**Regression test:** `deferred_transactions_are_retried_and_eventually_scheduled`

---

### Bug 2: Zero-cost conflict bypass

**Description:** The conflict-defer branch was guarded by `tx.cost > 0`. Zero-cost transactions touching hot accounts bypassed conflict detection entirely.

**Impact:** Zero-cost transactions could schedule immediately on maximally contested accounts, violating the conflict threshold invariant.

**Fix:** Conflict check is now unconditional. Cost is only relevant for budget enforcement, not conflict detection.

**Regression test:** `zero_cost_tx_does_not_bypass_conflict_detection`

---

## Testing Matrix

| Test | Purpose | Bug prevented |
|------|---------|---------------|
| `deferred_transactions_are_retried_and_eventually_scheduled` | Dead deferred queue regression | Bug 1 |
| `zero_cost_tx_does_not_bypass_conflict_detection` | Zero-cost bypass regression | Bug 2 |
| `deferred_transaction_is_dropped_after_max_retries` | Retry cap enforcement | I4 violation |
| `hotspot_decay_reduces_heat_gradually` | Heat decay correctness | Starvation |
| `budget_exhaustion_defers_without_conflict` | Budget vs conflict separation | Misclassification |
| `retried_transaction_succeeds_when_conflict_clears` | Full retry cycle | I4 violation |
| `read_only_accounts_do_not_cause_conflicts` | Read isolation | False conflicts |
| `generation_counter_wraps_safely` | Overflow safety | I2 violation |
| `metrics_accumulate_across_scheduling_passes` | Metrics correctness | I3 violation |
| `multiple_independent_accounts_schedule_separately` | No false conflicts | Throughput loss |
| `budget_backoff_retries_next_generation` | Budget retry path | Silent drop |
| `max_retry_count_drops_transaction` | Starvation prevention | I4 violation |
| `zero_cost_tx_in_high_conflict_defers` | Zero-cost + conflict | Bug 2 variant |
| `defers_conflicting_hot_accounts` | Basic conflict detection | I1 violation |
| `cleanup_removes_stale_hotspots` | Memory bounds | Unbounded growth |
| Fuzz target (5h 55m) | All 4 invariants, random configs | All of the above |

---

## Fuzzing

### Harness design

The fuzz target generates:
- Random `SchedulerConfig` (bounded ranges to ensure termination)
- Random sequences of scheduling passes
- Random transactions with random account accesses

After every pass, all 4 invariants are asserted.  
After all passes complete, 8192 drain passes verify I4.

### Configuration bounds in fuzzer
### Why long-duration fuzzing increases confidence but does not prove correctness

Fuzzing explores the input space stochastically. A 5h 55m run at ~4,300 exec/s on GitHub Actions produces approximately 91 million executions. This increases confidence that no invariant violation exists for inputs of this size and shape. It does not constitute a formal proof. Formal verification would require model checking or theorem proving, which is outside the scope of this research.

### Coverage stabilisation

Coverage typically stabilises within the first 100,000 executions (`cov: ~420 ft: ~2600`). Subsequent runs refine the corpus but rarely discover new coverage. This indicates the harness has exhausted the reachable state space for inputs within the size limits.

---

## Performance

### CI runner (GitHub Actions, ubuntu-latest)

| Metric | Value |
|--------|-------|
| Executions per second | ~4,300 |
| RSS at stabilisation | ~612 MB |
| Coverage (ft) | ~2,661 |
| Corpus entries | ~436 |

These figures describe the fuzz harness on an isolated in-memory struct. They are not transaction throughput figures and must not be compared to validator TPS benchmarks.

### Native machine (Apple Silicon M4)

| Metric | Value |
|--------|-------|
| Executions per second | ~8,500 (60s smoke run) |

### Future benchmark placeholders

- [ ] scheduler throughput (tx/sec) — pending Criterion integration
- [ ] hotspot-heavy workload latency
- [ ] 90% conflicting transaction throughput
- [ ] 90% independent transaction throughput
- [ ] retry-heavy workload allocations
- [ ] memory usage under sustained load

---

## Configuration Reference

```rust
SchedulerConfig {
    // Minimum heat score to defer a transaction (default: 1)
    conflict_threshold: u32,

    // Generations before a hotspot account is evicted (default: 16)
    max_generation_age: u32,

    // Heat right-shift per generation of age; 0 = no decay (default: 1)
    hotspot_decay_shift: u32,

    // Maximum retries before a transaction is dropped (default: 6)
    max_retry_count: u8,

    // Initial HashMap capacity for hotspot tracking (default: 4096)
    hotspot_capacity: usize,

    // Heat added on first write to an account (default: 2)
    initial_heat: u16,

    // Heat ceiling per account (default: 255)
    max_account_heat: u16,
}
```

---

## Roadmap

**Phase 1 — Standalone scheduler (current)**  
Conflict detection, deferred queue drain, retry cap, heat decay.  
All invariants verified. Bugs found and regression-tested.

**Phase 2 — Integration into Agave**  
Integrate with `anza-xyz/agave` transaction-context crate.  
Replace mock types with real `TransactionContext` and `AccountId`.

**Phase 3 — Real validator benchmarks**  
Measure against existing Agave scheduler on mainnet-representative workloads.  
Produce before/after numbers with Criterion.

**Phase 4 — RFC / PR**  
Open a focused Draft PR against `anza-xyz/agave` with:  
fuzz corpus, benchmark results, and invariant documentation as evidence.

---

## Repository Layout
---

## Quick Start

```bash
# Build
cargo build --release

# Run all tests
cargo test --lib

# 60-second local fuzz
cargo +nightly fuzz run scheduler_fuzz -- -max_total_time=60

# Full 5h 55m run
cargo +nightly fuzz run scheduler_fuzz -- -max_total_time=21300
```

---

## License

Apache-2.0 — see [LICENSE](LICENSE)

This is a research repository. It is not a production patch.  
© 2026 Eugeny (RFT-SIRM)
