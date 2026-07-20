use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccountId(pub u64);

#[derive(Clone, Debug)]
pub struct Transaction {
    pub id: usize,
    pub cost: u64,
    pub account_keys: Vec<AccountId>,
    pub writable: Vec<bool>,
}

impl Transaction {
    pub fn new(id: usize, cost: u64, account_keys: Vec<AccountId>, writable: Vec<bool>) -> Self {
        assert_eq!(account_keys.len(), writable.len());
        Self {
            id,
            cost,
            account_keys,
            writable,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SchedulerMetrics {
    pub scheduler_passes: u64,
    pub scheduled_txs: u64,
    pub deferred_txs: u64,
    pub dropped_txs: u64,
    pub lock_conflicts: u64,
    pub hot_accounts: u64,
}

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub conflict_threshold: u32,
    pub max_generation_age: u32,
    /// Controls how fast a hot account's heat decays per generation of age.
    /// Each generation of age right-shifts the stored heat by this amount
    /// (e.g. shift=1 halves heat per generation of age). shift=0 disables
    /// decay entirely (heat is only reduced by removal once an account's
    /// age exceeds `max_generation_age`).
    pub hotspot_decay_shift: u32,
    pub max_retry_count: u8,
    pub hotspot_capacity: usize,
    pub initial_heat: u16,
    pub max_account_heat: u16,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            conflict_threshold: 1,
            max_generation_age: 16,
            hotspot_decay_shift: 1,
            max_retry_count: 6,
            hotspot_capacity: 4096,
            initial_heat: 2,
            max_account_heat: 255,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HotAccountMeta {
    generation: u32,
    heat: u16,
}

/// A deferred transaction retains a full owned copy of the transaction it
/// couldn't schedule, along with its retry history. This is what makes
/// retry actually possible: without owning the transaction data, a
/// deferred entry would have nothing to reschedule later.
#[derive(Clone, Debug)]
struct DeferredTx {
    tx: Transaction,
    retry_count: u8,
    ready_generation: u32,
}

#[derive(Clone, Debug, Default)]
pub struct SchedulingSummary {
    pub scheduled: usize,
    pub deferred: usize,
    pub dropped: usize,
    pub scanned: usize,
    pub generation: u32,
}

#[derive(Clone, Debug)]
pub struct HybridScheduler {
    current_generation: u32,
    hotspot_accounts: HashMap<AccountId, HotAccountMeta>,
    deferred: Vec<DeferredTx>,
    metrics: SchedulerMetrics,
    config: SchedulerConfig,
}

impl Default for HybridScheduler {
    fn default() -> Self {
        Self::with_config(SchedulerConfig::default())
    }
}

impl HybridScheduler {
    pub fn with_config(config: SchedulerConfig) -> Self {
        Self {
            current_generation: 0,
            hotspot_accounts: HashMap::with_capacity(config.hotspot_capacity),
            deferred: Vec::new(),
            metrics: SchedulerMetrics::default(),
            config,
        }
    }

    pub fn metrics(&self) -> &SchedulerMetrics {
        &self.metrics
    }

    /// Single source of truth for hotspot heat decay. Used both when
    /// normalizing stored heat in `cleanup_hotspots` and (indirectly, since
    /// `cleanup_hotspots` always runs first in `schedule`) when reading
    /// heat in `tx_conflict_score`. Keeping decay in exactly one place
    /// prevents the two call sites from drifting into inconsistent decay
    /// curves.
    fn decayed_heat(heat: u16, age: u32, shift: u32) -> u16 {
        let shift_amount = age.saturating_mul(shift).min(15);
        heat >> shift_amount
    }

    /// Conflict score for a transaction, based on current (already
    /// generation-normalized) heat of the writable accounts it touches.
    ///
    /// This intentionally does NOT special-case `tx.cost == 0`: a
    /// zero-cost transaction touching a hot account is exactly as
    /// contended as any other transaction touching that account, and must
    /// be subject to the same conflict check. (Previously, a cost>0 guard
    /// on the conflict-defer branch let zero-cost transactions bypass
    /// conflict detection entirely — fixed here by not gating the score
    /// check on cost at all.)
    fn tx_conflict_score(&self, tx: &Transaction) -> u32 {
        let mut score = 0u32;

        for (index, account) in tx.account_keys.iter().enumerate() {
            if !tx.writable.get(index).copied().unwrap_or(false) {
                continue;
            }

            if let Some(meta) = self.hotspot_accounts.get(account) {
                score = score.saturating_add(meta.heat as u32);
            }
        }

        score
    }

    fn mark_hot_accounts(&mut self, tx: &Transaction) {
        let current_gen = self.current_generation;

        for (index, account) in tx.account_keys.iter().enumerate() {
            if !tx.writable.get(index).copied().unwrap_or(false) {
                continue;
            }

            self.hotspot_accounts
                .entry(account.clone())
                .and_modify(|meta| {
                    meta.generation = current_gen;
                    meta.heat = meta
                        .heat
                        .saturating_add(self.config.initial_heat)
                        .min(self.config.max_account_heat);
                })
                .or_insert(HotAccountMeta {
                    generation: current_gen,
                    heat: self.config.initial_heat,
                });
        }
    }

    /// Normalizes every tracked hotspot account to the current generation:
    /// decays heat according to how many generations have passed since it
    /// was last touched, and drops accounts that are either fully cold or
    /// older than `max_generation_age`. Always runs at the start of every
    /// `schedule()` pass, so by the time `tx_conflict_score` reads
    /// `meta.heat`, that value is already correct for the current
    /// generation — no further decay math is needed at read time.
    fn cleanup_hotspots(&mut self) {
        let current_gen = self.current_generation;
        let max_age = self.config.max_generation_age;
        let shift = self.config.hotspot_decay_shift;

        self.hotspot_accounts.retain(|_, meta| {
            let age = current_gen.wrapping_sub(meta.generation);
            if age > max_age {
                return false;
            }

            if age > 0 {
                meta.heat = Self::decayed_heat(meta.heat, age, shift);
                meta.generation = current_gen;
            }

            meta.heat > 0
        });
    }

    /// Attempts to schedule a single transaction (whether freshly-arrived
    /// or a retried deferred one). On conflict or budget exhaustion, either
    /// re-defers it with generation-based backoff, or drops it once it has
    /// exceeded `max_retry_count` retries — this is what prevents deferred
    /// transactions from being retried forever, and what previously never
    /// happened at all since nothing ever consumed the deferred queue.
    fn try_schedule_or_defer(
        &mut self,
        tx: Transaction,
        retry_count: u8,
        remaining_budget: &mut u64,
        scheduled: &mut usize,
        deferred_count: &mut usize,
        dropped_count: &mut usize,
    ) {
        let score = self.tx_conflict_score(&tx);
        let is_conflicted = score >= self.config.conflict_threshold;

        if is_conflicted {
            self.metrics.lock_conflicts += 1;

            if retry_count >= self.config.max_retry_count {
                *dropped_count += 1;
                return;
            }

            let backoff = 1u32 << (retry_count as u32).min(8);
            self.deferred.push(DeferredTx {
                tx,
                retry_count: retry_count + 1,
                ready_generation: self.current_generation + backoff,
            });
            *deferred_count += 1;
            return;
        }

        if tx.cost > *remaining_budget {
            if retry_count >= self.config.max_retry_count {
                *dropped_count += 1;
                return;
            }

            self.deferred.push(DeferredTx {
                tx,
                retry_count: retry_count + 1,
                ready_generation: self.current_generation + 1,
            });
            *deferred_count += 1;
            return;
        }

        self.mark_hot_accounts(&tx);
        *remaining_budget = remaining_budget.saturating_sub(tx.cost);
        *scheduled += 1;
        self.metrics.scheduled_txs += 1;
    }

    pub fn schedule(&mut self, transactions: &[Transaction], budget: u64) -> SchedulingSummary {
        self.metrics.scheduler_passes += 1;
        self.current_generation = self.current_generation.wrapping_add(1);
        self.cleanup_hotspots();

        let mut remaining_budget = budget;
        let mut scheduled = 0usize;
        let mut deferred_count = 0usize;
        let mut dropped_count = 0usize;
        let mut scanned = 0usize;

        // Retry deferred transactions that have reached their backoff
        // deadline before considering any freshly-arrived work. This is
        // the mechanism that was previously entirely missing: deferred
        // transactions were pushed into `self.deferred` but nothing ever
        // read them back out.
        let current_gen = self.current_generation;
        let (ready, still_waiting): (Vec<DeferredTx>, Vec<DeferredTx>) =
            std::mem::take(&mut self.deferred)
                .into_iter()
                .partition(|d| d.ready_generation <= current_gen);
        self.deferred = still_waiting;

        for deferred_tx in ready {
            scanned += 1;
            self.try_schedule_or_defer(
                deferred_tx.tx,
                deferred_tx.retry_count,
                &mut remaining_budget,
                &mut scheduled,
                &mut deferred_count,
                &mut dropped_count,
            );
        }

        for tx in transactions {
            scanned += 1;
            self.try_schedule_or_defer(
                tx.clone(),
                0,
                &mut remaining_budget,
                &mut scheduled,
                &mut deferred_count,
                &mut dropped_count,
            );
        }

        self.metrics.deferred_txs = self.deferred.len() as u64;
        self.metrics.dropped_txs += dropped_count as u64;
        self.metrics.hot_accounts = self.hotspot_accounts.len() as u64;

        SchedulingSummary {
            scheduled,
            deferred: deferred_count,
            dropped: dropped_count,
            scanned,
            generation: self.current_generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defers_conflicting_hot_accounts() {
        let mut scheduler = HybridScheduler::default();
        let hot = Transaction::new(1, 8, vec![AccountId(42)], vec![true]);
        let conflicting = Transaction::new(2, 4, vec![AccountId(42)], vec![true]);

        let first = scheduler.schedule(&[hot], 16);
        assert_eq!(first.scheduled, 1);

        let second = scheduler.schedule(&[conflicting], 16);
        assert_eq!(second.scheduled, 0);
        assert_eq!(second.deferred, 1);
        assert!(scheduler.metrics().lock_conflicts > 0);
    }

    #[test]
    fn cleanup_removes_stale_hotspots() {
        let mut scheduler = HybridScheduler::default();
        let tx = Transaction::new(1, 3, vec![AccountId(7)], vec![true]);
        scheduler.mark_hot_accounts(&tx);
        scheduler.current_generation = 64;
        scheduler.cleanup_hotspots();
        assert!(scheduler.hotspot_accounts.is_empty());
    }

    /// Regression test for the dead deferred-queue bug: previously,
    /// `self.deferred` was pushed to but never read from anywhere, so a
    /// deferred transaction was lost forever. This proves a deferred
    /// transaction is actually retried on a later `schedule()` pass and
    /// gets scheduled once the conflict clears.
    #[test]
    fn deferred_transactions_are_retried_and_eventually_scheduled() {
        let mut scheduler = HybridScheduler::default();

        let hot = Transaction::new(1, 5, vec![AccountId(99)], vec![true]);
        let conflict = Transaction::new(2, 5, vec![AccountId(99)], vec![true]);

        let first = scheduler.schedule(&[hot], 50);
        assert_eq!(first.scheduled, 1);

        let second = scheduler.schedule(&[conflict], 50);
        assert_eq!(second.scheduled, 0);
        assert_eq!(second.deferred, 1);
        assert_eq!(scheduler.metrics().deferred_txs, 1);

        // Advance one more generation with no new incoming work. A
        // scheduler that never retries deferred work would show
        // `scheduled == 0` here forever; the fixed scheduler retries and
        // schedules it once the hot account has decayed enough.
        let third = scheduler.schedule(&[], 50);
        assert_eq!(third.scheduled, 1, "deferred transaction was never retried");
        assert_eq!(scheduler.metrics().deferred_txs, 0);
    }

    /// Regression test for the zero-cost bypass bug: previously, a
    /// `tx.cost > 0` guard on the conflict-defer branch meant a zero-cost
    /// transaction touching a hot account was never deferred and was
    /// scheduled immediately regardless of contention.
    #[test]
    fn zero_cost_tx_does_not_bypass_conflict_detection() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            conflict_threshold: 1,
            ..SchedulerConfig::default()
        });

        let hot = Transaction::new(1, 8, vec![AccountId(42)], vec![true]);
        let first = scheduler.schedule(&[hot], 100);
        assert_eq!(first.scheduled, 1);

        let zero_cost_conflict = Transaction::new(2, 0, vec![AccountId(42)], vec![true]);
        let second = scheduler.schedule(&[zero_cost_conflict], 100);

        assert_eq!(
            second.scheduled, 0,
            "zero-cost tx bypassed conflict detection"
        );
        assert_eq!(second.deferred, 1);
    }

    /// Regression test proving deferred transactions are eventually
    /// dropped (with a `dropped_txs` metric bump) rather than retried
    /// forever once they exceed `max_retry_count`.
    #[test]
    fn deferred_transaction_is_dropped_after_max_retries() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            conflict_threshold: 1,
            max_retry_count: 2,
            // Disable decay so the account stays hot for the whole test,
            // guaranteeing every single retry re-conflicts.
            hotspot_decay_shift: 0,
            ..SchedulerConfig::default()
        });

        let hot = Transaction::new(1, 1, vec![AccountId(5)], vec![true]);
        scheduler.schedule(&[hot], 10);

        let conflict = Transaction::new(2, 1, vec![AccountId(5)], vec![true]);
        let first_defer = scheduler.schedule(&[conflict], 10);
        assert_eq!(first_defer.deferred, 1);

        for _ in 0..20 {
            scheduler.schedule(&[], 10);
            if scheduler.metrics().deferred_txs == 0 {
                break;
            }
        }

        assert_eq!(
            scheduler.metrics().deferred_txs,
            0,
            "deferred tx should eventually be dropped, not retried forever"
        );
        assert!(
            scheduler.metrics().dropped_txs >= 1,
            "expected the tx to be dropped after exceeding max_retry_count"
        );
    }
}
#[cfg(test)]
mod extended_scheduler_tests {
    use super::{AccountId, HybridScheduler, SchedulerConfig, Transaction};



    /// Test that hot accounts gradually cool down over generations.
    ///
    /// With default initial_heat=2 and decay_shift=1, after 2 empty passes
    /// the age becomes 2, so heat = 2 >> 2 = 0 and the account is evicted.
    /// Use initial_heat=40 so decay is observable: 40 → 20 → 10 → still hot.
    #[test]
    fn hotspot_decay_reduces_heat_gradually() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            hotspot_decay_shift: 1,
            max_generation_age: 32,
            initial_heat: 40,
            ..SchedulerConfig::default()
        });

        let tx = Transaction::new(1, 2, vec![AccountId(100)], vec![true]);
        scheduler.schedule(&[tx], 100);
        assert_eq!(scheduler.metrics().hot_accounts, 1);

        // Gen+1: age=1, heat = 40 >> 1 = 20 — still tracked.
        let _ = scheduler.schedule(&[], 100);
        assert_eq!(scheduler.metrics().hot_accounts, 1, "hot after 1 pass");

        // Gen+2: heat = 20 >> 1 = 10 — still tracked.
        let _ = scheduler.schedule(&[], 100);
        assert!(scheduler.metrics().hot_accounts > 0, "hot after 2 passes");
    }

    /// Test that budget exhaustion defers transactions (not conflicts).
    ///
    /// Budget is per scheduling-pass and resets each call.
    /// Both txs must be in the SAME pass so the second sees a depleted budget.
    #[test]
    fn budget_exhaustion_defers_without_conflict() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            conflict_threshold: 100, // very high — no conflicts
            ..SchedulerConfig::default()
        });

        let expensive = Transaction::new(1, 50, vec![AccountId(1)], vec![true]);
        let another   = Transaction::new(2, 60, vec![AccountId(2)], vec![true]);

        // Single pass, budget=100. expensive(50) schedules first, consuming 50 units.
        // another(60) then needs 60 but only 50 remain — budget exhaustion → deferred.
        let summary = scheduler.schedule(&[expensive, another], 100);
        assert_eq!(summary.scheduled, 1, "only the first tx fits the budget");
        assert_eq!(summary.deferred,  1, "second tx deferred due to budget exhaustion");
        assert_eq!(scheduler.metrics().lock_conflicts, 0, "no lock conflicts, only budget");
    }

    /// Test that a deferred transaction is retried and succeeds once the
    /// hot account cools down.
    ///
    /// Timeline (default: initial_heat=2, decay_shift=1, threshold=1):
    ///   Gen1: schedule([hot])      -> mark_hot(42, heat=2), scheduled=1
    ///   Gen2: schedule([conflict]) -> score=2>=1, conflict, backoff=1<<0=1,
    ///                                 ready_gen=3, deferred=1
    ///   Gen3: schedule([])         -> cleanup: age=2, heat=2>>2=0 → evicted
    ///                                 drain ready (gen=3<=3) → score=0 → SCHEDULED
    #[test]
    fn retried_transaction_succeeds_when_conflict_clears() {
        let mut scheduler = HybridScheduler::default();

        let hot      = Transaction::new(1, 5, vec![AccountId(42)], vec![true]);
        let conflict = Transaction::new(2, 5, vec![AccountId(42)], vec![true]);

        // Gen1: hot account scheduled and marked.
        let _ = scheduler.schedule(&[hot], 50);

        // Gen2: conflict detected, deferred with backoff=1 (ready at gen3).
        let defer1 = scheduler.schedule(&[conflict], 50);
        assert_eq!(defer1.deferred, 1);

        // Gen3: backoff elapsed, hotspot decayed to 0 → deferred tx scheduled.
        let final_pass = scheduler.schedule(&[], 50);
        assert_eq!(
            final_pass.scheduled, 1,
            "deferred tx should be scheduled once the hotspot cools"
        );
        assert_eq!(scheduler.metrics().deferred_txs, 0);
    }

    /// Test max_retry_count prevents infinite retry loops
    #[test]
    fn max_retry_count_drops_transaction() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            conflict_threshold: 1,
            max_retry_count: 2,
            hotspot_decay_shift: 0, // disable decay to guarantee conflict every time
            ..SchedulerConfig::default()
        });

        // Create a permanently hot account
        let hot = Transaction::new(1, 1, vec![AccountId(7)], vec![true]);
        scheduler.schedule(&[hot], 10);

        // Try to schedule a conflicting transaction
        let conflict = Transaction::new(2, 1, vec![AccountId(7)], vec![true]);
        let first = scheduler.schedule(&[conflict], 10);
        assert_eq!(first.deferred, 1);

        // Retry up to max_retry_count times
        for _ in 0..10 {
            scheduler.schedule(&[], 10);
            if scheduler.metrics().deferred_txs == 0 {
                break;
            }
        }

        // Should be dropped (not deferred forever)
        assert_eq!(
            scheduler.metrics().deferred_txs,
            0,
            "transaction should be dropped after max_retry_count, not retried forever"
        );
        assert!(
            scheduler.metrics().dropped_txs >= 1,
            "expected dropped_txs to increment"
        );
    }

    /// Test that multiple independent accounts don't interfere
    #[test]
    fn multiple_independent_accounts_schedule_separately() {
        let mut scheduler = HybridScheduler::default();

        let tx1 = Transaction::new(1, 5, vec![AccountId(10)], vec![true]);
        let tx2 = Transaction::new(2, 5, vec![AccountId(20)], vec![true]);
        let tx3 = Transaction::new(3, 5, vec![AccountId(30)], vec![true]);

        let summary = scheduler.schedule(&[tx1, tx2, tx3], 50);
        assert_eq!(summary.scheduled, 3);
        assert_eq!(summary.deferred, 0);
    }

    /// Test that read-only accounts (writable=false) don't contribute to conflict score
    #[test]
    fn read_only_accounts_do_not_cause_conflicts() {
        let mut scheduler = HybridScheduler::default();

        // First tx writes to account 50
        let writer = Transaction::new(1, 3, vec![AccountId(50)], vec![true]);
        let first = scheduler.schedule(&[writer], 50);
        assert_eq!(first.scheduled, 1);

        // Second tx reads from the same account 50 (read-only)
        let reader = Transaction::new(2, 3, vec![AccountId(50)], vec![false]);
        let second = scheduler.schedule(&[reader], 50);

        // Should schedule immediately (no conflict on read-only)
        assert_eq!(second.scheduled, 1);
        assert_eq!(second.deferred, 0);
    }

    /// Test budget backoff: deferred tx retries next generation
    #[test]
    fn deferred_by_budget_retries_next_generation() {
        let mut scheduler = HybridScheduler::default();

        let expensive = Transaction::new(1, 60, vec![AccountId(1)], vec![true]);

        let first = scheduler.schedule(&[expensive], 50);
        assert_eq!(first.deferred, 1); // budget exhausted

        // Same tx should retry next generation and succeed
        let second = scheduler.schedule(&[], 100);
        assert_eq!(second.scheduled, 1);
        assert_eq!(scheduler.metrics().deferred_txs, 0);
    }

    /// Test that generation counter wraps correctly
    #[test]
    fn generation_counter_wraps_safely() {
        let mut scheduler = HybridScheduler::default();

        // Simulate many generations by scheduling empty work
        for _ in 0..1000 {
            let _ = scheduler.schedule(&[], 100);
        }

        // Should not panic or underflow on large generation values
        let tx = Transaction::new(1, 1, vec![AccountId(1)], vec![true]);
        let result = scheduler.schedule(&[tx], 100);
        assert_eq!(result.scheduled, 1);
    }

    /// Test metrics accumulate correctly across passes
    #[test]
    fn metrics_accumulate_across_scheduling_passes() {
        let mut scheduler = HybridScheduler::default();

        let tx1 = Transaction::new(1, 5, vec![AccountId(1)], vec![true]);
        let tx2 = Transaction::new(2, 5, vec![AccountId(1)], vec![true]);

        let first = scheduler.schedule(&[tx1], 50);
        assert_eq!(scheduler.metrics().scheduled_txs, 1);

        let second = scheduler.schedule(&[tx2], 50);
        assert_eq!(second.deferred, 1);
        assert_eq!(scheduler.metrics().scheduled_txs, 1); // no new scheduled
        assert_eq!(scheduler.metrics().lock_conflicts, 1);

        // After retry succeeds
        let _ = scheduler.schedule(&[], 50);
        let _ = scheduler.schedule(&[], 50);
        assert_eq!(scheduler.metrics().scheduled_txs, 2);
    }

    /// Test that zero-cost transaction in conflict scenario still defers
    #[test]
    fn zero_cost_tx_in_high_conflict_defers() {
        let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
            conflict_threshold: 1,
            ..SchedulerConfig::default()
        });

        let hot = Transaction::new(1, 100, vec![AccountId(99)], vec![true]);
        scheduler.schedule(&[hot], 1000);

        // Zero-cost transaction on hot account
        let zero_cost = Transaction::new(2, 0, vec![AccountId(99)], vec![true]);
        let result = scheduler.schedule(&[zero_cost], 1000);

        assert_eq!(
            result.deferred, 1,
            "zero-cost tx should defer on conflict, not bypass"
        );
    }
}
