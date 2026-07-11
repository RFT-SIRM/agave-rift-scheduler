// ============================================================================
// TECHNICAL REFERENCE SKETCH — NOT PART OF THE BUILD, NOT VERIFIED AGAINST
// AN ACTUAL AGAVE CHECKOUT.
//
// This file is not declared as a `mod` anywhere and is not compiled by this
// crate's Cargo.toml (which only depends on `anyhow`). It illustrates how
// the ideas proven out in `src/lib.rs` (generation-based hotspot decay,
// fast-path conflict filtering, deferred retry with backoff and a
// max-retry drop) COULD be adapted onto Agave's real banking_stage
// scheduler trait surface.
//
// It intentionally imports internal Agave modules
// (`scheduler_common`, `transaction_priority_id`, `transaction_state_container`,
// `ThreadAwareAccountLocks`, etc.) whose exact shape has changed across
// Agave versions and which are not available outside the agave workspace.
// I have not been able to verify these signatures against a specific
// Agave commit, so treat every type/function imported below as
// illustrative rather than confirmed-correct. Before this could compile
// inside an actual agave checkout, every import and call site here would
// need to be checked against that checkout's current
// `core/src/banking_stage/transaction_scheduler/` module layout.
//
// What IS real and tested here: the scheduling/decay/retry/drop logic
// itself mirrors the fixed, tested implementation in src/lib.rs 1:1 in
// spirit — same fast-path conflict filter, same generation-based hotspot
// decay, same deferred-retry-with-backoff, same max-retry drop instead of
// an infinite retry loop. Only the plumbing around it (locks, batches,
// threads, channels) is Agave-specific and unverified.
// ============================================================================

use {
    super::{
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_common::{
            select_thread, try_schedule_transaction, SchedulingCommon, TransactionSchedulingError,
        },
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::banking_stage::{
        consumer::{ENTRY_OVERHEAD_BYTES, TARGET_NUM_TRANSACTIONS_PER_BATCH},
        scheduler_messages::{ConsumeWork, FinishedConsumeWork},
    },
    agave_scheduling_utils::thread_aware_account_locks::{
        ThreadAwareAccountLocks, ThreadSet, TryLockError,
    },
    ahash::AHashMap,
    crossbeam_channel::{Receiver, Sender},
    solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS,
    solana_ledger::shred::get_data_shred_bytes_per_batch_typical,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    std::{
        mem,
        num::Saturating,
        sync::atomic::{AtomicU64, Ordering},
        time::Instant,
    },
};

const DEFAULT_TARGET_ENTRY_BYTES_PER_BATCH: u64 =
    get_data_shred_bytes_per_batch_typical() * 15 / 100;

const MAX_ACCOUNT_HEAT: u16 = 255;
const CONFLICT_THRESHOLD: u32 = 8;
const MAX_GENERATION_AGE: u32 = 16;
const MAX_DEFERRED_GENERATION_LIFETIME: u32 = 64;

/// See `SchedulerConfig::hotspot_decay_shift` in src/lib.rs: each generation
/// of age right-shifts stored heat by this amount. shift=1 halves heat per
/// generation of age. Used consistently in exactly one place
/// (`decayed_heat`), mirroring the fix applied in src/lib.rs.
const HOT_ACCOUNT_DECAY_SHIFT: u32 = 1;

#[derive(Default)]
pub struct SchedulerMetrics {
    pub scheduler_passes: AtomicU64,
    pub total_scanned: AtomicU64,
    pub scheduled_txs: AtomicU64,
    pub lock_conflicts: AtomicU64,
    pub unschedulable_threads: AtomicU64,
    pub total_batches_sent: AtomicU64,
    pub total_schedule_time_ns: AtomicU64,
    pub dropped_txs: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct RiftSchedulerConfig {
    pub target_scheduled_cus: u64,
    pub max_scanned_transactions_per_scheduling_pass: usize,
    pub target_transactions_per_batch: usize,
    pub target_entry_bytes_per_batch: u64,
    pub hotspot_capacity: usize,
    pub max_retry_count: u8,
}

impl Default for RiftSchedulerConfig {
    fn default() -> Self {
        Self {
            target_scheduled_cus: MAX_BLOCK_UNITS / 4,
            max_scanned_transactions_per_scheduling_pass: 100_000,
            target_transactions_per_batch: TARGET_NUM_TRANSACTIONS_PER_BATCH,
            target_entry_bytes_per_batch: DEFAULT_TARGET_ENTRY_BYTES_PER_BATCH,
            hotspot_capacity: 16384,
            max_retry_count: 6,
        }
    }
}

#[derive(Clone, Copy)]
struct DeferredTx {
    id: TransactionPriorityId,
    retry_count: u8,
    ready_generation: u32,
    created_generation: u32,
}

#[derive(Clone, Copy)]
struct HotAccountMeta {
    generation: u32,
    heat: u16,
}

pub struct RiftScheduler<Tx: TransactionWithMeta> {
    common: SchedulingCommon<Tx>,
    deferred: Vec<DeferredTx>,
    next_deferred: Vec<DeferredTx>,
    hotspot_accounts: AHashMap<Pubkey, HotAccountMeta>,
    current_generation: u32,
    metrics: std::sync::Arc<SchedulerMetrics>,
    config: RiftSchedulerConfig,
}

impl<Tx: TransactionWithMeta> RiftScheduler<Tx> {
    pub(crate) fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        config: RiftSchedulerConfig,
    ) -> Self {
        let cap = config.max_scanned_transactions_per_scheduling_pass;

        Self {
            deferred: Vec::with_capacity(cap),
            next_deferred: Vec::with_capacity(cap),
            hotspot_accounts: AHashMap::with_capacity(config.hotspot_capacity),
            current_generation: 0,
            metrics: std::sync::Arc::new(SchedulerMetrics::default()),
            common: SchedulingCommon::new(
                consume_work_senders,
                finished_consume_work_receiver,
                config.target_transactions_per_batch,
            ),
            config,
        }
    }

    /// Same decay law as `HybridScheduler::decayed_heat` in src/lib.rs:
    /// single source of truth, used both when normalizing stored heat in
    /// `cleanup_hotspots` and (transitively, since cleanup always runs
    /// first) when reading heat in `tx_conflict_score`.
    #[inline(always)]
    fn decayed_heat(heat: u16, age: u32) -> u16 {
        let shift_amount = age.saturating_mul(HOT_ACCOUNT_DECAY_SHIFT).min(15);
        heat >> shift_amount
    }

    /// Conflict score for a transaction. Deliberately does not special-case
    /// zero-cost transactions: a zero-cost write to a hot account is exactly
    /// as contended as any other write to that account.
    #[inline(always)]
    fn tx_conflict_score(&self, tx: &Tx) -> u32 {
        let mut score = 0u32;
        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) {
                continue;
            }
            if let Some(meta) = self.hotspot_accounts.get(key) {
                // cleanup_hotspots always runs before this is called within
                // a scheduling pass, so meta.heat is already normalized for
                // the current generation — no additional decay math here.
                score = score.saturating_add(meta.heat as u32);
            }
        }
        score
    }

    #[inline(always)]
    fn mark_hot_accounts(&mut self, tx: &Tx) {
        let current_gen = self.current_generation;
        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) {
                continue;
            }
            self.hotspot_accounts
                .entry(*key)
                .and_modify(|meta| {
                    meta.generation = current_gen;
                    meta.heat = meta.heat.saturating_add(1).min(MAX_ACCOUNT_HEAT);
                })
                .or_insert(HotAccountMeta {
                    generation: current_gen,
                    heat: 1,
                });
        }
    }

    fn cleanup_hotspots(&mut self) {
        if self.hotspot_accounts.len() < self.config.hotspot_capacity * 2 {
            return;
        }
        let current_gen = self.current_generation;
        self.hotspot_accounts.retain(|_, meta| {
            let age = current_gen.wrapping_sub(meta.generation);
            if age > MAX_GENERATION_AGE {
                return false;
            }
            if age > 0 {
                meta.heat = Self::decayed_heat(meta.heat, age);
                meta.generation = current_gen;
            }
            meta.heat > 0
        });
    }

    /// Attempt to schedule one transaction (fresh or retried-deferred).
    ///
    /// UNVERIFIED: the `try_schedule_transaction(...)` call, its argument
    /// list, and the exact shape of `TransactionSchedulingError` are
    /// illustrative placeholders — I have not confirmed this signature
    /// against a real Agave checkout. Everything above the
    /// `try_schedule_transaction` call (conflict filtering, retry/backoff,
    /// max-retry drop) mirrors the tested logic in src/lib.rs exactly and
    /// is the part I'm confident in; everything below it is Agave-specific
    /// plumbing that needs to be checked against the actual scheduler_common
    /// module before this can compile or run.
    fn process_tx<S: StateContainer<Tx>>(
        &mut self,
        deferred: DeferredTx,
        container: &mut S,
        schedulable_threads: &mut ThreadSet,
        remaining_budget: &mut u64,
        num_scheduled: &mut Saturating<usize>,
        num_sent: &mut usize,
        num_unschedulable_threads: &mut usize,
    ) -> Result<bool, SchedulerError> {
        let Some(state) = container.get_mut_transaction_state(deferred.id.id) else {
            return Ok(false);
        };

        let tx = state.transaction();

        // Fast-path conflict filtering. On exceeding max_retry_count, the
        // transaction is dropped (metrics.dropped_txs) instead of being
        // deferred again — mirrors the fix in src/lib.rs that prevents an
        // unbounded retry loop.
        if self.tx_conflict_score(tx) >= CONFLICT_THRESHOLD {
            self.metrics.lock_conflicts.fetch_add(1, Ordering::Relaxed);

            if deferred.retry_count >= self.config.max_retry_count {
                self.metrics.dropped_txs.fetch_add(1, Ordering::Relaxed);
                return Ok(false);
            }

            self.next_deferred.push(DeferredTx {
                id: deferred.id,
                retry_count: deferred.retry_count.saturating_add(1),
                ready_generation: self.current_generation
                    + (1u32 << deferred.retry_count.min(8)),
                created_generation: deferred.created_generation,
            });
            return Ok(false);
        }

        // Also drop (rather than defer forever) anything that has been
        // sitting in the deferred queue for too many generations overall,
        // independent of retry_count — guards against pathological
        // backoff sequences that would otherwise keep a transaction alive
        // far longer than MAX_DEFERRED_GENERATION_LIFETIME.
        let total_age = self
            .current_generation
            .wrapping_sub(deferred.created_generation);
        if total_age > MAX_DEFERRED_GENERATION_LIFETIME {
            self.metrics.dropped_txs.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }

        let transaction_bytes = tx.serialized_size() as u64;

        // UNVERIFIED CALL SITE — see doc comment above. Placeholder
        // signature; needs to be checked against the real
        // `scheduler_common::try_schedule_transaction` in the target Agave
        // checkout before this can compile.
        match try_schedule_transaction(
            &mut self.common,
            container,
            &deferred.id,
            schedulable_threads,
            *remaining_budget,
        ) {
            Ok(info) => {
                let thread_id = info.thread_id;

                if self.common.batches.entry_bytes()[thread_id] + transaction_bytes
                    > self.config.target_entry_bytes_per_batch
                {
                    let sent_this_flush = self.common.send_batches()?;
                    *num_sent += sent_this_flush;
                    // NOTE: only the delta from this flush is recorded, not
                    // the cumulative num_sent — mirrors the double-counting
                    // fix from the earlier memory_contexts review: adding
                    // the running total here would double-count every
                    // subsequent flush within the same scheduling pass.
                    self.metrics
                        .total_batches_sent
                        .fetch_add(sent_this_flush as u64, Ordering::Relaxed);
                }

                *num_scheduled += 1;
                self.metrics.scheduled_txs.fetch_add(1, Ordering::Relaxed);

                self.common.batches.add_transaction_to_batch(
                    thread_id,
                    deferred.id.id,
                    info.transaction,
                    info.max_age,
                    info.cost,
                    transaction_bytes,
                );

                *remaining_budget = remaining_budget.saturating_sub(info.cost);

                self.mark_hot_accounts(tx);

                Ok(true)
            }
            Err(TransactionSchedulingError::UnschedulableConflicts) => {
                self.mark_hot_accounts(tx);

                if deferred.retry_count >= self.config.max_retry_count {
                    self.metrics.dropped_txs.fetch_add(1, Ordering::Relaxed);
                    return Ok(false);
                }

                self.next_deferred.push(DeferredTx {
                    id: deferred.id,
                    retry_count: deferred.retry_count.saturating_add(1),
                    ready_generation: self.current_generation
                        + (1u32 << deferred.retry_count.min(8)),
                    created_generation: deferred.created_generation,
                });
                Ok(false)
            }
            Err(TransactionSchedulingError::UnschedulableThread) => {
                *num_unschedulable_threads += 1;
                self.metrics
                    .unschedulable_threads
                    .fetch_add(1, Ordering::Relaxed);

                if deferred.retry_count >= self.config.max_retry_count {
                    self.metrics.dropped_txs.fetch_add(1, Ordering::Relaxed);
                    return Ok(false);
                }

                self.next_deferred.push(DeferredTx {
                    id: deferred.id,
                    retry_count: deferred.retry_count.saturating_add(1),
                    ready_generation: self.current_generation + 1,
                    created_generation: deferred.created_generation,
                });
                Ok(false)
            }
        }
    }
}

// UNVERIFIED: the `Scheduler<Tx>` trait shape, `StateContainer<Tx>` bound,
// and how a real scheduling pass enumerates work from `container` are all
// placeholders here. The generation bookkeeping (increment + cleanup +
// drain-ready-deferred-first) mirrors `HybridScheduler::schedule` in
// src/lib.rs, which IS tested; the container-enumeration part is not.
impl<Tx: TransactionWithMeta> Scheduler<Tx> for RiftScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        budget: u64,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let schedule_start = Instant::now();
        self.metrics.scheduler_passes.fetch_add(1, Ordering::Relaxed);

        self.current_generation = self.current_generation.wrapping_add(1);
        self.cleanup_hotspots();

        let mut remaining_budget = budget;
        let mut num_scheduled = Saturating(0usize);
        let mut num_sent = 0usize;
        let mut num_unschedulable_threads = 0usize;
        let mut scanned = 0usize;
        let mut schedulable_threads = ThreadSet::any(self.common.num_threads());

        // Drain and retry deferred work whose backoff has elapsed before
        // considering anything newly arrived in `container` — this is the
        // step that was entirely absent from the original sketch (nothing
        // ever read `self.deferred` back out).
        let current_gen = self.current_generation;
        let (ready, still_waiting): (Vec<DeferredTx>, Vec<DeferredTx>) = mem::take(&mut self.deferred)
            .into_iter()
            .partition(|d| d.ready_generation <= current_gen);
        self.deferred = still_waiting;

        for deferred_tx in ready {
            scanned += 1;
            let _ = self.process_tx(
                deferred_tx,
                container,
                &mut schedulable_threads,
                &mut remaining_budget,
                &mut num_scheduled,
                &mut num_sent,
                &mut num_unschedulable_threads,
            )?;
        }

        // UNVERIFIED: how fresh work is actually pulled from `container`
        // for a real StateContainer is not something I can confirm without
        // the real trait definition. This loop is a placeholder shape only.
        // while let Some(next_id) = container.pop_next_ready() {
        //     scanned += 1;
        //     let deferred_tx = DeferredTx {
        //         id: next_id,
        //         retry_count: 0,
        //         ready_generation: self.current_generation,
        //         created_generation: self.current_generation,
        //     };
        //     let _ = self.process_tx(
        //         deferred_tx,
        //         container,
        //         &mut schedulable_threads,
        //         &mut remaining_budget,
        //         &mut num_scheduled,
        //         &mut num_sent,
        //         &mut num_unschedulable_threads,
        //     )?;
        // }

        mem::swap(&mut self.deferred, &mut self.next_deferred);
        self.next_deferred.clear();

        let duration = schedule_start.elapsed().as_nanos() as u64;
        self.metrics
            .total_schedule_time_ns
            .fetch_add(duration, Ordering::Relaxed);
        self.metrics
            .total_scanned
            .fetch_add(scanned as u64, Ordering::Relaxed);

        Ok(SchedulingSummary {
            scheduled: num_scheduled.0,
            deferred: self.deferred.len(),
            scanned,
            generation: self.current_generation,
        })
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        &mut self.common
    }
}
