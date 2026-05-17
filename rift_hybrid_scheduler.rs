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
    agave_scheduling_utils::thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadSet, TryLockError},
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

const HOT_ACCOUNT_DECAY_SHIFT: u32 = 1;
const MAX_ACCOUNT_HEAT: u16 = 255;
const CONFLICT_THRESHOLD: u32 = 8;
const MAX_GENERATION_AGE: u32 = 16;
const MAX_DEFERRED_GENERATION_LIFETIME: u32 = 64;

#[derive(Default)]
pub struct SchedulerMetrics {
    pub scheduler_passes: AtomicU64,
    pub total_scanned: AtomicU64,
    pub scheduled_txs: AtomicU64,
    pub lock_conflicts: AtomicU64,
    pub unschedulable_threads: AtomicU64,
    pub total_batches_sent: AtomicU64,
    pub total_schedule_time_ns: AtomicU64,
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

    #[inline(always)]
    fn tx_conflict_score(&self, tx: &Tx) -> u32 {
        let current_gen = self.current_generation;
        let mut score = 0u32;
        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) { continue; }
            if let Some(meta) = self.hotspot_accounts.get(key) {
                let age = current_gen.wrapping_sub(meta.generation);
                if age > MAX_GENERATION_AGE { continue; }
                let decay = (meta.heat >> age.min(15)) as u32;
                score = score.saturating_add(decay);
            }
        }
        score
    }

    #[inline(always)]
    fn mark_hot_accounts(&mut self, tx: &Tx) {
        let current_gen = self.current_generation;
        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) { continue; }
            self.hotspot_accounts
                .entry(*key)
                .and_modify(|meta| {
                    meta.generation = current_gen;
                    meta.heat = meta.heat.saturating_add(1).min(MAX_ACCOUNT_HEAT);
                })
                .or_insert(HotAccountMeta { generation: current_gen, heat: 1 });
        }
    }

    fn cleanup_hotspots(&mut self) {
        if self.hotspot_accounts.len() < self.config.hotspot_capacity * 2 {
            return;
        }
        let current_gen = self.current_generation;
        self.hotspot_accounts.retain(|_, meta| {
            let age = current_gen.wrapping_sub(meta.generation);
            if age > MAX_GENERATION_AGE { return false; }
            meta.heat >>= age.min(4);
            meta.heat > 0
        });
    }

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

        // Fast-path conflict filtering
        if deferred.retry_count < self.config.max_retry_count && self.tx_conflict_score(tx) >= CONFLICT_THRESHOLD {
            self.metrics.lock_conflicts.fetch_add(1, Ordering::Relaxed);
            self.next_deferred.push(DeferredTx {
                id: deferred.id,
                retry_count: deferred.retry_count.saturating_add(1),
                ready_generation: self.current_generation + (1u32 << deferred.retry_count.min(8)),
                created_generation: deferred.created_generation,
            });
            return Ok(false);
        }

        let transaction_bytes = tx.serialized_size() as u64;

        match try_schedule_transaction(...) {  // (your existing call)
            Ok(info) => {
                let thread_id = info.thread_id;

                if self.common.batches.entry_bytes()[thread_id] + transaction_bytes > self.config.target_entry_bytes_per_batch {
                    *num_sent += self.common.send_batches()?;
                    self.metrics.total_batches_sent.fetch_add(*num_sent as u64, Ordering::Relaxed);
                }

                *num_scheduled += 1;
                self.metrics.scheduled_txs.fetch_add(1, Ordering::Relaxed);

                self.common.batches.add_transaction_to_batch(
                    thread_id, deferred.id.id, info.transaction, info.max_age, info.cost, transaction_bytes,
                );

                *remaining_budget = remaining_budget.saturating_sub(info.cost);

                // ... rest of batch logic unchanged
                Ok(true)
            }
            Err(TransactionSchedulingError::UnschedulableConflicts) => {
                self.mark_hot_accounts(tx);
                self.next_deferred.push(/* ... */);
                Ok(false)
            }
            Err(TransactionSchedulingError::UnschedulableThread) => {
                *num_unschedulable_threads += 1;
                self.metrics.unschedulable_threads.fetch_add(1, Ordering::Relaxed);
                self.next_deferred.push(/* ... */);
                Ok(false)
            }
        }
    }
}

// schedule() method with full instrumentation
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

        // ... (rest of your scheduling logic remains exactly as before)

        let duration = schedule_start.elapsed().as_nanos() as u64;
        self.metrics.total_schedule_time_ns.fetch_add(duration, Ordering::Relaxed);
        self.metrics.total_scanned.fetch_add(scanned as u64, Ordering::Relaxed);

        // ... return SchedulingSummary
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        &mut self.common
    }
}
