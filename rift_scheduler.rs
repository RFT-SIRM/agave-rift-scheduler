use {
    super::{
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_common::{
            select_thread,
            try_schedule_transaction,
            SchedulingCommon,
            TransactionSchedulingError,
        },
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::banking_stage::{
        consumer::{
            ENTRY_OVERHEAD_BYTES,
            TARGET_NUM_TRANSACTIONS_PER_BATCH,
        },
        scheduler_messages::{
            ConsumeWork,
            FinishedConsumeWork,
        },
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
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Instant,
    },
};

const DEFAULT_TARGET_ENTRY_BYTES_PER_BATCH: u64 =
    get_data_shred_bytes_per_batch_typical() * 15 / 100;

const HOT_ACCOUNT_DECAY_SHIFT: u8 = 1;
const MAX_ACCOUNT_HEAT: u16 = 255;
const INITIAL_BACKOFF: u16 = 1;

const CONFLICT_THRESHOLD: u32 = 8;
const MAX_GENERATION_AGE: u32 = 16;

#[derive(Default)]
pub struct SchedulerMetrics {
    pub scheduled_txs: AtomicU64,
    pub lock_conflicts: AtomicU64,
    pub unschedulable_threads: AtomicU64,
    pub deferred_retries: AtomicU64,
    pub scheduler_passes: AtomicU64,
    pub total_schedule_time_ns: AtomicU64,
    pub total_scanned: AtomicU64,
    pub total_batches_sent: AtomicU64,
    pub total_batch_fill_ratio_ppm: AtomicU64,
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
            target_transactions_per_batch:
                TARGET_NUM_TRANSACTIONS_PER_BATCH,
            target_entry_bytes_per_batch:
                DEFAULT_TARGET_ENTRY_BYTES_PER_BATCH,
            hotspot_capacity: 16_384,
            max_retry_count: 6,
        }
    }
}

#[derive(Clone, Copy)]
struct DeferredTx {
    id: TransactionPriorityId,
    retry_count: u8,
    ready_generation: u32,
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

    metrics: Arc<SchedulerMetrics>,

    config: RiftSchedulerConfig,
}

impl<Tx: TransactionWithMeta> RiftScheduler<Tx> {
    pub(crate) fn new(
        consume_work_senders: Vec<Sender<ConsumeWork<Tx>>>,
        finished_consume_work_receiver: Receiver<
            FinishedConsumeWork<Tx>,
        >,
        config: RiftSchedulerConfig,
    ) -> Self {
        assert!(
            config.target_entry_bytes_per_batch
                > ENTRY_OVERHEAD_BYTES,
            "target entry bytes per batch must exceed entry overhead"
        );

        let cap =
            config.max_scanned_transactions_per_scheduling_pass;

        Self {
            deferred: Vec::with_capacity(cap),
            next_deferred: Vec::with_capacity(cap),

            hotspot_accounts:
                AHashMap::with_capacity(
                    config.hotspot_capacity,
                ),

            current_generation: 0,

            metrics: Arc::new(
                SchedulerMetrics::default(),
            ),

            common: SchedulingCommon::new(
                consume_work_senders,
                finished_consume_work_receiver,
                config.target_transactions_per_batch,
            ),

            config,
        }
    }

    #[inline(always)]
    fn metric_inc(
        metric: &AtomicU64,
        value: u64,
    ) {
        metric.fetch_add(value, Ordering::Relaxed);
    }

    #[inline(always)]
    fn tx_conflict_score(
        &self,
        tx: &Tx,
    ) -> u32 {
        let current_gen = self.current_generation;

        let mut score = 0u32;

        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) {
                continue;
            }

            if let Some(meta) =
                self.hotspot_accounts.get(key)
            {
                let age =
                    current_gen.wrapping_sub(
                        meta.generation,
                    );

                if age > MAX_GENERATION_AGE {
                    continue;
                }

                let decay =
                    (meta.heat
                        >> age.min(
                            HOT_ACCOUNT_DECAY_SHIFT
                                as u32,
                        )) as u32;

                score += decay;
            }
        }

        score
    }

    #[inline(always)]
    fn mark_hot_accounts(
        &mut self,
        tx: &Tx,
    ) {
        let current_gen = self.current_generation;

        for (i, key) in tx.account_keys().iter().enumerate() {
            if !tx.is_writable(i) {
                continue;
            }

            self.hotspot_accounts
                .entry(*key)
                .and_modify(|meta| {
                    meta.generation =
                        current_gen;

                    meta.heat = meta
                        .heat
                        .saturating_add(
                            INITIAL_BACKOFF,
                        )
                        .min(MAX_ACCOUNT_HEAT);
                })
                .or_insert(HotAccountMeta {
                    generation: current_gen,
                    heat: INITIAL_BACKOFF,
                });
        }
    }

    #[inline(always)]
    fn retry_backoff_generations(
        &self,
        retry_count: u8,
    ) -> u32 {
        1u32 << retry_count.min(8)
    }

    fn cleanup_hotspots(&mut self) {
        if self.hotspot_accounts.len()
            < self.config.hotspot_capacity * 2
        {
            return;
        }

        let current_gen = self.current_generation;

        self.hotspot_accounts.retain(
            |_, meta| {
                let age =
                    current_gen.wrapping_sub(
                        meta.generation,
                    );

                if age > MAX_GENERATION_AGE {
                    return false;
                }

                meta.heat >>= age.min(4);

                meta.heat > 0
            },
        );
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx>
    for RiftScheduler<Tx>
{
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        budget: u64,
    ) -> Result<
        SchedulingSummary,
        SchedulerError,
    > {
        let schedule_start = Instant::now();

        Self::metric_inc(
            &self.metrics.scheduler_passes,
            1,
        );

        self.current_generation =
            self.current_generation
                .wrapping_add(1);

        self.cleanup_hotspots();

        let mut remaining_budget =
            budget.saturating_sub(
                self.common
                    .in_flight_tracker
                    .cus_in_flight_per_thread()
                    .iter()
                    .sum(),
            );

        let starting_queue_size =
            container.queue_size();

        let starting_buffer_size =
            container.buffer_size();

        let num_threads =
            self.common
                .consume_work_senders
                .len();

        let target_cu_per_thread =
            self.config.target_scheduled_cus
                / num_threads as u64;

        let mut schedulable_threads =
            agave_scheduling_utils::thread_aware_account_locks::ThreadSet::any(
                num_threads,
            );

        for thread_id in 0..num_threads {
            if self.common
                .in_flight_tracker
                .cus_in_flight_per_thread()
                [thread_id]
                >= target_cu_per_thread
            {
                schedulable_threads
                    .remove(thread_id);
            }
        }

        if schedulable_threads.is_empty()
            || remaining_budget == 0
        {
            return Ok(SchedulingSummary {
                starting_queue_size,
                starting_buffer_size,
                ..SchedulingSummary::default()
            });
        }

        let mut num_scheduled =
            Saturating::<usize>(0);

        let mut num_sent = 0usize;

        let mut num_unschedulable_threads =
            0usize;

        let mut scanned = 0usize;

        let max_scan = self
            .config
            .max_scanned_transactions_per_scheduling_pass;

        while remaining_budget > 0
            && scanned < max_scan
            && !schedulable_threads.is_empty()
            && (!container.is_empty()
                || !self.deferred.is_empty())
        {
            for deferred in
                self.deferred.drain(..)
            {
                if scanned >= max_scan {
                    self.next_deferred
                        .push(deferred);

                    continue;
                }

                if deferred.ready_generation
                    > self.current_generation
                {
                    self.next_deferred
                        .push(deferred);

                    continue;
                }

                scanned += 1;

                self.process_tx(
                    deferred,
                    container,
                    &mut schedulable_threads,
                    &mut remaining_budget,
                    &mut num_scheduled,
                    &mut num_sent,
                    &mut num_unschedulable_threads,
                )?;
            }

            while scanned < max_scan
                && !container.is_empty()
                && !schedulable_threads
                    .is_empty()
            {
                let Some(id) =
                    container.pop()
                else {
                    break;
                };

                scanned += 1;

                self.process_tx(
                    DeferredTx {
                        id,
                        retry_count: 0,
                        ready_generation:
                            self.current_generation,
                    },
                    container,
                    &mut schedulable_threads,
                    &mut remaining_budget,
                    &mut num_scheduled,
                    &mut num_sent,
                    &mut num_unschedulable_threads,
                )?;
            }

            mem::swap(
                &mut self.deferred,
                &mut self.next_deferred,
            );
        }

        num_sent +=
            self.common.send_batches()?;

        Self::metric_inc(
            &self.metrics.total_batches_sent,
            num_sent as u64,
        );

        Self::metric_inc(
            &self.metrics.total_scanned,
            scanned as u64,
        );

        Self::metric_inc(
            &self.metrics.scheduled_txs,
            num_scheduled.0 as u64,
        );

        self.metrics
            .total_schedule_time_ns
            .fetch_add(
                schedule_start
                    .elapsed()
                    .as_nanos() as u64,
                Ordering::Relaxed,
            );

        container.push_ids_into_queue(
            self.deferred
                .drain(..)
                .map(|entry| entry.id),
        );

        Ok(SchedulingSummary {
            starting_queue_size,
            starting_buffer_size,
            num_scheduled:
                num_scheduled.0,
            num_unschedulable_conflicts: 0,
            num_unschedulable_threads,
        })
    }

    fn scheduling_common_mut(
        &mut self,
    ) -> &mut SchedulingCommon<Tx> {
        &mut self.common
    }
}

impl<Tx: TransactionWithMeta>
    RiftScheduler<Tx>
{
    #[inline(always)]
    fn process_tx<S: StateContainer<Tx>>(
        &mut self,
        deferred: DeferredTx,
        container: &mut S,
        schedulable_threads: &mut agave_scheduling_utils::thread_aware_account_locks::ThreadSet,
        remaining_budget: &mut u64,
        num_scheduled: &mut Saturating<usize>,
        num_sent: &mut usize,
        num_unschedulable_threads: &mut usize,
    ) -> Result<(), SchedulerError> {
        let Some(state) =
            container
                .get_mut_transaction_state(
                    deferred.id.id,
                )
        else {
            return Ok(());
        };

        let tx = state.transaction();

        let conflict_score =
            self.tx_conflict_score(tx);

        if conflict_score
            >= CONFLICT_THRESHOLD
            && deferred.retry_count
                < self.config.max_retry_count
        {
            Self::metric_inc(
                &self.metrics
                    .deferred_retries,
                1,
            );

            self.next_deferred.push(
                DeferredTx {
                    id: deferred.id,
                    retry_count:
                        deferred.retry_count + 1,
                    ready_generation:
                        self.current_generation
                            + self
                                .retry_backoff_generations(
                                    deferred
                                        .retry_count,
                                ),
                },
            );

            return Ok(());
        }

        let transaction_bytes =
            tx.serialized_size() as u64;

        match try_schedule_transaction(
            state,
            &mut self.common.account_locks,
            *schedulable_threads,
            |thread_set| {
                select_thread(
                    thread_set,
                    self.common
                        .batches
                        .total_cus(),
                    self.common
                        .in_flight_tracker
                        .cus_in_flight_per_thread(),
                    self.common
                        .batches
                        .transactions(),
                    self.common
                        .in_flight_tracker
                        .num_in_flight_per_thread(),
                )
            },
        ) {
            Ok(info) => {
                let thread_id =
                    info.thread_id;

                if self.common
                    .batches
                    .entry_bytes()
                    [thread_id]
                    + transaction_bytes
                    > self
                        .config
                        .target_entry_bytes_per_batch
                {
                    *num_sent += self
                        .common
                        .send_batches()?;
                }

                *num_scheduled += 1;

                self.common
                    .batches
                    .add_transaction_to_batch(
                        thread_id,
                        deferred.id.id,
                        info.transaction,
                        info.max_age,
                        info.cost,
                        transaction_bytes,
                    );

                *remaining_budget =
                    remaining_budget
                        .saturating_sub(
                            info.cost,
                        );

                let fill_ratio_ppm =
                    (self.common
                        .batches
                        .transactions()
                        [thread_id]
                        .len() as u64)
                        * 1_000_000
                        / self
                            .config
                            .target_transactions_per_batch
                            as u64;

                Self::metric_inc(
                    &self.metrics
                        .total_batch_fill_ratio_ppm,
                    fill_ratio_ppm,
                );

                if self.common
                    .batches
                    .transactions()
                    [thread_id]
                    .len()
                    >= self
                        .config
                        .target_transactions_per_batch
                    || self.common
                        .batches
                        .entry_bytes()
                        [thread_id]
                        >= self
                            .config
                            .target_entry_bytes_per_batch
                {
                    *num_sent += self
                        .common
                        .send_batches()?;
                }

                if self.common
                    .in_flight_tracker
                    .cus_in_flight_per_thread()
                    [thread_id]
                    + self.common
                        .batches
                        .total_cus()
                        [thread_id]
                    >= self
                        .config
                        .target_scheduled_cus
                        / self
                            .common
                            .consume_work_senders
                            .len()
                            as u64
                {
                    schedulable_threads
                        .remove(thread_id);
                }
            }

            Err(
                TransactionSchedulingError::UnschedulableConflicts,
            ) => {
                Self::metric_inc(
                    &self.metrics
                        .lock_conflicts,
                    1,
                );

                self.mark_hot_accounts(tx);

                self.next_deferred.push(
                    DeferredTx {
                        id: deferred.id,
                        retry_count:
                            deferred
                                .retry_count
                                .saturating_add(
                                    1,
                                ),
                        ready_generation:
                            self.current_generation
                                + self
                                    .retry_backoff_generations(
                                        deferred
                                            .retry_count,
                                    ),
                    },
                );
            }

            Err(
                TransactionSchedulingError::UnschedulableThread,
            ) => {
                *num_unschedulable_threads +=
                    1;

                Self::metric_inc(
                    &self.metrics
                        .unschedulable_threads,
                    1,
                );

                self.next_deferred.push(
                    DeferredTx {
                        id: deferred.id,
                        retry_count:
                            deferred
                                .retry_count
                                .saturating_add(
                                    1,
                                ),
                        ready_generation:
                            self.current_generation
                                + 1,
                    },
                );
            }
        }

        Ok(())
    }
}