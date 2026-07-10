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
    pub lock_conflicts: u64,
    pub hot_accounts: u64,
}

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub conflict_threshold: u32,
    pub max_generation_age: u32,
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

#[derive(Clone, Copy, Debug)]
struct DeferredTx {
    id: usize,
    retry_count: u8,
    ready_generation: u32,
}

#[derive(Clone, Debug, Default)]
pub struct SchedulingSummary {
    pub scheduled: usize,
    pub deferred: usize,
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

    fn tx_conflict_score(&self, tx: &Transaction) -> u32 {
        let mut score = 0u32;
        let current_gen = self.current_generation;

        for (index, account) in tx.account_keys.iter().enumerate() {
            if !tx.writable.get(index).copied().unwrap_or(false) {
                continue;
            }

            if let Some(meta) = self.hotspot_accounts.get(account) {
                let age = current_gen.wrapping_sub(meta.generation);
                if age > self.config.max_generation_age {
                    continue;
                }

                let decay = (meta.heat as u32)
                    .saturating_add(1)
                    .saturating_sub(age as u32);
                if decay > 0 {
                    score = score.saturating_add(decay);
                }
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

    fn cleanup_hotspots(&mut self) {
        let current_gen = self.current_generation;

        self.hotspot_accounts.retain(|_, meta| {
            let age = current_gen.wrapping_sub(meta.generation);
            if age > self.config.max_generation_age {
                return false;
            }

            if age > 0 {
                meta.heat = meta.heat.saturating_sub(age.min(4) as u16);
            }

            meta.heat > 0
        });
    }

    pub fn schedule(&mut self, transactions: &[Transaction], budget: u64) -> SchedulingSummary {
        self.metrics.scheduler_passes += 1;
        self.current_generation = self.current_generation.wrapping_add(1);
        self.cleanup_hotspots();

        let mut remaining_budget = budget;
        let mut scheduled = 0usize;
        let mut deferred = 0usize;
        let mut scanned = 0usize;

        for tx in transactions {
            scanned += 1;
            let score = self.tx_conflict_score(tx);

            if score >= self.config.conflict_threshold && tx.cost <= remaining_budget && tx.cost > 0
            {
                self.metrics.lock_conflicts += 1;
                self.deferred.push(DeferredTx {
                    id: tx.id,
                    retry_count: 1,
                    ready_generation: self.current_generation + 1,
                });
                deferred += 1;
                continue;
            }

            if tx.cost > remaining_budget {
                self.deferred.push(DeferredTx {
                    id: tx.id,
                    retry_count: 0,
                    ready_generation: self.current_generation + 1,
                });
                deferred += 1;
                continue;
            }

            self.mark_hot_accounts(tx);
            remaining_budget = remaining_budget.saturating_sub(tx.cost);
            scheduled += 1;
            self.metrics.scheduled_txs += 1;
        }

        self.metrics.deferred_txs = self.deferred.len() as u64;
        self.metrics.hot_accounts = self.hotspot_accounts.len() as u64;

        SchedulingSummary {
            scheduled,
            deferred,
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
}
