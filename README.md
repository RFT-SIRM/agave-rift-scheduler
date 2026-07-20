# agave-rift-scheduler

**SIRM Deterministic Memory Scheduler** — контентно-адаптивный детерминированный планировщик транзакций для `banking_stage` виртуальной машины Agave (SVM).

Разработан в рамках исследовательского фреймворка **UltraCore RFT / SIRM** как продолжение работы по проекту [`agave-abiv2-memory-contexts`](https://github.com/RFT-SIRM/agave-abiv2-memory-contexts), в котором был обнаружен и зафиксирован критический баг утечки прав между вложенными CPI-фреймами (см. [anza-xyz/svm RFC #25 — per-frame rollback leakage](https://github.com/anza-xyz/agave/issues)).

---

## Контекст и мотивация

### Что было найдено ранее

В репозитории `agave-abiv2-memory-contexts` был обнаружен логический баг: при вложенном вызове CPI изменения writable-разрешений аккаунтов не откатывались при выходе из фрейма. Это создавало потенциальную утечку прав между транзакциями в одном батче.

Корректность реализации ядра памяти была подтверждена:
- фаззинговым прогоном **4.29 миллиарда итераций** без единого паника или нарушения инварианта
- фиксированным RSS **~505 МБ** на протяжении всего прогона (отсутствие утечек памяти)
- двумя независимыми инвариантами (`supply invariant`, `base_sum invariant`), верифицируемыми после каждой операции

### Проблема в текущем планировщике

Стандартный greedy-планировщик `banking_stage` под высокой нагрузкой страдает от:

- **Lock churn** — сотни транзакций конкурируют за один горячий аккаунт (AMM pool, Raydium), вызывая повторные блокировки
- **Мёртвая очередь отложенных** — транзакции помещались в deferred-очередь, но никогда оттуда не читались
- **Infinite retry** — конфликтующая транзакция могла зависнуть в очереди навсегда
- **Zero-cost bypass** — транзакция с `cost=0` обходила проверку конфликтов полностью

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                    HybridScheduler::schedule()                  │
│                                                                 │
│  1. increment generation                                        │
│  2. cleanup_hotspots()  ←── decay + evict cold accounts        │
│  3. drain deferred queue (ready_generation ≤ current_gen)      │
│  4. process fresh transactions                                  │
│                                                                 │
│  For each transaction:                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  tx_conflict_score()                                     │  │
│  │  ├─ score >= conflict_threshold → DEFER with backoff     │  │
│  │  │   └─ retry_count >= max_retry_count → DROP            │  │
│  │  ├─ cost > remaining_budget → DEFER (next gen)           │  │
│  │  │   └─ retry_count >= max_retry_count → DROP            │  │
│  │  └─ OK → mark_hot_accounts() → SCHEDULE                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘

HotspotTable (HashMap<AccountId, HotAccountMeta>)
  heat: u16  ──►  decayed_heat(heat, age, shift) = heat >> (age * shift)
  generation: u32  ──►  evicted when age > max_generation_age OR heat == 0
```

---

## Ключевые исправления

### 1. Живая deferred-очередь

**Было:** `self.deferred.push(...)` — транзакции писались, но никогда не читались.

**Стало:** Каждый пасс сначала дренирует все транзакции с `ready_generation <= current_generation`, затем обрабатывает новые.

### 2. Exponential backoff при конфликте

```
ready_generation = current_generation + (1 << retry_count)
```

Retry 0 → +1 gen, retry 1 → +2 gen, retry 2 → +4 gen, ..., retry 8+ → +256 gen.

### 3. Hard drop после max_retry_count

Транзакция не может зависнуть навсегда. После `max_retry_count` (по умолчанию 6) попыток она удаляется из очереди с инкрементом `SchedulerMetrics::dropped_txs`.

### 4. Zero-cost conflict bypass закрыт

Проверка конфликта не зависит от `tx.cost`. Транзакция с cost=0, обращающаяся к горячему аккаунту, обрабатывается идентично любой другой.

### 5. Единственный источник decay

`decayed_heat(heat, age, shift)` вызывается **только в одном месте** (`cleanup_hotspots`). `tx_conflict_score` читает уже нормализованные значения — исключает расхождение двух формул decay.

---

## Структура репозитория

```
agave-rift-scheduler/
├── src/
│   ├── lib.rs                    # Ядро планировщика: HybridScheduler, тесты (15 шт.)
│   └── main.rs                   # Запускаемый пример
├── fuzz/
│   ├── Cargo.toml
│   └── fuzz_targets/
│       └── scheduler_fuzz.rs     # libFuzzer харнесс (4 инварианта)
├── benches/
│   └── scheduler_bench.rs        # Criterion бенчмарки (4 сценария)
├── rift_hybrid_scheduler.rs      # Технический скетч адаптации под Agave banking_stage
├── .github/workflows/ci.yml      # CI: lint + тесты + бенчмарки + fuzz smoke 60s
└── README.md
```

---

## Публичный API

```rust
use agave_rift_scheduler::{AccountId, HybridScheduler, SchedulerConfig, Transaction};

let mut scheduler = HybridScheduler::with_config(SchedulerConfig {
    conflict_threshold:  4,    // минимальный накопленный heat для defer
    max_generation_age:  16,   // через сколько поколений аккаунт выбрасывается
    hotspot_decay_shift: 1,    // heat >>= age * shift каждое поколение
    max_retry_count:     6,    // после этого → drop
    hotspot_capacity:    4096, // начальная ёмкость HashMap
    initial_heat:        2,    // тепло при первом касании аккаунта
    max_account_heat:    255,  // потолок heat
});

let txs = vec![
    Transaction::new(id, cost, vec![AccountId(42)], vec![true /*writable*/]),
];

let summary = scheduler.schedule(&txs, budget);
// summary.scheduled / .deferred / .dropped / .scanned / .generation
```

---

## Тесты

```
cargo test --lib
```

**15 тестов** в двух группах:

| Группа | Что проверяет |
|--------|--------------|
| `tests` | Базовые сценарии: defer при конфликте, eviction устаревших hotspot-ов, retry и последующее расписывание, zero-cost bypass, drop после max_retry |
| `extended_scheduler_tests` | Постепенный decay heat, budget exhaustion ≠ conflict, независимые аккаунты не мешают друг другу, read-only не создаёт конфликт, wrap generation counter, накопление метрик |

Все 15 тестов — зелёные.

---

## Фаззинг

```bash
# Установить cargo-fuzz (нужен nightly)
cargo install cargo-fuzz

# Запустить (бессрочно, или с ограничением)
cd fuzz
cargo +nightly fuzz run scheduler_fuzz
cargo +nightly fuzz run scheduler_fuzz -- -max_total_time=21300  # 5ч55м
```

Харнесс генерирует через `arbitrary`:
- случайный `SchedulerConfig` (все параметры)
- произвольные последовательности пассов с батчами транзакций
- аккаунты из пула 0..255 (обеспечивает коллизии и реальный contention)

**Проверяемые инварианты на каждом пассе:**

1. `scheduled + deferred + dropped ≤ scanned` — учёт не нарушен
2. `generation > 0` — монотонно растёт
3. `scheduler_passes ≥ 1` — метрики не регрессируют
4. После 512 drain-пассов `deferred_txs == 0` — очередь обязательно опустошается

**Предшествующие результаты фаззинга (`agave-abiv2-memory-contexts`):**
- **4.29 млрд итераций** без паника
- **RSS ~505 МБ** (прямая линия, нет роста)
- **0 нарушений инварианта**

---

## Бенчмарки

```bash
cargo bench  # требует Rust >= 1.80
```

Четыре сценария, батчи 64 / 256 / 1024 / 4096 транзакций:

| Сценарий | Описание |
|----------|----------|
| `no_conflicts` | Все транзакции — разные аккаунты, нет contention |
| `hot_account` | Все транзакции — один аккаунт (AMM-pool worst case) |
| `mixed_10pct_hot` | 10 % транзакций на горячий аккаунт, 90 % независимые |
| `high_churn` | Много пассов по 64 tx, тест амортизированной стоимости cleanup |

---

## CI

Три джоба при каждом push / PR:

```
⚡ Lint & Test         cargo fmt --check + clippy + cargo test --lib
📊 Benchmarks          cargo build --benches  (Rust stable ≥ 1.80)
🔥 Fuzz smoke (60 s)   cargo +nightly fuzz run scheduler_fuzz -max_total_time=60
```

---

## Связь с `agave-abiv2-memory-contexts`

| Репозиторий | Что делает |
|-------------|-----------|
| [`agave-abiv2-memory-contexts`](https://github.com/RFT-SIRM/agave-abiv2-memory-contexts) | Ядро памяти ABIv2: rollback прав, динамический подсчёт регионов, fuzz 4.29B итераций |
| [`agave-rift-scheduler`](https://github.com/RFT-SIRM/agave-rift-scheduler) *(этот репо)* | Планировщик: conflict graph, hotspot decay, deferred retry, benches |

Оба репозитория — часть единого фреймворка **RFT/SIRM** для исследования безопасности и производительности Agave SVM.

---

## Статус

Экспериментальная исследовательская реализация. Не предназначена для использования в production-валидаторе без дополнительного аудита и интеграции в agave-workspace.

**`rift_hybrid_scheduler.rs`** в корне репозитория — технический скетч адаптации логики на реальные Agave-интерфейсы (`ThreadAwareAccountLocks`, `try_schedule_transaction`, `StateContainer`). Импорты не верифицированы против конкретного чекаута agave — использовать как архитектурный ориентир.

---

*RFT / SIRM — Solana Runtime Research, 2026*
