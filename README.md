# agave-rift-scheduler

Hybrid contention-aware transaction scheduler for Agave banking_stage.

## Overview

This repository contains an experimental hybrid scheduler designed to improve transaction scheduling under account contention. The focus is on reducing repeated lock retries, smoothing batch formation, and making hotspot behavior more adaptive.

![Rift Hybrid Architecture](hybrid_architecture.png)

The main implementation lives in [src/lib.rs](src/lib.rs), with a runnable example in [src/main.rs](src/main.rs). The repository is intentionally streamlined around that hybrid scheduler path.

## Motivation

The standard greedy scheduling flow can waste effort when a small set of accounts becomes hot. Under sustained contention, this can lead to:

- excessive lock churn
- unstable scheduling decisions
- unnecessary retries
- reduced throughput and batch efficiency

This project explores a hybrid strategy that preserves the existing scheduler structure while making contention handling more explicit and adaptive.

## Key Improvements

### 1. Generation-Based Hotspot Tracking

Hot accounts are tracked across scheduling generations. Heat decay is
computed in exactly one place (`decayed_heat`, driven by
`hotspot_decay_shift`) and applied once per generation in
`cleanup_hotspots`, so `tx_conflict_score` always reads an
already-normalized value — no risk of two decay formulas drifting apart.

### 2. Fast-Path Conflict Filtering

Transactions are screened before expensive lock handling. The conflict
check applies uniformly regardless of a transaction's cost — a zero-cost
transaction touching a hot account is treated exactly like any other
transaction touching that account.

### 3. Deferred Retry with Backoff

Conflicting or budget-exceeding transactions are pushed into a deferred
queue with generation-based backoff. Each `schedule()` pass first drains
and retries any deferred transaction whose backoff has elapsed, before
considering freshly-arrived work. Transactions that still conflict after
`max_retry_count` retries are dropped (and counted in
`SchedulerMetrics::dropped_txs`) rather than retried forever.

### 4. Progress-Guarded Scheduling Loop

`SchedulingSummary` and `SchedulerMetrics` expose `scheduled`, `deferred`,
`dropped`, `scanned`, and `generation`/`hot_accounts` counts so scheduling
behavior (including the retry-and-drop path) is directly observable and
testable, rather than being an opaque internal detail.

## Design Goals

This repository intentionally avoids a large rewrite. The objective is to:

- preserve compatibility with the existing scheduling infrastructure
- reduce contention-induced retry pressure
- keep the core implementation compact and readable
- make behaviors observable through scheduler metrics

## Current Status

Experimental research implementation.

`src/lib.rs` is a standalone, fully tested implementation of the hybrid
scheduling model (contention scoring, hotspot decay, deferred retry with
backoff, and max-retry drop) — it compiles, passes `cargo fmt --check`,
and has unit tests covering each of those behaviors, including regression
tests for previously-identified issues (dead deferred queue, zero-cost
conflict bypass).

`rift_hybrid_scheduler.rs` at the repository root is a **technical
reference sketch only** — it is not declared as a module anywhere and is
not part of this crate's build (`Cargo.toml` only depends on `anyhow`). It
sketches how the same scheduling/decay/retry logic could be adapted onto
Agave's actual banking_stage scheduler trait surface, but its Agave-facing
imports and call signatures (`scheduler_common`, `ThreadAwareAccountLocks`,
`try_schedule_transaction`, etc.) have not been verified against a
specific Agave checkout and should be checked against the target version
before any attempt to compile it inside an actual agave workspace.

The repository is intended for:

- scheduler architecture discussion
- contention-heavy experimentation
- hybrid scheduling design exploration
- future integration with broader Agave runtime work

## Repository Purpose

This repository exists to keep the focus on the hybrid scheduler path and to make the main implementation easier to inspect, compare, and extend.
