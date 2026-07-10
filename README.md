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

Hot accounts are tracked across scheduling generations, and their influence decays over time so recent contention matters more than stale contention.

### 2. Fast-Path Conflict Filtering

Transactions are screened before expensive lock handling so likely-conflicting work can be deferred earlier and retried more intelligently.

### 3. Deferred Retry with Backoff

Conflicting transactions are pushed into a deferred queue and retried with generation-based backoff instead of being reprocessed immediately.

### 4. Progress-Guarded Scheduling Loop

The scheduler includes safeguards to reduce livelock-like behavior and keep progress visible through metrics and scheduling state.

## Design Goals

This repository intentionally avoids a large rewrite. The objective is to:

- preserve compatibility with the existing scheduling infrastructure
- reduce contention-induced retry pressure
- keep the core implementation compact and readable
- make behaviors observable through scheduler metrics

## Current Status

Experimental research implementation.

The repository is intended for:

- scheduler architecture discussion
- contention-heavy experimentation
- hybrid scheduling design exploration
- future integration with broader Agave runtime work

## Repository Purpose

This repository exists to keep the focus on the hybrid scheduler path and to make the main implementation easier to inspect, compare, and extend.

