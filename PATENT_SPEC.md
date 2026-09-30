# RFT Contention-Aware Transaction Scheduler — Patent-Oriented Technical Disclosure

**Inventor / Rights Holder:** Evgeny / RFT-SIRM
**Repository:** `RFT-SIRM/agave-rift-scheduler`
**Status:** Technical disclosure for patent counsel review; not a filed patent application.

## 1. Title

**Contention-Aware Transaction Scheduling System and Method Using Per-Resource Contention Heat Mapping, Generation-Aware Deferred Execution, Bounded Retry Semantics, and Explicit Starvation Observability**

## 2. Technical Field

The disclosure relates to concurrent transaction scheduling in blockchain virtual machines, replicated state machines, distributed ledgers, databases, parallel execution engines, and other systems in which transactions compete for mutable execution resources.

## 3. Technical Problem

A conflict-aware scheduler may defer a transaction that cannot acquire required writable resources and reinsert it into a later scheduling pass. Under sustained contention, repeated deferral can continue without an explicit retry bound, leaving scheduling latency and starvation difficult to observe and scheduler-side state difficult to bound.

The disclosed architecture introduces explicit resource-level contention state, generation-aware deferral, bounded retries, terminal accounting, and deterministic observability.

## 4. Core Architecture

A scheduler comprises:

1. a transaction input and priority queue;
2. a conflict detector for mutable resources;
3. a per-resource contention heat map;
4. a generation counter;
5. a deferred transaction queue;
6. per-transaction retry state;
7. a configurable maximum retry count;
8. explicit scheduling metrics; and
9. bounded scheduler-state management.

For a resource `a` at generation `g`, contention state may be represented as:

`H(a,g) = {heat, generation_age, resource_id}`

Heat may increase following a scheduled write and decay as generations advance. Stale resource entries may be evicted after a configured age or capacity limit.

## 5. Generation-Aware Deferral

Each scheduling invocation advances a monotonic generation.

A deferred transaction receives a `ready_generation`. It becomes eligible for retry when:

`ready_generation <= current_generation`

This prevents uncontrolled immediate cycling of the same conflicting transaction and gives deferred work a deterministic temporal position.

## 6. Bounded Retry Semantics

On a conflict:

`retry_count := retry_count + 1`

If the retry count remains within `max_retry_count`, the transaction is deferred to a later eligible generation.

When the configured retry bound is exceeded, the transaction reaches an explicit terminal scheduling outcome, such as a dropped state, and the corresponding metric is incremented.

The mechanism therefore prevents indefinite retry loops by construction.

## 7. Deterministic Scheduling

For identical input transactions, initial scheduler state, conflict state, and configuration, the scheduling process is deterministic.

The observable outcome classifies work as:

- scheduled;
- deferred; or
- explicitly terminated/dropped.

The same state-transition sequence can therefore be reproduced by tests and fuzzing.

## 8. Starvation Observability

The scheduler exposes explicit state for repeated deferral, including:

- retry counts;
- scheduler passes;
- generation numbers;
- deferred queue depth;
- resource heat;
- hotspot age; and
- `dropped_transactions` or an equivalent terminal-outcome metric.

A transaction remaining indefinitely deferred is therefore not silently indistinguishable from ordinary queue residency.

## 9. Bounded Scheduler State

The architecture can bound scheduler-side dynamic state through:

- `max_retry_count`;
- bounded hotspot capacity;
- generation-based hotspot expiration;
- maximum resource heat;
- bounded deferred lifecycle.

This creates explicit limits on state growth caused by persistent contention.

## 10. State Machine

A representative transaction state machine is:

`READY -> SCHEDULED`

when required resources are available.

`READY -> DEFERRED`

when contention is detected and retry remains permitted.

`DEFERRED -> READY`

when the assigned generation becomes eligible.

`DEFERRED -> DROPPED`

when the retry bound is exceeded.

## 11. Accounting Invariant

For each scheduling pass:

`scheduled + deferred + dropped <= scanned`

Every transaction entering the scheduling operation must remain represented by an explicit outcome or deferred state.

An implementation may strengthen this to equality where every scanned transaction is assigned to exactly one category.

## 12. Generation and Drain Invariants

The generation counter and scheduler-pass counter are monotonic.

After external input stops, deferred work must reach a terminal state within a bounded execution envelope determined by the retry policy and scheduler configuration.

## 13. Heat-Map Embodiments

The heat function is not limited to a particular mathematical form. Implementations may use:

- integer accumulation;
- saturating counters;
- fixed-point decay;
- exponential decay;
- sliding windows;
- generation-based right-shift decay;
- exponentially weighted moving averages; or
- bucketed contention classes.

The resource identifier may be a blockchain account, database row, lock, memory region, actor, shard, or another mutable execution resource.

## 14. Retry-Policy Embodiments

The retry limit may be:

- global;
- per transaction class;
- per resource;
- per priority class;
- dynamically configured;
- derived from resource heat;
- derived from queue pressure; or
- controlled by an external policy layer.

A terminal retry outcome may be implemented as drop, demotion, alternate execution lane, serialization, or external resubmission.

## 15. Verification Architecture

The reference implementation uses deterministic tests and long-duration fuzzing to exercise:

1. transaction accounting;
2. generation monotonicity;
3. scheduler-pass monotonicity;
4. deferred-queue drainage;
5. heat decay;
6. stale-hotspot cleanup;
7. retry-cap enforcement; and
8. conflict/retry transitions.

Fuzzing increases confidence in tested state spaces but is not asserted to constitute a formal mathematical proof.

## 16. Reference Implementation

The repository `RFT-SIRM/agave-rift-scheduler` contains a research implementation of the disclosed architecture, including per-account heat tracking, generation-aware deferred execution, bounded retry counters, explicit dropped-transaction metrics, deterministic scheduler state, and fuzz-tested invariants.

The repository itself does not assert that this implementation is deployed in production Agave or adopted by Anza.

## 17. Potential Claim Concepts

### Claim 1 — Scheduler System

A computer-implemented transaction scheduler comprising a transaction queue, a mutable-resource conflict detector, a per-resource contention state structure, a deferred transaction queue, per-transaction retry state, a configurable retry threshold, and a controller configured to schedule, defer, or terminate a transaction according to resource contention and retry state.

### Claim 2 — Contention Heat

The scheduler of Claim 1 wherein the per-resource contention state structure maintains a heat value that changes following resource access and decays according to execution-generation age.

### Claim 3 — Generation-Aware Deferral

The scheduler of Claim 1 wherein a deferred transaction is associated with a ready generation and is not retried before that generation is eligible.

### Claim 4 — Bounded Retry

The scheduler of Claim 1 wherein exceeding the retry threshold produces an explicit terminal scheduling outcome and corresponding metric.

### Claim 5 — Bounded Resource State

The scheduler of Claim 1 wherein contention state is bounded by capacity, age, heat ceiling, or a combination thereof.

### Claim 6 — Deterministic State Transition

The scheduler of Claim 1 wherein identical initial state, transaction input, and configuration produce identical scheduling classifications.

### Claim 7 — Scheduling Method

A computer-implemented method comprising receiving transactions, detecting mutable-resource contention, updating resource-level contention state, deferring a conflicting transaction, incrementing retry state, assigning a future generation, retrying when eligible, and terminating repeated retry when a configured bound is exceeded.

### Claim 8 — Distributed Execution

The scheduler or method of any preceding claim implemented in a replicated, distributed, blockchain, virtual-machine, database, or parallel execution system.

## 18. Prior-Art Boundary

Patent counsel should evaluate the disclosure against prior art concerning transaction schedulers, concurrency control, blockchain account-lock scheduling, priority queues, starvation prevention, contention scoring, bounded retry systems, generational decay, deterministic parallel execution, and distributed scheduling.

This document does not itself establish novelty, non-obviousness, patentability, infringement, or freedom to operate.

## 19. Filing Boundary

This document is a technical disclosure prepared from the reference implementation and its documented architecture. It is intended to preserve the technical structure, alternatives, state transitions, and claim concepts for subsequent patent-counsel review and filing strategy.

## 20. Evidence Boundary

The reference implementation demonstrates the software architecture and its verification model. No statement in this document claims third-party adoption, production deployment, or incorporation into Agave unless independently documented.

**Document version:** 1.0
**Year:** 2026
