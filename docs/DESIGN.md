# Design Notes

Why this scheduler is built the way it is, including the alternatives that
were tried and rejected. The README covers *what* it does; this covers *why*,
and is the document to read before changing anything concurrent.

---

## 1. The central split: submission vs. ordering

The two halves of a priority scheduler want opposite things.

**Submission** wants no shared mutable state. Producers are often latency-
sensitive threads doing something else important — reading a sensor, servicing
an interrupt handler's queue — and the cost they pay to hand off work should
be constant and independent of how many other producers exist.

**Ordering** is inherently a shared total order. Deciding which of N queued
tasks runs next requires agreement about all N.

A single structure has to compromise. Two structures do not:

```
producers ──▶ lock-free MPMC ring ──▶ [worker drains under lock] ──▶ heap ──▶ execute
              (no ordering)                                          (ordered)
```

The key move is **where** the transfer happens. A worker that wants a task
must take the heap lock regardless. While it holds that lock it drains the
entire ring into the heap, then pops. So:

- Producers never touch the heap lock for *ordering* (see §3 for the one
  exception, and the scope note below for what submission does still lock).
- The lock is amortized across every task that arrived since the last drain.
  Under load a single acquisition moves dozens of tasks.
- The ring's FIFO order is irrelevant — the heap reorders everything anyway,
  which is why a *FIFO* lock-free queue is sufficient here and a lock-free
  *priority* queue is not needed.

**Scope note — what is actually lock-free.** The claim is about the *queue
handoff*, not about `submit_task` as a whole. Every submission also allocates
one `Task` and takes a sharded registry mutex so the id stays queryable, and a
submission that finds the ring full takes the heap mutex. So the call is not
lock-free end to end. What the ring buys is that the contended multi-producer
path is not a serialization point: registry contention is divided by
`registry_shards`, while a single shared queue mutex would not divide at all.
If submission throughput ever became the limit, the allocation and the
registry insert are the two things to attack — in that order.

A dedicated pump thread was considered instead of draining from the workers.
It was rejected: it adds a thread and a hop of latency, and it makes the pump
a single point of serialization that is *worse* than the heap lock because
nothing amortizes it.

### Why not a fully lock-free priority queue?

This is the obvious "more impressive" answer, and it is the wrong trade here.

A lock-free priority queue (skip-list-based, or a lock-free pairing heap) is
roughly an order of magnitude more code, and it brings the two hardest
problems in concurrent programming with it: ABA on the CAS'd links, and safe
memory reclamation (which needs hazard pointers, epochs, or RCU — each its own
subsystem). It wins when many threads pop concurrently.

At the pool sizes this targets (2–16 workers), the heap lock is held for
`O(log n)` pointer swaps. The measurements say it is not the constraint:
throughput is flat from 1 to 4 producers (1.39M → 1.27M tasks/s), so the
submission path is not saturating, and it is the submission path that the
lock-free structure protects. Meanwhile the empty-callback worker sweep shows
throughput *falling* as workers are added — the serialization point is real,
but it only binds when callbacks do no work, and a pool running no-op
callbacks is not a workload anyone has.

The correct next step, if very short tasks became the target, is **work
stealing** (per-worker deques) rather than a lock-free global heap — but that
gives up strict global priority ordering, which is the entire point of this
component.

---

## 2. The wakeup handshake

Workers must sleep when idle (a polling pool burns cores and destroys the
latency of everything else on the box) and wake promptly when work arrives.
Doing that without paying for a notify on every submission is the subtlest
part of the design.

The naive version has a lost-wakeup bug:

```cpp
// BROKEN
if (queue_is_empty_for_waiter()) { /* producer publishes here */ cv.wait(lock); }
```

A `condition_variable` only guarantees a notify is not lost if the notifier
holds (or has held, after the state change) the same mutex the waiter waits
on. Without that, a producer can publish work and notify in the window
between the waiter's failed predicate check and its actual sleep. The worker
then sleeps forever with work queued — a hang that appears under load, in
production, and never in a smoke test.

The standard fix is for the producer to take the mutex on every submission.
That works but reintroduces the contention the lock-free ring exists to avoid.

This implementation keeps both properties with a **Dekker-style handshake**:

```
producer:  store pending_       (seq_cst)  ;  load idle_workers_  (seq_cst)
worker:    store idle_workers_  (seq_cst)  ;  load pending_       (seq_cst)
```

Claim: they cannot both read a stale value. Proof: sequential consistency
gives a single total order over all four operations. Suppose the producer's
load of `idle_workers_` returns 0, i.e. it does not observe the worker's
store. Then that load precedes the worker's store in the total order. The
producer's store to `pending_` precedes its own load (program order), and the
worker's store precedes its own load. Chaining: `store pending_` → `load
idle_workers_` → `store idle_workers_` → `load pending_`. So the worker's load
of `pending_` *must* observe the producer's store. ∎

Therefore:

- **Producer sees no idle worker** → it may skip the notify entirely, because
  every worker is guaranteed to see the work before sleeping. This is the
  steady state under load, and it is why the queue handoff touches no mutex at
  all when the pool is busy (the registry lock in §1's scope note still
  applies).
- **Producer sees an idle worker** → it takes the queue mutex for zero work
  and then notifies. The empty critical section is not a trick; it is the only
  correct way to serialize against a waiter sitting between its predicate and
  its sleep.

`seq_cst` is required on exactly these four operations and is used nowhere
else on the hot path. Weakening any of them to acquire/release breaks the
proof — the total order is what the argument depends on.

### The same handshake guards shutdown

Shutdown needs the mirror image of this property: a producer must either see
the closed door and reject, or be seen by `stop()` and waited for.

```
producer:  store inflight_submitters_ (seq_cst) ; load lifecycle_            (seq_cst)
stop():    store lifecycle_           (seq_cst) ; load inflight_submitters_  (seq_cst)
```

This was first written with `acq_rel`/`acquire`, which looks sufficient and is
not: without a single total order, both sides can miss each other, and the
task that slips through is never run *and* never failed — so `outstanding_`
never returns to zero and anything waiting for the queue to drain hangs
forever. It reproduced at roughly one task per 50 rounds of 8 producers racing
`stop()`, and is now pinned by
`SchedulerLifecycle.StopRacingSubmitOnANeverStartedSchedulerLosesNothing`.

The lesson generalizes: whenever two threads each publish a fact and then
check the other's, the pattern is Dekker's and the ordering requirement is
`seq_cst`. Both places in this codebase that need it are marked as such.

### Why `pending_` is incremented *before* publishing

`pending_` is incremented before the task is put in the ring, making it an
upper bound on available work rather than an exact count. The alternative
(increment after publishing) allows this sequence:

1. Producer publishes the task to the ring.
2. Worker drains the ring, pops the task, decrements `pending_` — **from
   zero**, underflowing a `size_t` to `SIZE_MAX`.
3. Producer increments.

Incrementing first makes the number of pops provably never exceed the number
of increments, so underflow is impossible. The cost is that `pending_` can
transiently count a task that is not yet dequeueable, which a worker handles
with a bounded backoff (`kPublishRaceBackoff`, 50 µs, cut short by the
producer's own notify). That window is the few nanoseconds between a
producer's CAS on the ring cursor and its release-store of the pointer.

---

## 3. Backpressure, and why the ring is not a capacity limit

Two different limits are easy to conflate:

- **`ingress_capacity`** — the lock-free ring. A *performance* knob. When it
  fills, submission falls back to pushing directly onto the heap under the
  mutex, counted as `total_slow_path_submissions`. Slower, but submission
  stays total. Ring size therefore never affects correctness, only how much
  contention producers see.
- **`max_queue_depth`** — the *capacity* limit. When reached, submission is
  rejected (`REJECTED_QUEUE_FULL` / `QueueFull`).

Keeping these separate matters because it means an undersized ring degrades
gracefully and is visible in metrics, rather than dropping work.

The depth check happens **before** the `Task` is allocated. Under overload,
the rejection path must not allocate — otherwise overload becomes an allocator
storm, and the system's behaviour at the moment it is most stressed is
governed by the memory allocator rather than by the policy you configured.

An unbounded queue (`max_queue_depth = 0`) is the default only for API
compatibility with the original specification. **Real-time systems should set
a cap.** An unbounded queue converts overload into unbounded latency, which is
strictly worse than a fast, visible, countable rejection: the caller can retry,
shed, or alarm, and none of those are possible if the work is silently
accepted into a queue that will never drain.

---

## 4. Task state without per-task locks

At 100k tasks in flight, a mutex per task is meaningful memory and cache
footprint for no benefit. Instead:

**State** is a `std::atomic<TaskState>` mutated through a validated CAS loop.
The transition table is data, not control flow, so "no resurrection from a
terminal state" is impossible to violate by accident, and a task takes each
edge exactly once no matter how many threads race (verified by
`ExactlyOneThreadWinsATransition` with 16 threads × 200 rounds).

**The error message** is a `std::string`, which cannot be atomic. It is made
safe by two mechanisms rather than a lock:

1. `error_claimed_`, an atomic flag CAS'd by the thread that intends to fail
   the task. Exactly one thread wins, so the buffer is single-**writer** by
   construction rather than by convention. (In practice one worker owns a task,
   but "in practice" is not a memory model, and shutdown genuinely can race a
   worker.)
2. Publication through the state transition. The winner writes the string,
   then `transition_to(FAILED)` release-stores the state. `error_message()`
   acquire-loads the state and returns `""` unless it reads `FAILED`. That
   acquire synchronizes-with the release, so a reader that sees `FAILED` is
   guaranteed to see the complete string, and a reader that does not see
   `FAILED` never touches the buffer.

**Timestamps** are `std::atomic<int64_t>` nanosecond counts rather than
`std::atomic<TimePoint>`: guaranteed lock-free on any 64-bit target, with no
dependence on `TimePoint` being trivially copyable.

**The transition trace** is a fixed 4-entry array (the state machine's longest
path), so tracing never allocates. Each entry's timestamp doubles as its
published flag: the writer stores the state byte, then release-stores the
timestamp; a reader walks entries until it finds the `kUnstamped` sentinel and
so can never read a half-written entry.

### Why accessors instead of the specification's public fields

The specification sketched `Task` with public `state`, `error_message` and so
on. Those cannot be public members without inviting a data race — the state
must be atomic to be read while a worker mutates it, and the error buffer
needs the publication barrier above. The accessor form is the same API with
the concurrency made unavoidable rather than optional.

---

## 5. Metrics that do not become the bottleneck

The specification suggested a mutex-protected array of latency samples. That
design fails in three ways at target load: the array grows without bound
(100k samples/second), percentile computation becomes an `O(n log n)` sort
over millions of entries, and — worst — every completing worker serializes on
one lock at precisely the moment it wants to fetch more work. The measurement
apparatus would dominate the thing being measured.

The replacement is an HdrHistogram-style bucketed histogram:

- Values below 128 ns get exact, unit-width buckets.
- Above that, each power-of-two octave is split into 64 equal-width buckets,
  so bucket width is a constant *fraction* of the value. Relative error is
  bounded at 1/128 ≈ 0.8%, and reporting the bucket midpoint halves that to
  ±0.4%.
- `record()` is three relaxed atomic increments plus two CAS loops for
  min/max (contended only while a record is actually being set). No
  allocation, no lock, fixed 20 KB.

`bucket_index` and the bucket geometry are unit-tested for gap-free, non-
overlapping coverage across the entire 45-bit range, and the error bound is
asserted directly in `RelativeErrorIsBoundedEverywhere` — so if someone tunes
`kSubBits` down, the test says exactly what accuracy was given up.

Counters are each padded to their own cache line. With 8 producers calling
`on_submitted()`, sharing a line would turn every increment into a coherence
round trip between cores.

### Three distributions, not one

`queue_latency` (submit → worker claim) is stamped at `SCHEDULED` rather than
`EXECUTING`, so a task shed for an expired deadline still reports the wait it
actually experienced. This is the number that describes the *scheduler*;
end-to-end latency mixes in however long user callbacks take, and under
saturation it is dominated by queue depth. Reporting all three is what makes
the benchmark's burst-vs-paced distinction legible instead of a footnote.

---

## 6. Shutdown

Shutdown is where task schedulers leak and hang, because it races three
things: producers mid-submission, workers mid-execution, and the queue itself.

The sequence in `stop()`:

1. **Close the door** — `lifecycle_ = STOPPING`. New submissions are rejected.
2. **Wait for producers already past the door** — spin on
   `inflight_submitters_`. A producer increments this *before* reading the
   lifecycle, so the pair cannot interleave such that a task slips in behind
   shutdown. Without this step, a producer could publish a task after the
   workers concluded the queue was empty; that task would never run, and
   `outstanding_` would never return to zero, hanging any caller waiting for
   the queue to drain.
3. **Wake everyone** — with `pending_` now stable, a draining worker is
   guaranteed to see every queued task and to terminate.
4. **Join**, then `lifecycle_ = STOPPED`.
5. **Fail whatever is left** — a no-op under `drain_on_stop`, but essential
   without it: every accepted task must reach a terminal state, or a caller
   waiting on it waits forever.

The invariant worth stating explicitly: **every accepted task reaches a
terminal state.** Not "every task runs" — shutdown and overload can legitimately
prevent that — but every task ends up `COMPLETED` or `FAILED`, with a reason.
That is what makes `outstanding_tasks()` and `wait_until_idle()` trustworthy,
and it is asserted in every stress scenario.

`stop()` holds `workers_mutex_` across the join, which is why the pool-size
gauge is a separate atomic: if `get_metrics()` took that mutex, a callback
calling `get_metrics()` during shutdown would deadlock against the join.

---

## 7. Rejected alternatives, briefly

| Alternative | Why not |
|---|---|
| Lock-free priority queue | ~10× the code, ABA + reclamation hazards, wins only where measurements say there is no bottleneck (§1). |
| Dedicated pump thread | Extra thread, extra latency hop, unamortized serialization point. |
| Mutex on every submission | Correct and simple, but the contention the ring exists to remove; the Dekker handshake gets both (§2). |
| Mutex-protected latency array | Unbounded memory, `O(n log n)` percentiles, and a lock on the completion hot path (§5). |
| Per-task mutex | Memory and cache cost at 100k tasks for no benefit over CAS + publication (§4). |
| Evict only terminal tasks from the registry | Eviction is insert-driven, so one long-running task at the head pins the registry at its high-water mark. Measured, then replaced with a strict bound. |
| Interrupt overrunning callbacks | No safe preemption of arbitrary C++; killing a thread risks corrupting shared state. Cooperative cancellation is the right answer. |
| Poll for work instead of sleeping | Burns cores and wrecks the latency of everything else on the machine. |
| Signal `wait_until_idle` from task completion | A notify per completed task, to serve a function only tests and shutdown call. Bounded polling instead; the hot path stays free. |

---

## 8. What to build next

In priority order, with the reason each is deferred:

1. **Cooperative cancellation** — a token the callback polls, making deadline
   enforcement real rather than observational. This is the most valuable gap.
2. **`SCHED_FIFO` and core pinning on Linux** — the largest available tail-
   latency win, and genuinely platform-specific.
3. **Work stealing** — raises the ceiling for very short tasks; costs strict
   global priority ordering, so it should be an opt-in mode rather than a
   replacement.
4. **Priority aging** — the current design cannot starve equal work (the FIFO
   tiebreak guarantees that), but a permanently saturated high-priority stream
   will starve lower bands. That is usually the intended behaviour for a
   priority scheduler; aging should be opt-in for workloads where it is not.
5. **NUMA-aware sharding** — irrelevant on a single-socket box, material on a
   multi-socket server.
