# rtsched — Real-Time Priority Task Scheduler

A multi-threaded, deadline-aware priority task scheduler in C++17, built for
low-latency workloads where **task ordering and reliability are correctness
requirements, not optimizations**.

Sustained **200,000 tasks/second at a 25 µs p99 scheduling latency**, and
**1.2M tasks/second** in burst throughput, on an Apple M2 Max (12 cores).

```
                    producers                                    workers
                    ---------                                    -------
   submit_task() ──▶ ┌──────────────────────────┐
   submit_task() ──▶ │ lock-free MPMC ring      │──┐
   submit_task() ──▶ └──────────────────────────┘  │   ┌─────────────┐
                                                   ├──▶│ binary heap │──▶ execute
   submit_task() ──▶ ┌──────────────────────────┐  │   │ (priority)  │
                     │ mutex path (ring full)   │──┘   └─────────────┘
                     └──────────────────────────┘
                     │                              │
                     └── lock-free, no ordering ────┴── ordered, amortized lock
```

Submission is lock-free; ordering is provided by a mutex-protected heap that
workers drain in batches under a lock they had to take anyway. Producers never
pay for ordering, and the ordering cost is amortized across every task that
arrived since the last drain.

---

## Contents

- [Quick start](#quick-start)
- [Performance](#performance)
- [API reference](#api-reference)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Testing](#testing)
- [Known limitations](#known-limitations)
- [Further reading](#further-reading)

---

## Quick start

Requirements: a C++17 compiler, CMake ≥ 3.16. GoogleTest is used for the tests
and is found automatically or fetched at configure time.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j

./build/rtsched_unit_tests      # 113 unit tests, ~3 s
./build/rtsched_stress_tests    # 7 load scenarios, ~3 s
./build/rtsched_benchmark       # writes benchmark_results.csv
./build/rtsched_example         # annotated API tour

ctest --test-dir build -L unit  # or -L stress
```

### Minimal usage

```cpp
#include "rtsched/scheduler.h"

rtsched::PriorityScheduler scheduler(4);   // 4 worker threads
scheduler.start();

const std::uint64_t id = scheduler.submit_task(
    10,                                    // priority: 0 (lowest) .. 10 (highest)
    [](const rtsched::Task& task) {
      return do_work(task.id());           // true == success
    },
    std::chrono::milliseconds(500));       // deadline

scheduler.wait_until_idle(std::chrono::seconds(5));

if (scheduler.get_task_state(id) == rtsched::TaskState::FAILED) {
  std::cerr << scheduler.find_task(id)->error_message() << '\n';
}

const rtsched::Metrics m = scheduler.get_metrics();
std::cout << "p99 scheduling latency: " << m.p99_queue_latency_ms << " ms\n";

scheduler.stop();                          // graceful: drains, then joins
```

`examples/basic_usage.cpp` is a runnable version covering priorities,
exceptions, deadline misses, transition traces and metrics.

---

## Performance

Apple M2 Max (12 cores), macOS 26.6, Apple clang 17, `RelWithDebInfo`.
Full results: [`docs/benchmark_results.csv`](docs/benchmark_results.csv).
Reproduce with `./build/rtsched_benchmark`.

### Scheduling latency under sustained load

Offered load is held **below** capacity so the queue stays shallow. This is
the only configuration in which a latency percentile describes the scheduler
rather than the depth of the backlog.

| offered load | achieved | p50 | p99 | target |
|---|---|---|---|---|
| 50,000 tasks/s | 50,000 | 0.006 ms | **0.015 ms** | < 1 ms |
| 100,000 tasks/s | 99,998 | 0.006 ms | **0.016 ms** | < 1 ms |
| 200,000 tasks/s | 199,994 | 0.006 ms | **0.025 ms** | < 1 ms |

The p99 is ~40× under the 1 ms budget at twice the target throughput.

### Burst throughput

Producers submit as fast as they can; the pool is saturated throughout.

| scenario | workers | producers | throughput |
|---|---|---|---|
| `minimal_overhead` | 1 | 1 | 3,124,000 tasks/s |
| `single_priority` | 4 | 4 | 1,364,000 tasks/s |
| `mixed_priority` (11 bands) | 4 | 4 | 1,305,000 tasks/s |
| `high_contention` | 2 | 16 | 1,221,000 tasks/s |

**Read burst latencies as queueing delay, not scheduler overhead.** A task
submitted into a 100k-deep backlog waits for that backlog — the p99 of ~135 ms
in `mixed_priority` is 200k tasks draining through 4 workers, and no
scheduler can shorten it. The two families of numbers answer two different
questions, and the benchmark labels every row `burst` or `paced` so they are
never confused.

### What scaling actually looks like

Two worker sweeps at identical offered load, differing only in callback cost:

| workers | empty callback | 20 µs of work | speedup |
|---|---|---|---|
| 1 | 1,812,000 tasks/s | 48,200 tasks/s | 1.00× |
| 2 | 1,333,000 tasks/s | 92,600 tasks/s | 1.92× |
| 4 | 1,309,000 tasks/s | 178,600 tasks/s | 3.71× |
| 8 | 694,000 tasks/s | 321,800 tasks/s | 6.68× |

With empty callbacks, **more workers make throughput worse**: there is no work
to overlap, so the workers only contend for the heap lock. With 20 µs of real
compute the same pool scales at 84% efficiency to 8 threads.

This is the honest shape of the result, and it is the number that should drive
configuration: size the pool against callback duration, not against core
count. See [`docs/TUNING.md`](docs/TUNING.md).

### Overload behaviour

`deadline_pressure` submits 40,000 tasks with 10 ms deadlines to a 2-worker
pool. 26% are shed before execution — the scheduler detects that their
deadlines are unreachable and drops them rather than spending a worker on work
that is already useless. Every remaining task is verified to start *inside*
its deadline. Under a hard depth cap (`extreme_contention`: 16 producers, 2
workers, cap 8192), 11,428 tasks are accepted and 84,572 are rejected
immediately; nothing is lost, nothing is double-executed, and the queue never
exceeds its cap by more than one task per concurrent producer.

---

## API reference

### `PriorityScheduler`

| Member | Description |
|---|---|
| `PriorityScheduler(size_t num_workers = 4)` | Construct with a worker count. |
| `PriorityScheduler(SchedulerConfig)` | Construct with full configuration. Throws `std::invalid_argument` on an unusable config. |
| `void start()` | Spawn the pool. Idempotent; may be called again after `stop()` to restart. |
| `void stop()` | Graceful shutdown: reject new work, drain the queue (configurable), join. Idempotent. Must not be called from a callback. |
| `bool is_running() const` | True while accepting and executing work. |
| `Lifecycle lifecycle() const` | `CREATED`, `RUNNING`, `STOPPING` or `STOPPED`. |
| `uint64_t submit_task(int priority, TaskCallback, milliseconds deadline = 5000ms)` | Submit and return the task id. Throws `std::invalid_argument`, `SchedulerNotRunning` or `QueueFull`. |
| `SubmitResult try_submit_task(...)` | Non-throwing submission. Preferred in hot loops and for backpressure-aware producers. |
| `TaskState get_task_state(uint64_t) const` | Current state. Throws `UnknownTask` if the id is unknown or evicted. |
| `optional<TaskState> try_get_task_state(uint64_t) const` | Non-throwing form. |
| `shared_ptr<const Task> find_task(uint64_t) const` | Full task handle: error message, timings, transition trace. |
| `Metrics get_metrics() const` | Counters and latency percentiles. |
| `void reset_metrics()` | Clear counters and histograms; gauges are unaffected. |
| `size_t queue_depth() const` | Tasks queued and unclaimed. |
| `size_t active_workers() const` | Workers inside a callback. |
| `size_t outstanding_tasks() const` | Accepted but not yet terminal. |
| `bool wait_until_idle(milliseconds)` | Block until nothing is queued or executing. Returns false on timeout. |

Tasks may be submitted **before** `start()`; they queue and run when the pool
comes up. This is what makes ordering assertions in the tests deterministic.

### `Task`

Accessors rather than the public fields of the original specification sketch:
the state must be atomic and the error buffer needs a publication barrier, so
neither can be a bare public member without inviting a data race.
`task.id()`, `task.priority()`, `task.state()`, `task.deadline()`,
`task.error_message()`, `task.queue_latency()`, `task.trace(...)`.

State machine — every transition is validated and applied with a CAS, so a
task takes each edge exactly once no matter how many threads race:

```
PENDING ──▶ SCHEDULED ──▶ EXECUTING ──▶ COMPLETED
   │            │             │
   └────────────┴─────────────┴────────▶ FAILED
```

### `SchedulerConfig`

| Field | Default | Purpose |
|---|---|---|
| `num_workers` | 4 | Worker threads. |
| `ingress_capacity` | 65536 | Lock-free ring size (rounded to a power of two). Burst absorption. |
| `max_queue_depth` | 0 (unbounded) | Backpressure limit. **Set this in production.** |
| `history_per_shard` | 1024 | Queryable task ids per shard; total retention is `history_per_shard × registry_shards`. |
| `registry_shards` | 16 | Registry sharding, to spread submitter contention. |
| `drain_on_stop` | `true` | Finish queued work on shutdown vs. abandon it. |
| `fail_expired_before_execution` | `true` | Shed work whose deadline has already passed. |
| `enforce_deadline_after_execution` | `true` | Fail a task whose callback overran its deadline. |
| `enable_transition_trace` | `true` | Per-task transition timeline (bounded, allocation-free). |
| `trace_sink` | none | Optional per-transition callback for external tracing. |

### `Metrics`

Exact counters: `total_submitted`, `rejected`, `scheduled`, `executed`,
`completed`, `failed`, plus a failure breakdown
(`total_expired_before_start`, `total_deadline_missed`, `total_exceptions`)
and `total_slow_path_submissions` (ring-full fallbacks — a persistently
non-zero value means the ring is undersized).

Three separate latency distributions, each with p50/p95/p99/p99.9:

- **scheduling** (`*_queue_latency_ms`) — submit → worker claim. The
  scheduler's own contribution; this is the number to quote.
- **execution** (`*_execution_latency_ms`) — time inside the callback.
- **end-to-end** (`*_latency_ms`) — submit → terminal state.

A snapshot is not a linearizable instant: each counter is exact and monotonic,
and percentiles within one snapshot are mutually consistent, but identities
across *different* counters hold only at quiescence. Making the whole snapshot
atomic would mean locking the hot path on every completion.

---

## Architecture

### Two-stage queue

The submission path and the ordering path have opposite requirements.
Submission wants no shared mutable state; ordering is inherently a shared
total order. Splitting them lets each get what it needs:

1. **Ingress** — a bounded lock-free MPMC ring ([Vyukov's
   algorithm](include/rtsched/mpmc_queue.h)). A producer claims a slot with
   one CAS on a cursor and publishes with one release-store. No mutex, no
   allocation, no blocking. Preallocated, so overload cannot grow memory.
2. **Egress** — a mutex-protected binary heap ordered by
   `(priority DESC, deadline ASC, sequence ASC)`. A worker takes the lock
   once and does two things under it: drains *everything* from the ring into
   the heap, then pops the best task.

Batch draining is what makes the combination work. The ring's FIFO order
becomes irrelevant, the lock is amortized across every task that arrived
since the last drain, and a worker that was going to take the lock anyway
absorbs the ordering cost.

If the ring is full, submission falls back to pushing directly onto the heap
under the lock. Slower, but it keeps submission total: ring capacity is a
performance knob, never a correctness one.

### Ordering rule

`(priority DESC, deadline ASC, submission sequence ASC)` — higher priority
first; among equal priorities the nearest deadline first (earliest-deadline-
first, the classic real-time discipline); ties broken by submission order so
equal work is FIFO and cannot starve.

A useful consequence: equal-priority tasks with equal relative deadlines have
absolute deadlines ordered by submission time, so EDF degenerates to FIFO
exactly as the specification requires — without a special case.

### Worker wakeup

Workers sleep on a condition variable rather than polling. The subtle part is
not losing a wakeup while also not paying for one on every submission. The
scheduler uses a Dekker-style handshake:

```
producer:  store pending_ (seq_cst) ; load idle_workers_ (seq_cst)
worker:    store idle_workers_ (seq_cst) ; load pending_ (seq_cst)
```

Under sequential consistency these cannot both read stale values. So if a
producer sees no idle worker, every worker is guaranteed to observe the new
`pending_` before it sleeps — and the producer can skip the notify entirely.
When the pool is saturated (the steady state under load) submission never
touches the queue mutex at all. When a worker *is* asleep, the producer takes
the mutex for zero work before notifying, which is the only correct way to
close the window between a failed predicate check and the actual sleep.

### Deadline enforcement

Two enforcement points, because they answer different questions:

- **Before execution** — if the deadline has already passed when a worker
  claims the task, it is failed without running. This is load shedding, and it
  is the difference between degrading and collapsing: a worker spent on work
  that is already useless is a worker denied to work that still is.
- **After execution** — if the callback finishes past its deadline, the task
  is failed. This is *detection*, not interruption; see
  [limitations](#known-limitations).

---

## Design decisions

Longer form in [`docs/DESIGN.md`](docs/DESIGN.md).

**Why not a fully lock-free priority queue?** A lock-free skip-list or
pairing heap is roughly an order of magnitude more code and carries real ABA
and memory-reclamation hazards, in exchange for winning only when many
workers pop concurrently. At 2–16 workers the heap lock is held for
`O(log n)` pointer work and is not the bottleneck — the *submission* path was,
and that is the part made lock-free. Measurement backs this up: throughput is
flat from 1 to 4 producers, so the ingress path is not the limit.

**Why a bucketed histogram instead of a latency array?** The original sketch
suggested a mutex-protected array of samples. At 100k tasks/second that array
grows without bound and every completing worker serializes on the same lock at
the exact moment it wants more work. The
[HdrHistogram-style histogram](include/rtsched/metrics.h) instead does three
relaxed atomic increments, never allocates, occupies a fixed 20 KB, and bounds
its percentile error at ±0.4% — far tighter than run-to-run variance.

**Why no per-task mutex?** At 100k tasks in flight, a mutex per task is real
memory and cache footprint for no benefit. State transitions use a CAS on an
atomic; the error message is made single-writer by an atomic claim flag and
published through the release-store of the `FAILED` state, so a reader that
observes `FAILED` is guaranteed to observe the message.

**Why strict FIFO registry eviction?** Evicting only *terminal* tasks was
tried and is worse: eviction happens on insert, so one long-running task at
the head blocks the queue and the registry stays at its high-water mark long
after the burst. A strict bound is `O(1)` and predictable, and keeps exactly
the tasks a caller is most likely to ask about. Eviction drops only the
scheduler's reference — the queue and the running worker hold their own, so an
evicted task still executes and is still counted.

**Why is submission allowed before `start()`?** It makes staged queues
possible, which makes ordering deterministically testable. A scheduler whose
ordering can only be tested statistically is a scheduler whose ordering is not
really tested.

---

## Testing

120 tests across two binaries.

**113 unit tests** (`tests/`) — state machine including every invalid
transition, exactly-one-winner under 16 racing threads, queue delivery
exactly-once under 4×4 producers/consumers, histogram bucket geometry and
error bounds, priority/EDF/FIFO ordering, exception containment, deadline
shedding, backpressure, lifecycle and restart, registry eviction, and
concurrent read-API safety.

**7 stress scenarios** (`tests/scheduler_stress_test.cpp`) — the four from the
specification (100k tasks / 8 producers, mixed priority, deadline pressure,
16-producer contention) plus an unbounded-queue variant, a false-positive
check, and a 3-second sustained mixed load with fault injection.

Tests that matter most and why:

- *Every* concurrency test accounts for tasks **individually**, not by count.
  A count check would pass if a drop were paired with a double-execution.
- `AThrowingTaskDoesNotKillItsWorker` runs 500 throwing tasks through a
  *single* worker. If an exception escaped, the pool would silently lose a
  quarter of its capacity — the kind of failure that looks like a performance
  problem for weeks.
- `WorkersSleepAndWakeReliably` performs 3,000 sleep/wake cycles with a hard
  timeout on each. A lost wakeup is invisible in a smoke test and fatal in
  production; this is what proves the handshake above.
- `ConcurrentSubmitAndStopLosesNothing` races 8 producers against `stop()`
  for 30 rounds, asserting that every accepted task reaches a terminal state
  and every rejected one never ran.

### Sanitizers

The build supports `-DRTSCHED_ENABLE_TSAN=ON`, `-DRTSCHED_ENABLE_ASAN=ON` and
`-DRTSCHED_ENABLE_UBSAN=ON`, wired into CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml))
for Linux GCC and Clang.

**These were not validated on the development machine.** Its sanitizer
runtimes segfault at process startup — reproducible with a five-line program
using no part of this library — because the OS (macOS 26.6) is far newer than
the installed Command Line Tools SDK (15.4). Installing a matching Xcode /
CLT, or running the Linux CI job, is what makes the TSan claim verifiable. It
is asserted here as untested rather than quietly implied.

---

## Known limitations

Each of these is a deliberate scope boundary, not an oversight.

1. **A deadline overrun is detected, not interrupted.** C++ offers no safe way
   to preempt arbitrary user code — no cancellation points, no guarantee the
   callback leaves shared state consistent. Killing the thread would risk
   corrupting the process. Cooperative cancellation (pass the callback a token
   it polls) is the correct fix and belongs in the callback contract.
2. **Latency percentiles carry ±0.4% bucket error.** Counts, min, max and mean
   are exact. Documented in `metrics.h` and tested in `metrics_test.cpp`.
3. **No work stealing.** All workers share one heap, which is the serialization
   point visible in the empty-callback sweep. Per-worker deques with stealing
   would raise the ceiling for very short tasks, at the cost of strict global
   priority ordering — which for this use case is the wrong trade.
4. **No OS real-time priority integration.** Worker threads run at default
   scheduling priority. On Linux, `SCHED_FIFO` plus core pinning would cut
   tail latency further; that is platform-specific and intentionally out of
   scope here.
5. **`stop()` must not be called from a callback.** It joins the pool, and a
   thread cannot join itself.
6. **A registry deeper than its retention window loses observability of its
   oldest in-flight tasks.** They still execute and are still counted. Size
   `history_per_shard × registry_shards` above the expected in-flight count.

---

## Further reading

- [`docs/DESIGN.md`](docs/DESIGN.md) — design rationale, memory-ordering
  arguments, and alternatives that were rejected.
- [`docs/TUNING.md`](docs/TUNING.md) — how to size workers, the ring and the
  depth cap, with the measurements behind each recommendation.
- [`docs/benchmark_results.csv`](docs/benchmark_results.csv) — full results.

### Layout

```
include/rtsched/    task.h  mpmc_queue.h  metrics.h  scheduler.h
src/                task.cpp  metrics.cpp  scheduler.cpp
tests/              task_test  mpmc_queue_test  metrics_test
                    scheduler_test  scheduler_stress_test
benchmarks/         scheduler_benchmark.cpp
examples/           basic_usage.cpp
docs/               DESIGN.md  TUNING.md  benchmark_results.csv
```

MIT licensed.
