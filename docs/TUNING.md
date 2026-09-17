# Performance Tuning Guide

Every recommendation here is backed by a measurement in
[`benchmark_results.csv`](benchmark_results.csv), reproducible with
`./build/rtsched_benchmark`. Numbers are from an Apple M2 Max (12 cores),
macOS 26.6, Apple clang 17, `RelWithDebInfo`.

---

## First: measure the right thing

Before tuning anything, be clear about which number you are optimizing,
because the two most common ones move in opposite directions.

**Scheduling latency** — `metrics.p99_queue_latency_ms`, submit → worker
claim. This is the scheduler's own overhead. It is only meaningful when the
pool is **not saturated**: if you offer more work than the pool can serve, the
queue grows and latency becomes a report on queue depth, not on the scheduler.

**Throughput** — tasks/second at saturation. Meaningful only *when* saturated.

The same build gives 0.025 ms p99 at 200k tasks/s offered, and 135 ms p99 when
producers submit flat out. Both are correct; they answer different questions.
The benchmark labels every row `paced` or `burst` for this reason.

Diagnostic: if `queue_depth()` is persistently non-zero, you are saturated,
and your latency number is a queue-depth measurement.

---

## `num_workers` — size against callback duration, not core count

This is the setting people get wrong, and the measurement is unambiguous. Two
worker sweeps at identical offered load, differing only in callback cost:

| workers | empty callback | 20 µs of work | scaling efficiency |
|---|---|---|---|
| 1 | 1,812,000 tasks/s | 48,200 tasks/s | — |
| 2 | 1,333,000 tasks/s | 92,600 tasks/s | 96% |
| 4 | 1,309,000 tasks/s | 178,600 tasks/s | 93% |
| 8 | 694,000 tasks/s | 321,800 tasks/s | 84% |
| 16 | 782,000 tasks/s | — | — |

With empty callbacks, adding workers makes throughput **worse** — 3.4× worse
from 1 to 16. There is no work to overlap, so the workers only contend for the
heap lock and the cache lines behind it. With 20 µs of real compute, the same
pool scales at 84% efficiency to 8 threads.

**Rule of thumb.** Let `T` be the mean callback duration:

- `T < 5 µs` — the pool is overhead-bound. Use **1–2 workers**, and consider
  batching several logical operations into one task. Two workers on 20 µs work
  already beat sixteen on empty work by a wide margin.
- `5 µs ≤ T ≤ 1 ms` — the intended regime. Use **cores − 1** workers, leaving
  headroom for producers. The default of 4 is a good starting point.
- `T > 1 ms`, or callbacks that block on I/O — oversubscribe: **2 × cores** or
  more. Blocked workers are not consuming CPU, and an idle worker costs only a
  sleeping thread.

Do not set `num_workers` above the core count for CPU-bound callbacks. The OS
then preempts workers mid-task, which shows up as tail latency, not throughput.

---

## `ingress_capacity` — sized for bursts, not for total volume

The lock-free ring only needs to hold what producers can submit between two
worker drains — not the whole workload. A worker drains the *entire* ring on
every acquisition, so the steady-state occupancy is small.

Default 65,536 entries ≈ 1 MB (a `shared_ptr` per slot).

The metric that tells you it is undersized is `total_slow_path_submissions`:
submissions that found the ring full and fell back to the mutex. Work is never
lost — this is a performance signal only.

| observation | action |
|---|---|
| `slow_path == 0` | Correctly sized, or oversized. Leave it. |
| occasional non-zero under peak bursts | Fine. The fallback did its job. |
| persistently growing | Double `ingress_capacity`. If it keeps growing, the pool is under-provisioned — the ring is a buffer, not a fix for insufficient workers. |

Measured: the `high_contention` scenario (16 producers, 2 workers, a
deliberately tiny 1,024-entry ring) still runs at 1.22M tasks/s. The fallback
path is not a cliff.

Memory is `ingress_capacity × sizeof(shared_ptr)` = 16 bytes per slot,
preallocated at construction. In a memory-constrained target, 4,096 entries
(64 KB) is ample for moderate bursts.

---

## `max_queue_depth` — set it

Default is 0 (unbounded), for API compatibility with the original
specification. **Production real-time systems should set a cap.**

An unbounded queue converts overload into unbounded latency. A bounded queue
converts it into fast, countable rejections your caller can act on — retry,
shed, or alarm. None of those are available if the work is silently accepted
into a queue that will never drain.

Pick it from your latency budget rather than from memory:

```
max_queue_depth ≈ latency_budget / mean_callback_duration × num_workers
```

For a 100 ms budget, 50 µs callbacks and 4 workers: `100ms / 50µs × 4 = 8000`.
A task arriving at a full queue would have missed the budget anyway, so
rejecting it is strictly better than queueing it.

Notes:

- The cap can be overshot by at most **one task per concurrent producer**: the
  depth check and the enqueue are not one atomic step. Asserted in
  `Stress.ExtremeContention` (peak depth 8,202 against a cap of 8,192 with 16
  producers).
- Rejection happens **before** the `Task` is allocated, so overload does not
  become an allocator storm.
- Config validation rejects a cap below `num_workers`, which would starve
  workers permanently.

---

## Deadline policy

| Setting | Default | When to change it |
|---|---|---|
| `fail_expired_before_execution` | `true` | Keep it on for real-time work: it sheds load. Turn it off only when every task must run regardless of lateness (batch/ETL semantics). |
| `enforce_deadline_after_execution` | `true` | Turn it off if overrunning callbacks are expected and the *work* still counts. The miss is still counted in `total_deadline_missed` either way — observability does not depend on enforcement. |

Shedding is what separates degrading from collapsing. Measured in
`Stress.DeadlinePressure`: 40,000 tasks with 10 ms deadlines against 2
workers, 26% shed, whole backlog cleared in 20 ms. Every task that *did* run
is verified to have started inside its deadline. With shedding off, the same
backlog spends workers on tasks whose results are already worthless.

Set deadlines from a real budget. A deadline of `5s` (the default) on work that
matters within 10 ms means the scheduler cannot help you shed — it will run
everything, late.

---

## Observability cost

| Setting | Default | Cost | Notes |
|---|---|---|---|
| `enable_transition_trace` | `true` | ~4 relaxed stores/task | Bounded, allocation-free. Leave on; it is how you diagnose a latency spike after the fact. |
| `history_per_shard` | 1024 | `shared_ptr` + map node per retained task | Total retention is `history_per_shard × registry_shards` (16,384 by default). |
| `registry_shards` | 16 | negligible | Raise to 64+ if many producers submit concurrently; shard contention is per-submission. |
| `trace_sink` | unset | one null check when unset | When set, it runs **on the worker thread**. Keep it non-blocking, and never call back into the scheduler from it. |

Eviction is strictly oldest-first and does not consider state, so a backlog
deeper than the retention window loses *observability* of its oldest in-flight
tasks — they still execute and are still counted. If every queued task must
stay queryable, size total retention above your expected in-flight count
(i.e. above `max_queue_depth`).

---

## Platform-level tuning (beyond this library)

These are outside the library's scope but are where the remaining tail latency
lives. In rough order of impact:

1. **Real-time scheduling class.** On Linux, `SCHED_FIFO` on worker threads
   stops the OS from preempting a worker mid-task. Usually the single largest
   tail-latency win, and it requires `CAP_SYS_NICE`.
   ```cpp
   sched_param param{};
   param.sched_priority = 50;
   pthread_setschedparam(thread, SCHED_FIFO, &param);
   ```
2. **Core pinning.** `pthread_setaffinity_np` per worker, keeping producers off
   worker cores. Removes migration and preserves cache warmth.
3. **Pre-faulting and `mlockall(MCL_CURRENT | MCL_FUTURE)`.** A page fault
   inside a callback is a millisecond-scale outlier that no amount of
   scheduler tuning will fix.
4. **A dedicated allocator.** `submit_task` allocates one `Task`
   (`make_shared`). Under a global-lock allocator this can serialize
   submissions; `tcmalloc`/`jemalloc`, or a pool allocator sized to
   `max_queue_depth`, removes it.
5. **CPU frequency governor.** `performance` rather than `powersave`.
   Frequency ramp-up is visible in cold-start percentiles, which is why the
   benchmark discards a warmup pass.

---

## Diagnostic quick reference

| Symptom | Check | Likely cause |
|---|---|---|
| p99 latency high, throughput at target | `queue_depth()` persistently > 0 | Saturated. Add workers or shed load — this is a capacity problem, not a tuning one. |
| p99 latency high, queue shallow | `p99_execution_latency_ms` | A slow callback is blocking a worker; the scheduler is not the problem. |
| Throughput drops when workers are added | mean callback duration | Overhead-bound. Fewer workers, or batch more work per task. |
| `total_rejected` rising | `max_queue_depth` vs. offered load | Backpressure working as designed. Either provision more, or shed upstream. |
| `total_slow_path_submissions` rising | `ingress_capacity` | Ring undersized for burst shape, or pool under-provisioned. |
| `total_expired_before_start` rising | deadlines vs. capacity | Load shedding is engaged. Verify the deadlines are real budgets, not defaults. |
| `total_exceptions` non-zero | `find_task(id)->error_message()` | Callbacks are throwing. The scheduler contains it; the bug is upstream. |
| `wait_until_idle` times out | `active_workers()` | A callback is blocked or deadlocked. The scheduler cannot interrupt it (see README limitations). |
