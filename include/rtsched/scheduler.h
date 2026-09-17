// Copyright 2026. SPDX-License-Identifier: MIT
//
// PriorityScheduler: a multi-threaded, priority-ordered, deadline-aware task
// scheduler.
//
// ---------------------------------------------------------------------------
// Queue architecture
// ---------------------------------------------------------------------------
//
//   producers                                        workers
//   ---------                                        -------
//   submit_task() --> [ lock-free MPMC ring ] --\
//                          (ingress)             >-- [ binary heap ] --> run
//   submit_task() --> [ mutex (ring-full path) ]-/     (priority)
//
// Submission is lock-free in the common case: a producer CASes a slot in a
// bounded ring and leaves. It never touches the heap mutex, so N producers do
// not serialize against each other or against the workers.
//
// Ordering is provided by a mutex-protected binary heap. A worker that wants
// work takes the heap lock once and does two things under it: drains every
// ring entry into the heap, then pops the best task. Draining in batches under
// a lock the worker had to take anyway makes the ring's FIFO order irrelevant
// and amortizes the lock across many tasks.
//
// Why not a fully lock-free priority queue? A lock-free skip-list or pairing
// heap is roughly an order of magnitude more code and carries real ABA and
// memory-reclamation hazards, in exchange for winning only when many workers
// pop concurrently. At the pool sizes this targets (2-16 workers), the heap
// lock is held for O(log n) pointer work and is not the bottleneck -- the
// submission path was, and that is the part made lock-free. See docs/DESIGN.md.
//
// ---------------------------------------------------------------------------
// Ordering rule
// ---------------------------------------------------------------------------
//
// Tasks are ordered by (priority DESC, deadline ASC, submission sequence ASC):
// higher priority first; among equal priorities the nearest deadline first
// (earliest-deadline-first, the classic real-time discipline); ties broken by
// submission order so equal-priority equal-deadline work is FIFO and cannot
// starve.

#ifndef RTSCHED_SCHEDULER_H_
#define RTSCHED_SCHEDULER_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rtsched/metrics.h"
#include "rtsched/mpmc_queue.h"
#include "rtsched/task.h"

namespace rtsched {

// --- Errors -----------------------------------------------------------------

class SchedulerError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// submit_task() on a scheduler that has been stopped.
class SchedulerNotRunning : public SchedulerError {
 public:
  SchedulerNotRunning() : SchedulerError("rtsched: scheduler is not accepting tasks") {}
};

// submit_task() when max_queue_depth is reached.
class QueueFull : public SchedulerError {
 public:
  QueueFull() : SchedulerError("rtsched: task queue is at capacity") {}
};

// get_task_state() for an id that was never issued or has been evicted from
// the bounded history.
class UnknownTask : public SchedulerError {
 public:
  explicit UnknownTask(std::uint64_t id)
      : SchedulerError("rtsched: unknown or evicted task id " + std::to_string(id)) {}
};

// --- Configuration ----------------------------------------------------------

struct SchedulerConfig {
  // Worker threads. Default 4 per the spec; see docs/TUNING.md for how to
  // pick this against core count and callback duration.
  std::size_t num_workers = 4;

  // Capacity of the lock-free ingress ring, rounded up to a power of two.
  // Sized for burst absorption: it only needs to hold what producers can
  // submit between two worker drains. 64Ki x sizeof(shared_ptr) == 1 MB.
  std::size_t ingress_capacity = std::size_t{1} << 16;

  // Hard backpressure limit on queued (not yet claimed) tasks. 0 == unbounded.
  // Real-time systems should set this: an unbounded queue converts overload
  // into unbounded latency, which is worse than a fast, visible rejection.
  std::size_t max_queue_depth = 0;

  // How many task ids stay queryable per registry shard, evicted strictly
  // oldest-first. Total retention is history_per_shard x registry_shards
  // (default 16 Ki tasks), which bounds what get_task_state() observability
  // costs in memory.
  //
  // Set this above the expected in-flight task count if every queued task
  // must remain queryable: eviction is by submission order, not by state, so
  // a backlog deeper than the retention window loses visibility of its oldest
  // entries (they still execute and are still counted).
  std::size_t history_per_shard = 1024;

  // Registry shards, rounded up to a power of two. More shards == less
  // contention between submitters registering task ids.
  std::size_t registry_shards = 16;

  // stop() semantics: true drains the queue before joining workers; false
  // abandons queued work, failing it with "scheduler stopped".
  bool drain_on_stop = true;

  // If a task's deadline has already passed when a worker claims it, fail it
  // instead of running it. This is the real-time behaviour: work that can no
  // longer be useful should not consume a worker that could serve work that
  // still can.
  bool fail_expired_before_execution = true;

  // If the callback finishes after the deadline, mark the task FAILED. The
  // callback is not interrupted -- C++ has no safe way to preempt arbitrary
  // user code -- so this is detection, not enforcement. See docs/DESIGN.md.
  bool enforce_deadline_after_execution = true;

  // Record the per-task transition trace. Bounded and allocation-free, so the
  // cost is a handful of relaxed stores per task.
  bool enable_transition_trace = true;

  // Optional tracing hook, invoked on every state transition the scheduler
  // performs. Called on the worker thread: keep it fast and non-blocking, and
  // never call back into the scheduler from it. Null == disabled, zero cost.
  std::function<void(const Task&, TaskState /*from*/, TaskState /*to*/)> trace_sink;

  // Throws std::invalid_argument if the configuration is unusable.
  void validate() const;
};

// Non-throwing submission result.
enum class SubmitStatus : std::uint8_t {
  ACCEPTED = 0,
  REJECTED_NOT_RUNNING,   // stopped or stopping
  REJECTED_QUEUE_FULL,    // max_queue_depth reached
  REJECTED_INVALID_TASK,  // bad priority or null callback
};

const char* to_string(SubmitStatus status) noexcept;

struct SubmitResult {
  SubmitStatus status = SubmitStatus::REJECTED_INVALID_TASK;
  std::uint64_t task_id = 0;  // Valid only when status == ACCEPTED.

  bool accepted() const noexcept { return status == SubmitStatus::ACCEPTED; }
  explicit operator bool() const noexcept { return accepted(); }
};

// --- Scheduler --------------------------------------------------------------

class PriorityScheduler {
 public:
  enum class Lifecycle : std::uint8_t {
    CREATED = 0,  // constructed, workers not started; accepts submissions
    RUNNING = 1,
    STOPPING = 2,  // rejecting submissions, workers winding down
    STOPPED = 3,   // workers joined; start() may be called again
  };

  static constexpr std::chrono::milliseconds kDefaultDeadline{5000};

  explicit PriorityScheduler(std::size_t num_workers = 4);
  explicit PriorityScheduler(SchedulerConfig config);

  // Stops and joins. Never throws.
  ~PriorityScheduler();

  PriorityScheduler(const PriorityScheduler&) = delete;
  PriorityScheduler& operator=(const PriorityScheduler&) = delete;

  // --- Lifecycle ------------------------------------------------------------

  // Spawns the worker pool. Idempotent while running. May be called again
  // after stop() to restart with fresh workers; metrics and task ids carry
  // over across restarts.
  void start();

  // Graceful shutdown: stop accepting work, let workers finish (draining the
  // queue first if drain_on_stop), then join. Idempotent.
  //
  // Must not be called from inside a task callback: it joins the worker pool,
  // and a thread cannot join itself.
  void stop();

  bool is_running() const noexcept {
    return lifecycle_.load(std::memory_order_acquire) == Lifecycle::RUNNING;
  }
  Lifecycle lifecycle() const noexcept { return lifecycle_.load(std::memory_order_acquire); }

  // --- Submission -----------------------------------------------------------

  // Submits a task and returns its id. Thread-safe and lock-free unless the
  // ingress ring is full.
  //
  // Throws std::invalid_argument for a priority outside [kMinPriority,
  // kMaxPriority] or a null callback, SchedulerNotRunning after stop(), and
  // QueueFull when max_queue_depth is reached. Use try_submit_task() on paths
  // where rejection is expected and exceptions are too expensive.
  std::uint64_t submit_task(int priority, TaskCallback callback,
                            std::chrono::milliseconds deadline_ms = kDefaultDeadline);

  // Non-throwing form. Preferred in hot loops and for backpressure-aware
  // producers.
  SubmitResult try_submit_task(int priority, TaskCallback callback,
                               std::chrono::milliseconds deadline_ms = kDefaultDeadline) noexcept;

  // --- Introspection --------------------------------------------------------

  // Throws UnknownTask if the id was never issued, or if the task is terminal
  // and has been evicted from the bounded history.
  TaskState get_task_state(std::uint64_t task_id) const;

  // Non-throwing form: std::nullopt for unknown or evicted ids.
  std::optional<TaskState> try_get_task_state(std::uint64_t task_id) const;

  // Shared handle to a task, or nullptr if unknown/evicted. Lets callers read
  // the error message, timings and transition trace.
  std::shared_ptr<const Task> find_task(std::uint64_t task_id) const;

  Metrics get_metrics() const;

  // Clears counters and histograms. Gauges are unaffected. Intended for
  // benchmark harnesses that want per-scenario numbers.
  void reset_metrics() noexcept;

  // Tasks queued and not yet claimed by a worker.
  std::size_t queue_depth() const noexcept {
    return pending_.load(std::memory_order_acquire);
  }
  // Workers currently inside a callback.
  std::size_t active_workers() const noexcept {
    return active_workers_.load(std::memory_order_acquire);
  }
  std::size_t worker_count() const noexcept {
    return worker_count_.load(std::memory_order_acquire);
  }
  // Tasks accepted and not yet in a terminal state.
  std::size_t outstanding_tasks() const noexcept {
    return outstanding_.load(std::memory_order_acquire);
  }

  // Blocks until nothing is queued or executing, or until `timeout` elapses.
  // Returns true if the scheduler went idle.
  //
  // Implemented by polling with a bounded backoff rather than by signalling
  // from task completion: it is a test and shutdown helper, and paying a
  // notify on every completed task to serve it would tax the hot path for no
  // production benefit.
  bool wait_until_idle(std::chrono::milliseconds timeout);

  const SchedulerConfig& config() const noexcept { return config_; }

 private:
  // Heap entry. The ordering key is copied out of the task so the comparator
  // never chases a pointer, which keeps sift-up/down cache-friendly.
  struct QueueEntry {
    int priority;
    TimePoint deadline;
    std::uint64_t sequence;
    std::shared_ptr<Task> task;
  };

  // std::priority_queue exposes the *greatest* element, so "greater" must
  // mean "scheduled sooner": higher priority, then earlier deadline, then
  // lower sequence.
  struct QueueOrder {
    bool operator()(const QueueEntry& a, const QueueEntry& b) const noexcept {
      if (a.priority != b.priority) {
        return a.priority < b.priority;
      }
      if (a.deadline != b.deadline) {
        return a.deadline > b.deadline;
      }
      return a.sequence > b.sequence;
    }
  };

  struct RegistryShard {
    mutable std::mutex mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<Task>> tasks;
    std::deque<std::uint64_t> insertion_order;  // eviction candidates, oldest first
  };

  void spawn_workers(std::size_t count);
  void worker_loop(std::size_t worker_index);

  // Blocks until a task is available or the pool is shutting down. Returns
  // nullptr to tell the worker to exit.
  std::shared_ptr<Task> acquire_next_task();

  // Moves everything from the ingress ring into the heap. Caller holds
  // queue_mutex_.
  void drain_ingress_locked();

  void execute(const std::shared_ptr<Task>& task);

  // Wakes one sleeping worker, skipping the syscall when none are asleep.
  void wake_one_worker() noexcept;
  void wake_all_workers() noexcept;

  void register_task(const std::shared_ptr<Task>& task);
  RegistryShard& shard_for(std::uint64_t task_id) noexcept;
  const RegistryShard& shard_for(std::uint64_t task_id) const noexcept;

  // Fails everything still queued. Used by stop() when drain_on_stop is off.
  void abandon_queued_tasks();

  // Single exit point for accounting: records latencies, bumps the terminal
  // counter and releases the outstanding_ reservation. Exactly one call per
  // accepted task.
  void finalize(const std::shared_ptr<Task>& task, bool completed);
  void emit_trace(const Task& task, TaskState from, TaskState to) const;

  SchedulerConfig config_;

  std::atomic<Lifecycle> lifecycle_{Lifecycle::CREATED};
  std::atomic<std::uint64_t> next_task_id_{1};

  // Ingress: lock-free submission path.
  MpmcBoundedQueue<std::shared_ptr<Task>> ingress_;

  // Egress: priority ordering. queue_mutex_ also guards `pending_`
  // decrements, so that a worker holding the lock sees
  // pending_ == heap_.size() + (undrained ingress entries).
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  // Explicit vector + push_heap/pop_heap rather than std::priority_queue:
  // priority_queue::top() is const, so moving a shared_ptr out of it requires
  // a const_cast, and shutdown needs to walk the container to fail whatever is
  // still queued.
  std::vector<QueueEntry> heap_;

  // Queued-and-unclaimed count. Incremented by the producer *before* the task
  // is published, so it is an upper bound: a pop can never outrun an
  // increment and underflow it, at the cost of transiently counting a task
  // that is not dequeueable yet (see acquire_next_task). Read seq_cst in the
  // wakeup handshake -- see wake_one_worker().
  std::atomic<std::size_t> pending_{0};

  // Workers asleep on queue_cv_. Lets producers skip notify_one() entirely
  // when the pool is saturated.
  std::atomic<std::size_t> idle_workers_{0};

  std::atomic<std::size_t> active_workers_{0};

  // Pool size as a lock-free gauge. get_metrics() must not take
  // workers_mutex_: a callback calling get_metrics() while stop() holds that
  // mutex across join() would deadlock.
  std::atomic<std::size_t> worker_count_{0};

  // Tasks accepted but not yet terminal. Exact (unlike pending_, which can
  // transiently overcount), so it is what wait_until_idle() tests. pending_
  // alone would not do: between a worker popping a task and marking it
  // EXECUTING, the task is in no counter at all.
  std::atomic<std::size_t> outstanding_{0};

  // Producers inside submit_task(). stop() waits for this to drain so that no
  // task can be enqueued after the workers have decided the queue is empty.
  std::atomic<std::size_t> inflight_submitters_{0};

  std::vector<std::thread> workers_;
  mutable std::mutex workers_mutex_;  // guards workers_ across start/stop

  std::vector<RegistryShard> registry_;
  std::size_t registry_mask_;

  MetricsCollector metrics_;
};

}  // namespace rtsched

#endif  // RTSCHED_SCHEDULER_H_
