// Copyright 2026. SPDX-License-Identifier: MIT

#include "rtsched/scheduler.h"

#include <algorithm>
#include <utility>

namespace rtsched {
namespace {

// How long a worker waits when pending_ says work exists but nothing is
// dequeueable yet. That only happens while a producer sits between "reserved
// a ring slot" and "published the pointer" -- a window of nanoseconds. The
// wait is a bounded backoff, not a timeout: the producer's notify ends it
// immediately.
constexpr std::chrono::microseconds kPublishRaceBackoff{50};

// wait_until_idle() polling bounds.
constexpr std::chrono::microseconds kIdlePollMin{20};
constexpr std::chrono::microseconds kIdlePollMax{500};

// Decrements an atomic gauge on scope exit, so a throwing metrics call or an
// exotic callback exception cannot leak the count.
//
// `order` is seq_cst for the gauge that participates in the shutdown
// handshake (see try_submit_task) and acq_rel for plain gauges.
class ScopedGauge {
 public:
  explicit ScopedGauge(std::atomic<std::size_t>& gauge,
                       std::memory_order order = std::memory_order_acq_rel)
      : gauge_(gauge), order_(order) {
    gauge_.fetch_add(1, order_);
  }
  ~ScopedGauge() { gauge_.fetch_sub(1, order_); }

  ScopedGauge(const ScopedGauge&) = delete;
  ScopedGauge& operator=(const ScopedGauge&) = delete;

 private:
  std::atomic<std::size_t>& gauge_;
  std::memory_order order_;
};

std::size_t round_up_pow2_at_least(std::size_t value, std::size_t minimum) {
  std::size_t p = minimum;
  while (p < value) {
    p <<= 1;
  }
  return p;
}

// Validates in the member-initializer list, so a bad configuration throws
// before any member has been built from it.
SchedulerConfig validated(SchedulerConfig config) {
  config.validate();
  return config;
}

}  // namespace

// --- Configuration ----------------------------------------------------------

void SchedulerConfig::validate() const {
  if (num_workers == 0) {
    throw std::invalid_argument("rtsched: num_workers must be >= 1");
  }
  if (ingress_capacity < 2) {
    throw std::invalid_argument("rtsched: ingress_capacity must be >= 2");
  }
  if (registry_shards == 0) {
    throw std::invalid_argument("rtsched: registry_shards must be >= 1");
  }
  if (max_queue_depth != 0 && max_queue_depth < num_workers) {
    // A depth cap below the worker count would stall workers that have no
    // task and cannot be given one.
    throw std::invalid_argument("rtsched: max_queue_depth must be 0 or >= num_workers");
  }
}

const char* to_string(SubmitStatus status) noexcept {
  switch (status) {
    case SubmitStatus::ACCEPTED:
      return "ACCEPTED";
    case SubmitStatus::REJECTED_NOT_RUNNING:
      return "REJECTED_NOT_RUNNING";
    case SubmitStatus::REJECTED_QUEUE_FULL:
      return "REJECTED_QUEUE_FULL";
    case SubmitStatus::REJECTED_INVALID_TASK:
      return "REJECTED_INVALID_TASK";
  }
  return "UNKNOWN";
}

// --- Construction / destruction ---------------------------------------------

PriorityScheduler::PriorityScheduler(std::size_t num_workers)
    : PriorityScheduler([num_workers] {
        SchedulerConfig config;
        config.num_workers = num_workers;
        return config;
      }()) {}

PriorityScheduler::PriorityScheduler(SchedulerConfig config)
    : config_(validated(std::move(config))),
      ingress_(config_.ingress_capacity),
      registry_(round_up_pow2_at_least(config_.registry_shards, 1)),
      registry_mask_(registry_.size() - 1) {
  // Reserve so that drain_ingress_locked() does not reallocate while holding
  // the queue lock in the common case.
  heap_.reserve(std::min<std::size_t>(config_.ingress_capacity, std::size_t{1} << 12));
}

PriorityScheduler::~PriorityScheduler() {
  // Destruction must not throw, and stop() is the only thing here that could.
  try {
    stop();
  } catch (...) {
  }
}

// --- Lifecycle --------------------------------------------------------------

void PriorityScheduler::start() {
  std::lock_guard<std::mutex> lock(workers_mutex_);
  const Lifecycle current = lifecycle_.load(std::memory_order_acquire);
  if (current == Lifecycle::RUNNING) {
    return;  // Idempotent.
  }
  if (current == Lifecycle::STOPPING) {
    throw SchedulerError("rtsched: cannot start() while a stop() is in progress");
  }
  // Either the first start, or a restart after stop(). Workers from a
  // previous run were joined by stop(), so the vector is empty.
  workers_.clear();
  lifecycle_.store(Lifecycle::RUNNING, std::memory_order_release);
  spawn_workers(config_.num_workers);
}

void PriorityScheduler::spawn_workers(std::size_t count) {
  workers_.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    workers_.emplace_back([this, i] { worker_loop(i); });
  }
  worker_count_.store(workers_.size(), std::memory_order_release);
}

void PriorityScheduler::stop() {
  // Held across the join so the lifecycle cannot be observed half-changed.
  // Safe because nothing a worker calls acquires this mutex -- the pool-size
  // gauge is atomic precisely so that get_metrics() stays lock-free here.
  // Consequence: stop() must not be called from a task callback, which would
  // be a thread joining itself.
  std::lock_guard<std::mutex> lock(workers_mutex_);

  const Lifecycle current = lifecycle_.load(std::memory_order_acquire);
  if (current == Lifecycle::STOPPED) {
    return;  // Idempotent.
  }
  // Close the door first, so new submissions are rejected. seq_cst, not
  // release: this store is one half of the handshake described in
  // try_submit_task, and the argument depends on the total order.
  lifecycle_.store(Lifecycle::STOPPING, std::memory_order_seq_cst);

  // ...then wait for producers that were already past it. A producer
  // announces itself in inflight_submitters_ *before* reading the lifecycle,
  // so once this reads zero, no task can still be on its way in. Without the
  // barrier a producer could publish a task after the queue was declared
  // empty: it would never run, and outstanding_ would never return to zero,
  // hanging anything waiting for the queue to drain.
  while (inflight_submitters_.load(std::memory_order_seq_cst) != 0) {
    std::this_thread::yield();
  }

  if (current == Lifecycle::CREATED) {
    // Never started, so there are no workers to drain the queue and nothing
    // to join. Whatever was staged has to be failed rather than silently
    // lost. The barrier above applies here too -- a stop() racing a submit on
    // a never-started scheduler is the same hazard.
    lifecycle_.store(Lifecycle::STOPPED, std::memory_order_release);
    abandon_queued_tasks();
    return;
  }

  // pending_ is stable now, so a draining worker is guaranteed to see every
  // queued task and to terminate.
  wake_all_workers();

  for (std::thread& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  worker_count_.store(0, std::memory_order_release);
  lifecycle_.store(Lifecycle::STOPPED, std::memory_order_release);

  // With drain_on_stop the queue is already empty. Without it, whatever is
  // left must still be failed, so that every accepted task reaches a terminal
  // state and no caller waits forever on work that will never run.
  abandon_queued_tasks();
}

// --- Submission -------------------------------------------------------------

std::uint64_t PriorityScheduler::submit_task(int priority, TaskCallback callback,
                                             std::chrono::milliseconds deadline_ms) {
  // Validate before the atomics so the error is cheap and precise.
  if (priority < kMinPriority || priority > kMaxPriority) {
    throw std::invalid_argument("rtsched: priority must be within [" +
                                std::to_string(kMinPriority) + ", " +
                                std::to_string(kMaxPriority) + "]");
  }
  if (!callback) {
    throw std::invalid_argument("rtsched: task callback must not be empty");
  }

  const SubmitResult result = try_submit_task(priority, std::move(callback), deadline_ms);
  switch (result.status) {
    case SubmitStatus::ACCEPTED:
      return result.task_id;
    case SubmitStatus::REJECTED_NOT_RUNNING:
      throw SchedulerNotRunning();
    case SubmitStatus::REJECTED_QUEUE_FULL:
      throw QueueFull();
    case SubmitStatus::REJECTED_INVALID_TASK:
      break;
  }
  throw std::invalid_argument("rtsched: invalid task");
}

SubmitResult PriorityScheduler::try_submit_task(int priority, TaskCallback callback,
                                                std::chrono::milliseconds deadline_ms) noexcept {
  SubmitResult result;
  if (priority < kMinPriority || priority > kMaxPriority || !callback) {
    result.status = SubmitStatus::REJECTED_INVALID_TASK;
    return result;
  }

  // Announce ourselves before reading the lifecycle. This is the same
  // Dekker-style handshake as wake_one_worker(), mirrored:
  //
  //   producer: store inflight_submitters_ (seq_cst); load lifecycle_ (seq_cst)
  //   stop():   store lifecycle_ (seq_cst);           load inflight_submitters_ (seq_cst)
  //
  // Sequential consistency on all four is what makes it impossible for both
  // sides to read a stale value -- so either this producer sees the closed
  // door and rejects, or stop() sees this producer and waits for it. With
  // acquire/release instead of seq_cst there is no single total order and both
  // can miss each other, which leaks a task that is then never run and never
  // failed. That leak is real and is covered by
  // SchedulerLifecycle.StopRacingSubmitOnANeverStartedSchedulerLosesNothing.
  ScopedGauge submitter(inflight_submitters_, std::memory_order_seq_cst);

  const Lifecycle state = lifecycle_.load(std::memory_order_seq_cst);
  if (state != Lifecycle::RUNNING && state != Lifecycle::CREATED) {
    result.status = SubmitStatus::REJECTED_NOT_RUNNING;
    return result;
  }

  // Backpressure is checked before anything is allocated: under overload the
  // rejection path must not allocate, or overload becomes an allocator storm.
  if (config_.max_queue_depth != 0 &&
      pending_.load(std::memory_order_acquire) >= config_.max_queue_depth) {
    metrics_.on_rejected();
    result.status = SubmitStatus::REJECTED_QUEUE_FULL;
    return result;
  }

  const TimePoint now = Clock::now();
  const std::uint64_t id = next_task_id_.fetch_add(1, std::memory_order_relaxed);

  std::shared_ptr<Task> task;
  try {
    task = std::make_shared<Task>(id, priority, std::move(callback), now, now + deadline_ms,
                                  config_.enable_transition_trace);
    register_task(task);
  } catch (...) {
    // Allocation failure is the only realistic cause. Report it as a
    // rejection rather than propagating from a noexcept function.
    metrics_.on_rejected();
    result.status = SubmitStatus::REJECTED_QUEUE_FULL;
    return result;
  }

  metrics_.on_submitted();
  outstanding_.fetch_add(1, std::memory_order_acq_rel);
  // Reserve the queue slot before publishing the task. See the pending_
  // comment in scheduler.h for why the order matters.
  pending_.fetch_add(1, std::memory_order_seq_cst);

  // Fast path: lock-free ring. No mutex, no contention with the workers.
  std::shared_ptr<Task> to_enqueue = task;
  if (!ingress_.try_enqueue(std::move(to_enqueue))) {
    // Ring full: producers are outrunning the drain rate. Rather than
    // rejecting work the depth cap allows, fall back to pushing straight onto
    // the heap. Slower (it takes the queue lock), but it keeps submission
    // total and makes ring capacity a performance knob instead of a
    // correctness one.
    metrics_.on_slow_path_submission();
    std::lock_guard<std::mutex> lock(queue_mutex_);
    heap_.push_back(QueueEntry{priority, task->deadline(), id, task});
    std::push_heap(heap_.begin(), heap_.end(), QueueOrder{});
  }

  wake_one_worker();

  result.status = SubmitStatus::ACCEPTED;
  result.task_id = id;
  return result;
}

// --- Worker pool ------------------------------------------------------------

void PriorityScheduler::worker_loop(std::size_t /*worker_index*/) {
  for (;;) {
    std::shared_ptr<Task> task = acquire_next_task();
    if (!task) {
      return;  // Shutting down with nothing left to do.
    }
    execute(task);
  }
}

std::shared_ptr<Task> PriorityScheduler::acquire_next_task() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  for (;;) {
    const bool stopping = lifecycle_.load(std::memory_order_acquire) != Lifecycle::RUNNING;
    if (stopping && !config_.drain_on_stop) {
      // Checked before the heap is consulted: with draining disabled, queued
      // work is abandoned rather than finished.
      return nullptr;
    }

    // Batch-move the ring into the heap under a lock we already hold. This is
    // what makes lock-free submission compatible with strict priority
    // ordering: producers never pay for ordering, and the cost of ordering is
    // amortized across however many tasks arrived since the last drain.
    drain_ingress_locked();

    if (!heap_.empty()) {
      std::pop_heap(heap_.begin(), heap_.end(), QueueOrder{});
      std::shared_ptr<Task> task = std::move(heap_.back().task);
      heap_.pop_back();
      pending_.fetch_sub(1, std::memory_order_seq_cst);
      return task;
    }

    if (pending_.load(std::memory_order_seq_cst) != 0) {
      // A producer has reserved a slot but not published it yet; bounded
      // backoff, cut short by its notify.
      queue_cv_.wait_for(lock, kPublishRaceBackoff);
      continue;
    }
    if (stopping) {
      return nullptr;
    }

    // Publish that we are about to sleep, then re-test. Both stores are
    // seq_cst and pair with the producer's seq_cst load in wake_one_worker().
    idle_workers_.fetch_add(1, std::memory_order_seq_cst);
    queue_cv_.wait(lock, [this] {
      return pending_.load(std::memory_order_seq_cst) != 0 ||
             lifecycle_.load(std::memory_order_acquire) != Lifecycle::RUNNING;
    });
    idle_workers_.fetch_sub(1, std::memory_order_seq_cst);
  }
}

void PriorityScheduler::drain_ingress_locked() {
  std::shared_ptr<Task> task;
  while (ingress_.try_dequeue(task)) {
    const int priority = task->priority();
    const TimePoint deadline = task->deadline();
    const std::uint64_t sequence = task->id();
    heap_.push_back(QueueEntry{priority, deadline, sequence, std::move(task)});
    std::push_heap(heap_.begin(), heap_.end(), QueueOrder{});
  }
}

void PriorityScheduler::wake_one_worker() noexcept {
  // Dekker-style handshake, and the reason submission is lock-free whenever
  // the pool is busy:
  //
  //   producer: store pending_ (seq_cst); load idle_workers_ (seq_cst)
  //   worker:   store idle_workers_ (seq_cst); load pending_ (seq_cst)
  //
  // Under sequential consistency the two cannot both read stale values, so if
  // the producer sees no idle worker, every worker is guaranteed to observe
  // the new pending_ before it sleeps. No worker can therefore sleep on work
  // that nobody announced.
  if (idle_workers_.load(std::memory_order_seq_cst) == 0) {
    return;
  }
  // A worker may be between "predicate returned false" and "asleep on the
  // condition variable". Taking the queue lock for zero work serializes
  // against exactly that window, which is what makes the notify below
  // impossible to lose. condition_variable requires this; an atomic flag
  // cannot substitute for it.
  { std::lock_guard<std::mutex> lock(queue_mutex_); }
  queue_cv_.notify_one();
}

void PriorityScheduler::wake_all_workers() noexcept {
  { std::lock_guard<std::mutex> lock(queue_mutex_); }
  queue_cv_.notify_all();
}

// --- Execution --------------------------------------------------------------

void PriorityScheduler::execute(const std::shared_ptr<Task>& task) {
  if (!task->transition_to(TaskState::SCHEDULED)) {
    // Already terminal: shutdown failed it between the pop and here, and has
    // accounted for it. Nothing to do.
    return;
  }
  emit_trace(*task, TaskState::PENDING, TaskState::SCHEDULED);
  metrics_.on_scheduled();

  const TimePoint claimed_at = Clock::now();
  if (config_.fail_expired_before_execution && task->is_expired(claimed_at)) {
    // Real-time discipline: work whose deadline has already passed cannot be
    // useful, so it must not occupy a worker that could serve work that still
    // can. Shedding it is the difference between degrading and collapsing.
    const auto overdue_us =
        std::chrono::duration_cast<std::chrono::microseconds>(claimed_at - task->deadline())
            .count();
    if (task->fail_with("deadline expired before execution (overdue by " +
                        std::to_string(overdue_us) + " us)")) {
      emit_trace(*task, TaskState::SCHEDULED, TaskState::FAILED);
      metrics_.on_expired_before_start();
      finalize(task, /*completed=*/false);
    }
    return;
  }

  if (!task->transition_to(TaskState::EXECUTING)) {
    // Unreachable in practice: this worker owns the task once it is
    // SCHEDULED. If it ever happens, the thread that made the task terminal
    // owns its accounting, so returning here cannot double-count.
    return;
  }
  emit_trace(*task, TaskState::SCHEDULED, TaskState::EXECUTING);

  bool callback_ok = false;
  std::string error;
  {
    ScopedGauge active(active_workers_);
    metrics_.on_executed();
    // The scheduler is the last line of defence for user code: an escaping
    // exception must fail one task, never take down a worker thread and with
    // it a quarter of the pool's capacity.
    try {
      callback_ok = task->callback()(*task);
    } catch (const std::exception& e) {
      metrics_.on_exception();
      try {
        error = std::string("callback threw: ") + e.what();
      } catch (...) {
        error = "callback threw";  // what() copy failed; keep something useful
      }
    } catch (...) {
      metrics_.on_exception();
      error = "callback threw a non-std exception";
    }
  }

  const TimePoint finished_at = Clock::now();
  const bool overran = finished_at > task->deadline();
  if (overran) {
    metrics_.on_deadline_missed();
  }

  if (!error.empty()) {
    task->fail_with(std::move(error));
    emit_trace(*task, TaskState::EXECUTING, TaskState::FAILED);
    finalize(task, /*completed=*/false);
    return;
  }
  if (!callback_ok) {
    task->fail_with("callback reported failure");
    emit_trace(*task, TaskState::EXECUTING, TaskState::FAILED);
    finalize(task, /*completed=*/false);
    return;
  }
  if (overran && config_.enforce_deadline_after_execution) {
    const auto overrun_us =
        std::chrono::duration_cast<std::chrono::microseconds>(finished_at - task->deadline())
            .count();
    task->fail_with("deadline exceeded during execution (overran by " +
                    std::to_string(overrun_us) + " us)");
    emit_trace(*task, TaskState::EXECUTING, TaskState::FAILED);
    finalize(task, /*completed=*/false);
    return;
  }

  task->transition_to(TaskState::COMPLETED);
  emit_trace(*task, TaskState::EXECUTING, TaskState::COMPLETED);
  finalize(task, /*completed=*/true);
}

void PriorityScheduler::finalize(const std::shared_ptr<Task>& task, bool completed) {
  if (completed) {
    metrics_.on_completed();
  } else {
    metrics_.on_failed();
  }
  metrics_.record_total_latency(task->total_latency());
  metrics_.record_queue_latency(task->queue_latency());
  if (task->started_at() != TimePoint{}) {
    // Skipped for shed tasks: recording a zero would pull the execution-time
    // distribution toward zero and hide how long real work actually takes.
    metrics_.record_execution_latency(task->execution_latency());
  }
  outstanding_.fetch_sub(1, std::memory_order_acq_rel);
}

void PriorityScheduler::emit_trace(const Task& task, TaskState from, TaskState to) const {
  if (!config_.trace_sink) {
    return;  // Null check only: no allocation, no formatting, no cost.
  }
  config_.trace_sink(task, from, to);
}

void PriorityScheduler::abandon_queued_tasks() {
  std::vector<std::shared_ptr<Task>> abandoned;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    drain_ingress_locked();
    abandoned.reserve(heap_.size());
    for (QueueEntry& entry : heap_) {
      abandoned.push_back(std::move(entry.task));
    }
    heap_.clear();
    pending_.store(0, std::memory_order_seq_cst);
  }
  // Failed outside the queue lock: fail_with() allocates, and a user
  // trace_sink runs here too. Neither belongs under the hot lock.
  for (const std::shared_ptr<Task>& task : abandoned) {
    if (task->fail_with("scheduler stopped before execution")) {
      emit_trace(*task, TaskState::PENDING, TaskState::FAILED);
      finalize(task, /*completed=*/false);
    }
  }
}

// --- Registry ---------------------------------------------------------------

PriorityScheduler::RegistryShard& PriorityScheduler::shard_for(std::uint64_t task_id) noexcept {
  return registry_[task_id & registry_mask_];
}

const PriorityScheduler::RegistryShard& PriorityScheduler::shard_for(
    std::uint64_t task_id) const noexcept {
  return registry_[task_id & registry_mask_];
}

void PriorityScheduler::register_task(const std::shared_ptr<Task>& task) {
  RegistryShard& shard = shard_for(task->id());
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.tasks.emplace(task->id(), task);
  shard.insertion_order.push_back(task->id());

  // Strict FIFO eviction: the shard keeps exactly the most recently submitted
  // history_per_shard ids and nothing else. At 100k tasks/second an unbounded
  // registry is the first thing that would exhaust memory, so the bound is
  // hard rather than best-effort.
  //
  // Evicting only *terminal* tasks was the obvious alternative and is worse:
  // eviction happens on insert, so one long-running task at the head blocks
  // the queue and the map stays at its high-water mark long after the burst
  // that caused it. A strict bound is O(1), predictable, and keeps precisely
  // the tasks a caller is most likely to ask about.
  //
  // Consequence: with a backlog deeper than the retention limit, an in-flight
  // task can stop being queryable. Eviction only drops the scheduler's
  // *reference* -- the queue and the running worker hold their own, so the
  // task still runs and is still counted. Size history_per_shard x
  // registry_shards above the expected in-flight count for full visibility;
  // see docs/TUNING.md.
  while (shard.insertion_order.size() > config_.history_per_shard) {
    shard.tasks.erase(shard.insertion_order.front());
    shard.insertion_order.pop_front();
  }
}

std::shared_ptr<const Task> PriorityScheduler::find_task(std::uint64_t task_id) const {
  const RegistryShard& shard = shard_for(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  const auto it = shard.tasks.find(task_id);
  if (it == shard.tasks.end()) {
    return nullptr;
  }
  return it->second;
}

std::optional<TaskState> PriorityScheduler::try_get_task_state(std::uint64_t task_id) const {
  const std::shared_ptr<const Task> task = find_task(task_id);
  if (!task) {
    return std::nullopt;
  }
  return task->state();
}

TaskState PriorityScheduler::get_task_state(std::uint64_t task_id) const {
  const std::optional<TaskState> state = try_get_task_state(task_id);
  if (!state) {
    throw UnknownTask(task_id);
  }
  return *state;
}

// --- Metrics and waiting ----------------------------------------------------

Metrics PriorityScheduler::get_metrics() const {
  return metrics_.snapshot(pending_.load(std::memory_order_acquire),
                           active_workers_.load(std::memory_order_acquire),
                           worker_count_.load(std::memory_order_acquire));
}

void PriorityScheduler::reset_metrics() noexcept { metrics_.reset(); }

bool PriorityScheduler::wait_until_idle(std::chrono::milliseconds timeout) {
  const TimePoint deadline = Clock::now() + timeout;
  std::chrono::microseconds backoff = kIdlePollMin;
  for (;;) {
    // outstanding_ covers queued and executing work; inflight_submitters_
    // covers producers that have not published yet. Both must be zero, or
    // "idle" could be observed in the gap between them.
    if (outstanding_.load(std::memory_order_acquire) == 0 &&
        inflight_submitters_.load(std::memory_order_acquire) == 0) {
      return true;
    }
    if (Clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(backoff);
    backoff = std::min(backoff * 2, kIdlePollMax);
  }
}

}  // namespace rtsched
