// Copyright 2026. SPDX-License-Identifier: MIT
//
// Task definition and lifecycle state machine.
//
// A Task is created by PriorityScheduler::submit_task and owned by a
// std::shared_ptr. The same object is observed concurrently by
//   * the submitting thread (which may poll its state),
//   * the worker thread that executes it,
//   * any thread calling PriorityScheduler::get_task_state.
//
// Every mutable field is therefore either atomic or covered by a documented
// happens-before argument (see error_message()). No per-task mutex is used:
// at 100k tasks in flight, a mutex per task is a meaningful memory and
// cache-footprint cost for no benefit.

#ifndef RTSCHED_TASK_H_
#define RTSCHED_TASK_H_

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace rtsched {

// steady_clock: monotonic, immune to wall-clock adjustments. Never use
// system_clock for deadlines -- an NTP step would retroactively blow them.
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Nanos = std::chrono::nanoseconds;

// Task lifecycle:
//
//   PENDING ---> SCHEDULED ---> EXECUTING ---> COMPLETED
//      |             |              |
//      +-------------+--------------+-------> FAILED
//
// COMPLETED and FAILED are terminal. Transitions are validated; an invalid
// transition is refused rather than silently applied.
enum class TaskState : std::uint8_t {
  PENDING = 0,    // accepted by the scheduler, sitting in a queue
  SCHEDULED = 1,  // claimed by a worker, about to run
  EXECUTING = 2,  // inside the user callback
  COMPLETED = 3,  // callback returned true within its deadline
  FAILED = 4,     // callback returned false, threw, missed its deadline, or was rejected
};

inline constexpr std::size_t kTaskStateCount = 5;

// Human-readable state name; never null.
const char* to_string(TaskState state) noexcept;

// True for COMPLETED and FAILED.
bool is_terminal(TaskState state) noexcept;

// True if `from -> to` is an edge in the diagram above.
bool is_valid_transition(TaskState from, TaskState to) noexcept;

class Task;

// User work unit. Returns true on success, false to mark the task FAILED.
// Exceptions escaping the callback are caught and also mark the task FAILED.
using TaskCallback = std::function<bool(const Task&)>;

// Priority band. Higher value == scheduled sooner.
inline constexpr int kMinPriority = 0;
inline constexpr int kMaxPriority = 10;

class Task {
 public:
  // Bounded transition trace: at most one entry per state, so a task can
  // never record more than kMaxTraceEntries. Fixed size => no allocation on
  // the hot path.
  static constexpr std::size_t kMaxTraceEntries = 4;

  struct TraceEntry {
    TaskState state;
    TimePoint at;
  };

  Task(std::uint64_t id, int priority, TaskCallback callback, TimePoint created_at,
       TimePoint deadline, bool trace_enabled);

  // Shared across threads and identified by address; copying would be a bug.
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&) = delete;
  Task& operator=(Task&&) = delete;

  // --- Immutable identity (safe to read from any thread) ---------------------

  std::uint64_t id() const noexcept { return id_; }
  int priority() const noexcept { return priority_; }
  TimePoint created_at() const noexcept { return created_at_; }
  TimePoint deadline() const noexcept { return deadline_; }
  const TaskCallback& callback() const noexcept { return callback_; }

  // --- State ----------------------------------------------------------------

  TaskState state() const noexcept { return state_.load(std::memory_order_acquire); }
  bool is_terminal() const noexcept { return rtsched::is_terminal(state()); }

  // Attempts `state() -> next`. Returns false (and changes nothing) if the
  // transition is invalid or lost a race against another thread. Timestamps
  // and the trace entry are recorded only by the thread that wins the CAS.
  bool transition_to(TaskState next) noexcept;

  // Records `message` and transitions to FAILED. Returns false without
  // touching the task if it is already terminal, or if another thread is
  // already failing it. The message is published before the state change, so
  // any thread that observes FAILED also observes the message.
  bool fail_with(std::string message);

  // Returns the failure reason, or "" if the task has not failed.
  //
  // Lock-free and race-free by construction: fail_with() writes the string,
  // then releases the state store. This acquire-load of the state therefore
  // synchronizes-with that release, making the string safely readable. A
  // non-FAILED task never exposes the buffer to a reader.
  std::string error_message() const;

  // --- Timing ---------------------------------------------------------------

  bool is_expired(TimePoint now) const noexcept { return now > deadline_; }
  Nanos time_to_deadline(TimePoint now) const noexcept { return deadline_ - now; }

  // Zero-valued TimePoint if the corresponding transition has not happened.
  TimePoint scheduled_at() const noexcept { return load_stamp(scheduled_at_ns_); }
  TimePoint started_at() const noexcept { return load_stamp(started_at_ns_); }
  TimePoint finished_at() const noexcept { return load_stamp(finished_at_ns_); }

  // created_at -> scheduled_at: how long the task waited before a worker
  // claimed it. This is the scheduler's own overhead, isolated from however
  // long the user callback takes, and is what "scheduling latency" means in
  // the metrics. Measured at SCHEDULED rather than EXECUTING so that a task
  // dropped for an expired deadline still reports the wait it experienced.
  Nanos queue_latency() const noexcept;
  // started_at -> finished_at. Time inside the user callback.
  Nanos execution_latency() const noexcept;
  // created_at -> finished_at. End-to-end, what get_metrics() reports.
  Nanos total_latency() const noexcept;

  // Copies the recorded transitions into `out` and returns how many were
  // written. Safe to call concurrently with transitions; a transition in
  // flight is simply not yet visible.
  std::size_t trace(std::array<TraceEntry, kMaxTraceEntries>& out) const noexcept;

 private:
  static constexpr std::int64_t kUnstamped = INT64_MIN;

  static std::int64_t to_ns(TimePoint tp) noexcept {
    return std::chrono::duration_cast<Nanos>(tp.time_since_epoch()).count();
  }
  static TimePoint load_stamp(const std::atomic<std::int64_t>& cell) noexcept {
    const std::int64_t ns = cell.load(std::memory_order_acquire);
    return ns == kUnstamped ? TimePoint{} : TimePoint{Nanos{ns}};
  }
  void stamp(TaskState state, TimePoint at) noexcept;
  void record_trace(TaskState state, TimePoint at) noexcept;

  const std::uint64_t id_;
  const int priority_;
  const TaskCallback callback_;
  const TimePoint created_at_;
  const TimePoint deadline_;
  const bool trace_enabled_;

  std::atomic<TaskState> state_;

  std::atomic<std::int64_t> scheduled_at_ns_{kUnstamped};
  std::atomic<std::int64_t> started_at_ns_{kUnstamped};
  std::atomic<std::int64_t> finished_at_ns_{kUnstamped};

  // error_claimed_ hands exactly one thread the right to write
  // error_message_, so the buffer needs no mutex even if two threads race to
  // fail the same task. The winner writes the string, then release-stores
  // FAILED; readers acquire-load FAILED before touching the buffer.
  std::atomic<bool> error_claimed_{false};
  std::string error_message_;

  // Trace ring. `trace_at_ns_[i] == kUnstamped` means "not published yet",
  // which is what makes a concurrent reader safe without a lock.
  std::atomic<std::uint32_t> trace_count_{0};
  std::array<std::atomic<std::uint8_t>, kMaxTraceEntries> trace_state_{};
  std::array<std::atomic<std::int64_t>, kMaxTraceEntries> trace_at_ns_{};
};

}  // namespace rtsched

#endif  // RTSCHED_TASK_H_
