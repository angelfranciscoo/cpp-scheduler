// Copyright 2026. SPDX-License-Identifier: MIT

#include "rtsched/task.h"

#include <utility>

namespace rtsched {
namespace {

// Transition table indexed [from][to]. Encoding the state machine as data
// rather than a chain of if-statements keeps it auditable at a glance and
// makes the "no resurrection from a terminal state" rule impossible to
// violate by accident.
constexpr bool kTransitions[kTaskStateCount][kTaskStateCount] = {
    //            PENDING SCHEDULED EXECUTING COMPLETED FAILED
    /* PENDING */ {false, true, false, false, true},
    /* SCHEDULED */ {false, false, true, false, true},
    /* EXECUTING */ {false, false, false, true, true},
    /* COMPLETED */ {false, false, false, false, false},
    /* FAILED */ {false, false, false, false, false},
};

constexpr std::size_t index_of(TaskState state) noexcept {
  return static_cast<std::size_t>(state);
}

}  // namespace

const char* to_string(TaskState state) noexcept {
  switch (state) {
    case TaskState::PENDING:
      return "PENDING";
    case TaskState::SCHEDULED:
      return "SCHEDULED";
    case TaskState::EXECUTING:
      return "EXECUTING";
    case TaskState::COMPLETED:
      return "COMPLETED";
    case TaskState::FAILED:
      return "FAILED";
  }
  return "UNKNOWN";
}

bool is_terminal(TaskState state) noexcept {
  return state == TaskState::COMPLETED || state == TaskState::FAILED;
}

bool is_valid_transition(TaskState from, TaskState to) noexcept {
  const std::size_t f = index_of(from);
  const std::size_t t = index_of(to);
  if (f >= kTaskStateCount || t >= kTaskStateCount) {
    return false;
  }
  return kTransitions[f][t];
}

Task::Task(std::uint64_t id, int priority, TaskCallback callback, TimePoint created_at,
           TimePoint deadline, bool trace_enabled)
    : id_(id),
      priority_(priority),
      callback_(std::move(callback)),
      created_at_(created_at),
      deadline_(deadline),
      trace_enabled_(trace_enabled),
      state_(TaskState::PENDING) {
  for (std::size_t i = 0; i < kMaxTraceEntries; ++i) {
    trace_state_[i].store(static_cast<std::uint8_t>(TaskState::PENDING),
                          std::memory_order_relaxed);
    trace_at_ns_[i].store(kUnstamped, std::memory_order_relaxed);
  }
  record_trace(TaskState::PENDING, created_at);
}

bool Task::transition_to(TaskState next) noexcept {
  TaskState current = state_.load(std::memory_order_acquire);
  for (;;) {
    if (!is_valid_transition(current, next)) {
      return false;
    }
    // acq_rel on success: publishes everything the caller wrote before the
    // transition (notably error_message_) to whoever acquire-loads the state.
    if (state_.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      break;
    }
    // CAS failed: `current` now holds the observed state, retry validation.
  }

  const TimePoint now = Clock::now();
  stamp(next, now);
  record_trace(next, now);
  return true;
}

bool Task::fail_with(std::string message) {
  if (is_terminal()) {
    return false;  // Fast path: nothing to do, and no claim to release.
  }
  // Claim the error buffer before writing it. Ordinarily a single worker owns
  // a task and this always succeeds, but shutdown can race a worker, and
  // "ordinarily" is not a memory model. The claim makes the buffer
  // single-writer by construction rather than by convention.
  bool unclaimed = false;
  if (!error_claimed_.compare_exchange_strong(unclaimed, true, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
    return false;
  }
  error_message_ = std::move(message);
  // transition_to()'s release-store publishes the write above. It can still
  // fail if the task reached a terminal state after the check; the buffer is
  // then simply never exposed, because error_message() gates on FAILED.
  return transition_to(TaskState::FAILED);
}

std::string Task::error_message() const {
  if (state_.load(std::memory_order_acquire) != TaskState::FAILED) {
    return std::string();
  }
  return error_message_;
}

void Task::stamp(TaskState state, TimePoint at) noexcept {
  const std::int64_t ns = to_ns(at);
  switch (state) {
    case TaskState::SCHEDULED:
      scheduled_at_ns_.store(ns, std::memory_order_release);
      break;
    case TaskState::EXECUTING:
      started_at_ns_.store(ns, std::memory_order_release);
      break;
    case TaskState::COMPLETED:
    case TaskState::FAILED:
      finished_at_ns_.store(ns, std::memory_order_release);
      break;
    case TaskState::PENDING:
      break;
  }
}

void Task::record_trace(TaskState state, TimePoint at) noexcept {
  if (!trace_enabled_) {
    return;
  }
  const std::uint32_t slot = trace_count_.fetch_add(1, std::memory_order_relaxed);
  if (slot >= kMaxTraceEntries) {
    return;  // Unreachable given the state machine; cheap insurance.
  }
  trace_state_[slot].store(static_cast<std::uint8_t>(state), std::memory_order_relaxed);
  // Release-store of the timestamp publishes the state byte above. A reader
  // that sees kUnstamped stops, so it never reads a half-written entry.
  trace_at_ns_[slot].store(to_ns(at), std::memory_order_release);
}

std::size_t Task::trace(std::array<TraceEntry, kMaxTraceEntries>& out) const noexcept {
  std::size_t written = 0;
  for (std::size_t i = 0; i < kMaxTraceEntries; ++i) {
    const std::int64_t ns = trace_at_ns_[i].load(std::memory_order_acquire);
    if (ns == kUnstamped) {
      break;
    }
    out[written].state =
        static_cast<TaskState>(trace_state_[i].load(std::memory_order_relaxed));
    out[written].at = TimePoint{Nanos{ns}};
    ++written;
  }
  return written;
}

Nanos Task::queue_latency() const noexcept {
  const TimePoint claimed = scheduled_at();
  if (claimed == TimePoint{}) {
    return Nanos::zero();
  }
  return claimed - created_at_;
}

Nanos Task::execution_latency() const noexcept {
  const TimePoint started = started_at();
  const TimePoint finished = finished_at();
  if (started == TimePoint{} || finished == TimePoint{}) {
    return Nanos::zero();
  }
  return finished - started;
}

Nanos Task::total_latency() const noexcept {
  const TimePoint finished = finished_at();
  if (finished == TimePoint{}) {
    return Nanos::zero();
  }
  return finished - created_at_;
}

}  // namespace rtsched
