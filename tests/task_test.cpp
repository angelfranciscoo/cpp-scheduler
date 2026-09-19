// Copyright 2026. SPDX-License-Identifier: MIT
//
// Task lifecycle state machine tests.

#include "rtsched/task.h"

#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

namespace rtsched {
namespace {

using namespace std::chrono_literals;

std::shared_ptr<Task> make_task(int priority = 5, std::chrono::milliseconds ttl = 1s,
                                bool trace = true) {
  const TimePoint now = Clock::now();
  return std::make_shared<Task>(42, priority, [](const Task&) { return true; }, now, now + ttl,
                                trace);
}

const std::vector<TaskState>& all_states() {
  static const std::vector<TaskState> states{TaskState::PENDING, TaskState::SCHEDULED,
                                             TaskState::EXECUTING, TaskState::COMPLETED,
                                             TaskState::FAILED};
  return states;
}

// --- State machine ----------------------------------------------------------

TEST(TaskStateMachine, StartsPending) {
  const auto task = make_task();
  EXPECT_EQ(task->state(), TaskState::PENDING);
  EXPECT_FALSE(task->is_terminal());
}

TEST(TaskStateMachine, HappyPathTransitions) {
  const auto task = make_task();
  EXPECT_TRUE(task->transition_to(TaskState::SCHEDULED));
  EXPECT_EQ(task->state(), TaskState::SCHEDULED);
  EXPECT_TRUE(task->transition_to(TaskState::EXECUTING));
  EXPECT_EQ(task->state(), TaskState::EXECUTING);
  EXPECT_TRUE(task->transition_to(TaskState::COMPLETED));
  EXPECT_EQ(task->state(), TaskState::COMPLETED);
  EXPECT_TRUE(task->is_terminal());
}

TEST(TaskStateMachine, EveryStateCanFailBeforeTerminal) {
  for (const TaskState from : {TaskState::PENDING, TaskState::SCHEDULED, TaskState::EXECUTING}) {
    const auto task = make_task();
    // Walk to `from` through the legal path.
    if (from != TaskState::PENDING) {
      ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
    }
    if (from == TaskState::EXECUTING) {
      ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
    }
    ASSERT_EQ(task->state(), from);
    EXPECT_TRUE(task->transition_to(TaskState::FAILED)) << "from " << to_string(from);
    EXPECT_EQ(task->state(), TaskState::FAILED);
  }
}

TEST(TaskStateMachine, TerminalStatesAreFinal) {
  for (const TaskState terminal : {TaskState::COMPLETED, TaskState::FAILED}) {
    const auto task = make_task();
    ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
    ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
    ASSERT_TRUE(task->transition_to(terminal));

    for (const TaskState next : all_states()) {
      EXPECT_FALSE(task->transition_to(next))
          << to_string(terminal) << " -> " << to_string(next) << " must be refused";
    }
    EXPECT_EQ(task->state(), terminal);
  }
}

TEST(TaskStateMachine, SkippingStatesIsRefused) {
  const auto task = make_task();
  EXPECT_FALSE(task->transition_to(TaskState::EXECUTING));  // no PENDING -> EXECUTING
  EXPECT_FALSE(task->transition_to(TaskState::COMPLETED));  // no PENDING -> COMPLETED
  EXPECT_FALSE(task->transition_to(TaskState::PENDING));    // no self-loop
  EXPECT_EQ(task->state(), TaskState::PENDING);

  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  EXPECT_FALSE(task->transition_to(TaskState::COMPLETED));  // no SCHEDULED -> COMPLETED
  EXPECT_EQ(task->state(), TaskState::SCHEDULED);
}

// The transition table is the authority; this pins it down so a future edit
// cannot widen the state machine unnoticed.
TEST(TaskStateMachine, TransitionTableIsExactlyTheSpecifiedGraph) {
  const std::set<std::pair<TaskState, TaskState>> expected{
      {TaskState::PENDING, TaskState::SCHEDULED},   {TaskState::PENDING, TaskState::FAILED},
      {TaskState::SCHEDULED, TaskState::EXECUTING}, {TaskState::SCHEDULED, TaskState::FAILED},
      {TaskState::EXECUTING, TaskState::COMPLETED}, {TaskState::EXECUTING, TaskState::FAILED},
  };
  for (const TaskState from : all_states()) {
    for (const TaskState to : all_states()) {
      const bool allowed = expected.count({from, to}) != 0;
      EXPECT_EQ(is_valid_transition(from, to), allowed)
          << to_string(from) << " -> " << to_string(to);
    }
  }
}

TEST(TaskStateMachine, IsTerminalMatchesTheTwoEndStates) {
  EXPECT_FALSE(is_terminal(TaskState::PENDING));
  EXPECT_FALSE(is_terminal(TaskState::SCHEDULED));
  EXPECT_FALSE(is_terminal(TaskState::EXECUTING));
  EXPECT_TRUE(is_terminal(TaskState::COMPLETED));
  EXPECT_TRUE(is_terminal(TaskState::FAILED));
}

TEST(TaskStateMachine, StateNamesAreStable) {
  EXPECT_STREQ(to_string(TaskState::PENDING), "PENDING");
  EXPECT_STREQ(to_string(TaskState::SCHEDULED), "SCHEDULED");
  EXPECT_STREQ(to_string(TaskState::EXECUTING), "EXECUTING");
  EXPECT_STREQ(to_string(TaskState::COMPLETED), "COMPLETED");
  EXPECT_STREQ(to_string(TaskState::FAILED), "FAILED");
  // Out-of-range value must not walk off the switch.
  EXPECT_STREQ(to_string(static_cast<TaskState>(99)), "UNKNOWN");
  EXPECT_FALSE(is_valid_transition(static_cast<TaskState>(99), TaskState::FAILED));
}

// --- Concurrent transitions -------------------------------------------------

// The core thread-safety property: however many threads race, a task takes
// each transition exactly once.
TEST(TaskStateMachine, ExactlyOneThreadWinsATransition) {
  constexpr int kThreads = 16;
  constexpr int kRounds = 200;

  for (int round = 0; round < kRounds; ++round) {
    const auto task = make_task();
    std::atomic<int> winners{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
      threads.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        if (task->transition_to(TaskState::SCHEDULED)) {
          winners.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    go.store(true, std::memory_order_release);
    for (std::thread& t : threads) {
      t.join();
    }
    ASSERT_EQ(winners.load(), 1) << "round " << round;
    ASSERT_EQ(task->state(), TaskState::SCHEDULED);
  }
}

// COMPLETED and FAILED race: whichever wins, the other must not corrupt the
// task, and the error message must agree with the final state.
TEST(TaskStateMachine, CompletedAndFailedRaceLeavesConsistentState) {
  constexpr int kRounds = 500;
  for (int round = 0; round < kRounds; ++round) {
    const auto task = make_task();
    ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
    ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));

    std::atomic<bool> go{false};
    bool completed_won = false;
    bool failed_won = false;
    std::thread completer([&] {
      while (!go.load(std::memory_order_acquire)) {
      }
      completed_won = task->transition_to(TaskState::COMPLETED);
    });
    std::thread failer([&] {
      while (!go.load(std::memory_order_acquire)) {
      }
      failed_won = task->fail_with("racing failure");
    });
    go.store(true, std::memory_order_release);
    completer.join();
    failer.join();

    ASSERT_NE(completed_won, failed_won) << "exactly one transition must win";
    ASSERT_TRUE(task->is_terminal());
    if (failed_won) {
      EXPECT_EQ(task->state(), TaskState::FAILED);
      EXPECT_EQ(task->error_message(), "racing failure");
    } else {
      EXPECT_EQ(task->state(), TaskState::COMPLETED);
      // A completed task must never expose a failure reason.
      EXPECT_EQ(task->error_message(), "");
    }
  }
}

TEST(TaskStateMachine, ConcurrentFailuresProduceExactlyOneMessage) {
  constexpr int kThreads = 8;
  constexpr int kRounds = 200;
  for (int round = 0; round < kRounds; ++round) {
    const auto task = make_task();
    std::atomic<int> winners{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
      threads.emplace_back([&, i] {
        while (!go.load(std::memory_order_acquire)) {
        }
        if (task->fail_with("failure from thread " + std::to_string(i))) {
          winners.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    go.store(true, std::memory_order_release);
    for (std::thread& t : threads) {
      t.join();
    }
    ASSERT_EQ(winners.load(), 1);
    // Whichever thread won, the message must be intact -- not interleaved.
    const std::string message = task->error_message();
    EXPECT_EQ(message.rfind("failure from thread ", 0), 0u) << message;
  }
}

// --- Error reporting --------------------------------------------------------

TEST(TaskError, MessageIsOnlyVisibleOnceFailed) {
  const auto task = make_task();
  EXPECT_EQ(task->error_message(), "");
  ASSERT_TRUE(task->fail_with("sensor timeout"));
  EXPECT_EQ(task->state(), TaskState::FAILED);
  EXPECT_EQ(task->error_message(), "sensor timeout");
}

TEST(TaskError, FailingATerminalTaskIsRefusedAndPreservesTheOriginal) {
  const auto task = make_task();
  ASSERT_TRUE(task->fail_with("first"));
  EXPECT_FALSE(task->fail_with("second"));
  EXPECT_EQ(task->error_message(), "first");
}

TEST(TaskError, CompletedTaskNeverReportsAnError) {
  const auto task = make_task();
  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
  ASSERT_TRUE(task->transition_to(TaskState::COMPLETED));
  EXPECT_FALSE(task->fail_with("too late"));
  EXPECT_EQ(task->state(), TaskState::COMPLETED);
  EXPECT_EQ(task->error_message(), "");
}

// --- Identity, deadlines, timings -------------------------------------------

TEST(TaskIdentity, ImmutableFieldsRoundTrip) {
  const TimePoint created = Clock::now();
  const TimePoint deadline = created + 250ms;
  const Task task(7, 9, [](const Task&) { return false; }, created, deadline, true);
  EXPECT_EQ(task.id(), 7u);
  EXPECT_EQ(task.priority(), 9);
  EXPECT_EQ(task.created_at(), created);
  EXPECT_EQ(task.deadline(), deadline);
  ASSERT_TRUE(static_cast<bool>(task.callback()));
  EXPECT_FALSE(task.callback()(task));
}

TEST(TaskDeadline, ExpiryIsRelativeToTheSuppliedNow) {
  const TimePoint created = Clock::now();
  const Task task(1, 5, [](const Task&) { return true; }, created, created + 100ms, true);
  EXPECT_FALSE(task.is_expired(created));
  EXPECT_FALSE(task.is_expired(created + 99ms));
  EXPECT_FALSE(task.is_expired(created + 100ms));  // boundary: not yet past
  EXPECT_TRUE(task.is_expired(created + 101ms));
  EXPECT_EQ(task.time_to_deadline(created), std::chrono::duration_cast<Nanos>(100ms));
  EXPECT_LT(task.time_to_deadline(created + 150ms).count(), 0);
}

TEST(TaskTiming, StampsAreUnsetUntilTheTransitionHappens) {
  const auto task = make_task();
  EXPECT_EQ(task->scheduled_at(), TimePoint{});
  EXPECT_EQ(task->started_at(), TimePoint{});
  EXPECT_EQ(task->finished_at(), TimePoint{});
  EXPECT_EQ(task->queue_latency(), Nanos::zero());
  EXPECT_EQ(task->execution_latency(), Nanos::zero());
  EXPECT_EQ(task->total_latency(), Nanos::zero());

  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  EXPECT_NE(task->scheduled_at(), TimePoint{});
  EXPECT_GT(task->queue_latency().count(), 0);
  EXPECT_EQ(task->execution_latency(), Nanos::zero());  // not started yet

  ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
  EXPECT_NE(task->started_at(), TimePoint{});
  EXPECT_EQ(task->execution_latency(), Nanos::zero());  // not finished yet

  ASSERT_TRUE(task->transition_to(TaskState::COMPLETED));
  EXPECT_NE(task->finished_at(), TimePoint{});
  EXPECT_GT(task->execution_latency().count(), 0);
  EXPECT_GE(task->total_latency(), task->execution_latency());
}

TEST(TaskTiming, StampsAreMonotonicAndAdditive) {
  const auto task = make_task();
  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
  std::this_thread::sleep_for(2ms);
  ASSERT_TRUE(task->transition_to(TaskState::COMPLETED));

  EXPECT_LE(task->created_at(), task->scheduled_at());
  EXPECT_LE(task->scheduled_at(), task->started_at());
  EXPECT_LE(task->started_at(), task->finished_at());
  EXPECT_GE(task->execution_latency(), std::chrono::duration_cast<Nanos>(2ms));
  // total == queue + (scheduled -> started) + execution, so it must dominate.
  EXPECT_GE(task->total_latency(), task->queue_latency() + task->execution_latency());
}

TEST(TaskTiming, ShedTaskStillReportsItsQueueWait) {
  const auto task = make_task();
  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  ASSERT_TRUE(task->fail_with("expired"));
  // Never executed, but the wait it experienced is still measurable -- this is
  // why queue latency is stamped at SCHEDULED, not at EXECUTING.
  EXPECT_GT(task->queue_latency().count(), 0);
  EXPECT_EQ(task->execution_latency(), Nanos::zero());
  EXPECT_GT(task->total_latency().count(), 0);
}

// --- Transition trace -------------------------------------------------------

TEST(TaskTrace, RecordsEveryTransitionInOrder) {
  const auto task = make_task();
  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  ASSERT_TRUE(task->transition_to(TaskState::EXECUTING));
  ASSERT_TRUE(task->transition_to(TaskState::COMPLETED));

  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  const std::size_t count = task->trace(entries);
  ASSERT_EQ(count, 4u);
  EXPECT_EQ(entries[0].state, TaskState::PENDING);
  EXPECT_EQ(entries[1].state, TaskState::SCHEDULED);
  EXPECT_EQ(entries[2].state, TaskState::EXECUTING);
  EXPECT_EQ(entries[3].state, TaskState::COMPLETED);
  for (std::size_t i = 1; i < count; ++i) {
    EXPECT_LE(entries[i - 1].at, entries[i].at) << "trace timestamps must be monotonic";
  }
  EXPECT_EQ(entries[0].at, task->created_at());
}

TEST(TaskTrace, RefusedTransitionsAreNotRecorded) {
  const auto task = make_task();
  EXPECT_FALSE(task->transition_to(TaskState::COMPLETED));
  EXPECT_FALSE(task->transition_to(TaskState::EXECUTING));

  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  EXPECT_EQ(task->trace(entries), 1u);  // just the PENDING entry
  EXPECT_EQ(entries[0].state, TaskState::PENDING);
}

TEST(TaskTrace, ShortestPathIsTwoEntries) {
  const auto task = make_task();
  ASSERT_TRUE(task->fail_with("rejected"));
  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  ASSERT_EQ(task->trace(entries), 2u);
  EXPECT_EQ(entries[0].state, TaskState::PENDING);
  EXPECT_EQ(entries[1].state, TaskState::FAILED);
}

TEST(TaskTrace, CanBeDisabled) {
  const auto task = make_task(5, 1s, /*trace=*/false);
  ASSERT_TRUE(task->transition_to(TaskState::SCHEDULED));
  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  EXPECT_EQ(task->trace(entries), 0u);
  // Disabling the trace must not affect the state machine or the timestamps.
  EXPECT_EQ(task->state(), TaskState::SCHEDULED);
  EXPECT_NE(task->scheduled_at(), TimePoint{});
}

TEST(TaskTrace, ReadableConcurrentlyWithTransitions) {
  // A reader must never see a torn entry: either the entry is published with
  // a matching timestamp, or it is not visible at all.
  constexpr int kRounds = 300;
  for (int round = 0; round < kRounds; ++round) {
    const auto task = make_task();
    std::atomic<bool> stop{false};
    std::thread reader([&] {
      std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
      while (!stop.load(std::memory_order_acquire)) {
        const std::size_t count = task->trace(entries);
        ASSERT_LE(count, Task::kMaxTraceEntries);
        for (std::size_t i = 0; i < count; ++i) {
          ASSERT_NE(entries[i].at, TimePoint{});
          if (i > 0) {
            ASSERT_LE(entries[i - 1].at, entries[i].at);
          }
        }
      }
    });
    task->transition_to(TaskState::SCHEDULED);
    task->transition_to(TaskState::EXECUTING);
    task->transition_to(TaskState::COMPLETED);
    stop.store(true, std::memory_order_release);
    reader.join();
  }
}

}  // namespace
}  // namespace rtsched
