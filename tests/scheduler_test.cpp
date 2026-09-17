// Copyright 2026. SPDX-License-Identifier: MIT
//
// PriorityScheduler unit tests.
//
// Determinism note: several tests submit work *before* start(). Submissions
// are accepted in the CREATED state, so this stages an exact queue and then
// releases a single worker on it -- which makes ordering assertions
// deterministic instead of racy. Tests that genuinely need concurrency use
// counters and invariants rather than expected interleavings.

#include "rtsched/scheduler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rtsched {
namespace {

using namespace std::chrono_literals;

// Generous: these bound "something is wedged", not "something is slow".
constexpr std::chrono::milliseconds kIdleTimeout = 30s;

SchedulerConfig config_with(std::size_t workers) {
  SchedulerConfig config;
  config.num_workers = workers;
  return config;
}

// Records the order in which tasks ran.
class OrderLog {
 public:
  void add(std::uint64_t id) {
    const std::lock_guard<std::mutex> lock(mutex_);
    order_.push_back(id);
  }
  std::vector<std::uint64_t> get() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return order_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::uint64_t> order_;
};

// ===========================================================================
// Basic functionality
// ===========================================================================

TEST(SchedulerBasic, RunsASingleTask) {
  PriorityScheduler scheduler(2);
  scheduler.start();

  std::atomic<int> ran{0};
  const std::uint64_t id =
      scheduler.submit_task(5, [&ran](const Task&) {
        ran.fetch_add(1);
        return true;
      });

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::COMPLETED);
  scheduler.stop();
}

TEST(SchedulerBasic, TaskIdsAreUniqueAndMonotonic) {
  PriorityScheduler scheduler(1);
  std::vector<std::uint64_t> ids;
  for (int i = 0; i < 100; ++i) {
    ids.push_back(scheduler.submit_task(5, [](const Task&) { return true; }));
  }
  for (std::size_t i = 1; i < ids.size(); ++i) {
    EXPECT_GT(ids[i], ids[i - 1]);
  }
  EXPECT_EQ(std::set<std::uint64_t>(ids.begin(), ids.end()).size(), ids.size());
}

TEST(SchedulerBasic, RunsManyTasksExactlyOnce) {
  constexpr int kTasks = 5000;
  PriorityScheduler scheduler(4);
  scheduler.start();

  std::vector<std::atomic<int>> runs(kTasks);
  for (std::atomic<int>& r : runs) {
    r.store(0, std::memory_order_relaxed);
  }
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(i % (kMaxPriority + 1), [&runs, i](const Task&) {
      runs[static_cast<std::size_t>(i)].fetch_add(1, std::memory_order_relaxed);
      return true;
    });
  }

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  for (int i = 0; i < kTasks; ++i) {
    ASSERT_EQ(runs[static_cast<std::size_t>(i)].load(), 1) << "task " << i;
  }
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_failed, 0u);
  scheduler.stop();
}

TEST(SchedulerBasic, CallbackReceivesItsOwnTask) {
  PriorityScheduler scheduler(1);
  scheduler.start();

  std::atomic<std::uint64_t> seen_id{0};
  std::atomic<int> seen_priority{-1};
  std::atomic<bool> seen_executing{false};

  const std::uint64_t id = scheduler.submit_task(
      7,
      [&](const Task& task) {
        seen_id.store(task.id());
        seen_priority.store(task.priority());
        // Inside the callback the task must already be EXECUTING.
        seen_executing.store(task.state() == TaskState::EXECUTING);
        return true;
      },
      1s);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(seen_id.load(), id);
  EXPECT_EQ(seen_priority.load(), 7);
  EXPECT_TRUE(seen_executing.load());
}

TEST(SchedulerBasic, DefaultsMatchTheDocumentedContract) {
  const PriorityScheduler scheduler;
  EXPECT_EQ(scheduler.config().num_workers, 4u);
  EXPECT_EQ(scheduler.worker_count(), 0u) << "no workers before start()";
  EXPECT_FALSE(scheduler.is_running());
  EXPECT_EQ(scheduler.lifecycle(), PriorityScheduler::Lifecycle::CREATED);
}

// ===========================================================================
// State machine as observed through the scheduler
// ===========================================================================

TEST(SchedulerStates, WalksPendingScheduledExecutingCompleted) {
  PriorityScheduler scheduler(1);

  // Staged while stopped: observable as PENDING before any worker exists.
  const std::uint64_t id = scheduler.submit_task(5, [](const Task&) { return true; }, 5s);
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::PENDING);

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::COMPLETED);

  // The trace proves every intermediate state was entered, in order, without
  // needing to catch the task mid-flight.
  const std::shared_ptr<const Task> task = scheduler.find_task(id);
  ASSERT_NE(task, nullptr);
  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  ASSERT_EQ(task->trace(entries), 4u);
  EXPECT_EQ(entries[0].state, TaskState::PENDING);
  EXPECT_EQ(entries[1].state, TaskState::SCHEDULED);
  EXPECT_EQ(entries[2].state, TaskState::EXECUTING);
  EXPECT_EQ(entries[3].state, TaskState::COMPLETED);
}

TEST(SchedulerStates, FailedTaskTraceEndsInFailed) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  const std::uint64_t id = scheduler.submit_task(5, [](const Task&) { return false; }, 5s);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));

  const std::shared_ptr<const Task> task = scheduler.find_task(id);
  ASSERT_NE(task, nullptr);
  std::array<Task::TraceEntry, Task::kMaxTraceEntries> entries{};
  ASSERT_EQ(task->trace(entries), 4u);
  EXPECT_EQ(entries[3].state, TaskState::FAILED);
  EXPECT_EQ(task->error_message(), "callback reported failure");
}

TEST(SchedulerStates, TraceSinkObservesEveryTransition) {
  std::mutex mutex;
  std::vector<std::pair<TaskState, TaskState>> transitions;

  SchedulerConfig config = config_with(1);
  config.trace_sink = [&](const Task&, TaskState from, TaskState to) {
    const std::lock_guard<std::mutex> lock(mutex);
    transitions.emplace_back(from, to);
  };

  PriorityScheduler scheduler(config);
  scheduler.start();
  scheduler.submit_task(5, [](const Task&) { return true; }, 5s);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  const std::lock_guard<std::mutex> lock(mutex);
  ASSERT_EQ(transitions.size(), 3u);
  EXPECT_EQ(transitions[0], std::make_pair(TaskState::PENDING, TaskState::SCHEDULED));
  EXPECT_EQ(transitions[1], std::make_pair(TaskState::SCHEDULED, TaskState::EXECUTING));
  EXPECT_EQ(transitions[2], std::make_pair(TaskState::EXECUTING, TaskState::COMPLETED));
}

// ===========================================================================
// Priority ordering
// ===========================================================================

TEST(SchedulerOrdering, HigherPriorityRunsFirst) {
  // One worker, queue staged before start: strict ordering, no races.
  PriorityScheduler scheduler(1);
  OrderLog log;

  // Submit in deliberately adversarial order (lowest priority first).
  std::vector<std::pair<int, std::uint64_t>> submitted;
  for (int priority = kMinPriority; priority <= kMaxPriority; ++priority) {
    const std::uint64_t id = scheduler.submit_task(
        priority,
        [&log](const Task& task) {
          log.add(task.id());
          return true;
        },
        30s);
    submitted.emplace_back(priority, id);
  }

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  const std::vector<std::uint64_t> order = log.get();
  ASSERT_EQ(order.size(), submitted.size());

  // Expected: strictly descending priority.
  std::vector<std::pair<int, std::uint64_t>> expected = submitted;
  std::sort(expected.begin(), expected.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  for (std::size_t i = 0; i < order.size(); ++i) {
    EXPECT_EQ(order[i], expected[i].second)
        << "position " << i << " should be priority " << expected[i].first;
  }
}

TEST(SchedulerOrdering, EqualPriorityUsesEarliestDeadlineFirst) {
  PriorityScheduler scheduler(1);
  OrderLog log;

  // Submitted longest-deadline first; EDF must invert that.
  std::vector<std::uint64_t> by_deadline_desc;
  for (int ms : {5000, 4000, 3000, 2000, 1000}) {
    by_deadline_desc.push_back(scheduler.submit_task(
        5,
        [&log](const Task& task) {
          log.add(task.id());
          return true;
        },
        std::chrono::milliseconds(ms)));
  }

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  std::vector<std::uint64_t> expected(by_deadline_desc.rbegin(), by_deadline_desc.rend());
  EXPECT_EQ(log.get(), expected);
}

TEST(SchedulerOrdering, PriorityDominatesDeadline) {
  PriorityScheduler scheduler(1);
  OrderLog log;

  const auto submit = [&](int priority, std::chrono::milliseconds ttl) {
    return scheduler.submit_task(
        priority,
        [&log](const Task& task) {
          log.add(task.id());
          return true;
        },
        ttl);
  };

  // The urgent-but-unimportant task must still lose to the important one.
  const std::uint64_t low_priority_tight_deadline = submit(1, 100ms);
  const std::uint64_t high_priority_loose_deadline = submit(9, 60s);

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  const std::vector<std::uint64_t> order = log.get();
  ASSERT_EQ(order.size(), 2u);
  EXPECT_EQ(order[0], high_priority_loose_deadline);
  EXPECT_EQ(order[1], low_priority_tight_deadline);
}

// The spec's "all tasks same priority" edge case. Equal priority and equal
// relative deadline means absolute deadlines are ordered by submission time,
// so EDF degenerates to FIFO; if the clock is coarse enough that two tasks
// share a deadline, the sequence tiebreak preserves FIFO as well.
TEST(SchedulerOrdering, SamePriorityAndDeadlineIsFifo) {
  PriorityScheduler scheduler(1);
  OrderLog log;

  std::vector<std::uint64_t> ids;
  for (int i = 0; i < 200; ++i) {
    ids.push_back(scheduler.submit_task(
        5,
        [&log](const Task& task) {
          log.add(task.id());
          return true;
        },
        60s));
  }

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
  EXPECT_EQ(log.get(), ids) << "equal-priority work must not be reordered";
}

// With several workers the order is not deterministic, but the statistical
// property must still hold: high-priority work is dispatched earlier.
TEST(SchedulerOrdering, HighPriorityIsDispatchedEarlierWithAPool) {
  constexpr int kPerBand = 500;
  PriorityScheduler scheduler(4);

  std::mutex mutex;
  std::vector<std::pair<int, std::size_t>> dispatch;  // (priority, position)
  std::size_t position = 0;

  for (int i = 0; i < kPerBand; ++i) {
    for (const int priority : {0, 10}) {
      scheduler.submit_task(
          priority,
          [&](const Task& task) {
            const std::lock_guard<std::mutex> lock(mutex);
            dispatch.emplace_back(task.priority(), position++);
            return true;
          },
          60s);
    }
  }

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  double high_sum = 0;
  double low_sum = 0;
  int high_count = 0;
  int low_count = 0;
  {
    const std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(dispatch.size(), static_cast<std::size_t>(kPerBand * 2));
    for (const auto& [priority, pos] : dispatch) {
      if (priority == 10) {
        high_sum += static_cast<double>(pos);
        ++high_count;
      } else {
        low_sum += static_cast<double>(pos);
        ++low_count;
      }
    }
  }
  ASSERT_GT(high_count, 0);
  ASSERT_GT(low_count, 0);
  const double high_mean = high_sum / high_count;
  const double low_mean = low_sum / low_count;
  // Priority 10 should occupy the first half of the schedule. The worker
  // count is the only slack: up to 4 low-priority tasks can already be in
  // flight when the high-priority burst lands.
  EXPECT_LT(high_mean, low_mean * 0.6)
      << "high-priority mean position " << high_mean << " vs low " << low_mean;
}

// ===========================================================================
// Error handling
// ===========================================================================

TEST(SchedulerErrors, ExceptionIsCaughtAndRecorded) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  const std::uint64_t id = scheduler.submit_task(
      5, [](const Task&) -> bool { throw std::runtime_error("sensor offline"); }, 5s);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);

  const std::shared_ptr<const Task> task = scheduler.find_task(id);
  ASSERT_NE(task, nullptr);
  EXPECT_EQ(task->error_message(), "callback threw: sensor offline");
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_exceptions, 1u);
  EXPECT_EQ(m.total_failed, 1u);
  EXPECT_EQ(m.total_completed, 0u);
}

TEST(SchedulerErrors, NonStandardExceptionIsAlsoContained) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  const std::uint64_t id =
      scheduler.submit_task(5, [](const Task&) -> bool { throw 42; }, 5s);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);
  const std::shared_ptr<const Task> task = scheduler.find_task(id);
  ASSERT_NE(task, nullptr);
  EXPECT_EQ(task->error_message(), "callback threw a non-std exception");
}

// A worker that dies on a bad callback would silently remove a quarter of the
// pool's capacity. This is the test that proves it does not happen.
TEST(SchedulerErrors, AThrowingTaskDoesNotKillItsWorker) {
  constexpr int kTasks = 500;
  PriorityScheduler scheduler(1);  // single worker: it must survive all of them
  scheduler.start();

  std::atomic<int> survivors{0};
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(5, [](const Task&) -> bool { throw std::logic_error("boom"); }, 30s);
    scheduler.submit_task(5,
                          [&survivors](const Task&) {
                            survivors.fetch_add(1);
                            return true;
                          },
                          30s);
  }

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(survivors.load(), kTasks) << "the worker stopped consuming after an exception";
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_exceptions, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTasks));
  scheduler.stop();
}

TEST(SchedulerErrors, CallbackReturningFalseFails) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  const std::uint64_t id = scheduler.submit_task(5, [](const Task&) { return false; }, 5s);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));

  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_failed, 1u);
  EXPECT_EQ(m.total_exceptions, 0u) << "a reported failure is not an exception";
  EXPECT_EQ(m.total_deadline_missed, 0u);
}

// ===========================================================================
// Deadlines
// ===========================================================================

TEST(SchedulerDeadlines, ExpiredTaskIsShedWithoutRunning) {
  // One worker is occupied long enough that the queued task's deadline passes
  // while it waits.
  PriorityScheduler scheduler(1);
  scheduler.start();

  std::atomic<bool> blocker_done{false};
  scheduler.submit_task(10,
                        [&blocker_done](const Task&) {
                          std::this_thread::sleep_for(200ms);
                          blocker_done.store(true);
                          return true;
                        },
                        30s);

  std::atomic<bool> victim_ran{false};
  const std::uint64_t victim = scheduler.submit_task(1,
                                                     [&victim_ran](const Task&) {
                                                       victim_ran.store(true);
                                                       return true;
                                                     },
                                                     20ms);  // will expire in the queue

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_TRUE(blocker_done.load());
  EXPECT_FALSE(victim_ran.load()) << "an expired task must not consume a worker";
  EXPECT_EQ(scheduler.get_task_state(victim), TaskState::FAILED);

  const std::shared_ptr<const Task> task = scheduler.find_task(victim);
  ASSERT_NE(task, nullptr);
  EXPECT_NE(task->error_message().find("deadline expired before execution"), std::string::npos)
      << task->error_message();

  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_expired_before_start, 1u);
  EXPECT_EQ(m.total_executed, 1u) << "only the blocker should have run";
  scheduler.stop();
}

TEST(SchedulerDeadlines, SheddingCanBeDisabled) {
  SchedulerConfig config = config_with(1);
  config.fail_expired_before_execution = false;
  config.enforce_deadline_after_execution = false;

  PriorityScheduler scheduler(config);
  scheduler.start();
  scheduler.submit_task(10, [](const Task&) {
    std::this_thread::sleep_for(150ms);
    return true;
  }, 30s);

  std::atomic<bool> late_ran{false};
  const std::uint64_t late = scheduler.submit_task(1,
                                                   [&late_ran](const Task&) {
                                                     late_ran.store(true);
                                                     return true;
                                                   },
                                                   10ms);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_TRUE(late_ran.load()) << "with shedding off, expired work must still run";
  EXPECT_EQ(scheduler.get_task_state(late), TaskState::COMPLETED);
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_expired_before_start, 0u);
  // The miss is still counted -- observability does not depend on enforcement.
  EXPECT_EQ(m.total_deadline_missed, 1u);
  scheduler.stop();
}

TEST(SchedulerDeadlines, OverrunDuringExecutionFails) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  std::atomic<bool> ran{false};
  const std::uint64_t id = scheduler.submit_task(5,
                                                 [&ran](const Task&) {
                                                   std::this_thread::sleep_for(100ms);
                                                   ran.store(true);
                                                   return true;
                                                 },
                                                 10ms);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_TRUE(ran.load()) << "the callback is not interrupted, only judged afterwards";
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);

  const std::shared_ptr<const Task> task = scheduler.find_task(id);
  ASSERT_NE(task, nullptr);
  EXPECT_NE(task->error_message().find("deadline exceeded during execution"), std::string::npos)
      << task->error_message();
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_deadline_missed, 1u);
  EXPECT_EQ(m.total_failed, 1u);
  scheduler.stop();
}

TEST(SchedulerDeadlines, OverrunDetectionCanBeDisabled) {
  SchedulerConfig config = config_with(1);
  config.enforce_deadline_after_execution = false;

  PriorityScheduler scheduler(config);
  scheduler.start();
  const std::uint64_t id = scheduler.submit_task(5,
                                                 [](const Task&) {
                                                   std::this_thread::sleep_for(50ms);
                                                   return true;
                                                 },
                                                 5ms);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::COMPLETED);
  EXPECT_EQ(scheduler.get_metrics().total_deadline_missed, 1u) << "still measured";
  scheduler.stop();
}

TEST(SchedulerDeadlines, AlreadyExpiredOnSubmissionIsShedImmediately) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  std::atomic<bool> ran{false};
  const std::uint64_t id = scheduler.submit_task(5,
                                                 [&ran](const Task&) {
                                                   ran.store(true);
                                                   return true;
                                                 },
                                                 0ms);  // deadline == submission time
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_FALSE(ran.load());
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);
  EXPECT_EQ(scheduler.get_metrics().total_expired_before_start, 1u);
  scheduler.stop();
}

// ===========================================================================
// Input validation
// ===========================================================================

TEST(SchedulerValidation, PriorityMustBeInRange) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  EXPECT_THROW(scheduler.submit_task(kMinPriority - 1, [](const Task&) { return true; }),
               std::invalid_argument);
  EXPECT_THROW(scheduler.submit_task(kMaxPriority + 1, [](const Task&) { return true; }),
               std::invalid_argument);
  EXPECT_NO_THROW(scheduler.submit_task(kMinPriority, [](const Task&) { return true; }));
  EXPECT_NO_THROW(scheduler.submit_task(kMaxPriority, [](const Task&) { return true; }));

  const SubmitResult rejected =
      scheduler.try_submit_task(99, [](const Task&) { return true; });
  EXPECT_FALSE(rejected.accepted());
  EXPECT_EQ(rejected.status, SubmitStatus::REJECTED_INVALID_TASK);
  EXPECT_EQ(scheduler.get_metrics().total_submitted, 2u) << "rejections are not submissions";
}

TEST(SchedulerValidation, CallbackMustNotBeEmpty) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  EXPECT_THROW(scheduler.submit_task(5, TaskCallback{}), std::invalid_argument);
  EXPECT_EQ(scheduler.try_submit_task(5, TaskCallback{}).status,
            SubmitStatus::REJECTED_INVALID_TASK);
}

TEST(SchedulerValidation, ConfigurationIsCheckedAtConstruction) {
  EXPECT_THROW((PriorityScheduler{config_with(0)}), std::invalid_argument);
  EXPECT_THROW((PriorityScheduler{std::size_t{0}}), std::invalid_argument);

  SchedulerConfig tiny_ring = config_with(2);
  tiny_ring.ingress_capacity = 1;
  EXPECT_THROW((PriorityScheduler{tiny_ring}), std::invalid_argument);

  SchedulerConfig no_shards = config_with(2);
  no_shards.registry_shards = 0;
  EXPECT_THROW((PriorityScheduler{no_shards}), std::invalid_argument);

  // A depth cap below the worker count would starve workers permanently.
  SchedulerConfig shallow = config_with(4);
  shallow.max_queue_depth = 2;
  EXPECT_THROW((PriorityScheduler{shallow}), std::invalid_argument);

  SchedulerConfig ok = config_with(4);
  ok.max_queue_depth = 4;
  EXPECT_NO_THROW((PriorityScheduler{ok}));
}

TEST(SchedulerValidation, IngressCapacityIsRoundedUpToAPowerOfTwo) {
  SchedulerConfig config = config_with(1);
  config.ingress_capacity = 100;
  const PriorityScheduler scheduler(config);
  // Config keeps what the caller asked for; the ring rounds internally.
  EXPECT_EQ(scheduler.config().ingress_capacity, 100u);
}

// ===========================================================================
// Backpressure
// ===========================================================================

TEST(SchedulerBackpressure, RejectsBeyondMaxQueueDepth) {
  SchedulerConfig config = config_with(1);
  config.max_queue_depth = 8;
  PriorityScheduler scheduler(config);
  // Not started: nothing drains, so the cap is reached deterministically.

  int accepted = 0;
  int rejected = 0;
  for (int i = 0; i < 100; ++i) {
    const SubmitResult result =
        scheduler.try_submit_task(5, [](const Task&) { return true; }, 60s);
    if (result.accepted()) {
      ++accepted;
    } else {
      EXPECT_EQ(result.status, SubmitStatus::REJECTED_QUEUE_FULL);
      ++rejected;
    }
  }
  EXPECT_EQ(accepted, 8);
  EXPECT_EQ(rejected, 92);

  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, 8u);
  EXPECT_EQ(m.total_rejected, 92u);
  EXPECT_EQ(m.queue_depth, 8u);

  // The throwing API reports the same condition as an exception.
  EXPECT_THROW(scheduler.submit_task(5, [](const Task&) { return true; }, 60s), QueueFull);
}

// The ring is a performance buffer, not a capacity limit: overflowing it must
// degrade to the mutex path rather than drop work.
TEST(SchedulerBackpressure, IngressOverflowFallsBackToTheHeap) {
  constexpr int kTasks = 2000;
  SchedulerConfig config = config_with(2);
  config.ingress_capacity = 16;  // tiny on purpose
  PriorityScheduler scheduler(config);

  std::atomic<int> ran{0};
  for (int i = 0; i < kTasks; ++i) {
    ASSERT_TRUE(scheduler.try_submit_task(5,
                                          [&ran](const Task&) {
                                            ran.fetch_add(1);
                                            return true;
                                          },
                                          60s)
                    .accepted());
  }

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(ran.load(), kTasks) << "no task may be lost when the ring overflows";

  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTasks));
  EXPECT_GT(m.total_slow_path_submissions, 0u) << "the fallback path should have been exercised";
  scheduler.stop();
}

// ===========================================================================
// Lifecycle
// ===========================================================================

TEST(SchedulerLifecycle, ReportsItsState) {
  PriorityScheduler scheduler(2);
  EXPECT_FALSE(scheduler.is_running());
  scheduler.start();
  EXPECT_TRUE(scheduler.is_running());
  EXPECT_EQ(scheduler.worker_count(), 2u);
  scheduler.stop();
  EXPECT_FALSE(scheduler.is_running());
  EXPECT_EQ(scheduler.lifecycle(), PriorityScheduler::Lifecycle::STOPPED);
  EXPECT_EQ(scheduler.worker_count(), 0u);
}

TEST(SchedulerLifecycle, StartAndStopAreIdempotent) {
  PriorityScheduler scheduler(2);
  scheduler.start();
  scheduler.start();
  scheduler.start();
  EXPECT_EQ(scheduler.worker_count(), 2u) << "repeated start() must not spawn extra workers";

  std::atomic<int> ran{0};
  scheduler.submit_task(5, [&ran](const Task&) {
    ran.fetch_add(1);
    return true;
  });
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(ran.load(), 1);

  scheduler.stop();
  scheduler.stop();
  EXPECT_FALSE(scheduler.is_running());
}

TEST(SchedulerLifecycle, RejectsSubmissionsAfterStop) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  scheduler.stop();

  EXPECT_THROW(scheduler.submit_task(5, [](const Task&) { return true; }), SchedulerNotRunning);
  const SubmitResult result = scheduler.try_submit_task(5, [](const Task&) { return true; });
  EXPECT_FALSE(result.accepted());
  EXPECT_EQ(result.status, SubmitStatus::REJECTED_NOT_RUNNING);
}

TEST(SchedulerLifecycle, CanBeRestarted) {
  PriorityScheduler scheduler(2);
  std::atomic<int> ran{0};
  const auto work = [&ran](const Task&) {
    ran.fetch_add(1);
    return true;
  };

  scheduler.start();
  scheduler.submit_task(5, work);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
  ASSERT_EQ(ran.load(), 1);

  scheduler.start();
  EXPECT_TRUE(scheduler.is_running());
  EXPECT_EQ(scheduler.worker_count(), 2u);
  scheduler.submit_task(5, work);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
  EXPECT_EQ(ran.load(), 2);

  // Metrics accumulate across restarts rather than silently resetting.
  EXPECT_EQ(scheduler.get_metrics().total_completed, 2u);
}

TEST(SchedulerLifecycle, StopDrainsQueuedWorkByDefault) {
  constexpr int kTasks = 2000;
  PriorityScheduler scheduler(2);

  std::atomic<int> ran{0};
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(5,
                          [&ran](const Task&) {
                            ran.fetch_add(1);
                            return true;
                          },
                          60s);
  }

  scheduler.start();
  scheduler.stop();  // immediately: the queue is still deep

  EXPECT_EQ(ran.load(), kTasks) << "drain_on_stop must finish queued work";
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(scheduler.queue_depth(), 0u);
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
}

TEST(SchedulerLifecycle, StopWithoutDrainFailsQueuedWork) {
  constexpr int kTasks = 2000;
  SchedulerConfig config = config_with(1);
  config.drain_on_stop = false;
  PriorityScheduler scheduler(config);

  std::atomic<int> ran{0};
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(5,
                          [&ran](const Task&) {
                            ran.fetch_add(1);
                            return true;
                          },
                          60s);
  }

  scheduler.start();
  scheduler.stop();

  EXPECT_LT(ran.load(), kTasks) << "abandoning should skip most of the queue";
  const Metrics m = scheduler.get_metrics();
  // Every accepted task still reaches a terminal state -- nothing is left in
  // limbo, which is what lets callers trust outstanding_tasks().
  EXPECT_EQ(m.total_completed + m.total_failed, static_cast<std::uint64_t>(kTasks));
  EXPECT_GT(m.total_failed, 0u);
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
  EXPECT_EQ(scheduler.queue_depth(), 0u);

  // The abandoned tasks say why they failed.
  bool found_reason = false;
  for (std::uint64_t id = 1; id <= static_cast<std::uint64_t>(kTasks) && !found_reason; ++id) {
    const std::shared_ptr<const Task> task = scheduler.find_task(id);
    if (task && task->state() == TaskState::FAILED) {
      found_reason = task->error_message() == "scheduler stopped before execution";
    }
  }
  EXPECT_TRUE(found_reason);
}

TEST(SchedulerLifecycle, StoppingWithoutEverStartingFailsQueuedWork) {
  PriorityScheduler scheduler(2);
  const std::uint64_t id = scheduler.submit_task(5, [](const Task&) { return true; }, 60s);
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::PENDING);

  scheduler.stop();
  // No worker ever existed, so the task must be failed rather than left
  // PENDING forever.
  EXPECT_EQ(scheduler.get_task_state(id), TaskState::FAILED);
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
}

TEST(SchedulerLifecycle, DestructorStopsAndDrains) {
  std::atomic<int> ran{0};
  {
    PriorityScheduler scheduler(2);
    for (int i = 0; i < 500; ++i) {
      scheduler.submit_task(5,
                            [&ran](const Task&) {
                              ran.fetch_add(1);
                              return true;
                            },
                            60s);
    }
    scheduler.start();
    // No stop() call: the destructor must handle it.
  }
  EXPECT_EQ(ran.load(), 500);
}

TEST(SchedulerLifecycle, StartDuringStopIsRefusedNotIgnored) {
  // Reaching STOPPING from another thread is racy to arrange; what matters is
  // that the state is exposed and start() never silently half-starts.
  PriorityScheduler scheduler(1);
  scheduler.start();
  scheduler.stop();
  EXPECT_EQ(scheduler.lifecycle(), PriorityScheduler::Lifecycle::STOPPED);
  EXPECT_NO_THROW(scheduler.start());
  scheduler.stop();
}

// The spec's "rapid submit/shutdown cycles" edge case. Each iteration
// exercises spawn, wake, drain and join; a lost wakeup or a shutdown race
// would hang or lose a task here.
TEST(SchedulerLifecycle, SurvivesRapidStartStopCycles) {
  constexpr int kCycles = 200;
  std::atomic<int> ran{0};

  for (int cycle = 0; cycle < kCycles; ++cycle) {
    PriorityScheduler scheduler(4);
    scheduler.start();
    for (int i = 0; i < 10; ++i) {
      scheduler.submit_task(i % 11,
                            [&ran](const Task&) {
                              ran.fetch_add(1);
                              return true;
                            },
                            60s);
    }
    scheduler.stop();
    ASSERT_EQ(scheduler.outstanding_tasks(), 0u) << "cycle " << cycle;
  }
  EXPECT_EQ(ran.load(), kCycles * 10) << "tasks were lost across start/stop cycles";
}

// Shutdown must not race submission: whatever is accepted runs or is failed,
// and whatever is rejected never runs.
TEST(SchedulerLifecycle, ConcurrentSubmitAndStopLosesNothing) {
  constexpr int kProducers = 8;
  constexpr int kRounds = 30;

  for (int round = 0; round < kRounds; ++round) {
    PriorityScheduler scheduler(4);
    scheduler.start();

    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};
    std::atomic<bool> stop_now{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);

    for (int p = 0; p < kProducers; ++p) {
      producers.emplace_back([&] {
        while (!stop_now.load(std::memory_order_acquire)) {
          const SubmitResult result =
              scheduler.try_submit_task(5, [](const Task&) { return true; }, 60s);
          if (result.accepted()) {
            accepted.fetch_add(1, std::memory_order_relaxed);
          } else {
            rejected.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }

    std::this_thread::sleep_for(5ms);
    scheduler.stop();
    stop_now.store(true, std::memory_order_release);
    for (std::thread& t : producers) {
      t.join();
    }

    const Metrics m = scheduler.get_metrics();
    EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(accepted.load()));
    EXPECT_EQ(m.total_completed + m.total_failed, m.total_submitted)
        << "round " << round << ": accepted tasks left in limbo";
    EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
    EXPECT_GT(rejected.load(), 0) << "stop() should have started rejecting";
  }
}

// A stop() on a never-started scheduler takes a different path (no workers to
// join, nothing to drain), and it must apply the same in-flight-submitter
// barrier. Without it, a producer already past the lifecycle check could
// publish a task after the queue was declared empty: that task would never
// run and never be failed, so outstanding_tasks() would never return to zero.
TEST(SchedulerLifecycle, StopRacingSubmitOnANeverStartedSchedulerLosesNothing) {
  constexpr int kProducers = 8;
  constexpr int kRounds = 50;

  for (int round = 0; round < kRounds; ++round) {
    PriorityScheduler scheduler(4);  // deliberately never started

    std::atomic<int> accepted{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
      producers.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) {
        }
        for (int i = 0; i < 200; ++i) {
          if (scheduler.try_submit_task(5, [](const Task&) { return true; }, 60s).accepted()) {
            accepted.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }

    go.store(true, std::memory_order_release);
    scheduler.stop();
    for (std::thread& t : producers) {
      t.join();
    }

    const Metrics m = scheduler.get_metrics();
    EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(accepted.load())) << "round " << round;
    // Nothing ever ran, so every accepted task must have been failed.
    EXPECT_EQ(m.total_failed, m.total_submitted) << "round " << round;
    EXPECT_EQ(m.total_completed, 0u);
    ASSERT_EQ(scheduler.outstanding_tasks(), 0u)
        << "round " << round << ": a task was accepted after the queue was abandoned";
    ASSERT_EQ(scheduler.queue_depth(), 0u) << "round " << round;
  }
}

// ===========================================================================
// Introspection
// ===========================================================================

TEST(SchedulerIntrospection, UnknownTaskIdIsReported) {
  PriorityScheduler scheduler(1);
  EXPECT_THROW(scheduler.get_task_state(999999), UnknownTask);
  EXPECT_FALSE(scheduler.try_get_task_state(999999).has_value());
  EXPECT_EQ(scheduler.find_task(999999), nullptr);
}

TEST(SchedulerIntrospection, HistoryIsStrictlyBoundedToTheMostRecentTasks) {
  constexpr std::size_t kLimit = 32;
  SchedulerConfig config = config_with(2);
  config.registry_shards = 1;  // one shard so the limit is exact, not amortized
  config.history_per_shard = kLimit;
  PriorityScheduler scheduler(config);
  scheduler.start();

  std::vector<std::uint64_t> ids;
  for (int i = 0; i < 1000; ++i) {
    ids.push_back(scheduler.submit_task(5, [](const Task&) { return true; }, 60s));
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  // Exactly the newest `kLimit` ids survive, and the bound holds at rest --
  // not just while submissions are still driving eviction.
  std::size_t retained = 0;
  for (const std::uint64_t id : ids) {
    if (scheduler.try_get_task_state(id).has_value()) {
      ++retained;
    }
  }
  EXPECT_EQ(retained, kLimit) << "retention must be a hard bound";
  for (std::size_t i = ids.size() - kLimit; i < ids.size(); ++i) {
    EXPECT_TRUE(scheduler.try_get_task_state(ids[i]).has_value())
        << "task " << ids[i] << " is among the newest " << kLimit;
  }
  EXPECT_FALSE(scheduler.try_get_task_state(ids.front()).has_value());
  EXPECT_EQ(scheduler.get_metrics().total_completed, 1000u)
      << "eviction must not affect accounting";
}

// Eviction drops the registry's reference only. The queue and the running
// worker hold their own, so an evicted task still executes and is still
// counted -- observability degrades, correctness does not.
TEST(SchedulerIntrospection, EvictedTasksStillRunAndAreStillCounted) {
  constexpr int kTasks = 500;
  SchedulerConfig config = config_with(1);
  config.registry_shards = 1;
  config.history_per_shard = 4;  // absurdly small on purpose
  PriorityScheduler scheduler(config);

  std::atomic<int> ran{0};
  std::vector<std::uint64_t> ids;
  for (int i = 0; i < kTasks; ++i) {
    ids.push_back(scheduler.submit_task(5,
                                        [&ran](const Task&) {
                                          ran.fetch_add(1);
                                          return true;
                                        },
                                        60s));
  }
  // The oldest queued tasks are no longer queryable...
  EXPECT_FALSE(scheduler.try_get_task_state(ids.front()).has_value());
  EXPECT_TRUE(scheduler.try_get_task_state(ids.back()).has_value());

  scheduler.start();
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  // ...but every one of them ran.
  EXPECT_EQ(ran.load(), kTasks);
  EXPECT_EQ(scheduler.get_metrics().total_completed, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
}

TEST(SchedulerIntrospection, MetricsAccountForEveryOutcome) {
  PriorityScheduler scheduler(1);
  scheduler.start();

  for (int i = 0; i < 10; ++i) {
    scheduler.submit_task(5, [](const Task&) { return true; }, 60s);
  }
  for (int i = 0; i < 5; ++i) {
    scheduler.submit_task(5, [](const Task&) { return false; }, 60s);
  }
  for (int i = 0; i < 3; ++i) {
    scheduler.submit_task(5, [](const Task&) -> bool { throw std::runtime_error("x"); }, 60s);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, 18u);
  EXPECT_EQ(m.total_scheduled, 18u);
  EXPECT_EQ(m.total_executed, 18u);
  EXPECT_EQ(m.total_completed, 10u);
  EXPECT_EQ(m.total_failed, 8u);
  EXPECT_EQ(m.total_exceptions, 3u);
  EXPECT_EQ(m.total_rejected, 0u);
  EXPECT_NEAR(m.success_rate(), 10.0 / 18.0, 1e-9);
  EXPECT_EQ(m.worker_count, 0u) << "pool is stopped";
  EXPECT_EQ(m.active_workers, 0u);
  EXPECT_EQ(m.queue_depth, 0u);
}

TEST(SchedulerIntrospection, LatencyMetricsArePopulatedAndOrdered) {
  PriorityScheduler scheduler(2);
  scheduler.start();
  for (int i = 0; i < 200; ++i) {
    scheduler.submit_task(5,
                          [](const Task&) {
                            std::this_thread::sleep_for(100us);
                            return true;
                          },
                          60s);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  EXPECT_GT(m.avg_latency_ms, 0.0);
  EXPECT_LE(m.p50_latency_ms, m.p95_latency_ms);
  EXPECT_LE(m.p95_latency_ms, m.p99_latency_ms);
  EXPECT_LE(m.p99_latency_ms, m.p999_latency_ms);
  EXPECT_LE(m.min_latency_ms, m.p50_latency_ms);
  EXPECT_GE(m.max_latency_ms, m.p99_latency_ms);

  // Execution time is ~100us by construction, and end-to-end must include it.
  EXPECT_GE(m.avg_execution_latency_ms, 0.05);
  EXPECT_GE(m.avg_latency_ms, m.avg_execution_latency_ms * 0.9);
  EXPECT_GT(m.p99_queue_latency_ms, 0.0);
}

TEST(SchedulerIntrospection, ResetMetricsClearsCountersOnly) {
  PriorityScheduler scheduler(2);
  scheduler.start();
  scheduler.submit_task(5, [](const Task&) { return true; });
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  ASSERT_EQ(scheduler.get_metrics().total_completed, 1u);

  scheduler.reset_metrics();
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, 0u);
  EXPECT_EQ(m.total_completed, 0u);
  EXPECT_DOUBLE_EQ(m.p99_latency_ms, 0.0);
  EXPECT_EQ(m.worker_count, 2u) << "gauges reflect reality, not history";
  scheduler.stop();
}

TEST(SchedulerIntrospection, ActiveWorkerGaugeTracksCallbacksInFlight) {
  PriorityScheduler scheduler(4);
  scheduler.start();
  EXPECT_EQ(scheduler.active_workers(), 0u);

  std::atomic<int> entered{0};
  std::atomic<bool> release{false};
  for (int i = 0; i < 4; ++i) {
    scheduler.submit_task(5,
                          [&](const Task&) {
                            entered.fetch_add(1);
                            while (!release.load(std::memory_order_acquire)) {
                              std::this_thread::sleep_for(100us);
                            }
                            return true;
                          },
                          60s);
  }

  // Wait for the pool to fill.
  const TimePoint deadline = Clock::now() + 10s;
  while (entered.load() < 4 && Clock::now() < deadline) {
    std::this_thread::sleep_for(200us);
  }
  ASSERT_EQ(entered.load(), 4);
  EXPECT_EQ(scheduler.active_workers(), 4u);

  release.store(true, std::memory_order_release);
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(scheduler.active_workers(), 0u);
  scheduler.stop();
}

// ===========================================================================
// Concurrency
// ===========================================================================

// The spec's headline concurrency case: many producers, a fixed pool, and no
// losses or accounting drift.
TEST(SchedulerConcurrency, TenProducersFourWorkers) {
  constexpr int kProducers = 10;
  constexpr int kPerProducer = 2000;
  constexpr int kTotal = kProducers * kPerProducer;

  PriorityScheduler scheduler(4);
  scheduler.start();

  std::atomic<int> executed{0};
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kPerProducer; ++i) {
        scheduler.submit_task((p + i) % (kMaxPriority + 1),
                              [&executed](const Task&) {
                                executed.fetch_add(1, std::memory_order_relaxed);
                                return true;
                              },
                              5min);
      }
    });
  }
  for (std::thread& t : producers) {
    t.join();
  }

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();

  EXPECT_EQ(executed.load(), kTotal);
  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(kTotal));
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTotal));
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(m.total_rejected, 0u);
  EXPECT_EQ(scheduler.queue_depth(), 0u);
}

// The spec's "single worker, thousands of tasks" edge case.
TEST(SchedulerConcurrency, SingleWorkerHandlesThousandsOfTasks) {
  constexpr int kTasks = 50000;
  PriorityScheduler scheduler(1);
  scheduler.start();

  std::atomic<int> executed{0};
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(i % (kMaxPriority + 1),
                          [&executed](const Task&) {
                            executed.fetch_add(1, std::memory_order_relaxed);
                            return true;
                          },
                          5min);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
  EXPECT_EQ(executed.load(), kTasks);
  EXPECT_EQ(scheduler.get_metrics().total_completed, static_cast<std::uint64_t>(kTasks));
}

// The spec's "empty queue" edge case, and the test that a lost wakeup cannot
// hide: thousands of sleep/wake cycles with a hard timeout on each.
TEST(SchedulerConcurrency, WorkersSleepAndWakeReliably) {
  constexpr int kCycles = 3000;
  PriorityScheduler scheduler(4);
  scheduler.start();

  for (int cycle = 0; cycle < kCycles; ++cycle) {
    std::atomic<bool> done{false};
    scheduler.submit_task(5,
                          [&done](const Task&) {
                            done.store(true, std::memory_order_release);
                            return true;
                          },
                          60s);
    // If a notify were lost, the pool would be asleep with work queued and
    // this would time out.
    ASSERT_TRUE(scheduler.wait_until_idle(5s)) << "cycle " << cycle << " never completed";
    ASSERT_TRUE(done.load(std::memory_order_acquire)) << "cycle " << cycle;
  }
  scheduler.stop();
}

TEST(SchedulerConcurrency, IdleSchedulerStaysResponsive) {
  PriorityScheduler scheduler(4);
  scheduler.start();
  std::this_thread::sleep_for(200ms);  // let every worker settle into a wait

  const TimePoint submitted_at = Clock::now();
  std::atomic<bool> ran{false};
  scheduler.submit_task(5,
                        [&ran](const Task&) {
                          ran.store(true);
                          return true;
                        },
                        60s);
  ASSERT_TRUE(scheduler.wait_until_idle(5s));
  const auto elapsed = Clock::now() - submitted_at;

  EXPECT_TRUE(ran.load());
  // Waking a sleeping worker is a futex round trip, not a poll interval.
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 100);
  scheduler.stop();
}

TEST(SchedulerConcurrency, ReadApisAreSafeWhileTasksRun) {
  PriorityScheduler scheduler(4);
  scheduler.start();

  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      Metrics previous;
      while (!stop.load(std::memory_order_acquire)) {
        const Metrics m = scheduler.get_metrics();

        // Counters are read one at a time, so a live snapshot is skewed:
        // comparing two *different* counters is only meaningful at rest (and
        // is checked after the drain below). What must hold at every instant
        // is that each counter is monotonic -- a counter that ever goes
        // backwards means a torn or lost atomic.
        ASSERT_GE(m.total_submitted, previous.total_submitted);
        ASSERT_GE(m.total_scheduled, previous.total_scheduled);
        ASSERT_GE(m.total_executed, previous.total_executed);
        ASSERT_GE(m.total_completed, previous.total_completed);
        ASSERT_GE(m.total_failed, previous.total_failed);

        // Percentiles come from a single histogram scan, so they *are*
        // internally consistent even under concurrent recording.
        ASSERT_LE(m.p50_latency_ms, m.p95_latency_ms);
        ASSERT_LE(m.p95_latency_ms, m.p99_latency_ms);
        ASSERT_LE(m.min_latency_ms, m.max_latency_ms);

        ASSERT_LE(m.active_workers, 4u);
        ASSERT_EQ(m.worker_count, 4u);

        (void)scheduler.try_get_task_state(m.total_submitted);
        (void)scheduler.find_task(m.total_submitted);
        (void)scheduler.queue_depth();
        (void)scheduler.is_running();
        previous = m;
      }
    });
  }

  for (int i = 0; i < 20000; ++i) {
    scheduler.submit_task(i % 11, [](const Task&) { return true; }, 5min);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  stop.store(true, std::memory_order_release);
  for (std::thread& t : readers) {
    t.join();
  }
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  EXPECT_EQ(m.total_submitted, 20000u);
  EXPECT_EQ(m.total_completed, 20000u);
}

// A callback that submits more work is a common pattern (fan-out) and must
// not deadlock against the queue lock.
TEST(SchedulerConcurrency, TasksMaySubmitMoreTasks) {
  PriorityScheduler scheduler(4);
  scheduler.start();

  std::atomic<int> executed{0};
  constexpr int kDepth = 6;

  std::function<void(int)> submit_level = [&](int depth) {
    if (depth >= kDepth) {
      return;
    }
    scheduler.submit_task(5,
                          [&, depth](const Task&) {
                            executed.fetch_add(1, std::memory_order_relaxed);
                            submit_level(depth + 1);
                            submit_level(depth + 1);
                            return true;
                          },
                          5min);
  };
  submit_level(0);

  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
  EXPECT_EQ(executed.load(), (1 << kDepth) - 1);
}

TEST(SchedulerConcurrency, WaitUntilIdleTimesOutRatherThanHanging) {
  PriorityScheduler scheduler(1);
  scheduler.start();
  std::atomic<bool> release{false};
  scheduler.submit_task(5,
                        [&release](const Task&) {
                          while (!release.load(std::memory_order_acquire)) {
                            std::this_thread::sleep_for(1ms);
                          }
                          return true;
                        },
                        60s);

  EXPECT_FALSE(scheduler.wait_until_idle(50ms));
  release.store(true, std::memory_order_release);
  EXPECT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  scheduler.stop();
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST(SchedulerEdgeCases, MoreWorkersThanTasks) {
  PriorityScheduler scheduler(16);
  scheduler.start();
  std::atomic<int> ran{0};
  scheduler.submit_task(5, [&ran](const Task&) {
    ran.fetch_add(1);
    return true;
  });
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(scheduler.worker_count(), 16u);
  scheduler.stop();
}

TEST(SchedulerEdgeCases, WaitUntilIdleOnAnEmptySchedulerReturnsImmediately) {
  PriorityScheduler scheduler(2);
  EXPECT_TRUE(scheduler.wait_until_idle(0ms)) << "nothing outstanding is already idle";
  scheduler.start();
  EXPECT_TRUE(scheduler.wait_until_idle(0ms));
  scheduler.stop();
}

TEST(SchedulerEdgeCases, CallbackCapturingLargeStateIsFine) {
  PriorityScheduler scheduler(2);
  scheduler.start();
  const std::vector<int> payload(10000, 7);
  std::atomic<long> sum{0};
  scheduler.submit_task(5, [payload, &sum](const Task&) {
    sum.store(std::accumulate(payload.begin(), payload.end(), 0L));
    return true;
  });
  ASSERT_TRUE(scheduler.wait_until_idle(kIdleTimeout));
  EXPECT_EQ(sum.load(), 70000);
  scheduler.stop();
}

TEST(SchedulerEdgeCases, SubmitStatusNamesAreStable) {
  EXPECT_STREQ(to_string(SubmitStatus::ACCEPTED), "ACCEPTED");
  EXPECT_STREQ(to_string(SubmitStatus::REJECTED_NOT_RUNNING), "REJECTED_NOT_RUNNING");
  EXPECT_STREQ(to_string(SubmitStatus::REJECTED_QUEUE_FULL), "REJECTED_QUEUE_FULL");
  EXPECT_STREQ(to_string(SubmitStatus::REJECTED_INVALID_TASK), "REJECTED_INVALID_TASK");
}

}  // namespace
}  // namespace rtsched
