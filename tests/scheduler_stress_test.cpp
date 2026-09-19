// Copyright 2026. SPDX-License-Identifier: MIT
//
// Stress tests: the four scenarios from the specification.
//
//   1. High throughput      -- 8 producers, 100k tasks, 4 workers
//   2. Mixed priority       -- 50/50 split, high priority must win
//   3. Deadline pressure    -- short deadlines, misses must be detected
//   4. Extreme contention   -- 16 producers, 2 workers
//
// These are correctness tests under load, not benchmarks: every assertion is
// an invariant that must hold at any speed, so they stay meaningful on a
// loaded CI box or under ThreadSanitizer. Throughput is printed for context
// but only asserted against a floor low enough to be machine-independent;
// benchmarks/scheduler_benchmark.cpp is where performance is measured.
//
// Run with:  ./build/rtsched_stress_tests
// TSan:      cmake -B build-tsan -DRTSCHED_ENABLE_TSAN=ON && ctest -L stress

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

#include "rtsched/scheduler.h"

namespace rtsched {
namespace {

using namespace std::chrono_literals;

constexpr std::chrono::milliseconds kDrainTimeout = 120s;

double seconds_since(TimePoint start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}

void print_result(const char* scenario, const Metrics& m, double elapsed_s) {
  const double throughput =
      elapsed_s > 0 ? static_cast<double>(m.total_completed + m.total_failed) / elapsed_s : 0.0;
  std::printf(
      "\n  [%s]\n"
      "    wall clock         : %.3f s\n"
      "    throughput         : %.0f tasks/s\n"
      "    submitted/completed: %llu / %llu (failed %llu, rejected %llu)\n"
      "    scheduling latency : p50=%.3fms p99=%.3fms max=%.3fms\n"
      "    end-to-end latency : p50=%.3fms p95=%.3fms p99=%.3fms p99.9=%.3fms\n"
      "    deadline misses    : %llu shed + %llu overran\n"
      "    ingress slow path  : %llu\n",
      scenario, elapsed_s, throughput, static_cast<unsigned long long>(m.total_submitted),
      static_cast<unsigned long long>(m.total_completed),
      static_cast<unsigned long long>(m.total_failed),
      static_cast<unsigned long long>(m.total_rejected), m.p50_queue_latency_ms,
      m.p99_queue_latency_ms, m.max_queue_latency_ms, m.p50_latency_ms, m.p95_latency_ms,
      m.p99_latency_ms, m.p999_latency_ms,
      static_cast<unsigned long long>(m.total_expired_before_start),
      static_cast<unsigned long long>(m.total_deadline_missed),
      static_cast<unsigned long long>(m.total_slow_path_submissions));
  std::fflush(stdout);
}

// ===========================================================================
// 1. High throughput
// ===========================================================================
//
// 8 producers submit 100k tasks total into a 4-worker pool. The assertions are
// the ones that matter for a control system: nothing is dropped, nothing is
// double-executed, and the books balance.

TEST(Stress, HighThroughput) {
  constexpr int kProducers = 8;
  constexpr int kPerProducer = 12500;  // 100k total
  constexpr int kTotal = kProducers * kPerProducer;

  SchedulerConfig config;
  config.num_workers = 4;
  config.ingress_capacity = 1 << 17;
  config.history_per_shard = 64;  // 100k live registry entries would be pointless

  PriorityScheduler scheduler(config);
  scheduler.start();

  // Per-task execution tally: a count alone could hide a double-execution
  // paired with a drop.
  std::vector<std::atomic<std::uint8_t>> executions(kTotal);
  for (std::atomic<std::uint8_t>& e : executions) {
    e.store(0, std::memory_order_relaxed);
  }

  const TimePoint started = Clock::now();
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kPerProducer; ++i) {
        const int slot = p * kPerProducer + i;
        scheduler.submit_task((p * 7 + i) % (kMaxPriority + 1),
                              [&executions, slot](const Task&) {
                                executions[static_cast<std::size_t>(slot)].fetch_add(
                                    1, std::memory_order_relaxed);
                                return true;
                              },
                              5min);  // deadlines are not the subject here
      }
    });
  }
  for (std::thread& t : producers) {
    t.join();
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout)) << "the pool failed to drain";
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("high_throughput: 8 producers -> 4 workers, 100k tasks", m, elapsed);

  int missing = 0;
  int duplicated = 0;
  for (const std::atomic<std::uint8_t>& e : executions) {
    const std::uint8_t count = e.load(std::memory_order_relaxed);
    if (count == 0) {
      ++missing;
    } else if (count > 1) {
      ++duplicated;
    }
  }
  EXPECT_EQ(missing, 0) << missing << " tasks were never executed";
  EXPECT_EQ(duplicated, 0) << duplicated << " tasks were executed more than once";

  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(kTotal));
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTotal));
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(m.total_rejected, 0u);
  EXPECT_EQ(m.total_scheduled, m.total_executed);
  EXPECT_EQ(scheduler.queue_depth(), 0u);
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);

  // Floor, not a target: 5k/s would indicate something is structurally wrong
  // (a serialized submission path, a lost-wakeup poll loop) rather than merely
  // a busy machine. Real numbers live in the benchmark suite.
  const double throughput = static_cast<double>(kTotal) / elapsed;
  EXPECT_GT(throughput, 5000.0) << "only " << throughput << " tasks/s";
}

// ===========================================================================
// 2. Mixed priority workload
// ===========================================================================
//
// Half the load is urgent with a tight deadline, half is background. The
// urgent half must be scheduled first even though both halves are submitted
// interleaved.

TEST(Stress, MixedPriorityWorkload) {
  constexpr int kProducers = 4;
  constexpr int kPairsPerProducer = 5000;  // 40k tasks total
  constexpr int kTotal = kProducers * kPairsPerProducer * 2;

  SchedulerConfig config;
  config.num_workers = 4;
  config.ingress_capacity = 1 << 17;
  config.history_per_shard = 32;
  // Shedding off: this scenario measures ordering, and dropping expired
  // low-priority work would remove the very tasks whose lateness is the
  // signal.
  config.fail_expired_before_execution = false;
  config.enforce_deadline_after_execution = false;

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<std::size_t> dispatch_counter{0};
  std::atomic<std::uint64_t> high_position_sum{0};
  std::atomic<std::uint64_t> low_position_sum{0};
  std::atomic<int> high_count{0};
  std::atomic<int> low_count{0};

  const auto record = [&](const Task& task) {
    const std::size_t position = dispatch_counter.fetch_add(1, std::memory_order_relaxed);
    if (task.priority() >= 8) {
      high_position_sum.fetch_add(position, std::memory_order_relaxed);
      high_count.fetch_add(1, std::memory_order_relaxed);
    } else {
      low_position_sum.fetch_add(position, std::memory_order_relaxed);
      low_count.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  };

  const TimePoint started = Clock::now();
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&] {
      for (int i = 0; i < kPairsPerProducer; ++i) {
        scheduler.submit_task(9, record, 100ms);   // urgent
        scheduler.submit_task(2, record, 1000ms);  // background
      }
    });
  }
  for (std::thread& t : producers) {
    t.join();
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout));
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("mixed_priority: 50% urgent (100ms) / 50% background (1s)", m, elapsed);

  ASSERT_EQ(high_count.load(), kTotal / 2);
  ASSERT_EQ(low_count.load(), kTotal / 2);
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTotal));

  const double high_mean =
      static_cast<double>(high_position_sum.load()) / static_cast<double>(high_count.load());
  const double low_mean =
      static_cast<double>(low_position_sum.load()) / static_cast<double>(low_count.load());
  std::printf("    mean dispatch pos  : urgent=%.0f background=%.0f (of %d)\n", high_mean,
              low_mean, kTotal);
  std::fflush(stdout);

  // Perfect ordering would put the urgent mean at ~kTotal/4 and background at
  // ~3*kTotal/4. The producers submit concurrently with execution, so some
  // background work is inevitably dispatched during the ramp; requiring the
  // urgent mean to sit in the first 40% of the schedule is strict enough to
  // catch a broken comparator and loose enough not to flake.
  EXPECT_LT(high_mean, low_mean) << "urgent work was not scheduled earlier";
  EXPECT_LT(high_mean, static_cast<double>(kTotal) * 0.4);
  EXPECT_GT(low_mean, static_cast<double>(kTotal) * 0.5);

  // Fairness: background work must be late, but not starved.
  EXPECT_EQ(low_count.load(), kTotal / 2) << "low-priority work was starved";
}

// ===========================================================================
// 3. Deadline pressure
// ===========================================================================
//
// Far more work than the pool can serve, with 10ms deadlines. The scheduler
// must detect the misses, shed the hopeless work instead of running it, and
// keep the tasks that can still make their deadline moving.

TEST(Stress, DeadlinePressure) {
  constexpr int kTasks = 40000;

  SchedulerConfig config;
  config.num_workers = 2;
  config.ingress_capacity = 1 << 17;
  config.history_per_shard = 32;
  config.fail_expired_before_execution = true;  // the behaviour under test

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<int> executed{0};
  const TimePoint started = Clock::now();
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(5,
                          [&executed](const Task& task) {
                            executed.fetch_add(1, std::memory_order_relaxed);
                            // The guarantee shedding makes is about the
                            // moment of *claim*, not the moment the callback
                            // body happens to look at the clock: a task
                            // claimed 50us before its deadline runs, and may
                            // well be past that deadline a moment later.
                            // Asserting is_expired(now()) here would be
                            // testing a promise the scheduler never made --
                            // and would fail intermittently for tasks claimed
                            // right at the boundary. The real invariant is
                            // that the claim happened inside the deadline.
                            EXPECT_LE(task.scheduled_at(), task.deadline());
                            return true;
                          },
                          10ms);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout));
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("deadline_pressure: 40k tasks, 10ms deadlines, 2 workers", m, elapsed);

  const double miss_rate =
      static_cast<double>(m.total_expired_before_start) / static_cast<double>(kTasks);
  std::printf("    deadline miss rate : %.1f%% (%llu shed of %d)\n", miss_rate * 100.0,
              static_cast<unsigned long long>(m.total_expired_before_start), kTasks);
  std::fflush(stdout);

  // Accounting must balance no matter how much work is shed.
  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_completed + m.total_failed, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_executed, static_cast<std::uint64_t>(executed.load()));
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);

  // With 40k tasks and a 10ms budget, a large fraction cannot possibly make
  // it: the scheduler is required to notice.
  EXPECT_GT(m.total_expired_before_start, 0u) << "no deadline miss was detected";
  EXPECT_EQ(m.total_failed, m.total_expired_before_start + m.total_deadline_missed)
      << "a task failed for an unexplained reason";

  // Shedding must be fast: dropping the hopeless tail should let the whole
  // backlog clear in far less time than executing all of it would take.
  EXPECT_LT(elapsed, 30.0);
}

// A pool that can keep up must not shed anything: shedding is a response to
// overload, not a behaviour that leaks into normal operation.
TEST(Stress, NoFalseDeadlineMissesWhenKeepingUp) {
  constexpr int kTasks = 20000;
  SchedulerConfig config;
  config.num_workers = 4;
  config.ingress_capacity = 1 << 16;
  config.history_per_shard = 16;

  PriorityScheduler scheduler(config);
  scheduler.start();

  const TimePoint started = Clock::now();
  for (int i = 0; i < kTasks; ++i) {
    scheduler.submit_task(5, [](const Task&) { return true; }, 30s);
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout));
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("no_false_misses: 20k trivial tasks, 30s deadlines", m, elapsed);

  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTasks));
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(m.total_expired_before_start, 0u);
  EXPECT_EQ(m.total_deadline_missed, 0u);
  EXPECT_DOUBLE_EQ(m.success_rate(), 1.0);
}

// ===========================================================================
// 4. Extreme contention
// ===========================================================================
//
// 16 producers against 2 workers, with a bounded queue. Producers will
// outrun the pool by design, so the point is that overload is handled
// predictably: bounded memory, visible rejections, no losses, no deadlock.

TEST(Stress, ExtremeContention) {
  constexpr int kProducers = 16;
  constexpr int kPerProducer = 6000;  // 96k attempts
  constexpr std::size_t kMaxDepth = 8192;

  SchedulerConfig config;
  config.num_workers = 2;
  config.ingress_capacity = 1024;  // small: the fallback path will be hit hard
  config.max_queue_depth = kMaxDepth;
  config.history_per_shard = 16;

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<int> accepted{0};
  std::atomic<int> rejected{0};
  std::atomic<int> executed{0};
  std::atomic<std::size_t> peak_depth{0};

  const TimePoint started = Clock::now();
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&] {
      for (int i = 0; i < kPerProducer; ++i) {
        const SubmitResult result = scheduler.try_submit_task(
            i % (kMaxPriority + 1),
            [&executed](const Task&) {
              executed.fetch_add(1, std::memory_order_relaxed);
              return true;
            },
            5min);
        if (result.accepted()) {
          accepted.fetch_add(1, std::memory_order_relaxed);
        } else {
          EXPECT_EQ(result.status, SubmitStatus::REJECTED_QUEUE_FULL);
          rejected.fetch_add(1, std::memory_order_relaxed);
        }
        // Sample the depth to confirm the cap actually holds.
        const std::size_t depth = scheduler.queue_depth();
        std::size_t observed = peak_depth.load(std::memory_order_relaxed);
        while (depth > observed &&
               !peak_depth.compare_exchange_weak(observed, depth, std::memory_order_relaxed)) {
        }
      }
    });
  }
  for (std::thread& t : producers) {
    t.join();
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout)) << "contention caused a stall";
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("extreme_contention: 16 producers -> 2 workers, depth cap 8192", m, elapsed);
  std::printf("    accepted/rejected  : %d / %d (peak depth %zu)\n", accepted.load(),
              rejected.load(), peak_depth.load());
  std::fflush(stdout);

  // Every accepted task ran exactly once; every rejected one never ran.
  EXPECT_EQ(executed.load(), accepted.load()) << "accepted work was lost under contention";
  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(accepted.load()));
  EXPECT_EQ(m.total_rejected, static_cast<std::uint64_t>(rejected.load()));
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(accepted.load()));
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
  EXPECT_EQ(accepted.load() + rejected.load(), kProducers * kPerProducer);

  // The backpressure cap is a hard bound, not a suggestion. The queue can sit
  // slightly above it because the depth check and the enqueue are not one
  // atomic step: at most one extra task per concurrent producer.
  EXPECT_LE(peak_depth.load(), kMaxDepth + kProducers)
      << "queue depth exceeded its cap by more than the allowed slack";

  // Whether the ring actually overflowed is deliberately NOT asserted here.
  // Workers drain the whole ring on every lock acquisition, so overflow
  // requires producers to out-race that drain -- which happens in an
  // optimized build and not in a debug one. The invariant that matters is the
  // one checked above: whichever path a submission took, nothing was lost.
  // The fallback path itself is covered deterministically by
  // SchedulerBackpressure.IngressOverflowFallsBackToTheHeap, which stages
  // 2000 tasks against a 16-entry ring with the pool stopped, so nothing can
  // drain and overflow is certain.
  std::printf("    (ring overflowed %llu times; path coverage is asserted in the unit tests)\n",
              static_cast<unsigned long long>(m.total_slow_path_submissions));
}

// The same contention shape with an unbounded queue: memory grows, but
// nothing is rejected and nothing is lost.
TEST(Stress, UnboundedQueueUnderContentionLosesNothing) {
  constexpr int kProducers = 16;
  constexpr int kPerProducer = 3000;
  constexpr int kTotal = kProducers * kPerProducer;

  SchedulerConfig config;
  config.num_workers = 2;
  config.ingress_capacity = 512;
  config.max_queue_depth = 0;  // unbounded
  config.history_per_shard = 16;

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<int> executed{0};
  const TimePoint started = Clock::now();
  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&] {
      for (int i = 0; i < kPerProducer; ++i) {
        ASSERT_TRUE(scheduler.try_submit_task(5,
                                              [&executed](const Task&) {
                                                executed.fetch_add(1, std::memory_order_relaxed);
                                                return true;
                                              },
                                              5min)
                        .accepted());
      }
    });
  }
  for (std::thread& t : producers) {
    t.join();
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout));
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("unbounded_contention: 16 producers -> 2 workers, no depth cap", m, elapsed);

  EXPECT_EQ(executed.load(), kTotal);
  EXPECT_EQ(m.total_rejected, 0u) << "an unbounded queue must never reject";
  EXPECT_EQ(m.total_completed, static_cast<std::uint64_t>(kTotal));
}

// ===========================================================================
// 5. Sustained mixed load
// ===========================================================================
//
// Everything at once, over a longer window: producers joining and leaving,
// failures, exceptions, deadline pressure and concurrent metrics readers. The
// scheduler has to stay consistent through all of it.

TEST(Stress, SustainedMixedLoad) {
  constexpr auto kDuration = 3s;
  constexpr int kProducers = 6;

  SchedulerConfig config;
  config.num_workers = 4;
  config.ingress_capacity = 1 << 15;
  config.max_queue_depth = 50000;
  config.history_per_shard = 32;

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<bool> stop{false};
  std::atomic<int> accepted{0};
  std::atomic<int> rejected{0};
  std::vector<std::thread> threads;

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p] {
      int i = 0;
      while (!stop.load(std::memory_order_acquire)) {
        const int kind = (p + i) % 10;
        const auto callback = [kind](const Task&) -> bool {
          if (kind == 7) {
            throw std::runtime_error("synthetic failure");
          }
          if (kind == 8) {
            return false;
          }
          return true;
        };
        const auto deadline = (kind % 3 == 0) ? std::chrono::milliseconds(5)
                                              : std::chrono::milliseconds(30000);
        if (scheduler.try_submit_task(kind % (kMaxPriority + 1), callback, deadline).accepted()) {
          accepted.fetch_add(1, std::memory_order_relaxed);
        } else {
          rejected.fetch_add(1, std::memory_order_relaxed);
        }
        ++i;
      }
    });
  }

  // A reader thread checking snapshot invariants throughout. Cross-counter
  // comparisons are skewed on a live snapshot (each counter is read
  // separately), so this checks per-counter monotonicity and the bounds that
  // do hold at every instant; the accounting identity is asserted after the
  // drain.
  threads.emplace_back([&] {
    Metrics previous;
    while (!stop.load(std::memory_order_acquire)) {
      const Metrics m = scheduler.get_metrics();
      ASSERT_GE(m.total_submitted, previous.total_submitted);
      ASSERT_GE(m.total_completed, previous.total_completed);
      ASSERT_GE(m.total_failed, previous.total_failed);
      ASSERT_GE(m.total_rejected, previous.total_rejected);
      // The depth cap can be overshot by at most one task per concurrent
      // producer, because the check and the enqueue are not one atomic step.
      ASSERT_LE(m.queue_depth, 50000u + kProducers);
      ASSERT_LE(m.p50_latency_ms, m.p99_latency_ms);
      ASSERT_LE(m.active_workers, 4u);
      previous = m;
      std::this_thread::sleep_for(1ms);
    }
  });

  const TimePoint started = Clock::now();
  std::this_thread::sleep_for(kDuration);
  stop.store(true, std::memory_order_release);
  for (std::thread& t : threads) {
    t.join();
  }
  ASSERT_TRUE(scheduler.wait_until_idle(kDrainTimeout));
  const double elapsed = seconds_since(started);
  scheduler.stop();

  const Metrics m = scheduler.get_metrics();
  print_result("sustained_mixed: 6 producers, 4 workers, failures + deadlines", m, elapsed);

  EXPECT_EQ(m.total_submitted, static_cast<std::uint64_t>(accepted.load()));
  EXPECT_EQ(m.total_rejected, static_cast<std::uint64_t>(rejected.load()));
  EXPECT_EQ(m.total_completed + m.total_failed, m.total_submitted)
      << "some accepted task never reached a terminal state";
  EXPECT_EQ(scheduler.outstanding_tasks(), 0u);
  EXPECT_EQ(scheduler.queue_depth(), 0u);
  EXPECT_GT(m.total_exceptions, 0u) << "the failure-injection path never ran";
  EXPECT_GT(m.total_completed, 0u);
}

}  // namespace
}  // namespace rtsched
