// Copyright 2026. SPDX-License-Identifier: MIT
//
// End-to-end tour of the public API. Build and run:
//   cmake -S . -B build && cmake --build build && ./build/rtsched_example

#include <atomic>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>

#include "rtsched/scheduler.h"

using namespace std::chrono_literals;

int main() {
  // --- 1. Configure and start ----------------------------------------------
  rtsched::SchedulerConfig config;
  config.num_workers = 4;
  config.max_queue_depth = 100000;  // bounded: overload rejects instead of ballooning

  rtsched::PriorityScheduler scheduler(config);
  scheduler.start();

  // --- 2. Submit work at different priorities ------------------------------
  std::atomic<int> completed{0};

  const std::uint64_t critical = scheduler.submit_task(
      rtsched::kMaxPriority,
      [&completed](const rtsched::Task& task) {
        std::printf("  [p%2d] task %llu: critical work\n", task.priority(),
                    static_cast<unsigned long long>(task.id()));
        completed.fetch_add(1);
        return true;  // success
      },
      500ms);  // deadline

  for (int priority = 0; priority < 5; ++priority) {
    scheduler.submit_task(
        priority,
        [&completed](const rtsched::Task& task) {
          std::printf("  [p%2d] task %llu: background work\n", task.priority(),
                      static_cast<unsigned long long>(task.id()));
          completed.fetch_add(1);
          return true;
        },
        2s);
  }

  // --- 3. Failure modes the scheduler contains ------------------------------
  const std::uint64_t thrower = scheduler.submit_task(
      5, [](const rtsched::Task&) -> bool { throw std::runtime_error("sensor offline"); }, 1s);

  const std::uint64_t reporter =
      scheduler.submit_task(5, [](const rtsched::Task&) { return false; }, 1s);

  const std::uint64_t late = scheduler.submit_task(
      5,
      [](const rtsched::Task&) {
        std::this_thread::sleep_for(50ms);  // overruns its 10ms deadline
        return true;
      },
      10ms);

  // --- 4. Wait for the queue to drain --------------------------------------
  if (!scheduler.wait_until_idle(5s)) {
    std::fprintf(stderr, "scheduler did not go idle\n");
    return 1;
  }

  // --- 5. Inspect individual tasks -----------------------------------------
  std::printf("\nTask outcomes:\n");
  for (const std::uint64_t id : {critical, thrower, reporter, late}) {
    const std::shared_ptr<const rtsched::Task> task = scheduler.find_task(id);
    if (!task) {
      continue;
    }
    std::printf("  task %llu -> %-9s %s\n", static_cast<unsigned long long>(id),
                rtsched::to_string(task->state()), task->error_message().c_str());

    // The bounded transition trace: what happened, and when.
    std::array<rtsched::Task::TraceEntry, rtsched::Task::kMaxTraceEntries> trace{};
    const std::size_t count = task->trace(trace);
    std::printf("      trace:");
    for (std::size_t i = 0; i < count; ++i) {
      const auto offset_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                 trace[i].at - task->created_at())
                                 .count();
      std::printf(" %s(+%lldus)", rtsched::to_string(trace[i].state),
                  static_cast<long long>(offset_us));
    }
    std::printf("\n");
  }

  // --- 6. Metrics -----------------------------------------------------------
  const rtsched::Metrics m = scheduler.get_metrics();
  std::printf("\nMetrics:\n");
  std::printf("  submitted=%llu completed=%llu failed=%llu (success rate %.1f%%)\n",
              static_cast<unsigned long long>(m.total_submitted),
              static_cast<unsigned long long>(m.total_completed),
              static_cast<unsigned long long>(m.total_failed), m.success_rate() * 100.0);
  std::printf("  failures: %llu threw, %llu missed deadline, %llu shed before start\n",
              static_cast<unsigned long long>(m.total_exceptions),
              static_cast<unsigned long long>(m.total_deadline_missed),
              static_cast<unsigned long long>(m.total_expired_before_start));
  std::printf("  scheduling latency: avg=%.3fms p50=%.3fms p99=%.3fms\n", m.avg_queue_latency_ms,
              m.p50_queue_latency_ms, m.p99_queue_latency_ms);
  std::printf("  end-to-end latency: avg=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms\n",
              m.avg_latency_ms, m.p50_latency_ms, m.p95_latency_ms, m.p99_latency_ms);

  // --- 7. Graceful shutdown -------------------------------------------------
  scheduler.stop();
  std::printf("\nstopped; %d callbacks ran\n", completed.load());
  return 0;
}
