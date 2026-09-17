// Copyright 2026. SPDX-License-Identifier: MIT
//
// Benchmark suite: throughput and latency percentiles across the workload
// shapes a real-time system actually meets.
//
// Usage:
//   ./build/rtsched_benchmark [output.csv] [--quick] [--scale=N]
//
// Writes a CSV row per scenario and prints a summary table. Defaults to
// benchmark_results.csv in the working directory.
//
// Methodology
// -----------
//   * Callbacks are deliberately trivial (one relaxed atomic increment) so the
//     numbers measure the scheduler, not the payload. A "task" here is the
//     cost of submit -> order -> dispatch -> account.
//   * Each scenario runs a warmup pass first, which is discarded: it pages in
//     the ring, grows the heap's vector, and lets the CPU reach a steady
//     clock. Metrics are reset between the passes.
//   * The headline latency is *scheduling* latency (submit -> worker claim),
//     because that is the scheduler's own contribution. End-to-end latency is
//     reported alongside it; under heavy overload the two diverge sharply and
//     the difference is queue depth, not overhead.
//   * Throughput is computed over the whole run including the drain, so it is
//     sustained throughput rather than peak submission rate.
//   * Saturated vs paced: the `burst` scenarios submit as fast as producers
//     can, which is a throughput measurement -- their latencies are dominated
//     by queueing delay (a task submitted into a 100k backlog waits for the
//     backlog, and no scheduler can change that). The `paced_*` scenarios
//     hold the offered load below capacity so the queue stays shallow, which
//     is the only configuration in which a latency percentile describes the
//     scheduler rather than the queue depth. Quote paced numbers for latency
//     and burst numbers for throughput; quoting a burst p99 as "scheduling
//     latency" would be measuring the wrong thing.
//   * Producers are pinned to no particular core; on a machine with fewer
//     physical cores than (producers + workers), expect the contention
//     scenarios to reflect scheduler-of-scheduler effects. The CSV records
//     the thread counts so a result is always interpretable.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rtsched/scheduler.h"

namespace {

using namespace std::chrono_literals;
using rtsched::Clock;
using rtsched::Metrics;
using rtsched::PriorityScheduler;
using rtsched::SchedulerConfig;
using rtsched::Task;
using rtsched::TimePoint;

struct Options {
  std::string output = "benchmark_results.csv";
  double scale = 1.0;  // multiplies task counts
};

struct Scenario {
  std::string name;
  std::size_t workers = 4;
  std::size_t producers = 1;
  std::size_t tasks = 100000;
  int priority_bands = 1;                             // 1 == all equal priority
  std::chrono::milliseconds deadline = 30000ms;       // per-task budget
  std::size_t ingress_capacity = std::size_t{1} << 16;
  std::size_t max_queue_depth = 0;
  bool shed_expired = true;
  // Target aggregate submission rate in tasks/second. 0 == submit as fast as
  // possible (saturating). A non-zero value paces producers so the queue
  // stays shallow and the latency percentiles measure dispatch overhead.
  double target_rate_per_sec = 0.0;
  // Synthetic work per callback, in microseconds. Zero measures pure
  // scheduler overhead; a realistic value shows whether the pool scales.
  double work_us = 0.0;
  std::string note;
};

struct Result {
  std::string name;
  std::size_t workers = 0;
  std::size_t producers = 0;
  std::size_t tasks = 0;
  double elapsed_s = 0;
  double throughput = 0;
  double offered_rate = 0;  // 0 == saturating
  double work_us = 0;
  Metrics metrics;
};

// One pass of a scenario. Returns the wall-clock seconds from first submission
// to fully drained.
double run_pass(PriorityScheduler& scheduler, const Scenario& scenario, std::size_t tasks,
                std::atomic<std::uint64_t>& sink) {
  const std::size_t producers = std::max<std::size_t>(scenario.producers, 1);
  const std::size_t per_producer = tasks / producers;

  const double work_us = scenario.work_us;
  const auto callback = [&sink, work_us](const Task&) {
    // Just enough work that the compiler cannot elide the call.
    sink.fetch_add(1, std::memory_order_relaxed);
    if (work_us > 0.0) {
      // Busy-spin rather than sleep: this stands in for compute-bound work
      // (filtering, packing, a control-law update), which is what a
      // real-time pool actually runs. A sleep would measure the OS timer.
      const TimePoint until =
          Clock::now() + std::chrono::nanoseconds(static_cast<std::int64_t>(work_us * 1000.0));
      while (Clock::now() < until) {
      }
    }
    return true;
  };

  // Per-producer pacing interval, so the aggregate rate is the target.
  const bool paced = scenario.target_rate_per_sec > 0.0;
  const std::chrono::nanoseconds interval{
      paced ? static_cast<std::int64_t>(1e9 * static_cast<double>(producers) /
                                        scenario.target_rate_per_sec)
            : 0};

  const TimePoint started = Clock::now();
  std::vector<std::thread> threads;
  threads.reserve(producers);
  for (std::size_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      const TimePoint epoch = Clock::now();
      for (std::size_t i = 0; i < per_producer; ++i) {
        if (paced) {
          // Absolute schedule, so a slow iteration does not shift every
          // later submission (which would silently lower the offered rate).
          // Spin rather than sleep: a sleep quantum is ~1ms on a stock
          // kernel, which is larger than the latency being measured.
          const TimePoint due = epoch + interval * static_cast<std::int64_t>(i);
          while (Clock::now() < due) {
            std::this_thread::yield();
          }
        }
        const int priority =
            scenario.priority_bands <= 1
                ? 5
                : static_cast<int>((p * 31 + i) % static_cast<std::size_t>(scenario.priority_bands)) *
                      (rtsched::kMaxPriority / std::max(1, scenario.priority_bands - 1));
        // Retry on backpressure rather than dropping: a dropped task would
        // flatter the throughput number.
        while (!scheduler.try_submit_task(std::min(priority, rtsched::kMaxPriority), callback,
                                          scenario.deadline)
                    .accepted()) {
          if (!scheduler.is_running()) {
            return;
          }
          std::this_thread::yield();
        }
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }
  if (!scheduler.wait_until_idle(300s)) {
    std::fprintf(stderr, "  !! %s did not drain within 300s\n", scenario.name.c_str());
  }
  return std::chrono::duration<double>(Clock::now() - started).count();
}

Result run_scenario(const Scenario& scenario, double scale) {
  SchedulerConfig config;
  config.num_workers = scenario.workers;
  config.ingress_capacity = scenario.ingress_capacity;
  config.max_queue_depth = scenario.max_queue_depth;
  config.fail_expired_before_execution = scenario.shed_expired;
  config.enforce_deadline_after_execution = scenario.shed_expired;
  // Keep observability cheap: the registry is not what is being measured.
  config.history_per_shard = 16;
  config.registry_shards = 64;

  const std::size_t tasks =
      std::max<std::size_t>(static_cast<std::size_t>(static_cast<double>(scenario.tasks) * scale),
                            scenario.producers);

  PriorityScheduler scheduler(config);
  scheduler.start();

  std::atomic<std::uint64_t> sink{0};

  // Warmup: discarded. Faults in the ring pages, grows the heap vector to its
  // working size, and gets the CPU out of its idle clock state.
  run_pass(scheduler, scenario, std::min<std::size_t>(tasks / 10 + 1, 20000), sink);
  scheduler.reset_metrics();

  const double elapsed = run_pass(scheduler, scenario, tasks, sink);
  const Metrics metrics = scheduler.get_metrics();
  scheduler.stop();

  Result result;
  result.name = scenario.name;
  result.workers = scenario.workers;
  result.producers = scenario.producers;
  result.tasks = tasks;
  result.elapsed_s = elapsed;
  result.throughput = elapsed > 0 ? static_cast<double>(tasks) / elapsed : 0.0;
  result.offered_rate = scenario.target_rate_per_sec;
  result.work_us = scenario.work_us;
  result.metrics = metrics;
  return result;
}

// Positional aggregate initialization for a struct this wide is a trap: it
// breaks silently when a field is added. Every scenario is built by name.
Scenario scenario_of(std::string name, std::size_t workers, std::size_t producers,
                     std::size_t tasks, int priority_bands, std::string note) {
  Scenario scenario;
  scenario.name = std::move(name);
  scenario.workers = workers;
  scenario.producers = producers;
  scenario.tasks = tasks;
  scenario.priority_bands = priority_bands;
  scenario.note = std::move(note);
  return scenario;
}

std::vector<Scenario> build_scenarios() {
  std::vector<Scenario> scenarios;

  // --- The four headline shapes from the specification ---------------------

  scenarios.push_back(scenario_of("single_priority", 4, 4, 200000, 1,
                                  "all tasks equal priority; EDF degenerates to FIFO"));

  scenarios.push_back(scenario_of("mixed_priority", 4, 4, 200000, 11,
                                  "all 11 priority bands: full heap ordering cost"));

  {
    Scenario contention = scenario_of("high_contention", 2, 16, 200000, 11,
                                      "16 producers, 2 workers, 1Ki ring: fallback path hit");
    contention.ingress_capacity = 1024;
    scenarios.push_back(contention);
  }

  {
    Scenario pressure = scenario_of("deadline_pressure", 2, 8, 200000, 11,
                                    "10ms deadlines against an overloaded pool: shedding on");
    pressure.deadline = 10ms;
    scenarios.push_back(pressure);
  }

  // --- Worker scaling: where does adding threads stop helping? -------------

  for (const std::size_t workers : {std::size_t{1}, std::size_t{2}, std::size_t{4},
                                    std::size_t{8}, std::size_t{16}}) {
    Scenario scaling;
    scaling.name = "worker_scaling_" + std::to_string(workers);
    scaling.workers = workers;
    scaling.producers = 4;
    scaling.tasks = 200000;
    scaling.priority_bands = 11;
    scaling.note = "worker-count sweep at a fixed 4-producer offered load";
    scenarios.push_back(scaling);
  }

  // --- Producer scaling: does the lock-free ingress hold up? ---------------

  for (const std::size_t producers : {std::size_t{1}, std::size_t{2}, std::size_t{4},
                                      std::size_t{8}, std::size_t{16}}) {
    Scenario scaling;
    scaling.name = "producer_scaling_" + std::to_string(producers);
    scaling.workers = 4;
    scaling.producers = producers;
    scaling.tasks = 200000;
    scaling.priority_bands = 11;
    scaling.note = "producer-count sweep: submission-path contention";
    scenarios.push_back(scaling);
  }

  // --- Single-producer single-worker: the pure per-task overhead -----------

  scenarios.push_back(scenario_of("minimal_overhead", 1, 1, 200000, 1,
                                  "1 producer, 1 worker: uncontended per-task cost"));

  // --- Worker scaling with real work in the callback -----------------------
  //
  // The sweep above uses empty callbacks, so it measures the scheduler's own
  // serialization point (the heap lock) and gets *slower* with more workers.
  // That is a true result but an incomplete story: a thread pool exists to
  // overlap work, and with 20us of compute per task the same sweep should
  // scale close to linearly until it runs out of cores. Having both sweeps
  // side by side is what makes the first one interpretable.

  for (const std::size_t workers : {std::size_t{1}, std::size_t{2}, std::size_t{4},
                                    std::size_t{8}}) {
    Scenario scaling = scenario_of("worker_scaling_20us_" + std::to_string(workers), workers, 4,
                                   40000, 11, "20us of compute per task: pool should scale");
    scaling.work_us = 20.0;
    scenarios.push_back(scaling);
  }

  // --- Paced load: the only honest place to read a latency percentile ------
  //
  // Offered load is held below capacity, so the queue stays shallow and the
  // reported percentile is dispatch overhead rather than queueing delay.
  // These are the rows that speak to a "<1ms p99 scheduling latency" target.

  for (const double rate : {50000.0, 100000.0, 200000.0}) {
    Scenario paced;
    paced.name = "paced_" + std::to_string(static_cast<int>(rate / 1000)) + "k";
    paced.workers = 4;
    paced.producers = 4;
    paced.tasks = static_cast<std::size_t>(rate * 3);  // ~3 seconds of load
    paced.priority_bands = 11;
    paced.target_rate_per_sec = rate;
    paced.note = "offered load held at " + std::to_string(static_cast<int>(rate)) +
                 " tasks/s: latency reflects dispatch, not backlog";
    scenarios.push_back(paced);
  }

  // --- Bounded queue: what backpressure costs ------------------------------

  {
    Scenario bounded = scenario_of("bounded_queue", 4, 8, 200000, 11,
                                   "depth cap 8192: producers retry on backpressure");
    bounded.ingress_capacity = 1 << 12;
    bounded.max_queue_depth = 8192;
    scenarios.push_back(bounded);
  }

  return scenarios;
}

void write_csv(const std::string& path, const std::vector<Result>& results) {
  std::ofstream out(path);
  if (!out) {
    std::fprintf(stderr, "could not open %s for writing\n", path.c_str());
    return;
  }
  out << "scenario,workers,producers,tasks,offered_rate_tasks_per_sec,work_us_per_task,"
         "elapsed_s,throughput_tasks_per_sec,"
         "p50_latency_ms,p95_latency_ms,p99_latency_ms,p999_latency_ms,avg_latency_ms,"
         "p50_sched_latency_ms,p99_sched_latency_ms,max_sched_latency_ms,"
         "completed,failed,rejected,deadline_shed,deadline_overran,slow_path_submissions\n";
  out << std::fixed;
  for (const Result& r : results) {
    out << r.name << ',' << r.workers << ',' << r.producers << ',' << r.tasks << ','
        << std::setprecision(0) << r.offered_rate << ',' << std::setprecision(1) << r.work_us
        << ',' << std::setprecision(4) << r.elapsed_s << ',' << std::setprecision(0)
        << r.throughput
        << ',' << std::setprecision(4) << r.metrics.p50_latency_ms << ','
        << r.metrics.p95_latency_ms << ',' << r.metrics.p99_latency_ms << ','
        << r.metrics.p999_latency_ms << ',' << r.metrics.avg_latency_ms << ','
        << r.metrics.p50_queue_latency_ms << ',' << r.metrics.p99_queue_latency_ms << ','
        << r.metrics.max_queue_latency_ms << ',' << r.metrics.total_completed << ','
        << r.metrics.total_failed << ',' << r.metrics.total_rejected << ','
        << r.metrics.total_expired_before_start << ',' << r.metrics.total_deadline_missed << ','
        << r.metrics.total_slow_path_submissions << '\n';
  }
  std::printf("\nWrote %zu rows to %s\n", results.size(), path.c_str());
}

void print_table(const std::vector<Result>& results) {
  std::printf("\n%-24s %5s %3s %3s %10s %11s  %9s %9s  %9s %9s\n", "scenario", "load", "w",
              "p", "tasks", "tasks/s", "e2e p50", "e2e p99", "sch p50", "sch p99");
  std::printf("%s\n", std::string(112, '-').c_str());
  for (const Result& r : results) {
    std::printf("%-24s %5s %3zu %3zu %10zu %11.0f  %9.3f %9.3f  %9.3f %9.3f\n", r.name.c_str(),
                r.offered_rate > 0 ? "paced" : "burst", r.workers, r.producers, r.tasks,
                r.throughput, r.metrics.p50_latency_ms, r.metrics.p99_latency_ms,
                r.metrics.p50_queue_latency_ms, r.metrics.p99_queue_latency_ms);
  }
  std::printf(
      "\n(w = workers, p = producers, e2e = submit->terminal, sch = submit->worker claim;\n"
      " all latencies in ms. BURST rows saturate the pool: their latencies are queueing\n"
      " delay -- read them for throughput only. PACED rows hold offered load below\n"
      " capacity: read those for scheduling latency.)\n");
}

Options parse_args(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--quick") {
      options.scale = 0.1;
    } else if (arg.rfind("--scale=", 0) == 0) {
      options.scale = std::strtod(arg.c_str() + 8, nullptr);
      if (options.scale <= 0) {
        options.scale = 1.0;
      }
    } else if (arg == "--help" || arg == "-h") {
      std::printf("usage: rtsched_benchmark [output.csv] [--quick] [--scale=N]\n");
      std::exit(0);
    } else if (!arg.empty() && arg[0] != '-') {
      options.output = arg;
    }
  }
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_args(argc, argv);

  std::printf("rtsched benchmark suite\n");
  std::printf("  hardware concurrency : %u\n", std::thread::hardware_concurrency());
  std::printf("  task-count scale     : %.2fx\n", options.scale);
  std::printf("  output               : %s\n", options.output.c_str());

  const std::vector<Scenario> scenarios = build_scenarios();
  std::vector<Result> results;
  results.reserve(scenarios.size());

  for (const Scenario& scenario : scenarios) {
    std::printf("\nrunning %-24s (%zu workers, %zu producers)...", scenario.name.c_str(),
                scenario.workers, scenario.producers);
    std::fflush(stdout);
    const Result result = run_scenario(scenario, options.scale);
    std::printf(" %.0f tasks/s, p99 %.3f ms\n", result.throughput,
                result.metrics.p99_latency_ms);
    std::fflush(stdout);
    results.push_back(result);
  }

  print_table(results);
  write_csv(options.output, results);
  return 0;
}
