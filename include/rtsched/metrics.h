// Copyright 2026. SPDX-License-Identifier: MIT
//
// Metrics collection: counters and latency distributions.
//
// The spec sketch suggested "protect a latency array with a mutex". That does
// not survive contact with 100k tasks/second: the array grows without bound
// and every completing worker serializes on the same lock at the exact moment
// it is trying to pick up more work. Instead this uses an HdrHistogram-style
// bucketed histogram:
//
//   * O(1), allocation-free, lock-free record() -- three atomic increments.
//   * Fixed 20 KB footprint regardless of task count.
//   * Percentiles with a bounded relative error (< 1%, see kSubBits below),
//     which is far tighter than the run-to-run variance of the measurement
//     itself.
//
// Values are stored in nanoseconds and reported in milliseconds, because a
// p99 of "0.4" reads better than "400000" in an SLA conversation.

#ifndef RTSCHED_METRICS_H_
#define RTSCHED_METRICS_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "rtsched/mpmc_queue.h"  // kCacheLineSize
#include "rtsched/task.h"       // Nanos

namespace rtsched {

// Latency distribution over [0, ~2^45) nanoseconds (~9.8 hours).
//
// Layout: the first kLinearBuckets values get their own exact bucket. Above
// that, each power-of-two octave is split into kSubBuckets equal-width
// buckets, so the relative width of a bucket is constant at 1/kSubBuckets.
class LatencyHistogram {
 public:
  // 7 bits => 128 sub-buckets per octave => worst-case bucket width is 1/128
  // of the value, so a reported percentile (bucket midpoint) is within
  // +/-0.4% of the true sample. Costs 2560 counters == 20 KB.
  static constexpr int kSubBits = 7;
  static constexpr std::size_t kSubBuckets = std::size_t{1} << kSubBits;  // 128
  static constexpr std::size_t kLinearBuckets = kSubBuckets;             // exact 0..127 ns
  static constexpr std::size_t kMaxShift = 38;
  static constexpr std::size_t kBucketCount = kLinearBuckets + kMaxShift * (kSubBuckets / 2);

  struct Snapshot {
    std::uint64_t count = 0;
    std::uint64_t min_ns = 0;
    std::uint64_t max_ns = 0;
    double mean_ns = 0.0;
    // Cumulative counts, aligned with bucket indices. Held so several
    // percentiles can be read from one consistent scan.
    std::vector<std::uint64_t> buckets;

    // Linear interpolation is deliberately avoided: reporting the bucket
    // midpoint keeps the error bound stated above honest.
    std::uint64_t percentile_ns(double p) const;
    double percentile_ms(double p) const { return static_cast<double>(percentile_ns(p)) / 1e6; }
    double mean_ms() const { return mean_ns / 1e6; }
    double min_ms() const { return static_cast<double>(min_ns) / 1e6; }
    double max_ms() const { return static_cast<double>(max_ns) / 1e6; }
  };

  LatencyHistogram();

  LatencyHistogram(const LatencyHistogram&) = delete;
  LatencyHistogram& operator=(const LatencyHistogram&) = delete;

  // Lock-free and safe from any number of threads.
  void record(std::uint64_t value_ns) noexcept;

  std::uint64_t count() const noexcept { return count_.load(std::memory_order_relaxed); }

  // Reads the whole histogram. Concurrent record() calls may or may not be
  // included -- the snapshot is eventually consistent, never torn, and each
  // individual counter is read atomically.
  Snapshot snapshot() const;

  void reset() noexcept;

  // Bucket index for a value, and the value range a bucket covers. Exposed
  // for the unit tests that verify the error bound.
  static std::size_t bucket_index(std::uint64_t value_ns) noexcept;
  static std::uint64_t bucket_lower_bound(std::size_t index) noexcept;
  static std::uint64_t bucket_width(std::size_t index) noexcept;
  static std::uint64_t bucket_midpoint(std::size_t index) noexcept;

 private:
  alignas(kCacheLineSize) std::atomic<std::uint64_t> count_{0};
  alignas(kCacheLineSize) std::atomic<std::uint64_t> sum_ns_{0};
  alignas(kCacheLineSize) std::atomic<std::uint64_t> min_ns_{UINT64_MAX};
  alignas(kCacheLineSize) std::atomic<std::uint64_t> max_ns_{0};
  std::array<std::atomic<std::uint64_t>, kBucketCount> buckets_{};
};

// Point-in-time view of scheduler health. Cheap to copy; hand it to a
// telemetry exporter or print it.
//
// Consistency: each counter is read atomically, but the snapshot as a whole is
// not a linearizable instant -- counters are read one after another while work
// continues. Every counter is individually exact and monotonic, and the
// percentiles within one snapshot come from a single histogram scan and so are
// mutually consistent. Identities across *different* counters (for example
// completed + failed == submitted) hold once the scheduler is quiescent, not
// while it is running. Making the whole snapshot atomic would mean locking the
// hot path on every task completion, which costs more than it is worth for
// telemetry.
struct Metrics {
  // --- Counters (exact) -----------------------------------------------------
  std::uint64_t total_submitted = 0;  // accepted by submit_task
  std::uint64_t total_rejected = 0;   // refused: queue depth cap or shutdown
  std::uint64_t total_scheduled = 0;  // claimed by a worker
  std::uint64_t total_executed = 0;   // callback actually invoked
  std::uint64_t total_completed = 0;  // callback returned true, deadline met
  std::uint64_t total_failed = 0;     // any terminal failure

  // Failure breakdown. The three below are subsets of total_failed and
  // together with "callback returned false" account for all of it.
  std::uint64_t total_expired_before_start = 0;  // deadline gone before the callback ran
  std::uint64_t total_deadline_missed = 0;       // callback overran the deadline
  std::uint64_t total_exceptions = 0;            // callback threw

  // Submissions that found the lock-free ring full and fell back to the
  // mutex-protected heap. A persistently non-zero value means the ring is
  // undersized for the offered load; see docs/TUNING.md.
  std::uint64_t total_slow_path_submissions = 0;

  // --- End-to-end latency: submit -> terminal state (milliseconds) ----------
  double avg_latency_ms = 0.0;
  double p50_latency_ms = 0.0;
  double p95_latency_ms = 0.0;
  double p99_latency_ms = 0.0;
  double p999_latency_ms = 0.0;
  double min_latency_ms = 0.0;
  double max_latency_ms = 0.0;

  // --- Scheduling latency: submit -> callback entry -------------------------
  // This is the number a real-time reviewer cares about: it isolates the
  // scheduler's own overhead from however long user work takes.
  double avg_queue_latency_ms = 0.0;
  double p50_queue_latency_ms = 0.0;
  double p99_queue_latency_ms = 0.0;
  double max_queue_latency_ms = 0.0;

  // --- Execution latency: time inside the callback --------------------------
  double avg_execution_latency_ms = 0.0;
  double p99_execution_latency_ms = 0.0;

  // --- Instantaneous gauges -------------------------------------------------
  std::uint64_t queue_depth = 0;      // tasks queued, not yet claimed
  std::uint64_t active_workers = 0;   // workers inside a callback
  std::uint64_t worker_count = 0;     // configured pool size

  // Convenience: completed / (completed + failed), or 1.0 with no samples.
  double success_rate() const;
};

// Thread-safe metrics aggregator owned by the scheduler.
class MetricsCollector {
 public:
  MetricsCollector() = default;

  MetricsCollector(const MetricsCollector&) = delete;
  MetricsCollector& operator=(const MetricsCollector&) = delete;

  void on_submitted() noexcept { bump(submitted_); }
  void on_rejected() noexcept { bump(rejected_); }
  void on_scheduled() noexcept { bump(scheduled_); }
  void on_executed() noexcept { bump(executed_); }
  void on_completed() noexcept { bump(completed_); }
  void on_failed() noexcept { bump(failed_); }
  void on_expired_before_start() noexcept { bump(expired_before_start_); }
  void on_deadline_missed() noexcept { bump(deadline_missed_); }
  void on_exception() noexcept { bump(exceptions_); }
  void on_slow_path_submission() noexcept { bump(slow_path_); }

  void record_total_latency(Nanos d) noexcept { total_latency_.record(to_ns(d)); }
  void record_queue_latency(Nanos d) noexcept { queue_latency_.record(to_ns(d)); }
  void record_execution_latency(Nanos d) noexcept { execution_latency_.record(to_ns(d)); }

  // `queue_depth`, `active_workers` and `worker_count` are gauges the
  // scheduler owns, so they are passed in rather than duplicated here.
  Metrics snapshot(std::uint64_t queue_depth, std::uint64_t active_workers,
                   std::uint64_t worker_count) const;

  void reset() noexcept;

 private:
  // One counter per cache line. Under 8 producers hammering submit(), a
  // shared line would turn every increment into a coherence round trip.
  struct alignas(kCacheLineSize) Counter {
    std::atomic<std::uint64_t> value{0};
  };

  static void bump(Counter& c) noexcept {
    // relaxed: counters are independent statistics, not synchronization. The
    // snapshot is a consistent-enough view, not a linearizable one.
    c.value.fetch_add(1, std::memory_order_relaxed);
  }
  static std::uint64_t to_ns(Nanos d) noexcept {
    const std::int64_t n = d.count();
    return n < 0 ? 0 : static_cast<std::uint64_t>(n);
  }

  Counter submitted_;
  Counter rejected_;
  Counter scheduled_;
  Counter executed_;
  Counter completed_;
  Counter failed_;
  Counter expired_before_start_;
  Counter deadline_missed_;
  Counter exceptions_;
  Counter slow_path_;

  LatencyHistogram total_latency_;
  LatencyHistogram queue_latency_;
  LatencyHistogram execution_latency_;
};

}  // namespace rtsched

#endif  // RTSCHED_METRICS_H_
