// Copyright 2026. SPDX-License-Identifier: MIT
//
// Metrics and latency-histogram tests.
//
// The histogram trades a little accuracy for a lock-free, constant-memory
// record(). These tests pin down exactly how much accuracy: if the error bound
// documented in metrics.h ever regresses, the p99 numbers this project reports
// would quietly become fiction.

#include "rtsched/metrics.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

namespace rtsched {
namespace {

using namespace std::chrono_literals;

// --- Bucket geometry --------------------------------------------------------

TEST(HistogramGeometry, SmallValuesAreExact) {
  for (std::uint64_t v = 0; v < LatencyHistogram::kLinearBuckets; ++v) {
    const std::size_t index = LatencyHistogram::bucket_index(v);
    EXPECT_EQ(index, static_cast<std::size_t>(v));
    EXPECT_EQ(LatencyHistogram::bucket_width(index), 1u);
    EXPECT_EQ(LatencyHistogram::bucket_midpoint(index), v);
  }
}

TEST(HistogramGeometry, BucketsCoverTheRangeWithoutGapsOrOverlap) {
  // Walk the whole bucket array and check that each bucket starts exactly
  // where the previous one ended.
  std::uint64_t expected_next = 0;
  for (std::size_t i = 0; i < LatencyHistogram::kBucketCount; ++i) {
    const std::uint64_t lower = LatencyHistogram::bucket_lower_bound(i);
    ASSERT_EQ(lower, expected_next) << "gap or overlap at bucket " << i;
    expected_next = lower + LatencyHistogram::bucket_width(i);
  }
}

TEST(HistogramGeometry, EveryValueLandsInTheBucketThatContainsIt) {
  std::mt19937_64 rng(12345);
  for (int i = 0; i < 200000; ++i) {
    // Log-uniform over the whole supported range.
    const int exponent = static_cast<int>(rng() % 44);
    const std::uint64_t span = std::uint64_t{1} << exponent;
    const std::uint64_t value = span + (rng() % span);

    const std::size_t index = LatencyHistogram::bucket_index(value);
    const std::uint64_t lower = LatencyHistogram::bucket_lower_bound(index);
    const std::uint64_t width = LatencyHistogram::bucket_width(index);
    ASSERT_GE(value, lower) << "value " << value << " below bucket " << index;
    ASSERT_LT(value, lower + width) << "value " << value << " above bucket " << index;
  }
}

// The whole point of the sub-bucket split: relative error stays bounded no
// matter how large the value is.
TEST(HistogramGeometry, RelativeErrorIsBoundedEverywhere) {
  constexpr double kBound = 1.0 / static_cast<double>(LatencyHistogram::kSubBuckets);
  for (std::size_t i = LatencyHistogram::kLinearBuckets; i < LatencyHistogram::kBucketCount; ++i) {
    const double lower = static_cast<double>(LatencyHistogram::bucket_lower_bound(i));
    const double width = static_cast<double>(LatencyHistogram::bucket_width(i));
    // Worst case: the true value sits at one edge and we report the midpoint.
    const double error = (width / 2.0) / lower;
    ASSERT_LE(error, kBound) << "bucket " << i << " relative error " << error;
  }
}

TEST(HistogramGeometry, SaturatesInsteadOfOverflowing) {
  const std::size_t last = LatencyHistogram::kBucketCount - 1;
  EXPECT_EQ(LatencyHistogram::bucket_index(UINT64_MAX), last);
  EXPECT_EQ(LatencyHistogram::bucket_index(std::uint64_t{1} << 62), last);
}

// --- Recording and percentiles ---------------------------------------------

TEST(Histogram, EmptyHistogramReportsZeros) {
  LatencyHistogram histogram;
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_EQ(snapshot.count, 0u);
  EXPECT_EQ(snapshot.min_ns, 0u);
  EXPECT_EQ(snapshot.max_ns, 0u);
  EXPECT_DOUBLE_EQ(snapshot.mean_ns, 0.0);
  EXPECT_EQ(snapshot.percentile_ns(50.0), 0u);
  EXPECT_EQ(snapshot.percentile_ns(99.0), 0u);
}

TEST(Histogram, TracksCountSumAndExtremes) {
  LatencyHistogram histogram;
  histogram.record(100);
  histogram.record(200);
  histogram.record(300);

  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_EQ(snapshot.count, 3u);
  EXPECT_EQ(snapshot.min_ns, 100u);
  EXPECT_EQ(snapshot.max_ns, 300u);
  // min/max are exact even though bucket placement is approximate.
  EXPECT_NEAR(snapshot.mean_ns, 200.0, 0.001);
  EXPECT_NEAR(snapshot.mean_ms(), 0.0002, 1e-9);
}

TEST(Histogram, PercentilesOfAUniformDistribution) {
  LatencyHistogram histogram;
  // 1..10000 ns, one sample each: the p-th percentile is p * 100.
  for (std::uint64_t v = 1; v <= 10000; ++v) {
    histogram.record(v);
  }
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  ASSERT_EQ(snapshot.count, 10000u);

  const double tolerance = 1.0 / static_cast<double>(LatencyHistogram::kSubBuckets);
  for (const double p : {50.0, 90.0, 95.0, 99.0, 99.9}) {
    const double expected = p * 100.0;
    const double actual = static_cast<double>(snapshot.percentile_ns(p));
    EXPECT_NEAR(actual, expected, expected * tolerance)
        << "p" << p << " reported " << actual << ", expected ~" << expected;
  }
}

TEST(Histogram, PercentileBoundariesAreSaneAndMonotonic) {
  LatencyHistogram histogram;
  for (std::uint64_t v = 1; v <= 1000; ++v) {
    histogram.record(v * 1000);  // 1us .. 1ms
  }
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();

  // p0 selects the smallest sample, p100 the largest.
  EXPECT_LE(snapshot.percentile_ns(0.0), 1000u * 2);
  EXPECT_GE(snapshot.percentile_ns(100.0), 1000u * 1000 * 99 / 100);
  // Out-of-range percentiles clamp rather than misbehave.
  EXPECT_EQ(snapshot.percentile_ns(-5.0), snapshot.percentile_ns(0.0));
  EXPECT_EQ(snapshot.percentile_ns(150.0), snapshot.percentile_ns(100.0));

  std::uint64_t previous = 0;
  for (const double p : {0.0, 10.0, 25.0, 50.0, 75.0, 90.0, 99.0, 99.9, 100.0}) {
    const std::uint64_t value = snapshot.percentile_ns(p);
    EXPECT_GE(value, previous) << "percentiles must be non-decreasing (at p" << p << ")";
    previous = value;
  }
}

// A single slow outlier among many fast samples is exactly the shape of a
// real-time tail; the p99 must find it rather than average it away.
TEST(Histogram, TailOutlierIsVisibleAtP99ButNotAtP50) {
  LatencyHistogram histogram;
  for (int i = 0; i < 990; ++i) {
    histogram.record(1000);  // 1us
  }
  for (int i = 0; i < 10; ++i) {
    histogram.record(50'000'000);  // 50ms
  }
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_NEAR(static_cast<double>(snapshot.percentile_ns(50.0)), 1000.0, 20.0);
  EXPECT_GT(snapshot.percentile_ns(99.5), 10'000'000u);
  EXPECT_EQ(snapshot.max_ns, 50'000'000u);
  EXPECT_NEAR(snapshot.percentile_ms(50.0), 0.001, 0.0001);
}

TEST(Histogram, ResetClearsEverything) {
  LatencyHistogram histogram;
  for (std::uint64_t v = 1; v <= 100; ++v) {
    histogram.record(v * 1000);
  }
  ASSERT_EQ(histogram.count(), 100u);
  histogram.reset();
  EXPECT_EQ(histogram.count(), 0u);
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_EQ(snapshot.count, 0u);
  EXPECT_EQ(snapshot.min_ns, 0u);
  EXPECT_EQ(snapshot.max_ns, 0u);
  EXPECT_EQ(snapshot.percentile_ns(99.0), 0u);
}

TEST(Histogram, ZeroIsARecordableValue) {
  LatencyHistogram histogram;
  histogram.record(0);
  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_EQ(snapshot.count, 1u);
  EXPECT_EQ(snapshot.min_ns, 0u);
  EXPECT_EQ(snapshot.percentile_ns(50.0), 0u);
}

TEST(Histogram, ConcurrentRecordsAreNeverLost) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 50000;
  LatencyHistogram histogram;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&histogram] {
      for (int i = 0; i < kPerThread; ++i) {
        histogram.record(static_cast<std::uint64_t>(i % 1000) + 1);
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }

  const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
  EXPECT_EQ(snapshot.count, static_cast<std::uint64_t>(kThreads) * kPerThread);
  EXPECT_EQ(histogram.count(), snapshot.count);
  EXPECT_EQ(snapshot.min_ns, 1u);
  EXPECT_EQ(snapshot.max_ns, 1000u);
}

// A snapshot taken while records are landing must still be internally
// consistent: the bucket counts have to add up to the reported count, or
// percentile arithmetic would be computed against the wrong denominator.
TEST(Histogram, SnapshotIsSelfConsistentUnderConcurrentWrites) {
  LatencyHistogram histogram;
  std::atomic<bool> stop{false};
  std::thread writer([&] {
    std::uint64_t v = 1;
    while (!stop.load(std::memory_order_acquire)) {
      histogram.record(v);
      v = (v % 100000) + 1;
    }
  });

  for (int i = 0; i < 2000; ++i) {
    const LatencyHistogram::Snapshot snapshot = histogram.snapshot();
    const std::uint64_t summed =
        std::accumulate(snapshot.buckets.begin(), snapshot.buckets.end(), std::uint64_t{0});
    ASSERT_EQ(summed, snapshot.count);
    if (snapshot.count > 0) {
      ASSERT_LE(snapshot.percentile_ns(50.0), snapshot.percentile_ns(99.0));
    }
  }
  stop.store(true, std::memory_order_release);
  writer.join();
}

// --- MetricsCollector -------------------------------------------------------

TEST(MetricsCollector, CountersStartAtZero) {
  MetricsCollector collector;
  const Metrics m = collector.snapshot(0, 0, 4);
  EXPECT_EQ(m.total_submitted, 0u);
  EXPECT_EQ(m.total_completed, 0u);
  EXPECT_EQ(m.total_failed, 0u);
  EXPECT_EQ(m.worker_count, 4u);
  EXPECT_DOUBLE_EQ(m.success_rate(), 1.0) << "no samples must not report a failure rate";
}

TEST(MetricsCollector, EachCounterIsIndependent) {
  MetricsCollector collector;
  collector.on_submitted();
  collector.on_submitted();
  collector.on_rejected();
  collector.on_scheduled();
  collector.on_executed();
  collector.on_completed();
  collector.on_failed();
  collector.on_expired_before_start();
  collector.on_deadline_missed();
  collector.on_exception();
  collector.on_slow_path_submission();

  const Metrics m = collector.snapshot(7, 3, 4);
  EXPECT_EQ(m.total_submitted, 2u);
  EXPECT_EQ(m.total_rejected, 1u);
  EXPECT_EQ(m.total_scheduled, 1u);
  EXPECT_EQ(m.total_executed, 1u);
  EXPECT_EQ(m.total_completed, 1u);
  EXPECT_EQ(m.total_failed, 1u);
  EXPECT_EQ(m.total_expired_before_start, 1u);
  EXPECT_EQ(m.total_deadline_missed, 1u);
  EXPECT_EQ(m.total_exceptions, 1u);
  EXPECT_EQ(m.total_slow_path_submissions, 1u);
  EXPECT_EQ(m.queue_depth, 7u);
  EXPECT_EQ(m.active_workers, 3u);
  EXPECT_DOUBLE_EQ(m.success_rate(), 0.5);
}

TEST(MetricsCollector, LatencyHistogramsAreSeparate) {
  MetricsCollector collector;
  collector.record_total_latency(10ms);
  collector.record_queue_latency(1ms);
  collector.record_execution_latency(5ms);

  const Metrics m = collector.snapshot(0, 0, 1);
  EXPECT_NEAR(m.p50_latency_ms, 10.0, 0.1);
  EXPECT_NEAR(m.p50_queue_latency_ms, 1.0, 0.01);
  EXPECT_NEAR(m.p99_execution_latency_ms, 5.0, 0.05);
  EXPECT_NEAR(m.avg_latency_ms, 10.0, 0.1);
  EXPECT_NEAR(m.max_latency_ms, 10.0, 0.001);
}

TEST(MetricsCollector, NegativeDurationsAreClampedNotWrapped) {
  MetricsCollector collector;
  // A clock inversion must not become a 584-year latency sample.
  collector.record_total_latency(Nanos{-5000});
  const Metrics m = collector.snapshot(0, 0, 1);
  EXPECT_DOUBLE_EQ(m.max_latency_ms, 0.0);
  EXPECT_DOUBLE_EQ(m.avg_latency_ms, 0.0);
}

TEST(MetricsCollector, ResetClearsCountersAndHistograms) {
  MetricsCollector collector;
  collector.on_submitted();
  collector.on_completed();
  collector.record_total_latency(10ms);
  collector.reset();

  const Metrics m = collector.snapshot(0, 0, 2);
  EXPECT_EQ(m.total_submitted, 0u);
  EXPECT_EQ(m.total_completed, 0u);
  EXPECT_DOUBLE_EQ(m.p99_latency_ms, 0.0);
  EXPECT_DOUBLE_EQ(m.avg_latency_ms, 0.0);
  EXPECT_EQ(m.worker_count, 2u) << "gauges are passed in, not reset";
}

TEST(MetricsCollector, CountersAreExactUnderConcurrency) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 100000;
  MetricsCollector collector;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&collector] {
      for (int i = 0; i < kPerThread; ++i) {
        collector.on_submitted();
        if (i % 2 == 0) {
          collector.on_completed();
        } else {
          collector.on_failed();
        }
        collector.record_total_latency(Nanos{i + 1});
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }

  const Metrics m = collector.snapshot(0, 0, kThreads);
  constexpr std::uint64_t kTotal = static_cast<std::uint64_t>(kThreads) * kPerThread;
  EXPECT_EQ(m.total_submitted, kTotal);
  EXPECT_EQ(m.total_completed, kTotal / 2);
  EXPECT_EQ(m.total_failed, kTotal / 2);
  EXPECT_DOUBLE_EQ(m.success_rate(), 0.5);
}

}  // namespace
}  // namespace rtsched
