// Copyright 2026. SPDX-License-Identifier: MIT

#include "rtsched/metrics.h"

#include <algorithm>
#include <cmath>

namespace rtsched {
namespace {

constexpr std::size_t kHalfSubBuckets = LatencyHistogram::kSubBuckets / 2;

// Number of significant bits in `v` (v != 0).
inline int bit_width_u64(std::uint64_t v) noexcept {
  return 64 - __builtin_clzll(v);
}

}  // namespace

// --- LatencyHistogram bucket geometry ---------------------------------------
//
// index < kLinearBuckets            : exact, one nanosecond per bucket
// index >= kLinearBuckets           : octave `shift`, sub-bucket `sub`, where
//                                     lower = (kHalfSubBuckets + sub) << shift
//
// Because every value in an octave has the same bit width, `v >> shift` always
// lands in [kHalfSubBuckets, kSubBuckets), which is why `sub` is offset by
// kHalfSubBuckets and only half the sub-buckets are needed per octave.

std::size_t LatencyHistogram::bucket_index(std::uint64_t value_ns) noexcept {
  if (value_ns < kLinearBuckets) {
    return static_cast<std::size_t>(value_ns);
  }
  const int shift = bit_width_u64(value_ns) - kSubBits;
  if (shift > static_cast<int>(kMaxShift)) {
    return kBucketCount - 1;  // Saturate; exact max is tracked separately.
  }
  const std::size_t sub =
      static_cast<std::size_t>(value_ns >> shift) - kHalfSubBuckets;
  return kLinearBuckets + (static_cast<std::size_t>(shift) - 1) * kHalfSubBuckets + sub;
}

std::uint64_t LatencyHistogram::bucket_lower_bound(std::size_t index) noexcept {
  if (index < kLinearBuckets) {
    return index;
  }
  const std::size_t k = index - kLinearBuckets;
  const std::size_t shift = k / kHalfSubBuckets + 1;
  const std::size_t sub = k % kHalfSubBuckets;
  return static_cast<std::uint64_t>(kHalfSubBuckets + sub) << shift;
}

std::uint64_t LatencyHistogram::bucket_width(std::size_t index) noexcept {
  if (index < kLinearBuckets) {
    return 1;
  }
  const std::size_t shift = (index - kLinearBuckets) / kHalfSubBuckets + 1;
  return std::uint64_t{1} << shift;
}

std::uint64_t LatencyHistogram::bucket_midpoint(std::size_t index) noexcept {
  return bucket_lower_bound(index) + bucket_width(index) / 2;
}

LatencyHistogram::LatencyHistogram() {
  for (auto& bucket : buckets_) {
    bucket.store(0, std::memory_order_relaxed);
  }
}

void LatencyHistogram::record(std::uint64_t value_ns) noexcept {
  buckets_[bucket_index(value_ns)].fetch_add(1, std::memory_order_relaxed);
  count_.fetch_add(1, std::memory_order_relaxed);
  sum_ns_.fetch_add(value_ns, std::memory_order_relaxed);

  // CAS loops for the extremes. Contended only while a new record is actually
  // being set, which is rare after warmup.
  std::uint64_t observed = min_ns_.load(std::memory_order_relaxed);
  while (value_ns < observed &&
         !min_ns_.compare_exchange_weak(observed, value_ns, std::memory_order_relaxed)) {
  }
  observed = max_ns_.load(std::memory_order_relaxed);
  while (value_ns > observed &&
         !max_ns_.compare_exchange_weak(observed, value_ns, std::memory_order_relaxed)) {
  }
}

LatencyHistogram::Snapshot LatencyHistogram::snapshot() const {
  Snapshot out;
  out.buckets.resize(kBucketCount);
  std::uint64_t total = 0;
  for (std::size_t i = 0; i < kBucketCount; ++i) {
    const std::uint64_t c = buckets_[i].load(std::memory_order_relaxed);
    out.buckets[i] = c;
    total += c;
  }
  // Derive the count from the buckets rather than from count_ so that
  // percentile arithmetic is internally consistent even if a concurrent
  // record() lands mid-scan.
  out.count = total;
  const std::uint64_t sum = sum_ns_.load(std::memory_order_relaxed);
  out.mean_ns = total == 0 ? 0.0 : static_cast<double>(sum) / static_cast<double>(total);
  const std::uint64_t observed_min = min_ns_.load(std::memory_order_relaxed);
  out.min_ns = (total == 0 || observed_min == UINT64_MAX) ? 0 : observed_min;
  out.max_ns = total == 0 ? 0 : max_ns_.load(std::memory_order_relaxed);
  return out;
}

void LatencyHistogram::reset() noexcept {
  for (auto& bucket : buckets_) {
    bucket.store(0, std::memory_order_relaxed);
  }
  count_.store(0, std::memory_order_relaxed);
  sum_ns_.store(0, std::memory_order_relaxed);
  min_ns_.store(UINT64_MAX, std::memory_order_relaxed);
  max_ns_.store(0, std::memory_order_relaxed);
}

// `p` is a percentile in [0, 100].
std::uint64_t LatencyHistogram::Snapshot::percentile_ns(double p) const {
  if (count == 0 || buckets.empty()) {
    return 0;
  }
  const double clamped = std::min(100.0, std::max(0.0, p));
  // Rank of the sample we want, 1-based. ceil so that p100 selects the last
  // sample and p0 selects the first.
  std::uint64_t rank =
      static_cast<std::uint64_t>(std::ceil(clamped / 100.0 * static_cast<double>(count)));
  rank = std::min<std::uint64_t>(std::max<std::uint64_t>(rank, 1), count);

  std::uint64_t cumulative = 0;
  for (std::size_t i = 0; i < buckets.size(); ++i) {
    cumulative += buckets[i];
    if (cumulative >= rank) {
      return bucket_midpoint(i);
    }
  }
  return bucket_midpoint(buckets.size() - 1);
}

// --- Metrics ----------------------------------------------------------------

double Metrics::success_rate() const {
  const std::uint64_t terminal = total_completed + total_failed;
  if (terminal == 0) {
    return 1.0;
  }
  return static_cast<double>(total_completed) / static_cast<double>(terminal);
}

// --- MetricsCollector -------------------------------------------------------

Metrics MetricsCollector::snapshot(std::uint64_t queue_depth, std::uint64_t active_workers,
                                   std::uint64_t worker_count) const {
  Metrics m;
  m.total_submitted = submitted_.value.load(std::memory_order_relaxed);
  m.total_rejected = rejected_.value.load(std::memory_order_relaxed);
  m.total_scheduled = scheduled_.value.load(std::memory_order_relaxed);
  m.total_executed = executed_.value.load(std::memory_order_relaxed);
  m.total_completed = completed_.value.load(std::memory_order_relaxed);
  m.total_failed = failed_.value.load(std::memory_order_relaxed);
  m.total_expired_before_start = expired_before_start_.value.load(std::memory_order_relaxed);
  m.total_deadline_missed = deadline_missed_.value.load(std::memory_order_relaxed);
  m.total_exceptions = exceptions_.value.load(std::memory_order_relaxed);
  m.total_slow_path_submissions = slow_path_.value.load(std::memory_order_relaxed);

  const LatencyHistogram::Snapshot total = total_latency_.snapshot();
  m.avg_latency_ms = total.mean_ms();
  m.p50_latency_ms = total.percentile_ms(50.0);
  m.p95_latency_ms = total.percentile_ms(95.0);
  m.p99_latency_ms = total.percentile_ms(99.0);
  m.p999_latency_ms = total.percentile_ms(99.9);
  m.min_latency_ms = total.min_ms();
  m.max_latency_ms = total.max_ms();

  const LatencyHistogram::Snapshot queue = queue_latency_.snapshot();
  m.avg_queue_latency_ms = queue.mean_ms();
  m.p50_queue_latency_ms = queue.percentile_ms(50.0);
  m.p99_queue_latency_ms = queue.percentile_ms(99.0);
  m.max_queue_latency_ms = queue.max_ms();

  const LatencyHistogram::Snapshot exec = execution_latency_.snapshot();
  m.avg_execution_latency_ms = exec.mean_ms();
  m.p99_execution_latency_ms = exec.percentile_ms(99.0);

  m.queue_depth = queue_depth;
  m.active_workers = active_workers;
  m.worker_count = worker_count;
  return m;
}

void MetricsCollector::reset() noexcept {
  for (Counter* c : {&submitted_, &rejected_, &scheduled_, &executed_, &completed_, &failed_,
                     &expired_before_start_, &deadline_missed_, &exceptions_, &slow_path_}) {
    c->value.store(0, std::memory_order_relaxed);
  }
  total_latency_.reset();
  queue_latency_.reset();
  execution_latency_.reset();
}

}  // namespace rtsched
