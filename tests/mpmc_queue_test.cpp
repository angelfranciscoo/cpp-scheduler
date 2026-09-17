// Copyright 2026. SPDX-License-Identifier: MIT
//
// Lock-free bounded MPMC queue tests.
//
// A queue that loses or duplicates an entry under contention would silently
// drop tasks, so these tests are written to detect exactly that: every
// concurrent case accounts for each item individually rather than only
// checking counts.

#include "rtsched/mpmc_queue.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

namespace rtsched {
namespace {

// --- Capacity and geometry --------------------------------------------------

TEST(MpmcQueueCapacity, RoundsUpToPowerOfTwo) {
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(0), 2u);
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(1), 2u);
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(2), 2u);
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(3), 4u);
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(1000), 1024u);
  EXPECT_EQ(MpmcBoundedQueue<int>::round_up_pow2(1024), 1024u);

  EXPECT_EQ(MpmcBoundedQueue<int>(5).capacity(), 8u);
  EXPECT_EQ(MpmcBoundedQueue<int>(64).capacity(), 64u);
}

// --- Single-threaded semantics ----------------------------------------------

TEST(MpmcQueueBasic, EmptyQueueYieldsNothing) {
  MpmcBoundedQueue<int> queue(4);
  int value = -1;
  EXPECT_FALSE(queue.try_dequeue(value));
  EXPECT_EQ(value, -1) << "a failed dequeue must not touch the output";
  EXPECT_TRUE(queue.empty_approx());
}

TEST(MpmcQueueBasic, FifoOrderWithOneThread) {
  MpmcBoundedQueue<int> queue(8);
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(queue.try_enqueue(i));
  }
  for (int i = 0; i < 8; ++i) {
    int value = -1;
    ASSERT_TRUE(queue.try_dequeue(value));
    EXPECT_EQ(value, i);
  }
  int value = -1;
  EXPECT_FALSE(queue.try_dequeue(value));
}

TEST(MpmcQueueBasic, RefusesEnqueueWhenFull) {
  MpmcBoundedQueue<int> queue(4);
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(queue.try_enqueue(i));
  }
  EXPECT_FALSE(queue.try_enqueue(99)) << "bounded queue must reject, not grow";
  EXPECT_EQ(queue.size_approx(), 4u);

  int value = -1;
  ASSERT_TRUE(queue.try_dequeue(value));
  EXPECT_EQ(value, 0);
  EXPECT_TRUE(queue.try_enqueue(99)) << "a freed slot must become reusable";
}

// Wrapping is where a sequence-number bug shows up, so push the ring around
// many laps.
TEST(MpmcQueueBasic, SurvivesManyLapsAroundTheRing) {
  MpmcBoundedQueue<int> queue(4);
  for (int lap = 0; lap < 10000; ++lap) {
    ASSERT_TRUE(queue.try_enqueue(lap));
    int value = -1;
    ASSERT_TRUE(queue.try_dequeue(value));
    ASSERT_EQ(value, lap);
  }
  EXPECT_TRUE(queue.empty_approx());
}

TEST(MpmcQueueBasic, MovesNonCopyablePayloadsAndReleasesThem) {
  MpmcBoundedQueue<std::unique_ptr<int>> queue(4);
  ASSERT_TRUE(queue.try_enqueue(std::make_unique<int>(7)));
  std::unique_ptr<int> out;
  ASSERT_TRUE(queue.try_dequeue(out));
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(*out, 7);
}

// A dequeued cell must drop its reference, or the queue would pin every
// object it ever carried for as long as it lives.
TEST(MpmcQueueBasic, DequeuedCellReleasesItsReference) {
  MpmcBoundedQueue<std::shared_ptr<int>> queue(4);
  auto payload = std::make_shared<int>(1);
  const std::weak_ptr<int> observer = payload;
  ASSERT_TRUE(queue.try_enqueue(std::move(payload)));

  std::shared_ptr<int> out;
  ASSERT_TRUE(queue.try_dequeue(out));
  EXPECT_EQ(out.use_count(), 1) << "the ring cell must no longer hold a reference";
  out.reset();
  EXPECT_TRUE(observer.expired());
}

// --- Concurrency ------------------------------------------------------------

// The property that matters: under N producers and M consumers, every item is
// delivered exactly once. Counting is not enough -- a swap of two items would
// pass a count check -- so each value is tallied individually.
TEST(MpmcQueueConcurrency, DeliversEveryItemExactlyOnce) {
  constexpr int kProducers = 4;
  constexpr int kConsumers = 4;
  constexpr int kPerProducer = 20000;
  constexpr int kTotal = kProducers * kPerProducer;

  MpmcBoundedQueue<int> queue(1024);
  std::vector<std::atomic<int>> seen(kTotal);
  for (std::atomic<int>& s : seen) {
    s.store(0, std::memory_order_relaxed);
  }

  std::atomic<int> produced{0};
  std::atomic<int> consumed{0};
  std::atomic<bool> go{false};

  std::vector<std::thread> threads;
  threads.reserve(kProducers + kConsumers);

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p] {
      while (!go.load(std::memory_order_acquire)) {
      }
      for (int i = 0; i < kPerProducer; ++i) {
        const int value = p * kPerProducer + i;
        // Spin on a full ring: this test is about correctness under
        // contention, so nothing may be dropped.
        while (!queue.try_enqueue(value)) {
          std::this_thread::yield();
        }
        produced.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (int c = 0; c < kConsumers; ++c) {
    threads.emplace_back([&] {
      while (!go.load(std::memory_order_acquire)) {
      }
      int value = 0;
      while (consumed.load(std::memory_order_relaxed) < kTotal) {
        if (queue.try_dequeue(value)) {
          ASSERT_GE(value, 0);
          ASSERT_LT(value, kTotal);
          seen[static_cast<std::size_t>(value)].fetch_add(1, std::memory_order_relaxed);
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  go.store(true, std::memory_order_release);
  for (std::thread& t : threads) {
    t.join();
  }

  EXPECT_EQ(produced.load(), kTotal);
  EXPECT_EQ(consumed.load(), kTotal);
  std::size_t missing = 0;
  std::size_t duplicated = 0;
  for (std::size_t i = 0; i < seen.size(); ++i) {
    const int count = seen[i].load(std::memory_order_relaxed);
    if (count == 0) {
      ++missing;
    } else if (count > 1) {
      ++duplicated;
    }
  }
  EXPECT_EQ(missing, 0u) << missing << " items were lost";
  EXPECT_EQ(duplicated, 0u) << duplicated << " items were delivered more than once";
  EXPECT_TRUE(queue.empty_approx());
}

// A tiny ring forces near-constant full/empty transitions, which is the worst
// case for the sequence protocol.
TEST(MpmcQueueConcurrency, CorrectWithAMinimalRing) {
  constexpr int kProducers = 8;
  constexpr int kPerProducer = 4000;
  constexpr int kTotal = kProducers * kPerProducer;

  MpmcBoundedQueue<int> queue(2);
  std::vector<std::atomic<int>> seen(kTotal);
  for (std::atomic<int>& s : seen) {
    s.store(0, std::memory_order_relaxed);
  }
  std::atomic<int> consumed{0};
  std::vector<std::thread> threads;

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p] {
      for (int i = 0; i < kPerProducer; ++i) {
        while (!queue.try_enqueue(p * kPerProducer + i)) {
          std::this_thread::yield();
        }
      }
    });
  }
  for (int c = 0; c < 4; ++c) {
    threads.emplace_back([&] {
      int value = 0;
      while (consumed.load(std::memory_order_relaxed) < kTotal) {
        if (queue.try_dequeue(value)) {
          seen[static_cast<std::size_t>(value)].fetch_add(1, std::memory_order_relaxed);
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }

  for (std::size_t i = 0; i < seen.size(); ++i) {
    ASSERT_EQ(seen[i].load(std::memory_order_relaxed), 1) << "item " << i;
  }
}

// Reference counting is the other thing contention can corrupt: a lost
// decrement leaks, a double decrement is a use-after-free.
TEST(MpmcQueueConcurrency, SharedPointerReferenceCountsStayExact) {
  constexpr int kProducers = 4;
  constexpr int kPerProducer = 5000;
  constexpr int kTotal = kProducers * kPerProducer;

  struct Payload {
    int value = 0;
  };

  MpmcBoundedQueue<std::shared_ptr<Payload>> queue(256);
  std::atomic<int> live{0};
  std::atomic<int> consumed{0};
  std::vector<std::thread> threads;

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerProducer; ++i) {
        auto item = std::shared_ptr<Payload>(new Payload{i}, [&live](Payload* raw) {
          live.fetch_sub(1, std::memory_order_relaxed);
          delete raw;
        });
        live.fetch_add(1, std::memory_order_relaxed);
        while (!queue.try_enqueue(std::move(item))) {
          std::this_thread::yield();
        }
      }
    });
  }
  for (int c = 0; c < 4; ++c) {
    threads.emplace_back([&] {
      std::shared_ptr<Payload> item;
      while (consumed.load(std::memory_order_relaxed) < kTotal) {
        if (queue.try_dequeue(item)) {
          ASSERT_NE(item, nullptr);
          ASSERT_EQ(item.use_count(), 1);
          item.reset();
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }
  EXPECT_EQ(consumed.load(), kTotal);
  EXPECT_EQ(live.load(), 0) << "every payload must have been destroyed exactly once";
}

// A full ring must reject rather than block, even with every producer pushing.
TEST(MpmcQueueConcurrency, FullRingRejectsWithoutBlocking) {
  constexpr int kProducers = 8;
  MpmcBoundedQueue<int> queue(64);
  std::atomic<int> accepted{0};
  std::atomic<int> refused{0};
  std::vector<std::thread> threads;
  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&] {
      for (int i = 0; i < 1000; ++i) {
        if (queue.try_enqueue(i)) {
          accepted.fetch_add(1, std::memory_order_relaxed);
        } else {
          refused.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }
  // Nothing consumed, so exactly `capacity` enqueues can have succeeded.
  EXPECT_EQ(accepted.load(), 64);
  EXPECT_EQ(refused.load(), kProducers * 1000 - 64);
  EXPECT_EQ(queue.size_approx(), 64u);
}

}  // namespace
}  // namespace rtsched
