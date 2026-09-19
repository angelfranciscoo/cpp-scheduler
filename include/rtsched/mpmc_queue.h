// Copyright 2026. SPDX-License-Identifier: MIT
//
// Bounded lock-free multi-producer / multi-consumer queue.
//
// This is the Vyukov bounded MPMC algorithm: a power-of-two ring of cells,
// each carrying a sequence number that encodes whose turn it is. Producers and
// consumers claim a slot with a single CAS on a shared cursor, then publish
// their work with a release-store on the cell's own sequence number.
//
// Properties that matter for a real-time scheduler:
//   * Lock-free: no operation can block another indefinitely; a thread
//     descheduled mid-operation cannot stall the rest of the system, which is
//     the failure mode that makes mutexes risky on the submission path.
//   * Bounded and preallocated: no allocation, so no malloc lock and no
//     unbounded memory growth under producer overload. try_enqueue simply
//     fails when full, handing backpressure to the caller.
//   * Wait-free on the uncontended path: one CAS plus one store.
//
// It is NOT a priority queue -- ordering is FIFO. The scheduler uses it purely
// as a contention absorber in front of the priority heap; see scheduler.h.

#ifndef RTSCHED_MPMC_QUEUE_H_
#define RTSCHED_MPMC_QUEUE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace rtsched {

// libc++ does not ship std::hardware_destructive_interference_size on all the
// platforms we care about, and 64 is correct for x86-64 and arm64 alike
// (Apple silicon uses 128-byte lines for some purposes, which this still
// over-aligns safely).
inline constexpr std::size_t kCacheLineSize = 64;

template <typename T>
class MpmcBoundedQueue {
  static_assert(std::is_default_constructible<T>::value,
                "MpmcBoundedQueue preallocates cells and so needs a default-constructible T");
  static_assert(std::is_move_assignable<T>::value, "T must be move-assignable");

 public:
  // `capacity` is rounded up to a power of two (minimum 2) so the index
  // computation is a mask instead of a modulo.
  explicit MpmcBoundedQueue(std::size_t capacity)
      : capacity_(round_up_pow2(capacity)),
        mask_(capacity_ - 1),
        buffer_(new Cell[capacity_]) {
    for (std::size_t i = 0; i < capacity_; ++i) {
      // Cell i starts "ready for the producer at position i".
      buffer_[i].sequence.store(i, std::memory_order_relaxed);
    }
    enqueue_pos_.store(0, std::memory_order_relaxed);
    dequeue_pos_.store(0, std::memory_order_relaxed);
  }

  MpmcBoundedQueue(const MpmcBoundedQueue&) = delete;
  MpmcBoundedQueue& operator=(const MpmcBoundedQueue&) = delete;

  // Returns false if the ring is full. Never blocks.
  bool try_enqueue(T&& item) noexcept {
    Cell* cell;
    std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t diff =
          static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
      if (diff == 0) {
        // Cell is ours if we win the cursor.
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;  // Consumer has not caught up: full.
      } else {
        pos = enqueue_pos_.load(std::memory_order_relaxed);  // Lost a race; re-read.
      }
    }
    cell->data = std::move(item);
    // Publish: marks the cell readable by the consumer at `pos`.
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool try_enqueue(const T& item) noexcept(std::is_nothrow_copy_constructible<T>::value) {
    T copy = item;
    return try_enqueue(std::move(copy));
  }

  // Returns false if the ring is empty. Never blocks.
  bool try_dequeue(T& out) noexcept {
    Cell* cell;
    std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t diff =
          static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
      if (diff == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;  // Producer has not published yet: empty.
      } else {
        pos = dequeue_pos_.load(std::memory_order_relaxed);
      }
    }
    out = std::move(cell->data);
    cell->data = T();  // Drop any resources the moved-from object still holds.
    // Hand the cell to the producer one lap ahead.
    cell->sequence.store(pos + mask_ + 1, std::memory_order_release);
    return true;
  }

  std::size_t capacity() const noexcept { return capacity_; }

  // Approximate: both cursors are read without synchronization, so the result
  // is a hint for metrics and tests, never a correctness input. Clamped
  // because the cursors can be read out of order.
  std::size_t size_approx() const noexcept {
    const std::size_t tail = enqueue_pos_.load(std::memory_order_relaxed);
    const std::size_t head = dequeue_pos_.load(std::memory_order_relaxed);
    return tail > head ? tail - head : 0;
  }

  bool empty_approx() const noexcept { return size_approx() == 0; }

  static std::size_t round_up_pow2(std::size_t n) {
    if (n < 2) {
      return 2;
    }
    if ((n & (n - 1)) == 0) {
      return n;
    }
    std::size_t p = 2;
    while (p < n) {
      const std::size_t next = p << 1;
      if (next <= p) {
        throw std::length_error("MpmcBoundedQueue: capacity overflow");
      }
      p = next;
    }
    return p;
  }

 private:
  struct Cell {
    std::atomic<std::size_t> sequence;
    T data{};
  };

  const std::size_t capacity_;
  const std::size_t mask_;
  std::unique_ptr<Cell[]> buffer_;

  // The two cursors are the only globally shared hot words. Keeping them on
  // separate cache lines is the single highest-value optimization in this
  // file: sharing one line would put every producer in a coherence fight with
  // every consumer. Cells themselves are deliberately left unpadded -- at a
  // 65536-entry ring, padding every cell to a line would cost ~4 MB of
  // footprint to remove contention that the sequence protocol already spreads
  // across the ring.
  alignas(kCacheLineSize) std::atomic<std::size_t> enqueue_pos_{0};
  alignas(kCacheLineSize) std::atomic<std::size_t> dequeue_pos_{0};
  char pad_[kCacheLineSize - sizeof(std::atomic<std::size_t>)] = {};
};

}  // namespace rtsched

#endif  // RTSCHED_MPMC_QUEUE_H_
