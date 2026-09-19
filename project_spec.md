# Real-Time Event Processing System: Priority Scheduler

## Project Overview

Build a **high-performance, multi-threaded priority task scheduler** with state machine task lifecycle management. This system is designed for low-latency, high-throughput environments where task ordering and reliability matter (e.g., real-time control systems, defense applications).

**Target Performance:**
- Task scheduling latency: < 1ms (p99)
- System throughput: 50k-100k tasks/second
- Thread-safe, lock-free where possible
- Graceful handling of concurrent producers and consumers

---

## Architecture

### Core Components

1. **Task Definition & State Machine**
   - Task struct: `id, priority, payload, timestamp, state, deadline`
   - States: `PENDING → SCHEDULED → EXECUTING → COMPLETED | FAILED`
   - State transitions are thread-safe and logged for tracing

2. **Priority Queue (Lock-Free or Mutex-Protected)**
   - Min-heap ordered by (priority, deadline)
   - High-priority tasks scheduled before low-priority
   - If priorities equal, earlier deadline wins
   - **Consider:** Lock-free queue for submissions, mutex-protected priority queue for consumption (best of both worlds)

3. **Task Scheduler (Thread Pool)**
   - Fixed number of worker threads (default: 4, configurable)
   - Each worker:
     - Pulls highest-priority task from queue
     - Transitions task to SCHEDULED
     - Executes task (calls provided callback)
     - Handles errors gracefully
     - Transitions task to COMPLETED or FAILED
   - Shutdown gracefully: drain queue, join workers

4. **Task Execution Context**
   - Each task carries a callback: `std::function<bool(const Task&)>`
   - Callback returns `true` on success, `false` on failure
   - Timeout handling: tasks that exceed deadline are marked FAILED
   - Error logging: capture and store execution errors

5. **Monitoring & Metrics**
   - Track: tasks submitted, scheduled, completed, failed, avg latency, p50/p95/p99 latencies
   - Real-time statistics accessible via API

---

## API Design (C++)

### Core Classes

```cpp
// Task definition
class Task {
public:
    uint64_t id;
    int priority;  // 0 (lowest) to 10 (highest)
    std::function<bool(const Task&)> callback;
    std::chrono::steady_clock::time_point deadline;
    std::chrono::steady_clock::time_point created_at;
    
    enum State { PENDING, SCHEDULED, EXECUTING, COMPLETED, FAILED };
    State state;
    std::string error_message;
};

// Scheduler API
class PriorityScheduler {
public:
    PriorityScheduler(size_t num_workers = 4);
    ~PriorityScheduler();
    
    // Submit a task (thread-safe)
    // Returns task ID
    uint64_t submit_task(int priority, std::function<bool(const Task&)> callback, 
                         std::chrono::milliseconds deadline_ms = 5000ms);
    
    // Get task status
    Task::State get_task_state(uint64_t task_id);
    
    // Metrics (thread-safe)
    struct Metrics {
        uint64_t total_submitted;
        uint64_t total_completed;
        uint64_t total_failed;
        double avg_latency_ms;
        double p50_latency_ms;
        double p95_latency_ms;
        double p99_latency_ms;
    };
    Metrics get_metrics() const;
    
    // Lifecycle
    void start();
    void stop();  // Graceful shutdown
    bool is_running() const;
    
private:
    // Implementation details (thread pool, queues, metrics)
};
```

### Usage Example

```cpp
PriorityScheduler scheduler(4);  // 4 worker threads
scheduler.start();

// Submit high-priority task
uint64_t task_id = scheduler.submit_task(
    10,  // high priority
    [](const Task& t) {
        std::cout << "Executing task " << t.id << std::endl;
        return true;  // success
    },
    std::chrono::milliseconds(500)  // 500ms deadline
);

// Check metrics
auto metrics = scheduler.get_metrics();
std::cout << "P99 latency: " << metrics.p99_latency_ms << "ms" << std::endl;

scheduler.stop();
```

---

## Implementation Details

### Concurrency Strategy

1. **Lock-Free Task Submission (Optional but Recommended)**
   - Use a lock-free MPMC queue for task submissions
   - Reduces contention during high-throughput scenarios
   - Fallback: std::queue with std::mutex if lock-free is complex

2. **Priority Queue Access**
   - Protect with `std::mutex` and `std::condition_variable`
   - Workers wait on condition variable when queue is empty
   - New task submissions notify waiting workers

3. **Metrics Collection**
   - Atomic counters for submitted/completed/failed counts
   - Protect latency array with mutex (small, lock held briefly)
   - Use `std::chrono::steady_clock` for nanosecond precision

4. **Task Lifecycle Tracking**
   - Task state transitions are guarded by task-level mutex or atomic
   - Created timestamp set at submission
   - Completed/failed timestamp set at execution end
   - Latency = completed_at - created_at

### Error Handling

- Tasks that throw exceptions are caught, marked FAILED, error stored
- Timeouts: if task not completed by deadline, mark FAILED
- Graceful worker shutdown: process remaining queue, then exit
- No deadlocks: avoid circular lock dependencies

---

## Testing Strategy

### Unit Tests (std::gtest or similar)

1. **Basic Functionality**
   - Submit single task, verify it executes
   - Submit multiple tasks, verify all complete
   - Priority ordering: high-priority tasks execute before low-priority (same conditions)
   - State transitions: PENDING → SCHEDULED → COMPLETED

2. **Concurrency Correctness**
   - 10 producer threads submit tasks, 4 consumers process
   - No data races (use ThreadSanitizer)
   - No deadlocks (task scheduler never hangs)
   - Metrics are accurate under concurrent access

3. **Error Handling**
   - Task callback throws exception → caught, marked FAILED
   - Task exceeds deadline → marked FAILED
   - Scheduler shutdown while tasks in flight → graceful drain

4. **Edge Cases**
   - Empty queue (workers wait correctly)
   - Single worker, thousands of tasks
   - All tasks same priority (FIFO order)
   - Rapid submit/shutdown cycles

### Stress Tests

1. **High-Throughput Scenario**
   - 8 producer threads submit 100k tasks total (mix of priorities)
   - 4 consumer threads process
   - Measure: throughput (tasks/sec), latency percentiles
   - Verify: zero dropped tasks, no data corruption

2. **Mixed Priority Workload**
   - 50% high-priority (deadline 100ms), 50% low-priority (deadline 1000ms)
   - Verify high-priority tasks complete first
   - Measure scheduling fairness

3. **Deadline Pressure**
   - Submit tasks with very short deadlines (10ms)
   - Verify tasks marked FAILED if deadline exceeded
   - Measure deadline miss rate

4. **Contention Test**
   - Many producers (16), few workers (2)
   - Verify queue doesn't overflow, no memory leaks
   - Measure latency under extreme contention

### Benchmark Suite

Output: CSV file with results

```
scenario,throughput_tasks_per_sec,p50_latency_ms,p95_latency_ms,p99_latency_ms
single_priority,100000,0.1,0.5,1.2
mixed_priority,85000,0.2,0.8,2.1
high_contention,50000,1.5,5.0,8.3
deadline_pressure,45000,2.0,10.0,15.5
```

---

## Deliverables

1. **Source Code**
   - `scheduler.h` - Public API
   - `scheduler.cpp` - Implementation with concurrency
   - `task.h` - Task definition and state machine
   - `metrics.h` - Metrics collection

2. **Tests**
   - `scheduler_test.cpp` - Comprehensive unit tests
   - `scheduler_stress_test.cpp` - High-throughput and contention tests
   - Each test executable and passing

3. **Benchmarks**
   - `scheduler_benchmark.cpp` - Generates performance metrics
   - CSV output with latency percentiles and throughput
   - Clear comparison: single vs multi-priority, low vs high contention

4. **Documentation**
   - README with architecture overview
   - API reference (classes, methods, example usage)
   - Design decisions: why lock-free queue, why condition variables, etc.
   - Performance tuning guide: adjusting worker count, queue size, etc.

5. **Build System**
   - CMakeLists.txt with:
     - Main library target
     - Test executable target (gtest)
     - Benchmark executable target
   - No external dependencies beyond gtest/benchmark (standard C++17)

---

## Design Rationale (For Interview)

**Why This Approach?**

1. **Lock-Free Submissions**: Reduces contention during high-throughput scenarios. Shows understanding of concurrent data structures.
2. **Priority Queue with Mutex**: Simpler than full lock-free implementation, still high-performance for typical workloads.
3. **Condition Variables**: Workers efficiently wait for tasks instead of busy-polling.
4. **Comprehensive Testing**: Demonstrates reliability—critical for defense systems.
5. **Metrics & Monitoring**: Shows production mindset; defense applications need observability.

**What I'd Optimize Next:**
- Full lock-free priority queue (skip-list or similar)
- Work-stealing scheduler (even better load balancing)
- Per-worker thread-local storage to reduce lock contention
- Integration with OS-level real-time scheduling (Linux `SCHED_FIFO`)

---

## Timeline (1 Week with Claude Code)

- **Day 1**: Core scheduler, task queue, state machine
- **Day 2**: Worker thread pool, execution logic
- **Day 3**: Metrics collection, monitoring
- **Day 4**: Unit tests (basic + concurrency)
- **Day 5**: Stress tests, benchmark suite
- **Day 6**: Polish, documentation, CMake cleanup
- **Day 7**: Final review, GitHub push, resume update

---

## Resume Language

**For Your Resume:**
```
Real-Time Event Processing System (C++) | Oct 2026
  • Engineered high-performance priority task scheduler with lock-free queue submission
    and state machine task lifecycle, handling 50k-100k tasks/second with <1ms (p99) latency
  • Implemented multi-threaded worker pool with condition variable synchronization,
    achieving 95%+ task completion rate under extreme contention
  • Developed comprehensive stress tests and benchmark suite measuring latency percentiles
    and throughput across mixed-priority and deadline-constrained workloads
  • GitHub: [link]
```

This directly addresses GA's needs: **real-time constraints, concurrency, reliability, production-grade design**.