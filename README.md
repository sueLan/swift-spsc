# swift-spsc

A bounded, lock-free single-producer/single-consumer queue for Swift, based on
[`rigtorp/SPSCQueue.h`](https://github.com/rigtorp/SPSCQueue). @Powered by LLM.

The queue is generic, stores elements in an internal ring buffer, and uses
acquire/release atomic indices to coordinate exactly one producer with exactly
one consumer.

## Usage

Add the package to your Swift package dependencies, then:

```swift
import SPSCQueue

let queue = SPSCQueue<String>(capacity: 1_024)

// Producer thread/task
queue.push("hello")                 // waits while full
queue.emplace { makeMessage() }     // constructs after space is available
let inserted = queue.tryPush("now") // returns false immediately when full

// Consumer thread/task
if let message = queue.pop() {
    use(message)
}
```

`tryEmplace` is also available when construction should be skipped if the
queue is full. `count` and `isEmpty` are moment-in-time observations when the
producer and consumer are running concurrently.

Only one producer may use the producer operations at a time, and only one
consumer may call `pop` at a time.

## Test

```sh
swift test
```

## Benchmark

Both benchmarks default to 20,000,000 transfers through a 65,536-element queue
for 10 rounds. Optional arguments are `totalOps`, `capacity`, and `rounds`.
The Swift executable runs both the lock-free queue and an `NSLock`-protected
ring buffer using the same producer/consumer harness. One operation means one
completed enqueue/dequeue transfer. Duration is measured in seconds, throughput
in millions of transfers per second (Mops/s), and latency in nanoseconds per
transfer (`ns_per_op`). The latter is elapsed time divided by transfer count,
rather than an individually measured enqueue or dequeue latency.

```sh
swift build -c release --product swift-spsc-benchmark \
  -Xswiftc -cross-module-optimization
.build/release/swift-spsc-benchmark

clang++ -O3 -DNDEBUG -std=c++17 -pthread -I. \
  benchmark/cxx-benchmark.cpp -o .build/cxx-spsc-benchmark
.build/cxx-spsc-benchmark
```

The Swift benchmark uses `cross-module optimization` because the queue and
benchmark are separate SwiftPM targets. This allows the optimizer to see
through the module boundary, specialize the generic queue for `UInt64`, and
inline the `tryPush` and `pop` hot paths into the producer and consumer loops.
Without it, each transfer can retain out-of-line generic calls, element
metadata operations, and other abstraction overhead that is not representative
of the optimized queue algorithm.

### Benchmark results

Cross-module-optimized Swift release build on macOS 26.5.2 (`arm64`), running
on an Apple M4 with 16 GB RAM; using 20,000,000 transfers, capacity 65,536, and
10 rounds:

```text
Swift SPSCQueue benchmark
totalOps=20000000 capacity=65536 rounds=10
round=1 seconds=0.367638 throughput_mops=54.40 ns_per_op=18.38
round=2 seconds=0.350675 throughput_mops=57.03 ns_per_op=17.53
round=3 seconds=0.363459 throughput_mops=55.03 ns_per_op=18.17
round=4 seconds=0.351574 throughput_mops=56.89 ns_per_op=17.58
round=5 seconds=0.345295 throughput_mops=57.92 ns_per_op=17.26
round=6 seconds=0.341085 throughput_mops=58.64 ns_per_op=17.05
round=7 seconds=0.351972 throughput_mops=56.82 ns_per_op=17.60
round=8 seconds=0.349378 throughput_mops=57.24 ns_per_op=17.47
round=9 seconds=0.366671 throughput_mops=54.54 ns_per_op=18.33
round=10 seconds=0.380530 throughput_mops=52.56 ns_per_op=19.03
best_seconds=0.341085 best_throughput_mops=58.64 best_ns_per_op=17.05
avg_seconds=0.356828 avg_throughput_mops=56.05 avg_ns_per_op=17.84
```

The C++ benchmark was compiled with Apple Clang 17.0.0 (`arm64`) on the same
machine:

```text
C++ rigtorp::SPSCQueue benchmark
totalOps=20000000 capacity=65536 rounds=10
round=1 seconds=0.571771 throughput_mops=34.98 ns_per_op=28.59
round=2 seconds=0.559877 throughput_mops=35.72 ns_per_op=27.99
round=3 seconds=0.503701 throughput_mops=39.71 ns_per_op=25.19
round=4 seconds=0.359970 throughput_mops=55.56 ns_per_op=18.00
round=5 seconds=0.340041 throughput_mops=58.82 ns_per_op=17.00
round=6 seconds=0.513893 throughput_mops=38.92 ns_per_op=25.69
round=7 seconds=0.509041 throughput_mops=39.29 ns_per_op=25.45
round=8 seconds=0.506928 throughput_mops=39.45 ns_per_op=25.35
round=9 seconds=0.483746 throughput_mops=41.34 ns_per_op=24.19
round=10 seconds=0.524953 throughput_mops=38.10 ns_per_op=26.25
best_seconds=0.340041 best_throughput_mops=58.82 best_ns_per_op=17.00
avg_seconds=0.487392 avg_throughput_mops=41.03 avg_ns_per_op=24.37
```

### Swift queue with NSLock

The lock-based queue was rerun with `totalOps=20000000 capacity=65536 rounds=10`:

```sh
.build/release/swift-spsc-benchmark 20000000 65536 10
```

```text
Swift NSLock SPSCQueue benchmark
totalOps=20000000 capacity=65536 rounds=10
round=1 seconds=3.690615 throughput_mops=5.42 ns_per_op=184.53
round=2 seconds=3.912904 throughput_mops=5.11 ns_per_op=195.65
round=3 seconds=3.987343 throughput_mops=5.02 ns_per_op=199.37
round=4 seconds=3.885577 throughput_mops=5.15 ns_per_op=194.28
round=5 seconds=3.694432 throughput_mops=5.41 ns_per_op=184.72
round=6 seconds=3.832400 throughput_mops=5.22 ns_per_op=191.62
round=7 seconds=4.154782 throughput_mops=4.81 ns_per_op=207.74
round=8 seconds=3.731772 throughput_mops=5.36 ns_per_op=186.59
round=9 seconds=4.081564 throughput_mops=4.90 ns_per_op=204.08
round=10 seconds=2.599069 throughput_mops=7.70 ns_per_op=129.95
best_seconds=2.599069 best_throughput_mops=7.70 best_ns_per_op=129.95
avg_seconds=3.757046 avg_throughput_mops=5.32 avg_ns_per_op=187.85
```

All rounds passed FIFO order and checksum validation. In the same run, the
lock-free Swift queue averaged 0.418650 seconds, 47.77 Mops/s, and 20.93 ns per
transfer. The average throughput for each queue is calculated from its average
round duration.

Results vary with thread scheduling, CPU frequency, thermal state, and other
system activity, so averages or medians across repeated, alternating runs are
more meaningful than a single best round.

### C++ cache-line layout advantage

The C++ implementation has one hardware-level advantage that Swift does not
currently guarantee: it explicitly aligns the producer-owned write index and
consumer-owned read index to separate cache lines. It also pads the ring-buffer
allocation. This reduces false sharing and cache-coherency traffic between the
producer and consumer.

The Swift implementation uses separately allocated `ManagedAtomic` values and
does not control their cache-line placement. The allocator may place the two
atomic indices on the same cache line, so performance can be more sensitive to
allocation layout. Cross-module optimization removes generic call and
specialization overhead, but it does not provide the cache-line placement
guarantees present in the C++ implementation.
