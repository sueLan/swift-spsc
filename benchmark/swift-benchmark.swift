import Atomics
import Darwin
import Dispatch
import Foundation
import SPSCQueue

private struct BenchmarkResult {
    let checksum: UInt64
    let seconds: Double
    let throughputMops: Double
    let nanosecondsPerOperation: Double
}

private struct BenchmarkConfiguration {
    let totalOperations: Int
    let queueCapacity: Int
    let rounds: Int
}

private final class ChecksumBox: @unchecked Sendable {
    var value: UInt64 = 0
}

private final class WorkerContext: @unchecked Sendable {
    let body: () -> Void

    init(_ body: @escaping () -> Void) {
        self.body = body
    }
}

@_cdecl("swift_spsc_benchmark_worker")
private func benchmarkWorker(
    _ rawContext: UnsafeMutableRawPointer
) -> UnsafeMutableRawPointer? {
    let context = Unmanaged<WorkerContext>
        .fromOpaque(rawContext)
        .takeRetainedValue()
    context.body()
    return nil
}

private enum Benchmark {
    static func runOnce(
        configuration: BenchmarkConfiguration
    ) -> BenchmarkResult {
        let totalOperations = configuration.totalOperations
        let queue = SPSCQueue<UInt64>(
            capacity: configuration.queueCapacity
        )
        let ready = ManagedAtomic<Int>(0)
        let start = ManagedAtomic<Bool>(false)
        let checksum = ChecksumBox()

        let producerContext = WorkerContext {
            ready.wrappingIncrement(ordering: .releasing)
            while !start.load(ordering: .acquiring) {}

            for value in 0..<totalOperations {
                while !queue.tryPush(UInt64(value)) {}
            }
        }

        let consumerContext = WorkerContext {
            ready.wrappingIncrement(ordering: .releasing)
            while !start.load(ordering: .acquiring) {}

            var consumed = 0
            var localChecksum: UInt64 = 0
            while consumed < totalOperations {
                if let value = queue.pop() {
                    localChecksum &+= value
                    consumed += 1
                }
            }
            checksum.value = localChecksum
        }

        var producerThread: pthread_t?
        var consumerThread: pthread_t?
        let producerResult = pthread_create(
            &producerThread,
            nil,
            benchmarkWorker,
            Unmanaged.passRetained(producerContext).toOpaque()
        )
        precondition(
            producerResult == 0,
            "pthread_create failed for producer: \(producerResult)"
        )

        let consumerResult = pthread_create(
            &consumerThread,
            nil,
            benchmarkWorker,
            Unmanaged.passRetained(consumerContext).toOpaque()
        )
        precondition(
            consumerResult == 0,
            "pthread_create failed for consumer: \(consumerResult)"
        )

        while ready.load(ordering: .acquiring) < 2 {}

        let begin = DispatchTime.now().uptimeNanoseconds
        start.store(true, ordering: .releasing)

        pthread_join(producerThread!, nil)
        pthread_join(consumerThread!, nil)

        let end = DispatchTime.now().uptimeNanoseconds
        let seconds = Double(end - begin) / 1_000_000_000.0
        return BenchmarkResult(
            checksum: checksum.value,
            seconds: seconds,
            throughputMops:
                Double(totalOperations) / seconds / 1_000_000.0,
            nanosecondsPerOperation:
                seconds * 1_000_000_000.0 / Double(totalOperations)
        )
    }

    static func run() {
        let arguments = CommandLine.arguments
        let configuration = BenchmarkConfiguration(
            totalOperations: arguments.count > 1
                ? parsePositive(arguments[1], name: "totalOps")
                : 20_000_000,
            queueCapacity: arguments.count > 2
                ? parsePositive(arguments[2], name: "capacity")
                : 65_536,
            rounds: arguments.count > 3
                ? parsePositive(arguments[3], name: "rounds")
                : 10
        )

        let operationCount = UInt64(configuration.totalOperations)
        let expectedChecksum = operationCount &* (operationCount &- 1) / 2
        var best: BenchmarkResult?
        var totalSeconds = 0.0

        print("Swift SPSCQueue benchmark")
        print(
            "totalOps=\(configuration.totalOperations) " +
            "capacity=\(configuration.queueCapacity) " +
            "rounds=\(configuration.rounds)"
        )

        for round in 1...configuration.rounds {
            let result = runOnce(configuration: configuration)
            precondition(
                result.checksum == expectedChecksum,
                "checksum mismatch: expected=\(expectedChecksum) " +
                "actual=\(result.checksum)"
            )

            if best == nil || result.seconds < best!.seconds {
                best = result
            }
            totalSeconds += result.seconds

            print(
                "round=\(round) " +
                "seconds=\(format(result.seconds, precision: 6)) " +
                "throughput_mops=\(format(result.throughputMops)) " +
                "ns_per_op=\(format(result.nanosecondsPerOperation))"
            )
        }

        let averageSeconds = totalSeconds / Double(configuration.rounds)
        let averageThroughput =
            Double(configuration.totalOperations)
                / averageSeconds / 1_000_000.0
        let averageNanoseconds =
            averageSeconds * 1_000_000_000.0
                / Double(configuration.totalOperations)

        print(
            "best_seconds=\(format(best!.seconds, precision: 6)) " +
            "best_throughput_mops=\(format(best!.throughputMops)) " +
            "best_ns_per_op=\(format(best!.nanosecondsPerOperation))"
        )
        print(
            "avg_seconds=\(format(averageSeconds, precision: 6)) " +
            "avg_throughput_mops=\(format(averageThroughput)) " +
            "avg_ns_per_op=\(format(averageNanoseconds))"
        )
    }

    private static func parsePositive(_ value: String, name: String) -> Int {
        guard let parsed = Int(value), parsed > 0 else {
            fatalError("\(name) must be a positive integer")
        }
        return parsed
    }

    private static func format(
        _ value: Double,
        precision: Int = 2
    ) -> String {
        String(format: "%.\(precision)f", value)
    }
}

Benchmark.run()
