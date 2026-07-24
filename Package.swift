// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "swift-spsc",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(name: "SPSCQueue", targets: ["SPSCQueue"]),
        .executable(
            name: "swift-spsc-benchmark",
            targets: ["SPSCQueueBenchmark"]
        ),
    ],
    dependencies: [
        .package(
            url: "https://github.com/apple/swift-atomics.git",
            from: "1.2.0"
        ),
    ],
    targets: [
        .target(
            name: "SPSCQueue",
            dependencies: [
                .product(name: "Atomics", package: "swift-atomics"),
            ],
            path: "source"
        ),
        .executableTarget(
            name: "SPSCQueueBenchmark",
            dependencies: [
                "SPSCQueue",
                .product(name: "Atomics", package: "swift-atomics"),
            ],
            path: "benchmark",
            exclude: ["cxx-benchmark.cpp"],
            sources: ["swift-benchmark.swift"]
        ),
        .testTarget(
            name: "SPSCQueueTests",
            dependencies: ["SPSCQueue"]
        ),
    ]
)
