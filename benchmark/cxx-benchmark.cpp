#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "include/rigtorp/SPSCQueue.h"

struct BenchmarkResult {
  std::uint64_t checksum;
  double seconds;
  double throughput_mops;
  double ns_per_op;
};

struct SummaryStats {
  BenchmarkResult best;
  double average_seconds;
  double average_throughput_mops;
  double average_ns_per_op;
  double median_seconds;
  double median_throughput_mops;
  double median_ns_per_op;
  double p95_seconds;
  double p95_throughput_mops;
  double p95_ns_per_op;
};

double throughputFromSeconds(std::size_t totalOps, double seconds) {
  return static_cast<double>(totalOps) / seconds / 1'000'000.0;
}

double nsPerOpFromSeconds(std::size_t totalOps, double seconds) {
  return seconds * 1'000'000'000.0 / static_cast<double>(totalOps);
}

SummaryStats summarize(const std::vector<BenchmarkResult> &results,
                       std::size_t totalOps) {
  SummaryStats stats{};
  stats.best = results.front();

  std::vector<double> seconds;
  seconds.reserve(results.size());

  double totalSeconds = 0.0;
  for (const auto &result : results) {
    if (result.seconds < stats.best.seconds) {
      stats.best = result;
    }
    totalSeconds += result.seconds;
    seconds.push_back(result.seconds);
  }

  std::sort(seconds.begin(), seconds.end());

  const auto medianIndex = seconds.size() / 2;
  const auto p95Index = std::min(
      seconds.size() - 1,
      static_cast<std::size_t>(std::ceil(seconds.size() * 0.95) - 1));

  stats.average_seconds = totalSeconds / static_cast<double>(results.size());
  stats.average_throughput_mops =
      throughputFromSeconds(totalOps, stats.average_seconds);
  stats.average_ns_per_op = nsPerOpFromSeconds(totalOps, stats.average_seconds);
  if (seconds.size() % 2 == 0) {
    stats.median_seconds = (seconds[medianIndex - 1] + seconds[medianIndex]) / 2.0;
  } else {
    stats.median_seconds = seconds[medianIndex];
  }
  stats.median_throughput_mops =
      throughputFromSeconds(totalOps, stats.median_seconds);
  stats.median_ns_per_op = nsPerOpFromSeconds(totalOps, stats.median_seconds);
  stats.p95_seconds = seconds[p95Index];
  stats.p95_throughput_mops = throughputFromSeconds(totalOps, stats.p95_seconds);
  stats.p95_ns_per_op = nsPerOpFromSeconds(totalOps, stats.p95_seconds);

  return stats;
}

BenchmarkResult runBenchmark(std::size_t totalOps, std::size_t capacity) {
  rigtorp::SPSCQueue<std::uint64_t> queue(capacity);
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::uint64_t checksum = 0;

  auto producer = std::thread([&] {
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
    }

    for (std::uint64_t value = 0; value < totalOps; ++value) {
      while (!queue.try_push(value)) {
      }
    }
  });

  auto consumer = std::thread([&] {
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
    }

    std::size_t consumed = 0;
    std::uint64_t localChecksum = 0;
    while (consumed < totalOps) {
      if (auto *value = queue.front()) {
        localChecksum += *value;
        queue.pop();
        ++consumed;
      }
    }
    checksum = localChecksum;
  });

  while (ready.load(std::memory_order_acquire) < 2) {
  }

  auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);

  producer.join();
  consumer.join();

  auto end = std::chrono::steady_clock::now();
  const std::chrono::duration<double> elapsed = end - begin;
  const double seconds = elapsed.count();
  const double throughput = static_cast<double>(totalOps) / seconds / 1'000'000.0;
  const double nsPerOp = seconds * 1'000'000'000.0 / static_cast<double>(totalOps);

  return {
      checksum,
      seconds,
      throughput,
      nsPerOp,
  };
}

int main(int argc, char **argv) {
  const std::size_t totalOps =
      argc > 1 ? static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10))
               : 20'000'000;
  const std::size_t capacity =
      argc > 2 ? static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10))
               : 65'536;
  const int rounds = argc > 3 ? std::atoi(argv[3]) : 10;

  const std::uint64_t expectedChecksum =
      (static_cast<std::uint64_t>(totalOps) * (totalOps - 1)) / 2;

  std::vector<BenchmarkResult> results;
  results.reserve(rounds);

  std::cout << "C++ rigtorp::SPSCQueue benchmark\n";
  std::cout << "totalOps=" << totalOps << " capacity=" << capacity
            << " rounds=" << rounds << "\n";

  for (int round = 1; round <= rounds; ++round) {
    const auto result = runBenchmark(totalOps, capacity);
    if (result.checksum != expectedChecksum) {
      std::cerr << "checksum mismatch: expected=" << expectedChecksum
                << " actual=" << result.checksum << "\n";
      return 1;
    }

    results.push_back(result);

    std::cout << "round=" << round << " seconds=" << std::fixed
              << std::setprecision(6) << result.seconds
              << " throughput_mops=" << std::setprecision(2)
              << result.throughput_mops << " ns_per_op=" << std::setprecision(2)
              << result.ns_per_op << "\n";
  }

  const auto stats = summarize(results, totalOps);

  std::cout << "best_seconds=" << std::fixed << std::setprecision(6)
            << stats.best.seconds << " best_throughput_mops="
            << std::setprecision(2) << stats.best.throughput_mops
            << " best_ns_per_op=" << std::setprecision(2)
            << stats.best.ns_per_op << "\n";
  std::cout << "avg_seconds=" << std::fixed << std::setprecision(6)
            << stats.average_seconds << " avg_throughput_mops="
            << std::setprecision(2) << stats.average_throughput_mops
            << " avg_ns_per_op=" << std::setprecision(2)
            << stats.average_ns_per_op << "\n";
  std::cout << "median_seconds=" << std::fixed << std::setprecision(6)
            << stats.median_seconds << " median_throughput_mops="
            << std::setprecision(2) << stats.median_throughput_mops
            << " median_ns_per_op=" << std::setprecision(2)
            << stats.median_ns_per_op << "\n";
  std::cout << "p95_seconds=" << std::fixed << std::setprecision(6)
            << stats.p95_seconds << " p95_throughput_mops="
            << std::setprecision(2) << stats.p95_throughput_mops
            << " p95_ns_per_op=" << std::setprecision(2)
            << stats.p95_ns_per_op << "\n";

  return 0;
}
