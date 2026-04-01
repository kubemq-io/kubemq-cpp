#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>

#include <nlohmann/json.hpp>

namespace burnin {

// CRC32 for body integrity verification.
uint32_t Crc32(const std::string& data);

// PatternMetrics holds atomic counters for a single pattern.
struct PatternMetrics {
    std::atomic<uint64_t> sent{0};
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> lost{0};
    std::atomic<uint64_t> duplicated{0};
    std::atomic<uint64_t> corrupted{0};
    std::atomic<uint64_t> out_of_order{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> rpc_success{0};
    std::atomic<uint64_t> rpc_timeout{0};
    std::atomic<uint64_t> rpc_error{0};
    std::atomic<uint64_t> reconnections{0};

    // Snapshot the current values (thread-safe read).
    nlohmann::json Snapshot() const;

    // Reset all counters to zero.
    void Reset();
};

// AggregateMetrics collects metrics across all patterns.
class AggregateMetrics {
public:
    AggregateMetrics();

    PatternMetrics& GetPattern(const std::string& name);

    // Get a snapshot of all pattern metrics.
    nlohmann::json Snapshot() const;

    // Reset all metrics.
    void Reset();

    // Start time tracking
    void MarkStart();
    void MarkStop();
    double ElapsedSeconds() const;

private:
    mutable std::mutex mu_;
    std::map<std::string, PatternMetrics> patterns_;
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point stop_time_;
    bool running_ = false;
};

}  // namespace burnin
