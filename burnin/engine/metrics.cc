#include "engine/metrics.h"

#include <cstring>

namespace burnin {

// CRC32 lookup table (IEEE polynomial)
static uint32_t crc32_table[256];
static bool crc32_initialized = false;

static void InitCrc32Table() {
    if (crc32_initialized) return;
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
        crc32_table[i] = crc;
    }
    crc32_initialized = true;
}

uint32_t Crc32(const std::string& data) {
    InitCrc32Table();
    uint32_t crc = 0xFFFFFFFF;
    for (unsigned char c : data) {
        crc = crc32_table[(crc ^ c) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// --- PatternMetrics ---

nlohmann::json PatternMetrics::Snapshot() const {
    return nlohmann::json{
        {"sent", sent.load(std::memory_order_relaxed)},
        {"received", received.load(std::memory_order_relaxed)},
        {"errors", errors.load(std::memory_order_relaxed)},
        {"lost", lost.load(std::memory_order_relaxed)},
        {"duplicated", duplicated.load(std::memory_order_relaxed)},
        {"corrupted", corrupted.load(std::memory_order_relaxed)},
        {"out_of_order", out_of_order.load(std::memory_order_relaxed)},
        {"bytes_sent", bytes_sent.load(std::memory_order_relaxed)},
        {"bytes_received", bytes_received.load(std::memory_order_relaxed)},
        {"rpc_success", rpc_success.load(std::memory_order_relaxed)},
        {"rpc_timeout", rpc_timeout.load(std::memory_order_relaxed)},
        {"rpc_error", rpc_error.load(std::memory_order_relaxed)},
        {"reconnections", reconnections.load(std::memory_order_relaxed)},
    };
}

void PatternMetrics::Reset() {
    sent.store(0, std::memory_order_relaxed);
    received.store(0, std::memory_order_relaxed);
    errors.store(0, std::memory_order_relaxed);
    lost.store(0, std::memory_order_relaxed);
    duplicated.store(0, std::memory_order_relaxed);
    corrupted.store(0, std::memory_order_relaxed);
    out_of_order.store(0, std::memory_order_relaxed);
    bytes_sent.store(0, std::memory_order_relaxed);
    bytes_received.store(0, std::memory_order_relaxed);
    rpc_success.store(0, std::memory_order_relaxed);
    rpc_timeout.store(0, std::memory_order_relaxed);
    rpc_error.store(0, std::memory_order_relaxed);
    reconnections.store(0, std::memory_order_relaxed);
}

// --- AggregateMetrics ---

AggregateMetrics::AggregateMetrics() = default;

PatternMetrics& AggregateMetrics::GetPattern(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    return patterns_[name];
}

nlohmann::json AggregateMetrics::Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    nlohmann::json result;
    for (const auto& [name, metrics] : patterns_) {
        result[name] = metrics.Snapshot();
    }

    // Add aggregate totals
    uint64_t total_sent = 0, total_received = 0, total_errors = 0;
    for (const auto& [_, metrics] : patterns_) {
        total_sent += metrics.sent.load(std::memory_order_relaxed);
        total_received += metrics.received.load(std::memory_order_relaxed);
        total_errors += metrics.errors.load(std::memory_order_relaxed);
    }
    result["_totals"] = {
        {"sent", total_sent},
        {"received", total_received},
        {"errors", total_errors},
        {"elapsed_seconds", ElapsedSeconds()},
    };

    return result;
}

void AggregateMetrics::Reset() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [_, metrics] : patterns_) {
        metrics.Reset();
    }
}

void AggregateMetrics::MarkStart() {
    std::lock_guard<std::mutex> lock(mu_);
    start_time_ = std::chrono::steady_clock::now();
    running_ = true;
}

void AggregateMetrics::MarkStop() {
    std::lock_guard<std::mutex> lock(mu_);
    stop_time_ = std::chrono::steady_clock::now();
    running_ = false;
}

double AggregateMetrics::ElapsedSeconds() const {
    auto end = running_ ? std::chrono::steady_clock::now() : stop_time_;
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_time_);
    return duration.count() / 1000.0;
}

}  // namespace burnin
