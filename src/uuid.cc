// src/uuid.cc
#include "internal/uuid.h"

#include <cstdio>
#include <random>
#include <string>

namespace kubemq {
namespace internal {

std::string GenerateUUID() {
    static thread_local std::mt19937_64 gen{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist;

    uint64_t a = dist(gen);
    uint64_t b = dist(gen);

    // Set version 4 (bits 12-15 of time_hi_and_version)
    a = (a & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    // Set variant (bits 6-7 of clock_seq_hi_and_reserved)
    b = (b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

    char buf[37];
    std::snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx", static_cast<uint32_t>(a >> 32),
                  static_cast<uint16_t>((a >> 16) & 0xFFFF), static_cast<uint16_t>(a & 0xFFFF),
                  static_cast<uint16_t>(b >> 48),
                  static_cast<unsigned long long>(b & 0x0000FFFFFFFFFFFFULL));
    return std::string(buf);
}

}  // namespace internal
}  // namespace kubemq
