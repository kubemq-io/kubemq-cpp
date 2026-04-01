// src/internal/types/jitter.cc
#include "internal/types/jitter.h"

#include <random>

namespace kubemq {
namespace internal {

std::chrono::milliseconds ApplyJitter(std::chrono::milliseconds delay, JitterMode mode) {
    if (mode == JitterMode::kNone || delay.count() <= 0) {
        return delay;
    }

    static thread_local std::mt19937_64 gen{std::random_device{}()};

    if (mode == JitterMode::kFull) {
        // Random in [0, delay)
        std::uniform_int_distribution<int64_t> dist(0, delay.count() - 1);
        return std::chrono::milliseconds(dist(gen));
    }

    if (mode == JitterMode::kEqual) {
        // delay/2 + random in [0, delay/2)
        auto half = delay.count() / 2;
        if (half <= 0)
            return delay;
        std::uniform_int_distribution<int64_t> dist(0, half - 1);
        return std::chrono::milliseconds(half + dist(gen));
    }

    return delay;
}

}  // namespace internal
}  // namespace kubemq
