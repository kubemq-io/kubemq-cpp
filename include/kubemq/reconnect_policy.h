/// @file reconnect_policy.h
/// @brief Reconnection policy with exponential backoff and jitter for gRPC streams.

// include/kubemq/reconnect_policy.h
#pragma once

#include <chrono>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Jitter mode for randomizing backoff delays.
///
/// Applied during reconnection and retry backoff calculations to prevent
/// thundering-herd effects when multiple clients reconnect simultaneously.
///
/// @see ReconnectPolicy
/// @see RetryPolicy
enum class JitterMode {
    kNone,   ///< No jitter; use the computed delay exactly.
    kFull,   ///< Full jitter; random value in [0, delay).
    kEqual,  ///< Equal jitter; delay/2 + random value in [0, delay/2).
};

/// @brief Policy controlling automatic reconnection for gRPC streams.
///
/// Configures exponential backoff with optional jitter for stream handles
/// (EventStreamHandle, EventStoreStreamHandle, QueueUpstreamHandle) and
/// subscriptions. A ring buffer holds messages during reconnection to
/// prevent data loss.
///
/// @see ClientOptions
/// @see JitterMode
/// @example error_handling/reconnection/main.cc
struct KUBEMQ_API ReconnectPolicy {
    /// @brief Maximum number of reconnection attempts. 0 means infinite retries. Default: 0.
    int max_attempts = 0;

    /// @brief Initial delay before the first reconnection attempt. Default: 1000ms.
    std::chrono::milliseconds initial_delay{1000};

    /// @brief Upper bound on the backoff delay. Default: 30000ms (30s).
    std::chrono::milliseconds max_delay{30000};

    /// @brief Multiplier applied to the delay after each failed attempt. Default: 2.0.
    double backoff_multiplier = 2.0;

    /// @brief Jitter mode applied to backoff delays. Default: kFull.
    JitterMode jitter = JitterMode::kFull;

    /// @brief Ring buffer capacity for messages buffered during reconnection. Default: 1024.
    ///
    /// Uses a power-of-2 size for efficient modular indexing.
    /// The Go SDK uses 1000; the C++ SDK intentionally diverges.
    int buffer_size = 1024;
};

}  // namespace v1
}  // namespace kubemq
