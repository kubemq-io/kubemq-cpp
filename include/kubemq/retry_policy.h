/// @file retry_policy.h
/// @brief Retry policy with exponential backoff for individual RPC calls.

// include/kubemq/retry_policy.h
#pragma once

#include <chrono>

#include "kubemq/export.h"
#include "kubemq/reconnect_policy.h"  // For JitterMode

namespace kubemq {
inline namespace v1 {

/// @brief Policy controlling automatic retries for individual RPC calls.
///
/// Applied to unary RPCs (e.g., Client::SendEvent, Client::SendCommand) when
/// a transient error is detected. Uses exponential backoff with optional
/// jitter between retry attempts.
///
/// @see ClientOptions
/// @see JitterMode
/// @example error_handling/connection_error/main.cc
struct KUBEMQ_API RetryPolicy {
    /// @brief Maximum number of retries. 0 disables retries; capped at 10. Default: 3.
    int max_retries = 3;

    /// @brief Initial backoff delay before the first retry. Default: 100ms.
    std::chrono::milliseconds initial_backoff{100};

    /// @brief Upper bound on the backoff delay. Default: 10000ms (10s).
    std::chrono::milliseconds max_backoff{10000};

    /// @brief Multiplier applied to the backoff after each failed attempt. Default: 2.0.
    double multiplier = 2.0;

    /// @brief Jitter mode applied to backoff delays. Default: kFull.
    /// @see JitterMode
    JitterMode jitter = JitterMode::kFull;
};

}  // namespace v1
}  // namespace kubemq
