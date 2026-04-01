// src/internal/middleware/retry.cc
#include "internal/middleware/retry.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <thread>

#include "internal/types/jitter.h"
#include "kubemq/server_info.h"

namespace kubemq {
namespace internal {

// Calculate backoff delay for a given attempt.
// backoff = initial_backoff * multiplier^attempt, capped at max_backoff
static std::chrono::milliseconds CalculateBackoff(const RetryPolicy& policy, int attempt) {
    auto base_ms = static_cast<double>(policy.initial_backoff.count());
    auto computed = base_ms * std::pow(policy.multiplier, static_cast<double>(attempt));
    auto max_ms = static_cast<double>(policy.max_backoff.count());
    auto capped = std::min(computed, max_ms);
    auto delay = std::chrono::milliseconds(static_cast<int64_t>(capped));
    return ApplyJitter(delay, policy.jitter);
}

// Check if an error code should be retried based on idempotency.
static bool ShouldRetry(ErrorCode code, bool is_idempotent) {
    if (!IsRetryable(code)) {
        return false;
    }
    // Non-idempotent operations skip retry on timeout to avoid duplicate processing.
    if (!is_idempotent && code == ErrorCode::kTimeout) {
        return false;
    }
    return true;
}

Status RetryUnary(const RetryPolicy& policy, bool is_idempotent,
                  const std::function<Status()>& operation) {
    // No retries configured.
    if (policy.max_retries <= 0) {
        return operation();
    }

    Status last_status;
    for (int attempt = 0; attempt <= policy.max_retries; ++attempt) {
        last_status = operation();

        if (last_status.ok()) {
            return last_status;
        }

        // Check if we should retry.
        if (attempt < policy.max_retries && ShouldRetry(last_status.code(), is_idempotent)) {
            auto backoff = CalculateBackoff(policy, attempt);
            std::this_thread::sleep_for(backoff);
            continue;
        }

        // Not retryable or max retries reached.
        break;
    }

    return last_status;
}

template <typename T>
StatusOr<T> RetryUnaryOr(const RetryPolicy& policy, bool is_idempotent,
                         const std::function<StatusOr<T>()>& operation) {
    // No retries configured.
    if (policy.max_retries <= 0) {
        return operation();
    }

    StatusOr<T> last_result = Status(ErrorCode::kFatal, "retry loop did not execute");
    for (int attempt = 0; attempt <= policy.max_retries; ++attempt) {
        last_result = operation();

        if (last_result.ok()) {
            return last_result;
        }

        // Check if we should retry.
        if (attempt < policy.max_retries &&
            ShouldRetry(last_result.status().code(), is_idempotent)) {
            auto backoff = CalculateBackoff(policy, attempt);
            std::this_thread::sleep_for(backoff);
            continue;
        }

        // Not retryable or max retries reached.
        break;
    }

    return last_result;
}

// Explicit instantiations for commonly used types.
template StatusOr<kubemq::ServerInfo> RetryUnaryOr<kubemq::ServerInfo>(
    const RetryPolicy&, bool, const std::function<StatusOr<kubemq::ServerInfo>()>&);

}  // namespace internal
}  // namespace kubemq
