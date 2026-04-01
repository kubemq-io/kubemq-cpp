// src/internal/middleware/retry.h
#pragma once

#include <functional>

#include "kubemq/retry_policy.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// Execute unary operation with retry.
// is_idempotent controls whether kTimeout triggers retry:
//   - Idempotent operations (SendEvent, Ping): retry on all retryable codes including kTimeout
//   - Non-idempotent operations (SendCommand, SendQuery, SendQueueMessage): skip retry on kTimeout
Status RetryUnary(const RetryPolicy& policy, bool is_idempotent,
                  const std::function<Status()>& operation);

// Template version for operations returning StatusOr<T>.
template <typename T>
StatusOr<T> RetryUnaryOr(const RetryPolicy& policy, bool is_idempotent,
                         const std::function<StatusOr<T>()>& operation);

}  // namespace internal
}  // namespace kubemq
