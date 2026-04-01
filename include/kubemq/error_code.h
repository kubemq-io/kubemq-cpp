/// @file error_code.h
/// @brief Error codes for KubeMQ operations.
#pragma once

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Error codes for KubeMQ operations.
///
/// Maps to gRPC status codes with KubeMQ-specific additions.
/// Use ErrorCodeToString() to convert to a human-readable string.
/// Use IsRetryable() to check if an error is transient.
///
/// @see Status, StatusOr
enum class ErrorCode {
    /// @brief Operation succeeded.
    kOk = 0,
    /// @brief Retryable network error (gRPC UNAVAILABLE).
    kTransient,
    /// @brief Deadline exceeded (gRPC DEADLINE_EXCEEDED).
    kTimeout,
    /// @brief Rate-limited (gRPC RESOURCE_EXHAUSTED).
    kThrottling,
    /// @brief Token invalid (gRPC UNAUTHENTICATED).
    kAuthentication,
    /// @brief Permission denied (gRPC PERMISSION_DENIED).
    kAuthorization,
    /// @brief Input validation failed (gRPC INVALID_ARGUMENT).
    kValidation,
    /// @brief Resource not found (gRPC NOT_FOUND).
    kNotFound,
    /// @brief Unrecoverable server error (gRPC INTERNAL).
    kFatal,
    /// @brief Context cancelled (gRPC CANCELLED).
    kCancellation,
    /// @brief Buffer full -- backpressure applied.
    kBackpressure,
};

/// @brief Convert an ErrorCode to a human-readable string.
/// @param code The error code to convert.
/// @return A null-terminated C string name for the code.
KUBEMQ_API const char* ErrorCodeToString(ErrorCode code);

/// @brief Check if an error code represents a retryable error.
/// @param code The error code to check.
/// @return true if the error is transient and can be retried.
KUBEMQ_API bool IsRetryable(ErrorCode code);

}  // namespace v1
}  // namespace kubemq
