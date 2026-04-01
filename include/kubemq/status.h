/// @file status.h
/// @brief Status, StatusOr, and domain-specific error types for KubeMQ operations.
#pragma once

#include <exception>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "kubemq/error_code.h"
#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Represents the result of an operation (success or error).
///
/// Follows the error-value pattern. Check ok() before accessing details.
/// The default-constructed Status is OK.
///
/// @see StatusOr, ErrorCode
/// @example error_handling/connection_error/main.cc
class KUBEMQ_API Status {
public:
    /// @brief Construct an OK status.
    Status();  // OK status
    /// @brief Construct an error status.
    /// @param code The error code.
    /// @param message A human-readable error description.
    Status(ErrorCode code, std::string message);
    /// @brief Construct a detailed error status with context.
    /// @param code The error code.
    /// @param message A human-readable error description.
    /// @param operation The operation that failed.
    /// @param channel The channel associated with the error.
    /// @param request_id The request ID associated with the error.
    Status(ErrorCode code, std::string message, std::string operation, std::string channel,
           std::string request_id);

    /// @brief Check if the operation succeeded.
    /// @return true if no error.
    bool ok() const;
    /// @brief Get the error code.
    /// @return The ErrorCode (kOk if successful).
    ErrorCode code() const;
    /// @brief Get the error message.
    /// @return The human-readable error description.
    const std::string& message() const;
    /// @brief Check if the error is transient and can be retried.
    /// @return true if the error code indicates a retryable condition.
    bool is_retryable() const;
    /// @brief Get the operation that failed.
    /// @return The operation name, or empty string if not set.
    const std::string& operation() const;
    /// @brief Get the channel associated with the error.
    /// @return The channel name, or empty string if not set.
    const std::string& channel() const;
    /// @brief Get the request ID associated with the error.
    /// @return The request ID, or empty string if not set.
    const std::string& request_id() const;

    /// @brief Format the status as a human-readable string.
    /// @return A string representation including code, message, and context.
    std::string ToString() const;

private:
    ErrorCode code_;
    std::string message_;
    std::string operation_;
    std::string channel_;
    std::string request_id_;
};

/// @brief Sentinel error: the client has been closed.
KUBEMQ_API extern const Status kErrClientClosed;
/// @brief Sentinel error: the operation is not implemented.
KUBEMQ_API extern const Status kErrNotImplemented;
/// @brief Sentinel error: input validation failed.
KUBEMQ_API extern const Status kErrValidation;

/// @brief Exception thrown when accessing the value of an error StatusOr.
///
/// Thrown by StatusOr::value() when the StatusOr holds an error Status
/// instead of a value. Callers should check ok() before calling value().
class KUBEMQ_API BadStatusAccess : public std::exception {
public:
    explicit BadStatusAccess(Status status)
        : status_(std::move(status)), what_msg_("BadStatusAccess: " + status_.ToString()) {}

    const char* what() const noexcept override { return what_msg_.c_str(); }
    const Status& status() const noexcept { return status_; }

private:
    Status status_;
    std::string what_msg_;
};

/// @brief Holds either a value of type T (success) or a Status (error).
///
/// Use ok() to check the state, then value() or operator* to access the
/// contained value. Throws BadStatusAccess if value() is called when the
/// StatusOr holds an error.
///
/// @tparam T The value type on success.
/// @see Status, BadStatusAccess
template <typename T>
class StatusOr {
    static_assert(!std::is_same_v<T, Status>, "StatusOr<Status> is ill-formed");

public:
    /// @brief Construct from a value (success).
    /// @param value The success value.
    StatusOr(T value) : data_(std::move(value)) {}

    /// @brief Construct from an error status.
    /// @param status The error status (must not be OK).
    StatusOr(Status status) : data_(std::move(status)) {}

    /// @brief Check if this holds a value (success).
    /// @return true if a value is present.
    bool ok() const { return std::holds_alternative<T>(data_); }

    /// @brief Access the value (const lvalue reference).
    /// @return The contained value.
    /// @throws BadStatusAccess if this holds an error Status.
    const T& value() const& {
        if (!ok())
            throw BadStatusAccess(std::get<Status>(data_));
        return std::get<T>(data_);
    }
    /// @brief Access the value (lvalue reference).
    /// @return The contained value.
    /// @throws BadStatusAccess if this holds an error Status.
    T& value() & {
        if (!ok())
            throw BadStatusAccess(std::get<Status>(data_));
        return std::get<T>(data_);
    }
    /// @brief Access the value (rvalue reference, moves out).
    /// @return The contained value.
    /// @throws BadStatusAccess if this holds an error Status.
    T&& value() && {
        if (!ok())
            throw BadStatusAccess(std::get<Status>(std::move(data_)));
        return std::get<T>(std::move(data_));
    }

    /// @brief Dereference the value (const lvalue). Undefined behavior if error.
    /// @return The contained value.
    const T& operator*() const& { return std::get<T>(data_); }
    /// @brief Dereference the value (lvalue). Undefined behavior if error.
    /// @return The contained value.
    T& operator*() & { return std::get<T>(data_); }
    /// @brief Dereference the value (rvalue). Undefined behavior if error.
    /// @return The contained value.
    T&& operator*() && { return std::get<T>(std::move(data_)); }

    /// @brief Access the value via pointer (const). Undefined behavior if error.
    /// @return Pointer to the contained value.
    const T* operator->() const { return &std::get<T>(data_); }
    /// @brief Access the value via pointer. Undefined behavior if error.
    /// @return Pointer to the contained value.
    T* operator->() { return &std::get<T>(data_); }

    /// @brief Get the error status, or an OK status if this holds a value.
    /// @return The Status.
    const Status& status() const {
        static const Status ok_status;
        if (ok())
            return ok_status;
        return std::get<Status>(data_);
    }

    /// @brief Boolean conversion. Returns true if ok().
    explicit operator bool() const { return ok(); }

private:
    std::variant<T, Status> data_;
};

/// @brief Error indicating the reconnect buffer is full and messages were discarded.
class KUBEMQ_API BufferFullError {
public:
    /// @brief Construct a BufferFullError.
    /// @param buffer_size The buffer capacity.
    /// @param queued_count The number of messages that were queued.
    BufferFullError(int buffer_size, int queued_count)
        : buffer_size_(buffer_size), queued_count_(queued_count) {}
    /// @brief Get the buffer capacity.
    /// @return The buffer size.
    int buffer_size() const { return buffer_size_; }
    /// @brief Get the number of messages that were queued.
    /// @return The queued message count.
    int queued_count() const { return queued_count_; }
    /// @brief Format as a human-readable string.
    /// @return A string description of the error.
    std::string ToString() const;

private:
    int buffer_size_;
    int queued_count_;
};

/// @brief Error indicating a stream was broken with unacknowledged messages.
class KUBEMQ_API StreamBrokenError {
public:
    /// @brief Construct a StreamBrokenError.
    /// @param ids The IDs of messages that were not acknowledged.
    explicit StreamBrokenError(std::vector<std::string> ids)
        : unacknowledged_ids_(std::move(ids)) {}
    /// @brief Get the IDs of unacknowledged messages.
    /// @return A vector of message IDs.
    const std::vector<std::string>& unacknowledged_ids() const { return unacknowledged_ids_; }
    /// @brief Format as a human-readable string.
    /// @return A string description of the error.
    std::string ToString() const;

private:
    std::vector<std::string> unacknowledged_ids_;
};

/// @brief Error wrapping a transport-level (gRPC) failure.
class KUBEMQ_API TransportError {
public:
    /// @brief Construct a TransportError.
    /// @param operation The operation that failed.
    /// @param cause The underlying Status error.
    TransportError(std::string operation, Status cause)
        : operation_(std::move(operation)), cause_(std::move(cause)) {}
    /// @brief Get the operation that failed.
    /// @return The operation name.
    const std::string& operation() const { return operation_; }
    /// @brief Get the underlying cause.
    /// @return The error Status.
    const Status& cause() const { return cause_; }
    /// @brief Format as a human-readable string.
    /// @return A string description of the error.
    std::string ToString() const;

private:
    std::string operation_;
    Status cause_;
};

/// @brief Error indicating a subscription callback handler failed.
class KUBEMQ_API HandlerError {
public:
    /// @brief Construct a HandlerError.
    /// @param handler The handler name that failed.
    /// @param cause The underlying Status error.
    HandlerError(std::string handler, Status cause)
        : handler_(std::move(handler)), cause_(std::move(cause)) {}
    /// @brief Get the handler name.
    /// @return The handler name.
    const std::string& handler() const { return handler_; }
    /// @brief Get the underlying cause.
    /// @return The error Status.
    const Status& cause() const { return cause_; }
    /// @brief Format as a human-readable string.
    /// @return A string description of the error.
    std::string ToString() const;

private:
    std::string handler_;
    Status cause_;
};

}  // namespace v1
}  // namespace kubemq
