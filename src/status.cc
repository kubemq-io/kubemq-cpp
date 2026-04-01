// src/status.cc
#include "kubemq/status.h"

#include <sstream>

#include "kubemq/error_code.h"

namespace kubemq {
inline namespace v1 {

Status::Status() : code_(ErrorCode::kOk) {}

Status::Status(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

Status::Status(ErrorCode code, std::string message, std::string operation, std::string channel,
               std::string request_id)
    : code_(code),
      message_(std::move(message)),
      operation_(std::move(operation)),
      channel_(std::move(channel)),
      request_id_(std::move(request_id)) {}

bool Status::ok() const {
    return code_ == ErrorCode::kOk;
}
ErrorCode Status::code() const {
    return code_;
}
const std::string& Status::message() const {
    return message_;
}
bool Status::is_retryable() const {
    return IsRetryable(code_);
}
const std::string& Status::operation() const {
    return operation_;
}
const std::string& Status::channel() const {
    return channel_;
}
const std::string& Status::request_id() const {
    return request_id_;
}

std::string Status::ToString() const {
    if (ok())
        return "OK";
    std::ostringstream ss;
    ss << ErrorCodeToString(code_) << ": " << message_;
    if (!operation_.empty())
        ss << " [op=" << operation_ << "]";
    if (!channel_.empty())
        ss << " [ch=" << channel_ << "]";
    if (!request_id_.empty())
        ss << " [req=" << request_id_ << "]";
    return ss.str();
}

const Status kErrClientClosed{ErrorCode::kFatal, "kubemq: client closed"};
const Status kErrNotImplemented{ErrorCode::kFatal, "kubemq: not implemented"};
const Status kErrValidation{ErrorCode::kValidation, "kubemq: validation failed"};

// Domain-specific error types
std::string BufferFullError::ToString() const {
    std::ostringstream ss;
    ss << "buffer full: capacity=" << buffer_size_ << " queued=" << queued_count_;
    return ss.str();
}

std::string StreamBrokenError::ToString() const {
    std::ostringstream ss;
    ss << "stream broken: " << unacknowledged_ids_.size() << " unacknowledged messages";
    return ss.str();
}

std::string TransportError::ToString() const {
    std::ostringstream ss;
    ss << "transport error in " << operation_ << ": " << cause_.ToString();
    return ss.str();
}

std::string HandlerError::ToString() const {
    std::ostringstream ss;
    ss << "handler error in " << handler_ << ": " << cause_.ToString();
    return ss.str();
}

}  // namespace v1
}  // namespace kubemq
