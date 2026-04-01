// src/internal/types/error.cc
#include "internal/types/error.h"

namespace kubemq {
namespace internal {

Status MakeError(ErrorCode code, const std::string& message, const std::string& operation,
                 const std::string& channel, const std::string& request_id) {
    return Status(code, message, operation, channel, request_id);
}

Status FromServerError(const std::string& error_message, const std::string& operation,
                       const std::string& channel) {
    if (error_message.empty()) {
        return Status();
    }
    return Status(ErrorCode::kFatal, error_message, operation, channel, "");
}

Status ValidationError(const std::string& message, const std::string& operation,
                       const std::string& channel) {
    return Status(ErrorCode::kValidation, message, operation, channel, "");
}

bool IsConnectionError(const Status& status) {
    if (status.ok()) {
        return false;
    }
    return status.code() == ErrorCode::kTransient;
}

}  // namespace internal
}  // namespace kubemq
