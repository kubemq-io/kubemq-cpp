// src/validation.cc
#include "internal/validation.h"

#include <algorithm>

namespace kubemq {
namespace internal {

Status ValidateChannel(const std::string& channel, const std::string& operation) {
    if (channel.empty()) {
        return Status(ErrorCode::kValidation, "channel is required", operation, "", "");
    }
    auto is_ascii_space = [](unsigned char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    };
    if (std::any_of(channel.begin(), channel.end(), is_ascii_space)) {
        return Status(ErrorCode::kValidation, "channel must not contain whitespace", operation,
                      channel, "");
    }
    if (channel.back() == '.') {
        return Status(ErrorCode::kValidation, "channel must not end with '.'", operation, channel,
                      "");
    }
    return Status();
}

Status ValidateChannelNoWildcard(const std::string& channel, const std::string& operation) {
    auto s = ValidateChannel(channel, operation);
    if (!s.ok())
        return s;
    if (channel.find('*') != std::string::npos || channel.find('>') != std::string::npos) {
        return Status(ErrorCode::kValidation, "wildcards not allowed for send operations",
                      operation, channel, "");
    }
    return Status();
}

Status ValidateBody(const std::string& body, const std::string& metadata,
                    const std::string& operation) {
    if (body.empty() && metadata.empty()) {
        return Status(ErrorCode::kValidation, "body or metadata is required", operation, "", "");
    }
    return Status();
}

Status ValidateTimeout(std::chrono::milliseconds timeout, const std::string& operation) {
    if (timeout.count() <= 0) {
        return Status(ErrorCode::kValidation, "timeout must be > 0", operation, "", "");
    }
    return Status();
}

}  // namespace internal
}  // namespace kubemq
