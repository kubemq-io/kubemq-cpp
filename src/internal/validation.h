// src/internal/validation.h
#pragma once

#include <chrono>
#include <string>

#include "kubemq/status.h"

namespace kubemq {
namespace internal {

Status ValidateChannel(const std::string& channel, const std::string& operation);
Status ValidateChannelNoWildcard(const std::string& channel, const std::string& operation);
Status ValidateBody(const std::string& body, const std::string& metadata,
                    const std::string& operation);
Status ValidateTimeout(std::chrono::milliseconds timeout, const std::string& operation);

}  // namespace internal
}  // namespace kubemq
