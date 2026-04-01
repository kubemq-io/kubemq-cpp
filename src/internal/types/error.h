// src/internal/types/error.h
#pragma once

#include <string>

#include "kubemq/error_code.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// Create a Status from a simple error message with operation context.
Status MakeError(ErrorCode code, const std::string& message, const std::string& operation = "",
                 const std::string& channel = "", const std::string& request_id = "");

// Create a Status from a server-side error string (e.g., from proto Result.Error).
// Returns OK status if error_message is empty.
Status FromServerError(const std::string& error_message, const std::string& operation = "",
                       const std::string& channel = "");

// Create a validation error status.
Status ValidationError(const std::string& message, const std::string& operation = "",
                       const std::string& channel = "");

// Check if a status represents a connection-level error that should trigger reconnection.
bool IsConnectionError(const Status& status);

}  // namespace internal
}  // namespace kubemq
