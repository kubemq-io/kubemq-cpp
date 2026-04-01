// src/internal/middleware/error_mapper.h
#pragma once

#include <grpcpp/grpcpp.h>

#include "kubemq/error_code.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// Maps gRPC status codes to KubeMQ ErrorCode (17 codes mapped).
ErrorCode MapGrpcCode(grpc::StatusCode code);

// Convert gRPC Status to KubeMQ Status.
Status FromGrpcStatus(const grpc::Status& grpc_status, const std::string& operation = "",
                      const std::string& channel = "", const std::string& request_id = "");

// Check if gRPC error is a connection error (UNAVAILABLE, etc.).
bool IsConnectionError(const grpc::Status& status);

}  // namespace internal
}  // namespace kubemq
