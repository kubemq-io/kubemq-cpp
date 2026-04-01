// src/internal/middleware/error_mapper.cc
#include "internal/middleware/error_mapper.h"

namespace kubemq {
namespace internal {

ErrorCode MapGrpcCode(grpc::StatusCode code) {
    switch (code) {
        case grpc::StatusCode::OK:
            return ErrorCode::kOk;
        case grpc::StatusCode::CANCELLED:
            return ErrorCode::kCancellation;
        case grpc::StatusCode::INVALID_ARGUMENT:
            return ErrorCode::kValidation;
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return ErrorCode::kTimeout;
        case grpc::StatusCode::NOT_FOUND:
            return ErrorCode::kNotFound;
        case grpc::StatusCode::ALREADY_EXISTS:
            return ErrorCode::kValidation;
        case grpc::StatusCode::PERMISSION_DENIED:
            return ErrorCode::kAuthorization;
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return ErrorCode::kThrottling;
        case grpc::StatusCode::FAILED_PRECONDITION:
            return ErrorCode::kValidation;
        case grpc::StatusCode::ABORTED:
            return ErrorCode::kTransient;
        case grpc::StatusCode::OUT_OF_RANGE:
            return ErrorCode::kValidation;
        case grpc::StatusCode::UNIMPLEMENTED:
            return ErrorCode::kFatal;
        case grpc::StatusCode::INTERNAL:
            return ErrorCode::kFatal;
        case grpc::StatusCode::UNAVAILABLE:
            return ErrorCode::kTransient;
        case grpc::StatusCode::DATA_LOSS:
            return ErrorCode::kFatal;
        case grpc::StatusCode::UNAUTHENTICATED:
            return ErrorCode::kAuthentication;
        default:
            return ErrorCode::kFatal;
    }
}

Status FromGrpcStatus(const grpc::Status& grpc_status, const std::string& operation,
                      const std::string& channel, const std::string& request_id) {
    if (grpc_status.ok()) {
        return Status();
    }

    auto error_code = MapGrpcCode(grpc_status.error_code());
    std::string message = grpc_status.error_message();
    if (message.empty()) {
        message = "gRPC error";
    }

    return Status(error_code, message, operation, channel, request_id);
}

bool IsConnectionError(const grpc::Status& status) {
    if (status.ok()) {
        return false;
    }
    switch (status.error_code()) {
        case grpc::StatusCode::UNAVAILABLE:
        case grpc::StatusCode::ABORTED:
            return true;
        default:
            return false;
    }
}

}  // namespace internal
}  // namespace kubemq
