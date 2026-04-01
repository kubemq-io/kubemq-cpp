// src/error_code.cc
#include "kubemq/error_code.h"

namespace kubemq {
inline namespace v1 {

const char* ErrorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::kOk:
            return "OK";
        case ErrorCode::kTransient:
            return "TRANSIENT";
        case ErrorCode::kTimeout:
            return "TIMEOUT";
        case ErrorCode::kThrottling:
            return "THROTTLING";
        case ErrorCode::kAuthentication:
            return "AUTHENTICATION";
        case ErrorCode::kAuthorization:
            return "AUTHORIZATION";
        case ErrorCode::kValidation:
            return "VALIDATION";
        case ErrorCode::kNotFound:
            return "NOT_FOUND";
        case ErrorCode::kFatal:
            return "FATAL";
        case ErrorCode::kCancellation:
            return "CANCELLATION";
        case ErrorCode::kBackpressure:
            return "BACKPRESSURE";
    }
    return "UNKNOWN";
}

bool IsRetryable(ErrorCode code) {
    switch (code) {
        case ErrorCode::kTransient:
        case ErrorCode::kTimeout:
        case ErrorCode::kThrottling:
            return true;
        default:
            return false;
    }
}

}  // namespace v1
}  // namespace kubemq
