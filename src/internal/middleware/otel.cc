// src/internal/middleware/otel.cc
//
// No-op OpenTelemetry implementation.
// This file provides stub implementations when KUBEMQ_ENABLE_OTEL is OFF.
#include "internal/middleware/otel.h"

namespace kubemq {
namespace internal {

void InitOtel(void* /*tracer_provider*/, void* /*meter_provider*/) {
    // No-op: OpenTelemetry not enabled.
}

void ShutdownOtel() {
    // No-op: OpenTelemetry not enabled.
}

std::function<Status(const std::function<Status()>&)> MakeOtelInterceptor(
    const std::string& /*operation*/) {
    // Return identity interceptor: just call the next function in the chain.
    return [](const std::function<Status()>& next) -> Status { return next(); };
}

}  // namespace internal
}  // namespace kubemq
