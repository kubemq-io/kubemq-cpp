// src/internal/middleware/otel.h
//
// OpenTelemetry integration stubs.
// When KUBEMQ_ENABLE_OTEL is OFF (default), these are no-op implementations.
// When KUBEMQ_ENABLE_OTEL is ON, these will be replaced with real OTel integration.
#pragma once

#include <functional>
#include <string>

#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// No-op span that does nothing. When OTel is enabled, this will be replaced
// with a real span wrapper.
class OtelSpan {
public:
    OtelSpan() = default;
    explicit OtelSpan(const std::string& /*operation*/) {}

    void SetAttribute(const std::string& /*key*/, const std::string& /*value*/) {}
    void SetAttribute(const std::string& /*key*/, int64_t /*value*/) {}
    void SetStatus(bool /*ok*/, const std::string& /*message*/ = "") {}
    void End() {}
};

// No-op counter for metrics.
class OtelCounter {
public:
    OtelCounter() = default;
    explicit OtelCounter(const std::string& /*name*/) {}

    void Add(int64_t /*value*/, const std::string& /*channel*/ = "") {}
};

// No-op histogram for latency metrics.
class OtelHistogram {
public:
    OtelHistogram() = default;
    explicit OtelHistogram(const std::string& /*name*/) {}

    void Record(double /*value*/, const std::string& /*channel*/ = "") {}
};

// Initialize OpenTelemetry (no-op when disabled).
void InitOtel(void* /*tracer_provider*/, void* /*meter_provider*/);

// Shutdown OpenTelemetry (no-op when disabled).
void ShutdownOtel();

// Create a tracing interceptor (returns identity function when disabled).
std::function<Status(const std::function<Status()>&)> MakeOtelInterceptor(
    const std::string& operation);

}  // namespace internal
}  // namespace kubemq
