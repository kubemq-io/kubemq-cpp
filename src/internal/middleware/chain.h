// src/internal/middleware/chain.h
#pragma once

#include <functional>
#include <vector>

#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// An interceptor is a function that wraps an operation.
// It receives a 'next' function to call the next interceptor in the chain
// (or the actual operation at the end). It can perform pre/post processing.
using Interceptor = std::function<Status(const std::function<Status()>& next)>;

// Chain composes a list of interceptors into a single function.
// The interceptors are applied in order: the first interceptor wraps the second,
// which wraps the third, and so on. The innermost call invokes the actual operation.
//
// Usage:
//   auto chain = BuildChain({auth_interceptor, retry_interceptor, otel_interceptor});
//   Status result = chain(actual_operation);
std::function<Status(const std::function<Status()>&)> BuildChain(
    const std::vector<Interceptor>& interceptors);

// Convenience: run an operation through a chain of interceptors.
Status RunChain(const std::vector<Interceptor>& interceptors,
                const std::function<Status()>& operation);

// Template version for StatusOr<T> operations.
template <typename T>
using InterceptorOr = std::function<StatusOr<T>(const std::function<StatusOr<T>()>& next)>;

template <typename T>
StatusOr<T> RunChainOr(const std::vector<InterceptorOr<T>>& interceptors,
                       const std::function<StatusOr<T>()>& operation) {
    if (interceptors.empty()) {
        return operation();
    }

    // Build the chain from back to front.
    auto current = operation;
    for (auto it = interceptors.rbegin(); it != interceptors.rend(); ++it) {
        auto interceptor = *it;
        auto prev = current;
        current = [interceptor, prev]() -> StatusOr<T> { return interceptor(prev); };
    }

    return current();
}

}  // namespace internal
}  // namespace kubemq
