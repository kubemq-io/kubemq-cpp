// src/internal/middleware/chain.cc
#include "internal/middleware/chain.h"

namespace kubemq {
namespace internal {

std::function<Status(const std::function<Status()>&)> BuildChain(
    const std::vector<Interceptor>& interceptors) {
    if (interceptors.empty()) {
        return [](const std::function<Status()>& op) -> Status { return op(); };
    }

    // Build the chain from back to front: the last interceptor wraps the
    // actual operation, the second-to-last wraps that, and so on.
    return [interceptors](const std::function<Status()>& operation) -> Status {
        auto current = operation;
        for (auto it = interceptors.rbegin(); it != interceptors.rend(); ++it) {
            auto interceptor = *it;
            auto prev = current;
            current = [interceptor, prev]() -> Status { return interceptor(prev); };
        }
        return current();
    };
}

Status RunChain(const std::vector<Interceptor>& interceptors,
                const std::function<Status()>& operation) {
    auto chain = BuildChain(interceptors);
    return chain(operation);
}

}  // namespace internal
}  // namespace kubemq
