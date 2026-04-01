// src/internal/client_impl.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

#include "internal/transport/grpc_transport.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

class Client::Impl {
public:
    std::unique_ptr<internal::GrpcTransport> transport;
    ClientOptions opts;
    std::atomic<bool> closed{false};
    std::once_flag close_once;
    mutable std::mutex mu;

    // Track active subscriptions and stream handles for drain
    std::vector<std::weak_ptr<grpc::ClientContext>> active_contexts;
    mutable std::mutex handles_mu;

    void TrackContext(std::shared_ptr<grpc::ClientContext> ctx) {
        std::lock_guard<std::mutex> lock(handles_mu);
        active_contexts.push_back(ctx);
    }

    void DrainAll(std::chrono::milliseconds timeout) {
        std::lock_guard<std::mutex> lock(handles_mu);
        for (auto& weak_ctx : active_contexts) {
            if (auto ctx = weak_ctx.lock()) {
                ctx->TryCancel();
            }
        }
        active_contexts.clear();
    }

    Status CheckClosed() const {
        if (closed.load(std::memory_order_acquire)) {
            return kErrClientClosed;
        }
        return Status();
    }
};

}  // namespace v1
}  // namespace kubemq
