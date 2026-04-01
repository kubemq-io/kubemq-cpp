// src/internal/subscription_impl.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "kubemq.pb.h"
#include "kubemq/subscription.h"

namespace kubemq {
namespace internal {
class GrpcTransport;
}
inline namespace v1 {

class Subscription::Impl {
public:
    std::string id;
    std::shared_ptr<grpc::ClientContext> ctx;
    std::atomic<bool> done{false};
    std::thread reader_thread;
    std::once_flag join_once;

    // H-11: reconnection state
    ::kubemq_pb::Subscribe subscribe_proto;        // stored for re-subscribe
    internal::GrpcTransport* transport = nullptr;  // non-owning ref

    void Cancel() {
        done.store(true, std::memory_order_release);
        if (ctx) {
            ctx->TryCancel();
        }
    }

    void JoinReader() {
        std::call_once(join_once, [this]() {
            if (reader_thread.joinable()) {
                reader_thread.join();
            }
        });
    }
};

}  // namespace v1
}  // namespace kubemq
