// src/internal/stream_handle_impl.h
#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>

#include "internal/callback_guard.h"
#include "internal/middleware/error_mapper.h"
#include "internal/transport/grpc_transport.h"
#include "kubemq.pb.h"
#include "kubemq/client_options.h"
#include "kubemq/stream_handle.h"

namespace kubemq {
inline namespace v1 {

class EventStreamHandle::Impl {
public:
    std::unique_ptr<grpc::ClientContext> ctx;
    internal::GrpcTransport::EventStreamWriter stream;
    std::function<void(const Status&)> on_error;
    std::atomic<bool> done{false};
    std::thread reader_thread;
    std::mutex write_mu;
    std::mutex callback_mu;  // Protects on_error read/write
    std::once_flag join_once;
    ClientOptions opts;

    void StartReader() {
        reader_thread = std::thread([this]() {
            ::kubemq_pb::Result result;
            while (stream && stream->Read(&result)) {
                // Results from event stream are fire-and-forget; no callback needed
                // (unlike EventStore which tracks results)
            }
            done.store(true, std::memory_order_release);
            auto grpc_status = stream->Finish();
            if (!grpc_status.ok()) {
                std::lock_guard<std::mutex> lock(callback_mu);
                if (on_error) {
                    internal::SafeInvoke(
                        [&]() {
                            on_error(
                                internal::FromGrpcStatus(grpc_status, "EventStreamHandle::Finish"));
                        },
                        nullptr, "EventStreamHandle::Finish");
                }
            }
        });
    }

    void JoinReader() {
        std::call_once(join_once, [this]() {
            if (reader_thread.joinable()) {
                reader_thread.join();
            }
        });
    }
};

class EventStoreStreamHandle::Impl {
public:
    std::unique_ptr<grpc::ClientContext> ctx;
    internal::GrpcTransport::EventStreamWriter stream;
    std::function<void(const EventStoreResult&)> on_result;
    std::function<void(const Status&)> on_error;
    std::atomic<bool> done{false};
    std::thread reader_thread;
    std::mutex write_mu;
    std::mutex callback_mu;  // Protects on_result and on_error read/write
    std::once_flag join_once;
    ClientOptions opts;

    void StartReader() {
        reader_thread = std::thread([this]() {
            ::kubemq_pb::Result result;
            while (stream && stream->Read(&result)) {
                EventStoreResult store_result{result.eventid(), result.sent(), result.error()};
                std::lock_guard<std::mutex> lock(callback_mu);
                if (on_result) {
                    internal::SafeInvoke([&]() { on_result(store_result); }, on_error,
                                         "EventStoreStreamHandle::OnResult");
                }
            }
            done.store(true, std::memory_order_release);
            auto grpc_status = stream->Finish();
            if (!grpc_status.ok()) {
                std::lock_guard<std::mutex> lock(callback_mu);
                if (on_error) {
                    internal::SafeInvoke(
                        [&]() {
                            on_error(internal::FromGrpcStatus(grpc_status,
                                                              "EventStoreStreamHandle::Finish"));
                        },
                        nullptr, "EventStoreStreamHandle::Finish");
                }
            }
        });
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
