// src/internal/queue_stream_impl.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "internal/callback_guard.h"
#include "internal/middleware/error_mapper.h"
#include "internal/transport/converter.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "kubemq.pb.h"
#include "kubemq/client_options.h"
#include "kubemq/queue_downstream_receiver.h"
#include "kubemq/queue_message.h"
#include "kubemq/queue_upstream_handle.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

class QueueUpstreamHandle::Impl {
public:
    std::unique_ptr<grpc::ClientContext> ctx;
    internal::GrpcTransport::QueueUpstreamStream stream;
    std::function<void(const QueueUpstreamResult&)> on_result;
    std::function<void(const Status&)> on_error;
    std::atomic<bool> done{false};
    std::thread reader_thread;
    std::mutex write_mu;
    std::mutex callback_mu;  // Protects on_result and on_error read/write
    std::once_flag join_once;
    ClientOptions opts;

    void StartReader() {
        reader_thread = std::thread([this]() {
            ::kubemq_pb::QueuesUpstreamResponse response;
            while (stream && stream->Read(&response)) {
                std::lock_guard<std::mutex> lock(callback_mu);
                if (on_result) {
                    auto data = internal::QueueUpstreamResponseFromProto(response);
                    QueueUpstreamResult result{std::move(data.ref_request_id),
                                               std::move(data.results), data.is_error,
                                               std::move(data.error)};
                    internal::SafeInvoke([&]() { on_result(result); }, on_error,
                                         "QueueUpstreamHandle::OnResult");
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
                                                              "QueueUpstreamHandle::Finish"));
                        },
                        nullptr, "QueueUpstreamHandle::Finish");
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

class QueueDownstreamMessage::Impl {
public:
    QueueMessage message;
    std::string transaction_id;
    uint64_t sequence = 0;
    bool auto_ack = false;

    // Weak reference to the downstream stream for settlement;
    // the receiver (QueueDownstreamReceiver::Impl) owns the shared_ptr.
    std::weak_ptr<grpc::ClientReaderWriter<::kubemq_pb::QueuesDownstreamRequest,
                                           ::kubemq_pb::QueuesDownstreamResponse>>
        stream;
    std::shared_ptr<std::mutex> stream_mu;  // Shared mutex for stream writes

    Status SendSettle(int request_type, const std::string& requeue_channel = "") {
        if (auto_ack) {
            return Status(ErrorCode::kValidation, "cannot settle: poll used AutoAck",
                          "QueueDownstreamMessage::Settle", "", "");
        }
        auto s = stream.lock();
        if (!s) {
            return Status(ErrorCode::kFatal,
                          "receiver has been destroyed; settlement no longer possible",
                          "QueueDownstreamMessage::Settle", "", "");
        }
        if (transaction_id.empty()) {
            return Status(ErrorCode::kValidation, "message has no transaction ID",
                          "QueueDownstreamMessage::Settle", "", "");
        }

        ::kubemq_pb::QueuesDownstreamRequest req;
        req.set_requestid(internal::GenerateUUID());
        req.set_requesttypedata(
            static_cast<::kubemq_pb::QueuesDownstreamRequestType>(request_type));
        req.set_reftransactionid(transaction_id);
        req.add_sequencerange(static_cast<int64_t>(sequence));
        if (!requeue_channel.empty()) {
            req.set_requeuechannel(requeue_channel);
        }

        if (!stream_mu) {
            return Status(ErrorCode::kFatal, "no stream mutex available",
                          "QueueDownstreamMessage::Settle", "", "");
        }
        std::lock_guard<std::mutex> lock(*stream_mu);
        if (!s->Write(req)) {
            return Status(ErrorCode::kTransient, "failed to write settlement to downstream stream",
                          "QueueDownstreamMessage::Settle", "", "");
        }
        return Status();
    }
};

class PollResponse::Impl {
public:
    std::string transaction_id;
    std::vector<QueueDownstreamMessage> messages;
    bool is_error = false;
    std::string error;
    bool auto_ack = false;
    bool transaction_complete = false;

    // Keeps the receiver (and its gRPC stream/context) alive for settlement
    std::unique_ptr<QueueDownstreamReceiver> receiver;

    // Weak reference to the downstream stream for settlement;
    // the receiver (QueueDownstreamReceiver::Impl) owns the shared_ptr.
    std::weak_ptr<grpc::ClientReaderWriter<::kubemq_pb::QueuesDownstreamRequest,
                                           ::kubemq_pb::QueuesDownstreamResponse>>
        stream;
    std::shared_ptr<std::mutex> stream_mu;

    Status SendSettleAll(int request_type, const std::string& requeue_channel = "") {
        if (auto_ack) {
            return Status(ErrorCode::kValidation, "cannot settle: poll used AutoAck",
                          "PollResponse::Settle", "", "");
        }
        auto s = stream.lock();
        if (!s) {
            return Status(ErrorCode::kFatal,
                          "receiver has been destroyed; settlement no longer possible",
                          "PollResponse::Settle", "", "");
        }
        if (transaction_id.empty()) {
            return Status(ErrorCode::kValidation, "no transaction ID", "PollResponse::Settle", "",
                          "");
        }
        if (messages.empty()) {
            return Status();  // Nothing to settle
        }

        ::kubemq_pb::QueuesDownstreamRequest req;
        req.set_requestid(internal::GenerateUUID());
        req.set_requesttypedata(
            static_cast<::kubemq_pb::QueuesDownstreamRequestType>(request_type));
        req.set_reftransactionid(transaction_id);
        if (!requeue_channel.empty()) {
            req.set_requeuechannel(requeue_channel);
        }

        if (!stream_mu) {
            return Status(ErrorCode::kFatal, "no stream mutex available", "PollResponse::Settle",
                          "", "");
        }
        std::lock_guard<std::mutex> lock(*stream_mu);
        if (!s->Write(req)) {
            return Status(ErrorCode::kTransient, "failed to write settlement to downstream stream",
                          "PollResponse::Settle", "", "");
        }
        return Status();
    }
};

class QueueDownstreamReceiver::Impl {
public:
    std::unique_ptr<grpc::ClientContext> ctx;
    internal::GrpcTransport::QueueDownstreamStream stream;
    std::function<void(const Status&)> on_error;
    ClientOptions opts;
    std::shared_ptr<std::mutex> stream_mu = std::make_shared<std::mutex>();
    std::atomic<bool> closed{false};
};

}  // namespace v1
}  // namespace kubemq
