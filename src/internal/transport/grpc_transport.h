// src/internal/transport/grpc_transport.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "internal/transport/message_buffer.h"
#include "internal/transport/reconnect.h"
#include "internal/transport/state_machine.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/channel_info.h"
#include "kubemq/client_options.h"
#include "kubemq/command.h"
#include "kubemq/connection_state.h"
#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/query.h"
#include "kubemq/queue_message.h"
#include "kubemq/server_info.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

class GrpcTransport {
public:
    static StatusOr<std::unique_ptr<GrpcTransport>> Create(const ClientOptions& opts);

    // Lifecycle
    Status Close();
    ConnectionState State() const;

    // Ping
    StatusOr<ServerInfo> Ping();

    // --- Events ---
    Status SendEvent(const ::kubemq_pb::Event& proto_event);
    StatusOr<EventStoreResult> SendEventStore(const ::kubemq_pb::Event& proto_event);

    // Event stream (bidi): returns reader/writer pair
    using EventStreamWriter =
        std::shared_ptr<grpc::ClientReaderWriter<::kubemq_pb::Event, ::kubemq_pb::Result>>;
    StatusOr<EventStreamWriter> OpenEventStream(std::unique_ptr<grpc::ClientContext>& out_ctx);

    // Event subscription (server stream)
    using EventReader = std::shared_ptr<grpc::ClientReader<::kubemq_pb::EventReceive>>;
    StatusOr<EventReader> SubscribeToEvents(const ::kubemq_pb::Subscribe& sub,
                                            std::shared_ptr<grpc::ClientContext>& ctx);

    // --- RPC ---
    StatusOr<::kubemq_pb::Response> SendRequest(const ::kubemq_pb::Request& request);
    Status SendResponse(const ::kubemq_pb::Response& response);

    // Request subscription (server stream)
    using RequestReader = std::shared_ptr<grpc::ClientReader<::kubemq_pb::Request>>;
    StatusOr<RequestReader> SubscribeToRequests(const ::kubemq_pb::Subscribe& sub,
                                                std::shared_ptr<grpc::ClientContext>& ctx);

    // --- Queues (Simple) ---
    StatusOr<::kubemq_pb::SendQueueMessageResult> SendQueueMessage(
        const ::kubemq_pb::QueueMessage& msg);
    StatusOr<::kubemq_pb::QueueMessagesBatchResponse> SendQueueMessagesBatch(
        const ::kubemq_pb::QueueMessagesBatchRequest& req);
    StatusOr<::kubemq_pb::ReceiveQueueMessagesResponse> ReceiveQueueMessages(
        const ::kubemq_pb::ReceiveQueueMessagesRequest& req);
    StatusOr<::kubemq_pb::AckAllQueueMessagesResponse> AckAllQueueMessages(
        const ::kubemq_pb::AckAllQueueMessagesRequest& req);

    // --- Queues (Stream) ---
    using QueueUpstreamStream =
        std::shared_ptr<grpc::ClientReaderWriter<::kubemq_pb::QueuesUpstreamRequest,
                                                 ::kubemq_pb::QueuesUpstreamResponse>>;
    StatusOr<QueueUpstreamStream> OpenQueueUpstream(std::unique_ptr<grpc::ClientContext>& out_ctx);

    using QueueDownstreamStream =
        std::shared_ptr<grpc::ClientReaderWriter<::kubemq_pb::QueuesDownstreamRequest,
                                                 ::kubemq_pb::QueuesDownstreamResponse>>;
    StatusOr<QueueDownstreamStream> OpenQueueDownstream(
        std::unique_ptr<grpc::ClientContext>& out_ctx);

    // Connection helpers
    std::string ClientID() const;
    const ClientOptions& Options() const;

    ~GrpcTransport();

    // Access to state machine for wiring callbacks
    StateMachine* GetStateMachine() { return state_machine_.get(); }

private:
    GrpcTransport();
    Status Connect(const ClientOptions& opts);

    // Returns kErrClientClosed if transport is closed.
    Status CheckClosed() const;

    // Apply auth metadata to a client context.
    Status ApplyAuth(grpc::ClientContext& ctx) const;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<::kubemq_pb::kubemq::Stub> stub_;
    ClientOptions opts_;
    std::atomic<bool> closed_{false};
    std::once_flag close_once_;
    mutable std::mutex mu_;
    std::unique_ptr<internal::StateMachine> state_machine_;
    std::unique_ptr<internal::ReconnectLoop> reconnect_loop_;
    std::unique_ptr<internal::MessageBuffer> message_buffer_;
};

}  // namespace internal
}  // namespace kubemq
