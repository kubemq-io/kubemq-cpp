// src/internal/transport/grpc_transport.cc
#include "internal/transport/grpc_transport.h"

#include "internal/middleware/auth.h"
#include "internal/middleware/error_mapper.h"
#include "internal/transport/tls.h"
#include "internal/types/error.h"

namespace kubemq {
namespace internal {

GrpcTransport::GrpcTransport() = default;
GrpcTransport::~GrpcTransport() {
    Close();
}

StatusOr<std::unique_ptr<GrpcTransport>> GrpcTransport::Create(const ClientOptions& opts) {
    auto transport = std::unique_ptr<GrpcTransport>(new GrpcTransport());
    auto status = transport->Connect(opts);
    if (!status.ok()) {
        return status;
    }
    return transport;
}

Status GrpcTransport::Connect(const ClientOptions& opts) {
    opts_ = opts;

    // Build target address
    std::string target = opts_.host() + ":" + std::to_string(opts_.port());

    // Build channel arguments
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(opts_.max_receive_message_size());
    args.SetMaxSendMessageSize(opts_.max_send_message_size());

    // Keepalive settings
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, static_cast<int>(opts_.keepalive_time().count()));
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, static_cast<int>(opts_.keepalive_timeout().count()));
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                opts_.permit_keepalive_without_stream() ? 1 : 0);

    // Create channel credentials (handles PEM content and file-path fallback)
    auto creds_or = BuildChannelCredentials(opts_.tls_config());
    if (!creds_or.ok())
        return creds_or.status();
    auto creds = std::move(*creds_or);
    if (opts_.tls_config().is_enabled() && !opts_.tls_config().server_name_override().empty()) {
        args.SetSslTargetNameOverride(opts_.tls_config().server_name_override());
    }

    // Create channel
    channel_ = grpc::CreateCustomChannel(target, creds, args);
    if (!channel_) {
        return MakeError(ErrorCode::kTransient, "failed to create gRPC channel",
                         "GrpcTransport::Connect");
    }

    // Create stub
    stub_ = ::kubemq_pb::kubemq::NewStub(channel_);
    if (!stub_) {
        return MakeError(ErrorCode::kTransient, "failed to create gRPC stub",
                         "GrpcTransport::Connect");
    }

    // Wait for channel to be ready if configured
    if (opts_.wait_for_ready()) {
        auto deadline = std::chrono::system_clock::now() + opts_.connection_timeout();
        if (!channel_->WaitForConnected(deadline)) {
            return MakeError(ErrorCode::kTimeout,
                             "connection timeout waiting for channel to be ready",
                             "GrpcTransport::Connect");
        }
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        state_machine_ = std::make_unique<internal::StateMachine>(ConnectionState::kIdle);
        state_machine_->Transition(ConnectionState::kConnecting);
        state_machine_->Transition(ConnectionState::kReady);
    }

    return Status();
}

Status GrpcTransport::Close() {
    Status result;
    std::call_once(close_once_, [this, &result]() {
        closed_.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(mu_);
        if (state_machine_)
            state_machine_->Transition(ConnectionState::kClosed);
    });
    return result;
}

ConnectionState GrpcTransport::State() const {
    if (closed_.load(std::memory_order_acquire)) {
        return ConnectionState::kClosed;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (state_machine_) {
        return state_machine_->Current();
    }
    if (channel_) {
        auto grpc_state = channel_->GetState(false);
        switch (grpc_state) {
            case GRPC_CHANNEL_IDLE:
                return ConnectionState::kIdle;
            case GRPC_CHANNEL_CONNECTING:
                return ConnectionState::kReconnecting;
            case GRPC_CHANNEL_READY:
                return ConnectionState::kReady;
            case GRPC_CHANNEL_TRANSIENT_FAILURE:
                return ConnectionState::kReconnecting;
            case GRPC_CHANNEL_SHUTDOWN:
                return ConnectionState::kClosed;
        }
    }
    return ConnectionState::kIdle;
}

std::string GrpcTransport::ClientID() const {
    return opts_.client_id();
}

const ClientOptions& GrpcTransport::Options() const {
    return opts_;
}

Status GrpcTransport::CheckClosed() const {
    if (closed_.load(std::memory_order_acquire)) {
        return kErrClientClosed;
    }
    return Status();
}

Status GrpcTransport::ApplyAuth(grpc::ClientContext& ctx) const {
    return internal::InjectAuthToken(&ctx, opts_.auth_token(), opts_.credential_provider());
}

// --- Ping ---

StatusOr<ServerInfo> GrpcTransport::Ping() {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + opts_.connection_timeout());
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::Empty request;
    ::kubemq_pb::PingResult response;

    auto status = stub_->Ping(&ctx, request, &response);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::Ping");
    }

    return ServerInfo{response.host(), response.version(), response.serverstarttime(),
                      response.serveruptimeseconds()};
}

// --- Events ---

Status GrpcTransport::SendEvent(const ::kubemq_pb::Event& proto_event) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kDefaultSendTimeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::Result result;
    auto status = stub_->SendEvent(&ctx, proto_event, &result);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendEvent", proto_event.channel());
    }

    if (!result.error().empty()) {
        return FromServerError(result.error(), "GrpcTransport::SendEvent", proto_event.channel());
    }

    return Status();
}

StatusOr<EventStoreResult> GrpcTransport::SendEventStore(const ::kubemq_pb::Event& proto_event) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kDefaultSendTimeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::Result result;
    auto status = stub_->SendEvent(&ctx, proto_event, &result);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendEventStore", proto_event.channel());
    }

    if (!result.error().empty()) {
        return MakeError(ErrorCode::kFatal, result.error(), "GrpcTransport::SendEventStore",
                         proto_event.channel());
    }

    return EventStoreResult{result.eventid(), result.sent(), result.error()};
}

StatusOr<GrpcTransport::EventStreamWriter> GrpcTransport::OpenEventStream(
    std::unique_ptr<grpc::ClientContext>& out_ctx) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    auto ctx = std::make_unique<grpc::ClientContext>();
    auto auth_status = ApplyAuth(*ctx);
    if (!auth_status.ok())
        return auth_status;

    auto stream = stub_->SendEventsStream(ctx.get());
    if (!stream) {
        return MakeError(ErrorCode::kTransient, "failed to open event stream",
                         "GrpcTransport::OpenEventStream");
    }

    out_ctx = std::move(ctx);
    return EventStreamWriter(stream.release());
}

StatusOr<GrpcTransport::EventReader> GrpcTransport::SubscribeToEvents(
    const ::kubemq_pb::Subscribe& sub, std::shared_ptr<grpc::ClientContext>& ctx) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    auto auth_status = ApplyAuth(*ctx);
    if (!auth_status.ok())
        return auth_status;

    auto reader = stub_->SubscribeToEvents(ctx.get(), sub);
    if (!reader) {
        return MakeError(ErrorCode::kTransient, "failed to open event subscription stream",
                         "GrpcTransport::SubscribeToEvents", sub.channel());
    }

    return EventReader(reader.release());
}

// --- RPC (Commands & Queries) ---

StatusOr<::kubemq_pb::Response> GrpcTransport::SendRequest(const ::kubemq_pb::Request& request) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    auto timeout_ms = request.timeout() > 0 ? request.timeout()
                                            : static_cast<int32_t>(kDefaultRPCTimeout.count());
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::Response response;
    auto status = stub_->SendRequest(&ctx, request, &response);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendRequest", request.channel());
    }

    return response;
}

Status GrpcTransport::SendResponse(const ::kubemq_pb::Response& response) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kDefaultSendTimeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::Empty empty;
    auto status = stub_->SendResponse(&ctx, response, &empty);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendResponse", response.replychannel());
    }

    return Status();
}

StatusOr<GrpcTransport::RequestReader> GrpcTransport::SubscribeToRequests(
    const ::kubemq_pb::Subscribe& sub, std::shared_ptr<grpc::ClientContext>& ctx) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    auto auth_status = ApplyAuth(*ctx);
    if (!auth_status.ok())
        return auth_status;

    auto reader = stub_->SubscribeToRequests(ctx.get(), sub);
    if (!reader) {
        return MakeError(ErrorCode::kTransient, "failed to open request subscription stream",
                         "GrpcTransport::SubscribeToRequests", sub.channel());
    }

    return RequestReader(reader.release());
}

// --- Queues (Simple) ---

StatusOr<::kubemq_pb::SendQueueMessageResult> GrpcTransport::SendQueueMessage(
    const ::kubemq_pb::QueueMessage& msg) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kDefaultSendTimeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::SendQueueMessageResult result;
    auto status = stub_->SendQueueMessage(&ctx, msg, &result);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendQueueMessage", msg.channel());
    }

    return result;
}

StatusOr<::kubemq_pb::QueueMessagesBatchResponse> GrpcTransport::SendQueueMessagesBatch(
    const ::kubemq_pb::QueueMessagesBatchRequest& req) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kDefaultSendTimeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::QueueMessagesBatchResponse response;
    auto status = stub_->SendQueueMessagesBatch(&ctx, req, &response);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::SendQueueMessagesBatch");
    }

    return response;
}

StatusOr<::kubemq_pb::ReceiveQueueMessagesResponse> GrpcTransport::ReceiveQueueMessages(
    const ::kubemq_pb::ReceiveQueueMessagesRequest& req) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    auto timeout = std::chrono::seconds(req.waittimeseconds() > 0 ? req.waittimeseconds() + 5 : 30);
    ctx.set_deadline(std::chrono::system_clock::now() + timeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::ReceiveQueueMessagesResponse response;
    auto status = stub_->ReceiveQueueMessages(&ctx, req, &response);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::ReceiveQueueMessages", req.channel());
    }

    return response;
}

StatusOr<::kubemq_pb::AckAllQueueMessagesResponse> GrpcTransport::AckAllQueueMessages(
    const ::kubemq_pb::AckAllQueueMessagesRequest& req) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    grpc::ClientContext ctx;
    auto timeout = std::chrono::seconds(req.waittimeseconds() > 0 ? req.waittimeseconds() + 5 : 30);
    ctx.set_deadline(std::chrono::system_clock::now() + timeout);
    auto auth_status = ApplyAuth(ctx);
    if (!auth_status.ok())
        return auth_status;

    ::kubemq_pb::AckAllQueueMessagesResponse response;
    auto status = stub_->AckAllQueueMessages(&ctx, req, &response);
    if (!status.ok()) {
        return FromGrpcStatus(status, "GrpcTransport::AckAllQueueMessages", req.channel());
    }

    return response;
}

// --- Queues (Stream) ---

StatusOr<GrpcTransport::QueueUpstreamStream> GrpcTransport::OpenQueueUpstream(
    std::unique_ptr<grpc::ClientContext>& out_ctx) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    auto ctx = std::make_unique<grpc::ClientContext>();
    auto auth_status = ApplyAuth(*ctx);
    if (!auth_status.ok())
        return auth_status;

    auto stream = stub_->QueuesUpstream(ctx.get());
    if (!stream) {
        return MakeError(ErrorCode::kTransient, "failed to open queue upstream stream",
                         "GrpcTransport::OpenQueueUpstream");
    }

    out_ctx = std::move(ctx);
    return QueueUpstreamStream(stream.release());
}

StatusOr<GrpcTransport::QueueDownstreamStream> GrpcTransport::OpenQueueDownstream(
    std::unique_ptr<grpc::ClientContext>& out_ctx) {
    auto s = CheckClosed();
    if (!s.ok())
        return s;

    auto ctx = std::make_unique<grpc::ClientContext>();
    auto auth_status = ApplyAuth(*ctx);
    if (!auth_status.ok())
        return auth_status;

    auto stream = stub_->QueuesDownstream(ctx.get());
    if (!stream) {
        return MakeError(ErrorCode::kTransient, "failed to open queue downstream stream",
                         "GrpcTransport::OpenQueueDownstream");
    }

    out_ctx = std::move(ctx);
    return QueueDownstreamStream(stream.release());
}

}  // namespace internal
}  // namespace kubemq
