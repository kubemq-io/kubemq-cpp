// src/client_queues_stream.cc
#include <atomic>
#include <condition_variable>
#include <thread>

#include "internal/client_impl.h"
#include "internal/queue_stream_impl.h"
#include "internal/transport/converter.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "internal/validation.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

QueueUpstreamHandle::QueueUpstreamHandle() : impl_(std::make_unique<Impl>()) {}

QueueUpstreamHandle::~QueueUpstreamHandle() {
    Close();
}

Status QueueUpstreamHandle::Send(const std::string& request_id,
                                 const std::vector<QueueMessage>& messages) {
    ::kubemq_pb::QueuesUpstreamRequest proto_request;
    internal::QueueUpstreamRequestToProto(
        request_id.empty() ? internal::GenerateUUID() : request_id, messages,
        impl_->opts.client_id(), &proto_request);

    std::lock_guard<std::mutex> lock(impl_->write_mu);
    if (impl_->done.load(std::memory_order_acquire)) {
        return Status(ErrorCode::kFatal, "upstream stream is closed", "QueueUpstreamHandle::Send",
                      "", "");
    }
    if (!impl_->stream->Write(proto_request)) {
        return Status(ErrorCode::kTransient, "failed to write to upstream stream",
                      "QueueUpstreamHandle::Send", "", "");
    }
    return Status();
}

void QueueUpstreamHandle::Close() {
    {
        std::lock_guard<std::mutex> lock(impl_->write_mu);
        if (impl_->done.exchange(true, std::memory_order_acq_rel))
            return;
        if (impl_->stream) {
            impl_->stream->WritesDone();
        }
    }
    impl_->JoinReader();
}

bool QueueUpstreamHandle::IsDone() const {
    return impl_->done.load(std::memory_order_acquire);
}

void QueueUpstreamHandle::Wait() {
    impl_->JoinReader();
}

void QueueUpstreamHandle::SetOnResult(std::function<void(const QueueUpstreamResult&)> callback) {
    std::lock_guard<std::mutex> lock(impl_->callback_mu);
    impl_->on_result = std::move(callback);
}

void QueueUpstreamHandle::SetOnError(std::function<void(const Status&)> callback) {
    std::lock_guard<std::mutex> lock(impl_->callback_mu);
    impl_->on_error = std::move(callback);
}

QueueDownstreamMessage::QueueDownstreamMessage() : impl_(std::make_unique<Impl>()) {}
QueueDownstreamMessage::~QueueDownstreamMessage() = default;
QueueDownstreamMessage::QueueDownstreamMessage(QueueDownstreamMessage&&) noexcept = default;
QueueDownstreamMessage& QueueDownstreamMessage::operator=(QueueDownstreamMessage&&) noexcept =
    default;

const QueueMessage& QueueDownstreamMessage::message() const {
    return impl_->message;
}
const std::string& QueueDownstreamMessage::transaction_id() const {
    return impl_->transaction_id;
}
uint64_t QueueDownstreamMessage::sequence() const {
    return impl_->sequence;
}

Status QueueDownstreamMessage::Ack() {
    // AckRange = 3
    return impl_->SendSettle(3);
}

Status QueueDownstreamMessage::Nack() {
    // NAckRange = 5
    return impl_->SendSettle(5);
}

Status QueueDownstreamMessage::ReQueue(const std::string& channel) {
    if (channel.empty()) {
        return Status(ErrorCode::kValidation, "requeue channel is required",
                      "QueueDownstreamMessage::ReQueue", "", "");
    }
    // ReQueueRange = 7
    return impl_->SendSettle(7, channel);
}

PollResponse::PollResponse() : impl_(std::make_unique<Impl>()) {}
PollResponse::~PollResponse() = default;
PollResponse::PollResponse(PollResponse&&) noexcept = default;
PollResponse& PollResponse::operator=(PollResponse&&) noexcept = default;

const std::string& PollResponse::transaction_id() const {
    return impl_->transaction_id;
}
const std::vector<QueueDownstreamMessage>& PollResponse::messages() const {
    return impl_->messages;
}
std::vector<QueueDownstreamMessage>& PollResponse::messages() {
    return impl_->messages;
}
bool PollResponse::is_error() const {
    return impl_->is_error;
}
const std::string& PollResponse::error() const {
    return impl_->error;
}
bool PollResponse::transaction_complete() const {
    return impl_->transaction_complete;
}

Status PollResponse::AckAll() {
    // AckAll = 2
    return impl_->SendSettleAll(2);
}

Status PollResponse::NackAll() {
    // NAckAll = 4
    return impl_->SendSettleAll(4);
}

Status PollResponse::ReQueueAll(const std::string& channel) {
    if (channel.empty()) {
        return Status(ErrorCode::kValidation, "requeue channel is required",
                      "PollResponse::ReQueueAll", "", "");
    }
    // ReQueueAll = 6
    return impl_->SendSettleAll(6, channel);
}

QueueDownstreamReceiver::QueueDownstreamReceiver() : impl_(std::make_unique<Impl>()) {}

QueueDownstreamReceiver::~QueueDownstreamReceiver() {
    Close();
}

StatusOr<PollResponse> QueueDownstreamReceiver::Poll(const PollRequest& request) {
    if (impl_->closed.load(std::memory_order_acquire)) {
        return Status(ErrorCode::kFatal, "downstream receiver is closed",
                      "QueueDownstreamReceiver::Poll", "", "");
    }

    // 1. Resolve channel
    std::string channel = request.channel;

    // 2. Validate
    auto s = internal::ValidateChannel(channel, "QueueDownstreamReceiver::Poll");
    if (!s.ok())
        return s;

    // 3. Build and send Get request
    ::kubemq_pb::QueuesDownstreamRequest proto_request;
    proto_request.set_requestid(internal::GenerateUUID());
    proto_request.set_clientid(impl_->opts.client_id());
    proto_request.set_requesttypedata(::kubemq_pb::Get);  // Get = 1
    proto_request.set_channel(channel);
    proto_request.set_maxitems(request.max_items);
    proto_request.set_waittimeout(request.wait_timeout_seconds);
    proto_request.set_autoack(request.auto_ack);

    ::kubemq_pb::QueuesDownstreamResponse proto_response;
    {
        std::lock_guard<std::mutex> lock(*impl_->stream_mu);
        if (!impl_->stream->Write(proto_request)) {
            return Status(ErrorCode::kTransient,
                          "failed to write poll request to downstream stream",
                          "QueueDownstreamReceiver::Poll", channel, "");
        }

        // 4. Wait for response (under same lock to serialize Read+Write)
        if (!impl_->stream->Read(&proto_response)) {
            return Status(ErrorCode::kTransient,
                          "failed to read poll response from downstream stream",
                          "QueueDownstreamReceiver::Poll", channel, "");
        }
    }

    // 5. Check for server-side error
    if (proto_response.iserror()) {
        return Status(ErrorCode::kFatal, proto_response.error(), "QueueDownstreamReceiver::Poll",
                      channel, "");
    }

    // 6. Create PollResponse
    auto poll_response = PollResponse();
    poll_response.impl_->transaction_id = proto_response.transactionid();
    poll_response.impl_->is_error = proto_response.iserror();
    poll_response.impl_->error = proto_response.error();
    poll_response.impl_->auto_ack = request.auto_ack;
    poll_response.impl_->transaction_complete = proto_response.transactioncomplete();
    poll_response.impl_->stream = impl_->stream;
    poll_response.impl_->stream_mu = impl_->stream_mu;

    // 7. Convert messages
    for (const auto& proto_msg : proto_response.messages()) {
        auto downstream_msg = QueueDownstreamMessage();
        downstream_msg.impl_->message = internal::QueueMessageFromProto(proto_msg);
        downstream_msg.impl_->transaction_id = proto_response.transactionid();
        downstream_msg.impl_->sequence =
            proto_msg.has_attributes() ? proto_msg.attributes().sequence() : 0;
        downstream_msg.impl_->auto_ack = request.auto_ack;
        downstream_msg.impl_->stream = impl_->stream;
        downstream_msg.impl_->stream_mu = impl_->stream_mu;

        poll_response.impl_->messages.push_back(std::move(downstream_msg));
    }

    return poll_response;
}

void QueueDownstreamReceiver::Close() {
    if (impl_->closed.exchange(true, std::memory_order_acq_rel)) {
        return;  // Already closed
    }
    // Send CloseByClient request
    if (impl_->stream) {
        ::kubemq_pb::QueuesDownstreamRequest close_req;
        close_req.set_requestid(internal::GenerateUUID());
        close_req.set_requesttypedata(::kubemq_pb::CloseByClient);
        std::lock_guard<std::mutex> lock(*impl_->stream_mu);
        impl_->stream->Write(close_req);
        impl_->stream->WritesDone();
    }
}

void QueueDownstreamReceiver::SetOnError(std::function<void(const Status&)> callback) {
    impl_->on_error = std::move(callback);
}

// --- Client Queue Stream Methods ---

StatusOr<std::unique_ptr<QueueUpstreamHandle>> Client::QueueUpstream(
    std::function<void(const QueueUpstreamResult&)> on_result,
    std::function<void(const Status&)> on_error) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Open bidi stream
    std::unique_ptr<grpc::ClientContext> ctx;
    auto stream_or = impl_->transport->OpenQueueUpstream(ctx);
    if (!stream_or.ok())
        return stream_or.status();

    // 3. Create handle
    auto handle = std::unique_ptr<QueueUpstreamHandle>(new QueueUpstreamHandle());
    handle->impl_->ctx = std::move(ctx);
    handle->impl_->stream = std::move(*stream_or);
    handle->impl_->opts = impl_->opts;

    // 4. Store callbacks before starting reader (no race)
    handle->impl_->on_result = std::move(on_result);
    handle->impl_->on_error = std::move(on_error);

    // 5. Start background reader
    handle->impl_->StartReader();

    return handle;
}

StatusOr<std::unique_ptr<QueueUpstreamHandle>> Client::QueueUpstream() {
    return QueueUpstream(nullptr, nullptr);
}

StatusOr<std::unique_ptr<QueueDownstreamReceiver>> Client::NewQueueDownstreamReceiver() {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Open bidi stream
    std::unique_ptr<grpc::ClientContext> ctx;
    auto stream_or = impl_->transport->OpenQueueDownstream(ctx);
    if (!stream_or.ok())
        return stream_or.status();

    // 3. Create receiver
    auto receiver = std::unique_ptr<QueueDownstreamReceiver>(new QueueDownstreamReceiver());
    receiver->impl_->ctx = std::move(ctx);
    receiver->impl_->stream = std::move(*stream_or);
    receiver->impl_->opts = impl_->opts;

    return receiver;
}

StatusOr<PollResponse> Client::PollQueue(const PollRequest& request) {
    // Create a downstream receiver, poll, and move receiver into PollResponse
    // so the stream stays alive for settlement operations (Ack/Nack/ReQueue).
    auto receiver_or = NewQueueDownstreamReceiver();
    if (!receiver_or.ok())
        return receiver_or.status();

    auto receiver = std::move(*receiver_or);
    auto poll_result = receiver->Poll(request);
    if (!poll_result.ok())
        return poll_result;

    // Transfer receiver ownership into the PollResponse to keep the
    // gRPC stream and ClientContext alive for settlement.
    poll_result->impl_->receiver = std::move(receiver);

    return poll_result;
}

}  // namespace v1
}  // namespace kubemq
