// src/client_queues.cc
#include "internal/client_impl.h"
#include "internal/transport/converter.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "internal/validation.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

// --- Client Queue Simple Methods ---

StatusOr<QueueSendResult> Client::SendQueueMessage(const QueueMessage& message) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = message.channel();
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Validate
    s = internal::ValidateChannelNoWildcard(channel, "SendQueueMessage");
    if (!s.ok())
        return s;

    s = internal::ValidateBody(message.body(), message.metadata(), "SendQueueMessage");
    if (!s.ok())
        return s;

    // 4. Convert to proto
    ::kubemq_pb::QueueMessage proto_msg;
    internal::QueueMessageToProto(message, impl_->opts.client_id(), &proto_msg);
    proto_msg.set_channel(channel);

    // 5. Call transport.SendQueueMessage()
    auto result_or = impl_->transport->SendQueueMessage(proto_msg);
    if (!result_or.ok())
        return result_or.status();

    // 6. Convert result
    QueueSendResult result = internal::QueueSendResultFromProto(*result_or);

    // 7. Check for server-side error
    if (result.is_error) {
        return Status(ErrorCode::kFatal, result.error, "SendQueueMessage", channel,
                      result.message_id);
    }

    return result;
}

StatusOr<std::vector<QueueSendResult>> Client::SendQueueMessages(
    const std::vector<QueueMessage>& messages) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    if (messages.empty()) {
        return Status(ErrorCode::kValidation, "messages list is empty", "SendQueueMessages", "",
                      "");
    }

    // 2. Build batch request
    ::kubemq_pb::QueueMessagesBatchRequest batch_request;
    batch_request.set_batchid(internal::GenerateUUID());

    for (const auto& msg : messages) {
        // Resolve effective channel per message
        std::string channel = msg.channel();
        if (channel.empty()) {
            channel = impl_->opts.default_channel();
        }

        // Validate each message
        s = internal::ValidateChannelNoWildcard(channel, "SendQueueMessages");
        if (!s.ok())
            return s;

        s = internal::ValidateBody(msg.body(), msg.metadata(), "SendQueueMessages");
        if (!s.ok())
            return s;

        auto* proto_msg = batch_request.add_messages();
        internal::QueueMessageToProto(msg, impl_->opts.client_id(), proto_msg);
        proto_msg->set_channel(channel);
    }

    // 3. Call transport.SendQueueMessagesBatch()
    auto response_or = impl_->transport->SendQueueMessagesBatch(batch_request);
    if (!response_or.ok())
        return response_or.status();

    const auto& proto_response = *response_or;

    // 4. Convert results
    std::vector<QueueSendResult> results;
    results.reserve(proto_response.results_size());
    for (const auto& proto_result : proto_response.results()) {
        results.push_back(internal::QueueSendResultFromProto(proto_result));
    }

    return results;
}

StatusOr<ReceiveQueueMessagesResponse> Client::ReceiveQueueMessages(
    const ReceiveQueueMessagesRequest& request) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = request.channel;
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Validate
    s = internal::ValidateChannel(channel, "ReceiveQueueMessages");
    if (!s.ok())
        return s;

    // 4. Convert to proto request
    ::kubemq_pb::ReceiveQueueMessagesRequest proto_request;
    proto_request.set_requestid(request.request_id.empty() ? internal::GenerateUUID()
                                                           : request.request_id);
    proto_request.set_clientid(request.client_id.empty() ? impl_->opts.client_id()
                                                         : request.client_id);
    proto_request.set_channel(channel);
    proto_request.set_maxnumberofmessages(request.max_number_of_messages);
    proto_request.set_waittimeseconds(request.wait_time_seconds);
    proto_request.set_ispeak(request.is_peek);

    // 5. Call transport.ReceiveQueueMessages()
    auto response_or = impl_->transport->ReceiveQueueMessages(proto_request);
    if (!response_or.ok())
        return response_or.status();

    const auto& proto_response = *response_or;

    // 6. Check for server-side error
    if (proto_response.iserror()) {
        return Status(ErrorCode::kFatal, proto_response.error(), "ReceiveQueueMessages", channel,
                      proto_request.requestid());
    }

    // 7. Convert response
    ReceiveQueueMessagesResponse result;
    result.request_id = proto_response.requestid();
    result.messages_received = proto_response.messagesreceived();
    result.messages_expired = proto_response.messagesexpired();
    result.is_peek = proto_response.ispeak();
    result.is_error = proto_response.iserror();
    result.error = proto_response.error();

    for (const auto& proto_msg : proto_response.messages()) {
        result.messages.push_back(internal::QueueMessageFromProto(proto_msg));
    }

    return result;
}

StatusOr<AckAllQueueMessagesResponse> Client::AckAllQueueMessages(
    const AckAllQueueMessagesRequest& request) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = request.channel;
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Validate
    s = internal::ValidateChannel(channel, "AckAllQueueMessages");
    if (!s.ok())
        return s;

    // 4. Convert to proto request
    ::kubemq_pb::AckAllQueueMessagesRequest proto_request;
    proto_request.set_requestid(request.request_id.empty() ? internal::GenerateUUID()
                                                           : request.request_id);
    proto_request.set_clientid(request.client_id.empty() ? impl_->opts.client_id()
                                                         : request.client_id);
    proto_request.set_channel(channel);
    proto_request.set_waittimeseconds(request.wait_time_seconds);

    // 5. Call transport.AckAllQueueMessages()
    auto response_or = impl_->transport->AckAllQueueMessages(proto_request);
    if (!response_or.ok())
        return response_or.status();

    const auto& proto_response = *response_or;

    // 6. Check for server-side error
    if (proto_response.iserror()) {
        return Status(ErrorCode::kFatal, proto_response.error(), "AckAllQueueMessages", channel,
                      proto_request.requestid());
    }

    // 7. Convert response
    AckAllQueueMessagesResponse result;
    result.request_id = proto_response.requestid();
    result.affected_messages = proto_response.affectedmessages();
    result.is_error = proto_response.iserror();
    result.error = proto_response.error();

    return result;
}

StatusOr<QueueSendResult> Client::SendQueueMessageSimple(const std::string& channel,
                                                         const std::string& body,
                                                         int expiration_seconds,
                                                         int delay_seconds) {
    auto builder = QueueMessage::Builder().SetChannel(channel).SetBody(body);

    if (expiration_seconds > 0) {
        builder.SetExpirationSeconds(expiration_seconds);
    }
    if (delay_seconds > 0) {
        builder.SetDelaySeconds(delay_seconds);
    }

    auto msg_or = builder.Build();
    if (!msg_or.ok())
        return msg_or.status();
    return SendQueueMessage(*msg_or);
}

}  // namespace v1
}  // namespace kubemq
