// src/client_commands.cc
#include <thread>

#include "internal/callback_guard.h"
#include "internal/client_impl.h"
#include "internal/subscription_impl.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "internal/validation.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

StatusOr<CommandResponse> Client::SendCommand(const Command& command) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = command.channel();
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Resolve effective client_id
    std::string client_id = command.client_id();
    if (client_id.empty()) {
        client_id = impl_->opts.client_id();
    }

    // 4. Validate
    s = internal::ValidateChannelNoWildcard(channel, "SendCommand");
    if (!s.ok())
        return s;

    auto timeout = command.timeout();
    if (timeout.count() <= 0) {
        timeout = std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultRPCTimeout);
    }
    s = internal::ValidateTimeout(timeout, "SendCommand");
    if (!s.ok())
        return s;

    s = internal::ValidateBody(command.body(), command.metadata(), "SendCommand");
    if (!s.ok())
        return s;

    // 5. Convert to pb::Request (RequestTypeData=Command)
    ::kubemq_pb::Request proto_request;
    proto_request.set_requestid(command.id().empty() ? internal::GenerateUUID() : command.id());
    proto_request.set_requesttypedata(::kubemq_pb::Request::Command);
    proto_request.set_clientid(client_id);
    proto_request.set_channel(channel);
    proto_request.set_metadata(command.metadata());
    proto_request.set_body(command.body());
    proto_request.set_timeout(static_cast<int32_t>(timeout.count()));
    for (const auto& [k, v] : command.tags()) {
        (*proto_request.mutable_tags())[k] = v;
    }

    // 6. Call transport.SendRequest() (non-idempotent: skip timeout retry)
    auto response_or = impl_->transport->SendRequest(proto_request);
    if (!response_or.ok())
        return response_or.status();

    const auto& proto_resp = *response_or;

    // 7. Check for server-side error
    if (!proto_resp.error().empty()) {
        return Status(ErrorCode::kFatal, proto_resp.error(), "SendCommand", channel,
                      proto_request.requestid());
    }

    // 8. Convert pb::Response -> CommandResponse
    CommandResponse result;
    result.command_id = proto_resp.requestid();
    result.response_client_id = proto_resp.clientid();
    result.reply_channel = proto_resp.replychannel();
    result.executed = proto_resp.executed();
    result.executed_at = proto_resp.timestamp();
    result.error = proto_resp.error();
    for (const auto& [k, v] : proto_resp.tags()) {
        result.tags[k] = v;
    }

    return result;
}

StatusOr<std::unique_ptr<Subscription>> Client::SubscribeToCommands(
    const std::string& channel, const std::string& group,
    std::function<void(const CommandReceive&)> on_command,
    std::function<void(const Status&)> on_error) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve channel
    std::string effective_channel = channel;
    if (effective_channel.empty()) {
        effective_channel = impl_->opts.default_channel();
    }

    // 3. Validate channel
    s = internal::ValidateChannel(effective_channel, "SubscribeToCommands");
    if (!s.ok())
        return s;

    // 4. Build Subscribe proto (SubscribeType=Commands)
    ::kubemq_pb::Subscribe sub;
    sub.set_subscribetypedata(::kubemq_pb::Subscribe::Commands);
    sub.set_clientid(impl_->opts.client_id());
    sub.set_channel(effective_channel);
    sub.set_group(group);

    // 5. Call transport SubscribeToRequests (server stream)
    auto ctx = std::make_shared<grpc::ClientContext>();
    auto reader_or = impl_->transport->SubscribeToRequests(sub, ctx);
    if (!reader_or.ok())
        return reader_or.status();

    // 6. Create Subscription handle
    auto subscription = std::unique_ptr<Subscription>(new Subscription());
    subscription->impl_->id = internal::GenerateUUID();
    subscription->impl_->ctx = ctx;

    // H-11: store reconnection state
    subscription->impl_->subscribe_proto = sub;
    subscription->impl_->transport = impl_->transport.get();

    // 7. Start callback dispatch thread with auto-reconnect (H-11)
    auto reader = std::move(*reader_or);
    auto sub_impl = subscription->impl_.get();
    subscription->impl_->reader_thread = std::thread([reader, on_command = std::move(on_command),
                                                      on_error = std::move(on_error),
                                                      sub_impl]() mutable {
        grpc::Status last_status;

        // Outer loop: survives reconnections
        while (!sub_impl->done.load(std::memory_order_acquire)) {
            // Inner read loop: process messages from current stream
            ::kubemq_pb::Request proto_request;
            while (reader->Read(&proto_request)) {
                // Filter: only handle Command type
                if (proto_request.requesttypedata() != ::kubemq_pb::Request::Command) {
                    continue;
                }
                CommandReceive cmd;
                cmd.id = proto_request.requestid();
                cmd.client_id = proto_request.clientid();
                cmd.channel = proto_request.channel();
                cmd.metadata = proto_request.metadata();
                cmd.body = proto_request.body();
                cmd.response_to = proto_request.replychannel();
                cmd.timeout = std::chrono::seconds(proto_request.timeout());
                for (const auto& [k, v] : proto_request.tags()) {
                    cmd.tags[k] = v;
                }
                internal::SafeInvoke(
                    [&]() {
                        if (on_command)
                            on_command(cmd);
                    },
                    on_error, "SubscribeToCommands::OnCommand");
            }

            // Stream ended -- get status
            last_status = reader->Finish();

            // Check if intentional close
            if (sub_impl->done.load(std::memory_order_acquire))
                break;

            // Stream broke unexpectedly -- attempt reconnect with exponential backoff
            auto backoff = std::chrono::seconds(1);
            const auto max_backoff = std::chrono::seconds(30);
            bool reconnected = false;

            while (!sub_impl->done.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(backoff);
                if (sub_impl->done.load(std::memory_order_acquire))
                    break;

                auto new_ctx = std::make_shared<grpc::ClientContext>();
                auto result =
                    sub_impl->transport->SubscribeToRequests(sub_impl->subscribe_proto, new_ctx);
                if (result.ok()) {
                    sub_impl->ctx = new_ctx;
                    reader = std::move(*result);
                    reconnected = true;
                    break;
                }

                backoff = std::min(backoff * 2, max_backoff);
            }

            if (!reconnected)
                break;
        }

        // Thread exiting -- fire on_error only for unexpected termination
        if (!sub_impl->done.load(std::memory_order_acquire)) {
            sub_impl->done.store(true, std::memory_order_release);
            internal::SafeInvoke(
                [&]() {
                    if (on_error) {
                        on_error(Status(
                            ErrorCode::kTransient,
                            "commands subscription stream ended: " + last_status.error_message(),
                            "SubscribeToCommands", "", ""));
                    }
                },
                nullptr, "SubscribeToCommands::OnError");
        }
    });

    return subscription;
}

Status Client::SendCommandResponse(const CommandReply& reply) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Convert CommandReply -> pb::Response
    ::kubemq_pb::Response proto_response;
    proto_response.set_clientid(reply.client_id_.empty() ? impl_->opts.client_id()
                                                         : reply.client_id_);
    proto_response.set_requestid(reply.request_id_);
    proto_response.set_replychannel(reply.response_to_);
    proto_response.set_metadata(reply.metadata_);
    proto_response.set_body(reply.body_);
    proto_response.set_executed(reply.executed());
    proto_response.set_timestamp(reply.executed_at_);
    proto_response.set_error(reply.error_);
    for (const auto& [k, v] : reply.tags_) {
        (*proto_response.mutable_tags())[k] = v;
    }

    // 3. Call transport.SendResponse()
    return impl_->transport->SendResponse(proto_response);
}

StatusOr<CommandResponse> Client::SendCommandSimple(const std::string& channel,
                                                    const std::string& body,
                                                    std::chrono::milliseconds timeout) {
    auto cmd_or = Command::Builder().SetChannel(channel).SetBody(body).SetTimeout(timeout).Build();
    if (!cmd_or.ok())
        return cmd_or.status();
    return SendCommand(*cmd_or);
}

}  // namespace v1
}  // namespace kubemq
