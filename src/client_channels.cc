// src/client_channels.cc
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

namespace {

constexpr const char* kValidChannelTypeError =
    "invalid channel type; must be one of: events, events_store, "
    "commands, queries, queues";

bool IsValidChannelType(const std::string& channel_type) {
    return channel_type == kChannelTypeEvents || channel_type == kChannelTypeEventsStore ||
           channel_type == kChannelTypeCommands || channel_type == kChannelTypeQueries ||
           channel_type == kChannelTypeQueues;
}

// Validate channel management request parameters.
Status ValidateChannelManagement(const std::string& channel_name, const std::string& channel_type,
                                 const std::string& operation) {
    if (channel_name.empty()) {
        return Status(ErrorCode::kValidation, "channel name is required", operation, "", "");
    }
    if (!IsValidChannelType(channel_type)) {
        return Status(ErrorCode::kValidation, kValidChannelTypeError, operation, "", "");
    }
    return Status();
}

}  // namespace

// --- Channel Management ---

Status Client::CreateChannel(const std::string& channel_name, const std::string& channel_type) {
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    s = ValidateChannelManagement(channel_name, channel_type, "CreateChannel");
    if (!s.ok())
        return s;

    ::kubemq_pb::Request request;
    auto request_id = internal::GenerateUUID();
    internal::BuildChannelManagementRequest(impl_->opts.client_id(), channel_name, channel_type,
                                            "create-channel", request_id, &request);

    auto response_or = impl_->transport->SendRequest(request);
    if (!response_or.ok())
        return response_or.status();

    if (!response_or->error().empty()) {
        return Status(ErrorCode::kFatal, response_or->error(), "CreateChannel", channel_name,
                      request_id);
    }

    return Status();
}

Status Client::DeleteChannel(const std::string& channel_name, const std::string& channel_type) {
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    s = ValidateChannelManagement(channel_name, channel_type, "DeleteChannel");
    if (!s.ok())
        return s;

    ::kubemq_pb::Request request;
    auto request_id = internal::GenerateUUID();
    internal::BuildChannelManagementRequest(impl_->opts.client_id(), channel_name, channel_type,
                                            "delete-channel", request_id, &request);

    auto response_or = impl_->transport->SendRequest(request);
    if (!response_or.ok())
        return response_or.status();

    if (!response_or->error().empty()) {
        return Status(ErrorCode::kFatal, response_or->error(), "DeleteChannel", channel_name,
                      request_id);
    }

    return Status();
}

StatusOr<std::vector<ChannelInfo>> Client::ListChannels(const std::string& channel_type,
                                                        const std::string& search) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Validate
    if (!IsValidChannelType(channel_type)) {
        return Status(ErrorCode::kValidation, kValidChannelTypeError, "ListChannels", "", "");
    }

    // 3. Build request
    ::kubemq_pb::Request request;
    auto request_id = internal::GenerateUUID();
    internal::BuildChannelManagementRequest(impl_->opts.client_id(), "", channel_type,
                                            "list-channels", request_id, &request);
    if (!search.empty()) {
        (*request.mutable_tags())["search"] = search;
    }

    // 4. Send via transport
    auto response_or = impl_->transport->SendRequest(request);
    if (!response_or.ok())
        return response_or.status();

    const auto& response = *response_or;

    // 5. Check response error
    if (!response.error().empty()) {
        return Status(ErrorCode::kFatal, response.error(), "ListChannels", "", request_id);
    }

    // 6. Parse JSON response body
    return internal::ParseChannelListResponse(response.body());
}

}  // namespace v1
}  // namespace kubemq
