// src/internal/transport/converter.cc
#include "internal/transport/converter.h"

#include <nlohmann/json.hpp>

#include "kubemq.pb.h"

namespace kubemq {
namespace internal {

// --- Utility ---

std::string FirstNonEmpty(const std::string& a, const std::string& b) {
    if (!a.empty()) {
        return a;
    }
    return b;
}

// Helper to copy proto map<string,string> -> unordered_map<string,string>.
static std::unordered_map<std::string, std::string> CopyTags(
    const google::protobuf::Map<std::string, std::string>& proto_tags) {
    std::unordered_map<std::string, std::string> tags;
    tags.reserve(static_cast<size_t>(proto_tags.size()));
    for (const auto& [key, value] : proto_tags) {
        tags[key] = value;
    }
    return tags;
}

// Helper to copy unordered_map -> proto map.
static void SetTags(const std::unordered_map<std::string, std::string>& tags,
                    google::protobuf::Map<std::string, std::string>* proto_tags) {
    for (const auto& [key, value] : tags) {
        (*proto_tags)[key] = value;
    }
}

// --- Event conversions ---

void EventToProto(const kubemq::v1::Event& event, const std::string& client_id,
                  kubemq_pb::Event* proto) {
    proto->set_eventid(event.id());
    proto->set_clientid(FirstNonEmpty(event.client_id(), client_id));
    proto->set_channel(event.channel());
    proto->set_metadata(event.metadata());
    proto->set_body(event.body());
    proto->set_store(false);
    SetTags(event.tags(), proto->mutable_tags());
}

void EventStoreToProto(const kubemq::v1::EventStore& event, const std::string& client_id,
                       kubemq_pb::Event* proto) {
    proto->set_eventid(event.id());
    proto->set_clientid(FirstNonEmpty(event.client_id(), client_id));
    proto->set_channel(event.channel());
    proto->set_metadata(event.metadata());
    proto->set_body(event.body());
    proto->set_store(true);
    SetTags(event.tags(), proto->mutable_tags());
}

kubemq::v1::EventStreamResult EventStreamResultFromProto(const kubemq_pb::Result& result) {
    kubemq::v1::EventStreamResult r;
    r.event_id = result.eventid();
    r.sent = result.sent();
    r.error = result.error();
    return r;
}

kubemq::v1::EventStoreResult EventStoreResultFromProto(const kubemq_pb::Result& result) {
    kubemq::v1::EventStoreResult r;
    r.id = result.eventid();
    r.sent = result.sent();
    r.error = result.error();
    return r;
}

kubemq::v1::EventReceive EventReceiveFromProto(const kubemq_pb::EventReceive& proto) {
    kubemq::v1::EventReceive event;
    event.id = proto.eventid();
    event.channel = proto.channel();
    event.metadata = proto.metadata();
    event.body = proto.body();
    event.timestamp = proto.timestamp();
    event.sequence = proto.sequence();
    event.tags = CopyTags(proto.tags());
    return event;
}

kubemq::v1::EventStoreReceive EventStoreReceiveFromProto(const kubemq_pb::EventReceive& proto) {
    kubemq::v1::EventStoreReceive event;
    event.id = proto.eventid();
    event.channel = proto.channel();
    event.metadata = proto.metadata();
    event.body = proto.body();
    event.timestamp = proto.timestamp();
    event.sequence = proto.sequence();
    event.tags = CopyTags(proto.tags());
    return event;
}

// --- Command conversions ---

void CommandToProto(const kubemq::v1::Command& command, const std::string& client_id,
                    kubemq_pb::Request* proto) {
    proto->set_requestid(command.id());
    proto->set_requesttypedata(kubemq_pb::Request::Command);
    proto->set_clientid(FirstNonEmpty(command.client_id(), client_id));
    proto->set_channel(command.channel());
    proto->set_metadata(command.metadata());
    proto->set_body(command.body());
    proto->set_timeout(static_cast<int32_t>(command.timeout().count()));
    SetTags(command.tags(), proto->mutable_tags());
}

kubemq::v1::CommandResponse CommandResponseFromProto(const kubemq_pb::Response& proto) {
    kubemq::v1::CommandResponse resp;
    resp.command_id = proto.requestid();
    resp.response_client_id = proto.clientid();
    resp.reply_channel = proto.replychannel();
    resp.executed = proto.executed();
    resp.executed_at = proto.timestamp();
    resp.error = proto.error();
    resp.tags = CopyTags(proto.tags());
    return resp;
}

kubemq::v1::CommandReceive CommandReceiveFromProto(const kubemq_pb::Request& proto) {
    kubemq::v1::CommandReceive cmd;
    cmd.id = proto.requestid();
    cmd.client_id = proto.clientid();
    cmd.channel = proto.channel();
    cmd.metadata = proto.metadata();
    cmd.body = proto.body();
    cmd.response_to = proto.replychannel();
    cmd.timeout = std::chrono::seconds(proto.timeout());
    cmd.tags = CopyTags(proto.tags());
    return cmd;
}

void CommandReplyToProto(const kubemq::v1::CommandReply& reply, kubemq_pb::Response* proto) {
    proto->set_clientid(reply.client_id());
    proto->set_requestid(reply.request_id());
    proto->set_replychannel(reply.response_to());
    proto->set_metadata(reply.metadata());
    proto->set_body(reply.body());
    proto->set_timestamp(reply.executed_at());
    proto->set_executed(reply.executed());
    proto->set_error(reply.error());
    SetTags(reply.tags(), proto->mutable_tags());
}

// --- Query conversions ---

void QueryToProto(const kubemq::v1::Query& query, const std::string& client_id,
                  kubemq_pb::Request* proto) {
    proto->set_requestid(query.id());
    proto->set_requesttypedata(kubemq_pb::Request::Query);
    proto->set_clientid(FirstNonEmpty(query.client_id(), client_id));
    proto->set_channel(query.channel());
    proto->set_metadata(query.metadata());
    proto->set_body(query.body());
    proto->set_timeout(static_cast<int32_t>(query.timeout().count()));
    proto->set_cachekey(query.cache_key());
    proto->set_cachettl(static_cast<int32_t>(
        std::chrono::duration_cast<std::chrono::seconds>(query.cache_ttl()).count()));
    SetTags(query.tags(), proto->mutable_tags());
}

kubemq::v1::QueryResponse QueryResponseFromProto(const kubemq_pb::Response& proto) {
    kubemq::v1::QueryResponse resp;
    resp.query_id = proto.requestid();
    resp.reply_channel = proto.replychannel();
    resp.executed = proto.executed();
    resp.executed_at = proto.timestamp();
    resp.metadata = proto.metadata();
    resp.response_client_id = proto.clientid();
    resp.body = proto.body();
    resp.cache_hit = proto.cachehit();
    resp.error = proto.error();
    resp.tags = CopyTags(proto.tags());
    return resp;
}

kubemq::v1::QueryReceive QueryReceiveFromProto(const kubemq_pb::Request& proto) {
    kubemq::v1::QueryReceive q;
    q.id = proto.requestid();
    q.client_id = proto.clientid();
    q.channel = proto.channel();
    q.metadata = proto.metadata();
    q.body = proto.body();
    q.response_to = proto.replychannel();
    q.timeout = std::chrono::seconds(proto.timeout());
    q.cache_key = proto.cachekey();
    q.cache_ttl = std::chrono::seconds(proto.cachettl());
    q.tags = CopyTags(proto.tags());
    return q;
}

void QueryReplyToProto(const kubemq::v1::QueryReply& reply, kubemq_pb::Response* proto) {
    proto->set_clientid(reply.client_id());
    proto->set_requestid(reply.request_id());
    proto->set_replychannel(reply.response_to());
    proto->set_metadata(reply.metadata());
    proto->set_body(reply.body());
    proto->set_timestamp(reply.executed_at());
    proto->set_executed(reply.executed());
    proto->set_error(reply.error());
    proto->set_cachehit(reply.cache_hit());
    SetTags(reply.tags(), proto->mutable_tags());
}

// --- Queue conversions ---

void QueueMessageToProto(const kubemq::v1::QueueMessage& msg, const std::string& client_id,
                         kubemq_pb::QueueMessage* proto) {
    proto->set_messageid(msg.id());
    proto->set_clientid(FirstNonEmpty(msg.client_id(), client_id));
    proto->set_channel(msg.channel());
    proto->set_metadata(msg.metadata());
    proto->set_body(msg.body());
    SetTags(msg.tags(), proto->mutable_tags());

    // Set policy if present.
    const auto* policy = msg.policy();
    if (policy != nullptr) {
        auto* pb_policy = proto->mutable_policy();
        pb_policy->set_expirationseconds(static_cast<int32_t>(policy->expiration_seconds));
        pb_policy->set_delayseconds(static_cast<int32_t>(policy->delay_seconds));
        pb_policy->set_maxreceivecount(static_cast<int32_t>(policy->max_receive_count));
        pb_policy->set_maxreceivequeue(policy->max_receive_queue);
    }
}

kubemq::v1::QueueMessage QueueMessageFromProto(const kubemq_pb::QueueMessage& proto) {
    // Build the QueueMessage via the builder to populate all fields.
    auto builder = kubemq::v1::QueueMessage::Builder();
    builder.SetId(proto.messageid());
    builder.SetChannel(proto.channel());
    builder.SetClientId(proto.clientid());
    builder.SetMetadata(proto.metadata());
    builder.SetBody(proto.body());

    // Copy tags.
    std::unordered_map<std::string, std::string> tags;
    for (const auto& [key, value] : proto.tags()) {
        tags[key] = value;
    }
    builder.SetTags(std::move(tags));

    // Copy policy.
    if (proto.has_policy()) {
        kubemq::v1::QueuePolicy policy;
        policy.expiration_seconds = static_cast<int>(proto.policy().expirationseconds());
        policy.delay_seconds = static_cast<int>(proto.policy().delayseconds());
        policy.max_receive_count = static_cast<int>(proto.policy().maxreceivecount());
        policy.max_receive_queue = proto.policy().maxreceivequeue();
        builder.SetPolicy(policy);
    }

    // Build without validation (internal use, data comes from server).
    auto result = builder.Build();
    if (!result.ok()) {
        // Should not happen for server-provided data, return default.
        return kubemq::v1::QueueMessage();
    }

    auto msg = std::move(*result);

    // Populate attributes from proto (friend access).
    if (proto.has_attributes()) {
        const auto& attrs = proto.attributes();
        msg.attributes_.timestamp = attrs.timestamp();
        msg.attributes_.sequence = attrs.sequence();
        msg.attributes_.md5_of_body = attrs.md5ofbody();
        msg.attributes_.receive_count = attrs.receivecount();
        msg.attributes_.re_routed = attrs.rerouted();
        msg.attributes_.re_routed_from_queue = attrs.reroutedfromqueue();
        msg.attributes_.expiration_at = attrs.expirationat();
        msg.attributes_.delayed_to = attrs.delayedto();
    }

    return msg;
}

kubemq::v1::QueueSendResult QueueSendResultFromProto(
    const kubemq_pb::SendQueueMessageResult& proto) {
    kubemq::v1::QueueSendResult result;
    result.message_id = proto.messageid();
    result.sent_at = proto.sentat();
    result.expiration_at = proto.expirationat();
    result.delayed_to = proto.delayedto();
    result.is_error = proto.iserror();
    result.error = proto.error();
    return result;
}

// --- Queue Simple API conversions ---

void ReceiveQueueMessagesRequestToProto(const kubemq::v1::ReceiveQueueMessagesRequest& req,
                                        kubemq_pb::ReceiveQueueMessagesRequest* proto) {
    proto->set_requestid(req.request_id);
    proto->set_clientid(req.client_id);
    proto->set_channel(req.channel);
    proto->set_maxnumberofmessages(req.max_number_of_messages);
    proto->set_waittimeseconds(req.wait_time_seconds);
    proto->set_ispeak(req.is_peek);
}

kubemq::v1::ReceiveQueueMessagesResponse ReceiveQueueMessagesResponseFromProto(
    const kubemq_pb::ReceiveQueueMessagesResponse& proto) {
    kubemq::v1::ReceiveQueueMessagesResponse resp;
    resp.request_id = proto.requestid();
    resp.messages_received = proto.messagesreceived();
    resp.messages_expired = proto.messagesexpired();
    resp.is_peek = proto.ispeak();
    resp.is_error = proto.iserror();
    resp.error = proto.error();

    resp.messages.reserve(static_cast<size_t>(proto.messages_size()));
    for (const auto& pb_msg : proto.messages()) {
        resp.messages.push_back(QueueMessageFromProto(pb_msg));
    }

    return resp;
}

void AckAllQueueMessagesRequestToProto(const kubemq::v1::AckAllQueueMessagesRequest& req,
                                       kubemq_pb::AckAllQueueMessagesRequest* proto) {
    proto->set_requestid(req.request_id);
    proto->set_clientid(req.client_id);
    proto->set_channel(req.channel);
    proto->set_waittimeseconds(req.wait_time_seconds);
}

kubemq::v1::AckAllQueueMessagesResponse AckAllQueueMessagesResponseFromProto(
    const kubemq_pb::AckAllQueueMessagesResponse& proto) {
    kubemq::v1::AckAllQueueMessagesResponse resp;
    resp.request_id = proto.requestid();
    resp.affected_messages = proto.affectedmessages();
    resp.is_error = proto.iserror();
    resp.error = proto.error();
    return resp;
}

// --- Server Info ---

kubemq::v1::ServerInfo ServerInfoFromProto(const kubemq_pb::PingResult& proto) {
    kubemq::v1::ServerInfo info;
    info.host = proto.host();
    info.version = proto.version();
    info.server_start_time = proto.serverstarttime();
    info.server_up_time_seconds = proto.serveruptimeseconds();
    return info;
}

// --- Channel Management ---

void BuildChannelManagementRequest(const std::string& client_id, const std::string& channel_name,
                                   const std::string& channel_type, const std::string& operation,
                                   const std::string& request_id, kubemq_pb::Request* proto) {
    proto->set_requestid(request_id);
    proto->set_requesttypedata(kubemq_pb::Request::Query);
    proto->set_clientid(client_id);
    proto->set_channel(kInternalChannel);
    proto->set_metadata(operation);
    proto->set_timeout(10000);  // 10 seconds in milliseconds.

    auto* tags = proto->mutable_tags();
    (*tags)["channel"] = channel_name;
    (*tags)["channel_type"] = channel_type;
    (*tags)["client_id"] = client_id;
}

StatusOr<std::vector<kubemq::v1::ChannelInfo>> ParseChannelListResponse(
    const std::string& json_body) {
    if (json_body.empty()) {
        return std::vector<kubemq::v1::ChannelInfo>{};
    }

    try {
        auto json = nlohmann::json::parse(json_body);

        if (!json.is_array()) {
            return Status(ErrorCode::kFatal, "expected JSON array in channel list response");
        }

        std::vector<kubemq::v1::ChannelInfo> channels;
        channels.reserve(json.size());

        for (const auto& item : json) {
            kubemq::v1::ChannelInfo info;
            info.name = item.value("name", "");
            info.type = item.value("type", "");
            info.last_activity = item.value("lastActivity", static_cast<int64_t>(0));
            info.is_active = item.value("isActive", false);

            // Parse incoming stats.
            if (item.contains("incoming")) {
                const auto& inc = item["incoming"];
                info.incoming.messages = inc.value("messages", static_cast<int64_t>(0));
                info.incoming.volume = inc.value("volume", static_cast<int64_t>(0));
                info.incoming.responses = inc.value("responses", static_cast<int64_t>(0));
                info.incoming.waiting = inc.value("waiting", static_cast<int64_t>(0));
                info.incoming.expired = inc.value("expired", static_cast<int64_t>(0));
                info.incoming.delayed = inc.value("delayed", static_cast<int64_t>(0));
            }

            // Parse outgoing stats.
            if (item.contains("outgoing")) {
                const auto& out = item["outgoing"];
                info.outgoing.messages = out.value("messages", static_cast<int64_t>(0));
                info.outgoing.volume = out.value("volume", static_cast<int64_t>(0));
                info.outgoing.responses = out.value("responses", static_cast<int64_t>(0));
                info.outgoing.waiting = out.value("waiting", static_cast<int64_t>(0));
                info.outgoing.expired = out.value("expired", static_cast<int64_t>(0));
                info.outgoing.delayed = out.value("delayed", static_cast<int64_t>(0));
            }

            channels.push_back(std::move(info));
        }

        return channels;

    } catch (const nlohmann::json::parse_error& e) {
        return Status(ErrorCode::kFatal,
                      std::string("failed to parse channel list JSON: ") + e.what());
    } catch (const nlohmann::json::type_error& e) {
        return Status(ErrorCode::kFatal,
                      std::string("unexpected type in channel list JSON: ") + e.what());
    }
}

// --- Queue Stream conversions ---

void QueueUpstreamRequestToProto(const std::string& request_id,
                                 const std::vector<kubemq::v1::QueueMessage>& messages,
                                 const std::string& client_id,
                                 kubemq_pb::QueuesUpstreamRequest* proto) {
    proto->set_requestid(request_id);
    for (const auto& msg : messages) {
        auto* pb_msg = proto->add_messages();
        QueueMessageToProto(msg, client_id, pb_msg);
    }
}

QueueUpstreamResultData QueueUpstreamResponseFromProto(
    const kubemq_pb::QueuesUpstreamResponse& proto) {
    QueueUpstreamResultData data;
    data.ref_request_id = proto.refrequestid();
    data.is_error = proto.iserror();
    data.error = proto.error();

    data.results.reserve(static_cast<size_t>(proto.results_size()));
    for (const auto& res : proto.results()) {
        data.results.push_back(QueueSendResultFromProto(res));
    }

    return data;
}

}  // namespace internal
}  // namespace kubemq
