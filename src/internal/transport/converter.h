// src/internal/transport/converter.h
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "kubemq/channel_info.h"
#include "kubemq/command.h"
#include "kubemq/connection_state.h"
#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/query.h"
#include "kubemq/queue_message.h"
#include "kubemq/server_info.h"
#include "kubemq/status.h"

// Forward-declare proto types from generated headers.
namespace kubemq_pb {
class Event;
class EventReceive;
class Result;
class Request;
class Response;
class Subscribe;
class QueueMessage;
class SendQueueMessageResult;
class PingResult;
class QueuesUpstreamRequest;
class QueuesUpstreamResponse;
class QueuesDownstreamRequest;
class QueuesDownstreamResponse;
class QueueMessagesBatchRequest;
class QueueMessagesBatchResponse;
class ReceiveQueueMessagesRequest;
class ReceiveQueueMessagesResponse;
class AckAllQueueMessagesRequest;
class AckAllQueueMessagesResponse;
}  // namespace kubemq_pb

namespace kubemq {
namespace internal {

// Internal channel constant for management requests (matches Go SDK).
constexpr const char* kInternalChannel = "kubemq.cluster.internal.requests";

// --- Utility ---

// Return the first non-empty string, or empty if both are empty.
std::string FirstNonEmpty(const std::string& a, const std::string& b);

// --- Event conversions ---

// SDK Event -> proto Event (Store=false)
void EventToProto(const kubemq::v1::Event& event, const std::string& client_id,
                  kubemq_pb::Event* proto);

// SDK EventStore -> proto Event (Store=true)
void EventStoreToProto(const kubemq::v1::EventStore& event, const std::string& client_id,
                       kubemq_pb::Event* proto);

// proto Result -> EventStreamResult
kubemq::v1::EventStreamResult EventStreamResultFromProto(const kubemq_pb::Result& result);

// proto Result -> EventStoreResult
kubemq::v1::EventStoreResult EventStoreResultFromProto(const kubemq_pb::Result& result);

// proto EventReceive -> SDK EventReceive (for non-store subscriptions)
kubemq::v1::EventReceive EventReceiveFromProto(const kubemq_pb::EventReceive& proto);

// proto EventReceive -> SDK EventStoreReceive (for store subscriptions)
kubemq::v1::EventStoreReceive EventStoreReceiveFromProto(const kubemq_pb::EventReceive& proto);

// --- Command conversions ---

// SDK Command -> proto Request (RequestTypeData=Command)
void CommandToProto(const kubemq::v1::Command& command, const std::string& client_id,
                    kubemq_pb::Request* proto);

// proto Response -> CommandResponse
kubemq::v1::CommandResponse CommandResponseFromProto(const kubemq_pb::Response& proto);

// proto Request -> CommandReceive (for subscriptions)
kubemq::v1::CommandReceive CommandReceiveFromProto(const kubemq_pb::Request& proto);

// SDK CommandReply -> proto Response
void CommandReplyToProto(const kubemq::v1::CommandReply& reply, kubemq_pb::Response* proto);

// --- Query conversions ---

// SDK Query -> proto Request (RequestTypeData=Query, includes CacheKey/CacheTTL)
void QueryToProto(const kubemq::v1::Query& query, const std::string& client_id,
                  kubemq_pb::Request* proto);

// proto Response -> QueryResponse
kubemq::v1::QueryResponse QueryResponseFromProto(const kubemq_pb::Response& proto);

// proto Request -> QueryReceive (for subscriptions)
kubemq::v1::QueryReceive QueryReceiveFromProto(const kubemq_pb::Request& proto);

// SDK QueryReply -> proto Response
void QueryReplyToProto(const kubemq::v1::QueryReply& reply, kubemq_pb::Response* proto);

// --- Queue conversions ---

// SDK QueueMessage -> proto QueueMessage (includes Policy)
void QueueMessageToProto(const kubemq::v1::QueueMessage& msg, const std::string& client_id,
                         kubemq_pb::QueueMessage* proto);

// proto QueueMessage -> SDK QueueMessage
kubemq::v1::QueueMessage QueueMessageFromProto(const kubemq_pb::QueueMessage& proto);

// proto SendQueueMessageResult -> QueueSendResult
kubemq::v1::QueueSendResult QueueSendResultFromProto(
    const kubemq_pb::SendQueueMessageResult& proto);

// --- Queue Simple API conversions ---

// SDK ReceiveQueueMessagesRequest -> proto ReceiveQueueMessagesRequest
void ReceiveQueueMessagesRequestToProto(const kubemq::v1::ReceiveQueueMessagesRequest& req,
                                        kubemq_pb::ReceiveQueueMessagesRequest* proto);

// proto ReceiveQueueMessagesResponse -> SDK ReceiveQueueMessagesResponse
kubemq::v1::ReceiveQueueMessagesResponse ReceiveQueueMessagesResponseFromProto(
    const kubemq_pb::ReceiveQueueMessagesResponse& proto);

// SDK AckAllQueueMessagesRequest -> proto AckAllQueueMessagesRequest
void AckAllQueueMessagesRequestToProto(const kubemq::v1::AckAllQueueMessagesRequest& req,
                                       kubemq_pb::AckAllQueueMessagesRequest* proto);

// proto AckAllQueueMessagesResponse -> SDK AckAllQueueMessagesResponse
kubemq::v1::AckAllQueueMessagesResponse AckAllQueueMessagesResponseFromProto(
    const kubemq_pb::AckAllQueueMessagesResponse& proto);

// --- Server Info ---

// proto PingResult -> ServerInfo
kubemq::v1::ServerInfo ServerInfoFromProto(const kubemq_pb::PingResult& proto);

// --- Channel Management ---

// Build a proto Request for channel management operations.
// operation: "create-channel", "delete-channel", "list-channels"
void BuildChannelManagementRequest(const std::string& client_id, const std::string& channel_name,
                                   const std::string& channel_type, const std::string& operation,
                                   const std::string& request_id, kubemq_pb::Request* proto);

// Parse JSON response body from ListChannels into ChannelInfo vector.
StatusOr<std::vector<kubemq::v1::ChannelInfo>> ParseChannelListResponse(
    const std::string& json_body);

// --- Queue Stream conversions ---

// Build a QueuesUpstreamRequest from SDK messages.
void QueueUpstreamRequestToProto(const std::string& request_id,
                                 const std::vector<kubemq::v1::QueueMessage>& messages,
                                 const std::string& client_id,
                                 kubemq_pb::QueuesUpstreamRequest* proto);

// proto QueuesUpstreamResponse -> QueueUpstreamResult (from queue_upstream_handle.h)
// This returns individual fields rather than a specific type to avoid circular deps.
struct QueueUpstreamResultData {
    std::string ref_request_id;
    std::vector<kubemq::v1::QueueSendResult> results;
    bool is_error = false;
    std::string error;
};
QueueUpstreamResultData QueueUpstreamResponseFromProto(
    const kubemq_pb::QueuesUpstreamResponse& proto);

}  // namespace internal
}  // namespace kubemq
