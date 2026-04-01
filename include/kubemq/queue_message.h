/// @file queue_message.h
/// @brief Queue message types for guaranteed delivery messaging.
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq_pb {
class QueueMessage;
}  // namespace kubemq_pb

namespace kubemq {
inline namespace v1 {
class QueueMessage;  // Forward declaration for friend in internal namespace.
}  // namespace v1
namespace internal {
// Forward-declare converter so QueueMessage can friend it.
kubemq::v1::QueueMessage QueueMessageFromProto(const kubemq_pb::QueueMessage&);
}  // namespace internal
inline namespace v1 {

/// @brief Delivery policy for queue messages.
/// @see QueueMessage::Builder::SetPolicy
struct KUBEMQ_API QueuePolicy {
    /// @brief Message TTL in seconds, 0 = no expiration.
    int expiration_seconds = 0;
    /// @brief Delay before delivery in seconds, 0 = immediate.
    int delay_seconds = 0;
    /// @brief Max delivery attempts before dead-lettering, 0 = unlimited.
    int max_receive_count = 0;
    /// @brief Dead-letter queue name.
    std::string max_receive_queue;
};

/// @brief Server-assigned attributes of a received queue message.
struct KUBEMQ_API QueueMessageAttributes {
    /// @brief Server timestamp (Unix nanoseconds).
    int64_t timestamp = 0;
    /// @brief Message sequence number.
    uint64_t sequence = 0;
    /// @brief MD5 hash of the message body.
    std::string md5_of_body;
    /// @brief Number of times this message has been delivered.
    int receive_count = 0;
    /// @brief Whether this message was re-routed from another queue.
    bool re_routed = false;
    /// @brief Source queue if re-routed.
    std::string re_routed_from_queue;
    /// @brief Expiration timestamp (Unix nanoseconds).
    int64_t expiration_at = 0;
    /// @brief Delayed delivery timestamp (Unix nanoseconds).
    int64_t delayed_to = 0;
};

/// @brief Result of sending a queue message.
/// @see Client::SendQueueMessage
struct KUBEMQ_API QueueSendResult {
    /// @brief Assigned message ID.
    std::string message_id;
    /// @brief Send timestamp (Unix nanoseconds).
    int64_t sent_at = 0;
    /// @brief Expiration timestamp (Unix nanoseconds).
    int64_t expiration_at = 0;
    /// @brief Delayed delivery timestamp (Unix nanoseconds).
    int64_t delayed_to = 0;
    /// @brief Whether an error occurred.
    bool is_error = false;
    /// @brief Error message if send failed.
    std::string error;
};

/// @brief A message for the queue messaging pattern.
///
/// Use QueueMessage::Builder to construct instances for sending.
///
/// @see QueuePolicy, QueueMessageAttributes, Client::SendQueueMessage
/// @example queues/send_receive/main.cc
class KUBEMQ_API QueueMessage {
public:
    /// @brief Get the message ID.
    /// @return The message identifier.
    const std::string& id() const;
    /// @brief Get the target queue channel.
    /// @return The channel name.
    const std::string& channel() const;
    /// @brief Get the string metadata.
    /// @return The metadata string.
    const std::string& metadata() const;
    /// @brief Get the message body.
    /// @return The body content.
    const std::string& body() const;
    /// @brief Get the client ID.
    /// @return The client identifier.
    const std::string& client_id() const;
    /// @brief Get the key-value tags.
    /// @return The tags map.
    const std::unordered_map<std::string, std::string>& tags() const;
    /// @brief Get the delivery policy.
    /// @return Pointer to the policy, or nullptr if not set.
    const QueuePolicy* policy() const;
    /// @brief Get server-assigned attributes.
    /// @return Pointer to the attributes, or nullptr if not set.
    const QueueMessageAttributes* attributes() const;

    /// @brief Builder for constructing QueueMessage instances.
    class Builder {
    public:
        /// @brief Set the message ID (auto-generated if omitted).
        /// @param id Message identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetId(const std::string& id);
        /// @brief Set the target queue channel (required).
        /// @param channel Channel name.
        /// @return Reference to this Builder for chaining.
        Builder& SetChannel(const std::string& channel);
        /// @brief Set optional string metadata.
        /// @param metadata Metadata string.
        /// @return Reference to this Builder for chaining.
        Builder& SetMetadata(const std::string& metadata);
        /// @brief Set the message body (copy).
        /// @param body Message body.
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(const std::string& body);
        /// @brief Set the message body (move).
        /// @param body Message body (moved).
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(std::string&& body);
        /// @brief Override the client ID for this message.
        /// @param client_id Client identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetClientId(const std::string& client_id);
        /// @brief Set key-value tags (replaces existing).
        /// @param tags Tag map.
        /// @return Reference to this Builder for chaining.
        Builder& SetTags(std::unordered_map<std::string, std::string> tags);
        /// @brief Add a single tag.
        /// @param key Tag key.
        /// @param value Tag value.
        /// @return Reference to this Builder for chaining.
        Builder& AddTag(const std::string& key, const std::string& value);
        /// @brief Set the delivery policy.
        /// @param policy Queue delivery policy.
        /// @return Reference to this Builder for chaining.
        Builder& SetPolicy(const QueuePolicy& policy);
        /// @brief Set message TTL in seconds.
        /// @param seconds TTL value (0 = no expiration).
        /// @return Reference to this Builder for chaining.
        Builder& SetExpirationSeconds(int seconds);
        /// @brief Set delay before delivery.
        /// @param seconds Delay value (0 = immediate).
        /// @return Reference to this Builder for chaining.
        Builder& SetDelaySeconds(int seconds);
        /// @brief Set max delivery attempts before dead-lettering.
        /// @param count Max attempts (0 = unlimited).
        /// @return Reference to this Builder for chaining.
        Builder& SetMaxReceiveCount(int count);
        /// @brief Set the dead-letter queue name.
        /// @param queue Target queue for dead-lettered messages.
        /// @return Reference to this Builder for chaining.
        Builder& SetMaxReceiveQueue(const std::string& queue);
        /// @brief Validate and build the QueueMessage.
        /// @return The built QueueMessage, or a validation error Status.
        [[nodiscard]] StatusOr<QueueMessage> Build();

    private:
        std::string id_;
        std::string channel_;
        std::string metadata_;
        std::string body_;
        std::string client_id_;
        std::unordered_map<std::string, std::string> tags_;
        QueuePolicy policy_;
    };

private:
    friend class Builder;
    friend QueueMessage kubemq::internal::QueueMessageFromProto(const kubemq_pb::QueueMessage&);
    std::string id_;
    std::string channel_;
    std::string metadata_;
    std::string body_;
    std::string client_id_;
    std::unordered_map<std::string, std::string> tags_;
    QueuePolicy policy_;
    QueueMessageAttributes attributes_;
};

/// @brief Configuration for receiving queue messages (simple API).
/// @see Client::ReceiveQueueMessages
struct KUBEMQ_API ReceiveQueueMessagesRequest {
    /// @brief Unique request identifier.
    std::string request_id;
    /// @brief Client identifier.
    std::string client_id;
    /// @brief Queue channel to receive from.
    std::string channel;
    /// @brief Max messages to receive (1-1024).
    int32_t max_number_of_messages = 1;  // 1-1024
    /// @brief Seconds to wait for messages (0-3600).
    int32_t wait_time_seconds = 5;  // 0-3600
    /// @brief true = view without consuming.
    bool is_peek = false;  // true = view without consuming
};

/// @brief Response from a queue receive operation.
/// @see Client::ReceiveQueueMessages
struct KUBEMQ_API ReceiveQueueMessagesResponse {
    /// @brief Original request identifier.
    std::string request_id;
    /// @brief Received messages.
    std::vector<QueueMessage> messages;
    /// @brief Number of messages received.
    int32_t messages_received = 0;
    /// @brief Number of messages that expired.
    int32_t messages_expired = 0;
    /// @brief Whether this was a peek operation.
    bool is_peek = false;
    /// @brief Whether an error occurred.
    bool is_error = false;
    /// @brief Error message if receive failed.
    std::string error;
};

/// @brief Configuration for acknowledging all messages in a queue.
/// @see Client::AckAllQueueMessages
struct KUBEMQ_API AckAllQueueMessagesRequest {
    /// @brief Unique request identifier.
    std::string request_id;
    /// @brief Client identifier.
    std::string client_id;
    /// @brief Queue channel to ack.
    std::string channel;
    /// @brief Seconds to wait for ack completion.
    int32_t wait_time_seconds = 0;
};

/// @brief Response from an ack-all operation.
/// @see Client::AckAllQueueMessages
struct KUBEMQ_API AckAllQueueMessagesResponse {
    /// @brief Original request identifier.
    std::string request_id;
    /// @brief Number of messages acknowledged.
    uint64_t affected_messages = 0;
    /// @brief Whether an error occurred.
    bool is_error = false;
    /// @brief Error message if ack-all failed.
    std::string error;
};

}  // namespace v1
}  // namespace kubemq
