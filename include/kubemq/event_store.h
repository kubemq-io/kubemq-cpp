/// @file event_store.h
/// @brief Persistent event message types with replay support.
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Represents a persistent event message stored by the broker.
///
/// Unlike Event, EventStore messages are persisted and can be replayed
/// using SubscriptionOption. Use EventStore::Builder to construct instances.
///
/// @see EventStoreReceive, SubscriptionOption, Client::SendEventStore
/// @example events_store/persistent_pubsub/main.cc
class KUBEMQ_API EventStore {
public:
    /// @brief Get the event ID.
    /// @return The event identifier.
    const std::string& id() const;
    /// @brief Get the target channel.
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

    /// @brief Builder for constructing EventStore instances.
    class Builder {
    public:
        /// @brief Set the event ID (auto-generated if omitted).
        /// @param id Event identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetId(const std::string& id);
        /// @brief Set the target channel (required).
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
        /// @brief Override the client ID for this event.
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
        /// @brief Validate and build the EventStore.
        /// @return The built EventStore, or a validation error Status.
        [[nodiscard]] StatusOr<EventStore> Build();

    private:
        std::string id_;
        std::string channel_;
        std::string metadata_;
        std::string body_;
        std::string client_id_;
        std::unordered_map<std::string, std::string> tags_;
    };

private:
    friend class Builder;
    std::string id_;
    std::string channel_;
    std::string metadata_;
    std::string body_;
    std::string client_id_;
    std::unordered_map<std::string, std::string> tags_;
};

/// @brief Acknowledgment result for a sent persistent event.
/// @see Client::SendEventStore
struct KUBEMQ_API EventStoreResult {
    /// @brief Event identifier.
    std::string id;
    /// @brief Whether the event was successfully stored.
    bool sent = false;
    /// @brief Error message if storage failed.
    std::string error;
};

/// @brief Data received when a persistent event arrives on a subscription.
/// @see Client::SubscribeToEventsStore
struct KUBEMQ_API EventStoreReceive {
    /// @brief Event identifier.
    std::string id;
    /// @brief Sequence number in the event store.
    uint64_t sequence = 0;
    /// @brief Unix timestamp in nanoseconds.
    int64_t timestamp = 0;  // Unix nanoseconds
    /// @brief Channel the event was received on.
    std::string channel;
    /// @brief String metadata.
    std::string metadata;
    /// @brief Message body.
    std::string body;
    /// @brief Key-value tags.
    std::unordered_map<std::string, std::string> tags;
};

}  // namespace v1
}  // namespace kubemq
