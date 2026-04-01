/// @file query.h
/// @brief Synchronous query RPC message types with caching support.
#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Represents a synchronous query request (RPC pattern with response data).
///
/// Queries expect a response within a timeout. Support optional caching
/// via cache_key/cache_ttl. Use Query::Builder to construct instances.
///
/// @see QueryReceive, QueryResponse, QueryReply, Client::SendQuery
/// @example queries/send_query/main.cc
class KUBEMQ_API Query {
public:
    /// @brief Get the query ID.
    /// @return The query identifier.
    const std::string& id() const;
    /// @brief Get the target channel.
    /// @return The channel name.
    const std::string& channel() const;
    /// @brief Get the string metadata.
    /// @return The metadata string.
    const std::string& metadata() const;
    /// @brief Get the query body.
    /// @return The body content.
    const std::string& body() const;
    /// @brief Get the response timeout.
    /// @return The timeout duration.
    std::chrono::milliseconds timeout() const;
    /// @brief Get the client ID.
    /// @return The client identifier.
    const std::string& client_id() const;
    /// @brief Get the cache key.
    /// @return The cache key string.
    const std::string& cache_key() const;
    /// @brief Get the cache TTL.
    /// @return The cache duration.
    std::chrono::milliseconds cache_ttl() const;
    /// @brief Get the key-value tags.
    /// @return The tags map.
    const std::unordered_map<std::string, std::string>& tags() const;

    /// @brief Builder for constructing Query instances.
    class Builder {
    public:
        /// @brief Set the query ID (auto-generated if omitted).
        /// @param id Query identifier.
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
        /// @brief Set the query body (copy).
        /// @param body Query body.
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(const std::string& body);
        /// @brief Set the query body (move).
        /// @param body Query body (moved).
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(std::string&& body);
        /// @brief Set the response timeout (required).
        /// @param timeout Maximum wait duration for a response.
        /// @return Reference to this Builder for chaining.
        Builder& SetTimeout(std::chrono::milliseconds timeout);
        /// @brief Override the client ID for this query.
        /// @param client_id Client identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetClientId(const std::string& client_id);
        /// @brief Set the cache key for server-side caching.
        /// @param key Cache key string.
        /// @return Reference to this Builder for chaining.
        Builder& SetCacheKey(const std::string& key);
        /// @brief Set the cache TTL (time-to-live).
        /// @param ttl Cache duration.
        /// @return Reference to this Builder for chaining.
        Builder& SetCacheTTL(std::chrono::milliseconds ttl);
        /// @brief Set key-value tags (replaces existing).
        /// @param tags Tag map.
        /// @return Reference to this Builder for chaining.
        Builder& SetTags(std::unordered_map<std::string, std::string> tags);
        /// @brief Add a single tag.
        /// @param key Tag key.
        /// @param value Tag value.
        /// @return Reference to this Builder for chaining.
        Builder& AddTag(const std::string& key, const std::string& value);
        /// @brief Validate and build the Query.
        /// @return The built Query, or a validation error Status.
        [[nodiscard]] StatusOr<Query> Build();

    private:
        std::string id_;
        std::string channel_;
        std::string metadata_;
        std::string body_;
        std::chrono::milliseconds timeout_{0};
        std::string client_id_;
        std::string cache_key_;
        std::chrono::milliseconds cache_ttl_{0};
        std::unordered_map<std::string, std::string> tags_;
    };

private:
    friend class Builder;
    std::string id_;
    std::string channel_;
    std::string metadata_;
    std::string body_;
    std::chrono::milliseconds timeout_{0};
    std::string client_id_;
    std::string cache_key_;
    std::chrono::milliseconds cache_ttl_{0};
    std::unordered_map<std::string, std::string> tags_;
};

/// @brief Data received when a query arrives on a subscription.
/// @see Client::SubscribeToQueries
struct KUBEMQ_API QueryReceive {
    /// @brief Query identifier.
    std::string id;
    /// @brief Channel the query was received on.
    std::string channel;
    /// @brief ID of the client that sent the query.
    std::string client_id;
    /// @brief String metadata.
    std::string metadata;
    /// @brief Query body.
    std::string body;
    /// @brief Channel to send the reply to.
    std::string response_to;
    /// @brief Response timeout.
    std::chrono::milliseconds timeout{0};
    /// @brief Cache key if caching was requested.
    std::string cache_key;
    /// @brief Cache TTL if caching was requested.
    std::chrono::milliseconds cache_ttl{0};
    /// @brief Key-value tags.
    std::unordered_map<std::string, std::string> tags;
};

/// @brief Response received after sending a query.
/// @see Client::SendQuery
struct KUBEMQ_API QueryResponse {
    /// @brief ID of the original query.
    std::string query_id;
    /// @brief Channel the reply was sent on.
    std::string reply_channel;
    /// @brief Whether the query was executed successfully.
    bool executed = false;
    /// @brief Timestamp when the query was executed (Unix nanoseconds).
    int64_t executed_at = 0;
    /// @brief Response metadata.
    std::string metadata;
    /// @brief ID of the client that responded.
    std::string response_client_id;
    /// @brief Response body.
    std::string body;
    /// @brief Whether the response was served from cache.
    bool cache_hit = false;
    /// @brief Error message if execution failed.
    std::string error;
    /// @brief Key-value tags.
    std::unordered_map<std::string, std::string> tags;
};

/// @brief A reply to a received query.
///
/// Construct using QueryReply::Builder and send via
/// Client::SendQueryResponse().
///
/// @see Client::SendQueryResponse
/// @example queries/handle_query/main.cc
class KUBEMQ_API QueryReply {
public:
    /// @brief Get the original request ID.
    /// @return The request identifier.
    const std::string& request_id() const { return request_id_; }
    /// @brief Get the reply channel.
    /// @return The response channel name.
    const std::string& response_to() const { return response_to_; }
    /// @brief Get the string metadata.
    /// @return The metadata string.
    const std::string& metadata() const { return metadata_; }
    /// @brief Get the reply body.
    /// @return The body content.
    const std::string& body() const { return body_; }
    /// @brief Get the client ID.
    /// @return The client identifier.
    const std::string& client_id() const { return client_id_; }
    /// @brief Get the key-value tags.
    /// @return The tags map.
    const std::unordered_map<std::string, std::string>& tags() const { return tags_; }
    /// @brief Get the error message.
    /// @return The error string (empty if no error).
    const std::string& error() const { return error_; }
    /// @brief Get the execution timestamp.
    /// @return Unix timestamp in nanoseconds.
    int64_t executed_at() const { return executed_at_; }
    /// @brief Get whether this is a cache hit.
    /// @return true if response was served from cache.
    bool cache_hit() const { return cache_hit_; }
    /// @brief Get whether the query was executed.
    /// @return true if executed successfully.
    bool executed() const { return executed_; }

    /// @brief Builder for constructing QueryReply instances.
    class Builder {
    public:
        /// @brief Set the original request ID (required).
        /// @param id Request identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetRequestId(const std::string& id);
        /// @brief Set the reply channel (required).
        /// @param channel Reply channel from QueryReceive::response_to.
        /// @return Reference to this Builder for chaining.
        Builder& SetResponseTo(const std::string& channel);
        /// @brief Set optional metadata.
        /// @param metadata Metadata string.
        /// @return Reference to this Builder for chaining.
        Builder& SetMetadata(const std::string& metadata);
        /// @brief Set the reply body.
        /// @param body Reply body.
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(const std::string& body);
        /// @brief Set the client ID.
        /// @param client_id Client identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetClientId(const std::string& client_id);
        /// @brief Set key-value tags.
        /// @param tags Tag map.
        /// @return Reference to this Builder for chaining.
        Builder& SetTags(std::unordered_map<std::string, std::string> tags);
        /// @brief Set error message (indicates query failure).
        /// @param error Error description.
        /// @return Reference to this Builder for chaining.
        Builder& SetError(const std::string& error);
        /// @brief Set execution timestamp.
        /// @param timestamp Unix timestamp in nanoseconds.
        /// @return Reference to this Builder for chaining.
        Builder& SetExecutedAt(int64_t timestamp);
        /// @brief Set cache hit flag.
        /// @param hit true if serving from cache.
        /// @return Reference to this Builder for chaining.
        Builder& SetCacheHit(bool hit);
        /// @brief Set whether the query was executed.
        /// @param v true if executed successfully.
        /// @return Reference to this Builder for chaining.
        Builder& SetExecuted(bool v);
        /// @brief Validate and build the QueryReply.
        /// @return The built QueryReply, or a validation error Status.
        [[nodiscard]] StatusOr<QueryReply> Build();

    private:
        std::string request_id_;
        std::string response_to_;
        std::string metadata_;
        std::string body_;
        std::string client_id_;
        std::unordered_map<std::string, std::string> tags_;
        std::string error_;
        int64_t executed_at_ = 0;
        bool cache_hit_ = false;
        bool executed_ = true;
    };

private:
    friend class Builder;
    friend class Client;
    std::string request_id_;
    std::string response_to_;
    std::string metadata_;
    std::string body_;
    std::string client_id_;
    std::unordered_map<std::string, std::string> tags_;
    std::string error_;
    int64_t executed_at_ = 0;
    bool cache_hit_ = false;
    bool executed_ = true;
};

}  // namespace v1
}  // namespace kubemq
