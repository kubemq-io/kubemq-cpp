/// @file command.h
/// @brief Synchronous command RPC message types.
#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Represents a synchronous command request (RPC pattern).
///
/// Commands expect a response within a timeout. Use Command::Builder
/// to construct instances.
///
/// @see CommandReceive, CommandResponse, CommandReply, Client::SendCommand
/// @example commands/send_command/main.cc
class KUBEMQ_API Command {
public:
    /// @brief Get the command ID.
    /// @return The command identifier.
    const std::string& id() const;
    /// @brief Get the target channel.
    /// @return The channel name.
    const std::string& channel() const;
    /// @brief Get the string metadata.
    /// @return The metadata string.
    const std::string& metadata() const;
    /// @brief Get the command body.
    /// @return The body content.
    const std::string& body() const;
    /// @brief Get the response timeout.
    /// @return The timeout duration.
    std::chrono::milliseconds timeout() const;
    /// @brief Get the client ID.
    /// @return The client identifier.
    const std::string& client_id() const;
    /// @brief Get the key-value tags.
    /// @return The tags map.
    const std::unordered_map<std::string, std::string>& tags() const;

    /// @brief Builder for constructing Command instances.
    class Builder {
    public:
        /// @brief Set the command ID (auto-generated if omitted).
        /// @param id Command identifier.
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
        /// @brief Set the command body (copy).
        /// @param body Command body.
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(const std::string& body);
        /// @brief Set the command body (move).
        /// @param body Command body (moved).
        /// @return Reference to this Builder for chaining.
        Builder& SetBody(std::string&& body);
        /// @brief Set the response timeout (required).
        /// @param timeout Maximum wait duration for a response.
        /// @return Reference to this Builder for chaining.
        Builder& SetTimeout(std::chrono::milliseconds timeout);
        /// @brief Override the client ID for this command.
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
        /// @brief Validate and build the Command.
        /// @return The built Command, or a validation error Status.
        [[nodiscard]] StatusOr<Command> Build();

    private:
        std::string id_;
        std::string channel_;
        std::string metadata_;
        std::string body_;
        std::chrono::milliseconds timeout_{0};
        std::string client_id_;
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
    std::unordered_map<std::string, std::string> tags_;
};

/// @brief Data received when a command arrives on a subscription.
/// @see Client::SubscribeToCommands
struct KUBEMQ_API CommandReceive {
    /// @brief Command identifier.
    std::string id;
    /// @brief ID of the client that sent the command.
    std::string client_id;
    /// @brief Channel the command was received on.
    std::string channel;
    /// @brief String metadata.
    std::string metadata;
    /// @brief Command body.
    std::string body;
    /// @brief Channel to send the reply to.
    std::string response_to;
    /// @brief Response timeout.
    std::chrono::milliseconds timeout{0};
    /// @brief Key-value tags.
    std::unordered_map<std::string, std::string> tags;
};

/// @brief Response received after sending a command.
/// @see Client::SendCommand
struct KUBEMQ_API CommandResponse {
    /// @brief ID of the original command.
    std::string command_id;
    /// @brief ID of the client that responded.
    std::string response_client_id;
    /// @brief Channel the reply was sent on.
    std::string reply_channel;
    /// @brief Whether the command was executed successfully.
    bool executed = false;
    /// @brief Timestamp when the command was executed (Unix nanoseconds).
    int64_t executed_at = 0;
    /// @brief Error message if execution failed.
    std::string error;
    /// @brief Key-value tags.
    std::unordered_map<std::string, std::string> tags;
};

/// @brief A reply to a received command.
///
/// Construct using CommandReply::Builder and send via
/// Client::SendCommandResponse().
///
/// @see Client::SendCommandResponse
/// @example commands/handle_command/main.cc
class KUBEMQ_API CommandReply {
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
    /// @brief Get whether the command was executed.
    /// @return true if executed successfully.
    bool executed() const { return executed_; }

    /// @brief Builder for constructing CommandReply instances.
    class Builder {
    public:
        /// @brief Set the original request ID (required).
        /// @param id Request identifier.
        /// @return Reference to this Builder for chaining.
        Builder& SetRequestId(const std::string& id);
        /// @brief Set the reply channel (required).
        /// @param channel Reply channel from CommandReceive::response_to.
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
        /// @brief Set error message (indicates command failure).
        /// @param error Error description.
        /// @return Reference to this Builder for chaining.
        Builder& SetError(const std::string& error);
        /// @brief Set execution timestamp.
        /// @param timestamp Unix timestamp in nanoseconds.
        /// @return Reference to this Builder for chaining.
        Builder& SetExecutedAt(int64_t timestamp);
        /// @brief Set whether the command was executed.
        /// @param v true if executed successfully.
        /// @return Reference to this Builder for chaining.
        Builder& SetExecuted(bool v);
        /// @brief Validate and build the CommandReply.
        /// @return The built CommandReply, or a validation error Status.
        [[nodiscard]] StatusOr<CommandReply> Build();

    private:
        std::string request_id_;
        std::string response_to_;
        std::string metadata_;
        std::string body_;
        std::string client_id_;
        std::unordered_map<std::string, std::string> tags_;
        std::string error_;
        int64_t executed_at_ = 0;
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
    bool executed_ = true;
};

}  // namespace v1
}  // namespace kubemq
