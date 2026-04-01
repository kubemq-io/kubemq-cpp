/// @file client.h
/// @brief Main client for interacting with KubeMQ broker.
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kubemq/channel_info.h"
#include "kubemq/client_options.h"
#include "kubemq/command.h"
#include "kubemq/connection_state.h"
#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/export.h"
#include "kubemq/query.h"
#include "kubemq/queue_downstream_receiver.h"
#include "kubemq/queue_message.h"
#include "kubemq/queue_upstream_handle.h"
#include "kubemq/server_info.h"
#include "kubemq/status.h"
#include "kubemq/stream_handle.h"
#include "kubemq/subscription.h"
#include "kubemq/subscription_option.h"

namespace kubemq {
inline namespace v1 {

/// @brief Thread-safe client for KubeMQ message broker operations.
///
/// The Client class provides methods for all KubeMQ messaging patterns:
/// Events, Events Store, Commands, Queries, and Queues. Create via
/// the static Create() factory method.
///
/// @note Thread-safe. All public methods can be called from multiple threads.
/// @see ClientOptions, Event, EventStore, Command, Query, QueueMessage
/// @example connection/connect/main.cc
class KUBEMQ_API Client {
public:
    /// @brief Create a new Client and connect to the broker.
    ///
    /// Establishes a gRPC connection using the provided options. If
    /// ClientOptions::check_connection is true (default), verifies connectivity
    /// with a Ping before returning.
    ///
    /// @param options Connection and behavior configuration.
    /// @return A connected Client, or an error Status if connection fails.
    ///         Callers should check ok() before accessing the value.
    /// @see ClientOptions
    /// @example connection/connect/main.cc
    [[nodiscard]] static StatusOr<std::unique_ptr<Client>> Create(const ClientOptions& options);

    /// @brief Get current connection state.
    /// @return The current ConnectionState enum value.
    /// @see ConnectionState
    ConnectionState State() const;

    /// @brief Health-check the connected broker.
    /// @return Server information on success, or an error Status.
    /// @see ServerInfo
    /// @example connection/ping/main.cc
    [[nodiscard]] StatusOr<ServerInfo> Ping();

    /// @brief Gracefully shut down the client and release resources.
    /// @return OK Status on success, or an error if shutdown fails.
    /// @example connection/close/main.cc
    [[nodiscard]] Status Close();

    ~Client();

    // Non-copyable, non-movable (PIMPL)
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    // === Events ===

    /// @brief Send a single fire-and-forget event.
    /// @param event Event to send (must have channel set).
    /// @return OK Status on success.
    /// @see Event
    /// @example events/basic_pubsub/main.cc
    [[nodiscard]] Status SendEvent(const Event& event);
    /// @brief Open a persistent event stream for high-throughput sending.
    /// @param on_error Callback invoked on stream errors.
    /// @return An EventStreamHandle for sending events, or an error Status.
    /// @see EventStreamHandle
    /// @example events/stream_send/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<EventStreamHandle>> SendEventStream(
        std::function<void(const Status&)> on_error);
    /// @brief Open event stream without callbacks.
    /// @return An EventStreamHandle, or an error Status.
    /// @deprecated Use the overload accepting an on_error callback.
    [[nodiscard]] StatusOr<std::unique_ptr<EventStreamHandle>> SendEventStream();
    /// @brief Subscribe to real-time events on a channel.
    /// @param channel Channel name to subscribe to.
    /// @param group Consumer group name (empty for no group).
    /// @param on_event Callback invoked for each received event.
    /// @param on_error Callback invoked on subscription errors.
    /// @return A Subscription handle, or an error Status.
    /// @see Subscription, EventReceive
    /// @example events/basic_pubsub/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<Subscription>> SubscribeToEvents(
        const std::string& channel, const std::string& group,
        std::function<void(const EventReceive&)> on_event,
        std::function<void(const Status&)> on_error);
    /// @brief Convenience: build and send an event in one call.
    /// @param channel Target channel name.
    /// @param body Message body.
    /// @param metadata Optional string metadata (default: "").
    /// @param tags Optional key-value tags (default: empty).
    /// @return OK Status on success.
    /// @see Event::Builder
    [[nodiscard]] Status PublishEvent(
        const std::string& channel, const std::string& body, const std::string& metadata = "",
        const std::unordered_map<std::string, std::string>& tags = {});

    // === Events Store ===

    /// @brief Send a persistent event to Events Store.
    /// @param event EventStore to send.
    /// @return Acknowledgment result, or an error Status.
    /// @see EventStore, EventStoreResult
    /// @example events_store/persistent_pubsub/main.cc
    [[nodiscard]] StatusOr<EventStoreResult> SendEventStore(const EventStore& event);
    /// @brief Open a persistent event store stream.
    /// @param on_result Callback invoked with the result of each sent event.
    /// @param on_error Callback invoked on stream errors.
    /// @return An EventStoreStreamHandle, or an error Status.
    /// @see EventStoreStreamHandle
    /// @example events_store/stream_send/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<EventStoreStreamHandle>> SendEventStoreStream(
        std::function<void(const EventStoreResult&)> on_result,
        std::function<void(const Status&)> on_error);
    /// @brief Open event store stream without callbacks.
    /// @return An EventStoreStreamHandle, or an error Status.
    /// @deprecated Use the overload accepting callbacks.
    [[nodiscard]] StatusOr<std::unique_ptr<EventStoreStreamHandle>> SendEventStoreStream();
    /// @brief Subscribe to persistent events with replay.
    /// @param channel Channel name to subscribe to.
    /// @param group Consumer group name (empty for no group).
    /// @param start_option Where to start reading from.
    /// @param on_event Callback invoked for each received event.
    /// @param on_error Callback invoked on subscription errors.
    /// @return A Subscription handle, or an error Status.
    /// @see SubscriptionOption
    /// @example events_store/replay_from_sequence/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<Subscription>> SubscribeToEventsStore(
        const std::string& channel, const std::string& group,
        const SubscriptionOption& start_option,
        std::function<void(const EventStoreReceive&)> on_event,
        std::function<void(const Status&)> on_error);
    /// @brief Convenience: build and send a persistent event.
    /// @param channel Target channel name.
    /// @param body Message body.
    /// @param metadata Optional string metadata (default: "").
    /// @param tags Optional key-value tags (default: empty).
    /// @return Acknowledgment result, or an error Status.
    /// @see EventStore::Builder
    [[nodiscard]] StatusOr<EventStoreResult> PublishEventStore(
        const std::string& channel, const std::string& body, const std::string& metadata = "",
        const std::unordered_map<std::string, std::string>& tags = {});

    // === Queues - Stream API ===

    /// @brief Open an upstream queue stream for batch sending.
    /// @param on_result Callback invoked with the result of each batch.
    /// @param on_error Callback invoked on stream errors.
    /// @return A QueueUpstreamHandle, or an error Status.
    /// @see QueueUpstreamHandle
    /// @example queues_stream/stream_send/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<QueueUpstreamHandle>> QueueUpstream(
        std::function<void(const QueueUpstreamResult&)> on_result,
        std::function<void(const Status&)> on_error);
    /// @brief Open upstream queue stream without callbacks.
    /// @return A QueueUpstreamHandle, or an error Status.
    /// @deprecated Use the overload accepting callbacks.
    [[nodiscard]] StatusOr<std::unique_ptr<QueueUpstreamHandle>> QueueUpstream();
    /// @brief Create a downstream receiver for polling queues.
    /// @return A QueueDownstreamReceiver, or an error Status.
    /// @see QueueDownstreamReceiver
    /// @example queues_stream/stream_receive/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<QueueDownstreamReceiver>> NewQueueDownstreamReceiver();
    /// @brief Poll a queue for messages (convenience wrapper).
    /// @param request Poll configuration (channel, max items, timeout, auto-ack).
    /// @return A PollResponse with messages, or an error Status.
    /// @see PollRequest, PollResponse
    /// @example queues_stream/poll_mode/main.cc
    [[nodiscard]] StatusOr<PollResponse> PollQueue(const PollRequest& request);

    // === Queues - Simple API ===

    /// @brief Send a single message to a queue.
    /// @param message Queue message to send.
    /// @return Send result with message ID and timestamps, or an error Status.
    /// @see QueueMessage, QueueSendResult
    /// @example queues/send_receive/main.cc
    [[nodiscard]] StatusOr<QueueSendResult> SendQueueMessage(const QueueMessage& message);
    /// @brief Send a batch of messages to queues.
    /// @param messages Vector of queue messages to send.
    /// @return Vector of send results, or an error Status.
    /// @see QueueMessage
    /// @example queues/batch_send/main.cc
    [[nodiscard]] StatusOr<std::vector<QueueSendResult>> SendQueueMessages(
        const std::vector<QueueMessage>& messages);
    /// @brief Receive messages from a queue (simple API).
    /// @param request Receive configuration (channel, max messages, timeout, peek).
    /// @return Response with received messages, or an error Status.
    /// @see ReceiveQueueMessagesRequest
    /// @example queues/send_receive/main.cc
    [[nodiscard]] StatusOr<ReceiveQueueMessagesResponse> ReceiveQueueMessages(
        const ReceiveQueueMessagesRequest& request);
    /// @brief Acknowledge all pending messages in a queue.
    /// @param request Ack-all configuration (channel, wait timeout).
    /// @return Response with affected message count, or an error Status.
    /// @see AckAllQueueMessagesRequest
    /// @example queues/ack_all/main.cc
    [[nodiscard]] StatusOr<AckAllQueueMessagesResponse> AckAllQueueMessages(
        const AckAllQueueMessagesRequest& request);
    /// @brief Convenience: send a queue message with minimal parameters.
    /// @param channel Target queue channel name.
    /// @param body Message body.
    /// @param expiration_seconds Message TTL in seconds (default: 0 = no expiration).
    /// @param delay_seconds Delay before delivery in seconds (default: 0 = immediate).
    /// @return Send result, or an error Status.
    /// @see QueueMessage::Builder
    [[nodiscard]] StatusOr<QueueSendResult> SendQueueMessageSimple(const std::string& channel,
                                                                   const std::string& body,
                                                                   int expiration_seconds = 0,
                                                                   int delay_seconds = 0);

    // === RPC - Commands ===

    /// @brief Send a command and wait for a response.
    /// @param command Command with timeout.
    /// @return Command response, or an error Status.
    /// @see Command, CommandResponse
    /// @example commands/send_command/main.cc
    [[nodiscard]] StatusOr<CommandResponse> SendCommand(const Command& command);
    /// @brief Subscribe to incoming commands.
    /// @param channel Channel name to subscribe to.
    /// @param group Consumer group name (empty for no group).
    /// @param on_command Callback invoked for each received command.
    /// @param on_error Callback invoked on subscription errors.
    /// @return A Subscription handle, or an error Status.
    /// @see CommandReceive
    /// @example commands/handle_command/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<Subscription>> SubscribeToCommands(
        const std::string& channel, const std::string& group,
        std::function<void(const CommandReceive&)> on_command,
        std::function<void(const Status&)> on_error);
    /// @brief Send a response to a received command.
    /// @param reply Command reply to send.
    /// @return OK Status on success.
    /// @see CommandReply
    /// @example commands/handle_command/main.cc
    [[nodiscard]] Status SendCommandResponse(const CommandReply& reply);
    /// @brief Convenience: send a command with minimal parameters.
    /// @param channel Target channel name.
    /// @param body Command body.
    /// @param timeout Maximum wait time for response.
    /// @return Command response, or an error Status.
    /// @see Command::Builder
    [[nodiscard]] StatusOr<CommandResponse> SendCommandSimple(const std::string& channel,
                                                              const std::string& body,
                                                              std::chrono::milliseconds timeout);

    // === RPC - Queries ===

    /// @brief Send a query and wait for a response.
    /// @param query Query with timeout.
    /// @return Query response, or an error Status.
    /// @see Query, QueryResponse
    /// @example queries/send_query/main.cc
    [[nodiscard]] StatusOr<QueryResponse> SendQuery(const Query& query);
    /// @brief Subscribe to incoming queries.
    /// @param channel Channel name to subscribe to.
    /// @param group Consumer group name (empty for no group).
    /// @param on_query Callback invoked for each received query.
    /// @param on_error Callback invoked on subscription errors.
    /// @return A Subscription handle, or an error Status.
    /// @see QueryReceive
    /// @example queries/handle_query/main.cc
    [[nodiscard]] StatusOr<std::unique_ptr<Subscription>> SubscribeToQueries(
        const std::string& channel, const std::string& group,
        std::function<void(const QueryReceive&)> on_query,
        std::function<void(const Status&)> on_error);
    /// @brief Send a response to a received query.
    /// @param reply Query reply to send.
    /// @return OK Status on success.
    /// @see QueryReply
    /// @example queries/handle_query/main.cc
    [[nodiscard]] Status SendQueryResponse(const QueryReply& reply);
    /// @brief Convenience: send a query with minimal parameters.
    /// @param channel Target channel name.
    /// @param body Query body.
    /// @param timeout Maximum wait time for response.
    /// @return Query response, or an error Status.
    /// @see Query::Builder
    [[nodiscard]] StatusOr<QueryResponse> SendQuerySimple(const std::string& channel,
                                                          const std::string& body,
                                                          std::chrono::milliseconds timeout);

    // === Channel Management ===

    /// @brief Create a channel of the specified type.
    /// @param channel_name Name of the channel to create.
    /// @param channel_type One of the kChannelType* constants.
    /// @return OK Status on success.
    /// @see kChannelTypeEvents
    [[nodiscard]] Status CreateChannel(const std::string& channel_name,
                                       const std::string& channel_type);
    /// @brief Delete a channel of the specified type.
    /// @param channel_name Name of the channel to delete.
    /// @param channel_type One of the kChannelType* constants.
    /// @return OK Status on success.
    [[nodiscard]] Status DeleteChannel(const std::string& channel_name,
                                       const std::string& channel_type);
    /// @brief List channels of the specified type.
    /// @param channel_type One of the kChannelType* constants.
    /// @param search Optional prefix filter (default: "" for all).
    /// @return Vector of channel info, or an error Status.
    /// @see ChannelInfo
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListChannels(const std::string& channel_type,
                                                                  const std::string& search = "");

    // Convenience aliases

    /// @brief Convenience: create an events channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see CreateChannel
    [[nodiscard]] Status CreateEventsChannel(const std::string& name);
    /// @brief Convenience: create an events store channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see CreateChannel
    [[nodiscard]] Status CreateEventsStoreChannel(const std::string& name);
    /// @brief Convenience: create a commands channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see CreateChannel
    [[nodiscard]] Status CreateCommandsChannel(const std::string& name);
    /// @brief Convenience: create a queries channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see CreateChannel
    [[nodiscard]] Status CreateQueriesChannel(const std::string& name);
    /// @brief Convenience: create a queues channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see CreateChannel
    [[nodiscard]] Status CreateQueuesChannel(const std::string& name);
    /// @brief Convenience: delete an events channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see DeleteChannel
    [[nodiscard]] Status DeleteEventsChannel(const std::string& name);
    /// @brief Convenience: delete an events store channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see DeleteChannel
    [[nodiscard]] Status DeleteEventsStoreChannel(const std::string& name);
    /// @brief Convenience: delete a commands channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see DeleteChannel
    [[nodiscard]] Status DeleteCommandsChannel(const std::string& name);
    /// @brief Convenience: delete a queries channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see DeleteChannel
    [[nodiscard]] Status DeleteQueriesChannel(const std::string& name);
    /// @brief Convenience: delete a queues channel.
    /// @param name Channel name.
    /// @return OK Status on success.
    /// @see DeleteChannel
    [[nodiscard]] Status DeleteQueuesChannel(const std::string& name);
    /// @brief Convenience: list events channels.
    /// @param search Optional prefix filter (default: "").
    /// @return Vector of channel info, or an error Status.
    /// @see ListChannels
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListEventsChannels(
        const std::string& search = "");
    /// @brief Convenience: list events store channels.
    /// @param search Optional prefix filter (default: "").
    /// @return Vector of channel info, or an error Status.
    /// @see ListChannels
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListEventsStoreChannels(
        const std::string& search = "");
    /// @brief Convenience: list commands channels.
    /// @param search Optional prefix filter (default: "").
    /// @return Vector of channel info, or an error Status.
    /// @see ListChannels
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListCommandsChannels(
        const std::string& search = "");
    /// @brief Convenience: list queries channels.
    /// @param search Optional prefix filter (default: "").
    /// @return Vector of channel info, or an error Status.
    /// @see ListChannels
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListQueriesChannels(
        const std::string& search = "");
    /// @brief Convenience: list queues channels.
    /// @param search Optional prefix filter (default: "").
    /// @return Vector of channel info, or an error Status.
    /// @see ListChannels
    [[nodiscard]] StatusOr<std::vector<ChannelInfo>> ListQueuesChannels(
        const std::string& search = "");

private:
    Client();
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace v1
}  // namespace kubemq
