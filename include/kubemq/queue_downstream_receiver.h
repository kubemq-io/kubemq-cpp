/// @file queue_downstream_receiver.h
/// @brief Queue message polling, acknowledgement, and transaction management.

// include/kubemq/queue_downstream_receiver.h
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "kubemq/export.h"
#include "kubemq/queue_message.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Parameters for a queue poll operation.
///
/// Configures channel, batch size, timeout, and auto-acknowledgement
/// behavior for QueueDownstreamReceiver::Poll().
///
/// @see QueueDownstreamReceiver::Poll
/// @see PollResponse
/// @example queues_stream/poll_mode/main.cc
struct KUBEMQ_API PollRequest {
    /// @brief Channel name to poll from.
    std::string channel;

    /// @brief Maximum number of messages to retrieve in one poll. Default: 1.
    int32_t max_items = 1;

    /// @brief How long the broker waits for messages before returning an empty response. Default: 5
    /// seconds.
    int32_t wait_timeout_seconds = 5;

    /// @brief When true, messages are automatically acknowledged on delivery. Default: false.
    bool auto_ack = false;
};

/// @brief A single message received from a queue poll within a transaction.
///
/// Wraps a QueueMessage with transaction context, allowing per-message
/// acknowledgement, negative-acknowledgement, or re-queue operations.
/// Must be acknowledged (or the transaction completes/expires) to remove
/// the message from the queue.
///
/// @note Move-only type. Thread-safe for individual Ack/Nack/ReQueue calls.
/// @see PollResponse
/// @see QueueMessage
/// @example queues_stream/ack_range/main.cc
class KUBEMQ_API QueueDownstreamMessage {
public:
    /// @brief Returns the underlying queue message.
    /// @return Const reference to the received QueueMessage.
    const QueueMessage& message() const;

    /// @brief Returns the transaction ID for this message.
    /// @return Transaction ID string used for ack/nack operations.
    const std::string& transaction_id() const;

    /// @brief Returns the queue sequence number of this message.
    /// @return Monotonically increasing sequence number assigned by the broker.
    uint64_t sequence() const;

    /// @brief Acknowledges this message, removing it from the queue.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status Ack();

    /// @brief Negatively acknowledges this message, returning it to the queue for redelivery.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status Nack();

    /// @brief Re-queues this message to a different channel.
    /// @param channel The target channel to move the message to.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status ReQueue(const std::string& channel);

    /// @brief Move constructor.
    QueueDownstreamMessage(QueueDownstreamMessage&&) noexcept;

    /// @brief Move assignment operator.
    QueueDownstreamMessage& operator=(QueueDownstreamMessage&&) noexcept;

    /// @brief Destructor.
    ~QueueDownstreamMessage();

    QueueDownstreamMessage(const QueueDownstreamMessage&) = delete;
    QueueDownstreamMessage& operator=(const QueueDownstreamMessage&) = delete;

private:
    friend class PollResponse;
    friend class QueueDownstreamReceiver;
    class Impl;
    std::unique_ptr<Impl> impl_;
    QueueDownstreamMessage();
};

/// @brief Response from a queue poll operation containing messages and transaction context.
///
/// Provides batch-level operations (AckAll, NackAll, ReQueueAll) as well as
/// access to individual QueueDownstreamMessage objects for per-message control.
///
/// @note Move-only type. Thread-safe for batch ack/nack operations.
/// @see QueueDownstreamReceiver::Poll
/// @see QueueDownstreamMessage
/// @see PollRequest
/// @example queues_stream/stream_receive/main.cc
/// @example queues_stream/ack_range/main.cc
class KUBEMQ_API PollResponse {
public:
    /// @brief Returns the transaction ID for this poll batch.
    /// @return Transaction ID string shared by all messages in this response.
    const std::string& transaction_id() const;

    /// @brief Returns the received messages (const).
    /// @return Const reference to the vector of downstream messages.
    const std::vector<QueueDownstreamMessage>& messages() const;

    /// @brief Returns the received messages (mutable).
    /// @return Mutable reference to the vector of downstream messages.
    std::vector<QueueDownstreamMessage>& messages();

    /// @brief Checks whether the poll response indicates an error.
    /// @return true if the broker returned an error.
    bool is_error() const;

    /// @brief Returns the error description.
    /// @return Error string; empty when is_error() is false.
    const std::string& error() const;

    /// @brief Checks whether the transaction has already completed.
    /// @return true if the transaction has been fully resolved.
    bool transaction_complete() const;

    /// @brief Acknowledges all messages in this response.
    /// @return Status::OK on success, or an error status.
    /// @example queues/ack_all/main.cc
    [[nodiscard]] Status AckAll();

    /// @brief Negatively acknowledges all messages, returning them to the queue.
    /// @return Status::OK on success, or an error status.
    /// @example queues_stream/nack_all/main.cc
    [[nodiscard]] Status NackAll();

    /// @brief Re-queues all messages to a different channel.
    /// @param channel The target channel to move all messages to.
    /// @return Status::OK on success, or an error status.
    /// @example queues_stream/requeue_all/main.cc
    [[nodiscard]] Status ReQueueAll(const std::string& channel);

    /// @brief Move constructor.
    PollResponse(PollResponse&&) noexcept;

    /// @brief Move assignment operator.
    PollResponse& operator=(PollResponse&&) noexcept;

    /// @brief Destructor.
    ~PollResponse();

    PollResponse(const PollResponse&) = delete;
    PollResponse& operator=(const PollResponse&) = delete;

private:
    friend class QueueDownstreamReceiver;
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    PollResponse();
};

/// @brief Receiver for polling queue messages from the broker.
///
/// Created by Client::CreateQueueDownstreamReceiver. Maintains a persistent
/// gRPC stream for receiving queue messages via poll-based consumption.
/// Each Poll() call returns a PollResponse that must be acknowledged within
/// the transaction timeout.
///
/// @note Thread-safe. Poll() calls are serialized internally.
/// @see Client
/// @see PollRequest
/// @see PollResponse
/// @example queues_stream/stream_receive/main.cc
/// @example queues_stream/poll_mode/main.cc
class KUBEMQ_API QueueDownstreamReceiver {
public:
    /// @brief Polls for queue messages from the broker.
    /// @param request Poll parameters specifying channel, batch size, and timeout.
    /// @return A PollResponse on success, or an error status.
    /// @see PollRequest
    [[nodiscard]] StatusOr<PollResponse> Poll(const PollRequest& request);

    /// @brief Closes the receiver and tears down the underlying stream.
    void Close();

    /// @brief Sets the error callback.
    /// @param callback Invoked when a stream-level error occurs.
    void SetOnError(std::function<void(const Status&)> callback);

    /// @brief Destructor. Closes the receiver if still open.
    ~QueueDownstreamReceiver();

    QueueDownstreamReceiver(const QueueDownstreamReceiver&) = delete;
    QueueDownstreamReceiver& operator=(const QueueDownstreamReceiver&) = delete;
    QueueDownstreamReceiver(QueueDownstreamReceiver&&) = delete;
    QueueDownstreamReceiver& operator=(QueueDownstreamReceiver&&) = delete;

private:
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    QueueDownstreamReceiver();
};

}  // namespace v1
}  // namespace kubemq
