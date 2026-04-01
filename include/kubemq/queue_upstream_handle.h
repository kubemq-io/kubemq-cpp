/// @file queue_upstream_handle.h
/// @brief Streaming handle for publishing queue messages in batches.

// include/kubemq/queue_upstream_handle.h
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "kubemq/export.h"
#include "kubemq/queue_message.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Result of a batch queue-upstream send operation.
///
/// Correlates with a Send() call via the ref_request_id field. Contains
/// per-message results indicating which messages were accepted by the broker.
/// @see QueueUpstreamHandle
/// @see QueueSendResult
struct KUBEMQ_API QueueUpstreamResult {
    /// @brief Request ID echoed from the corresponding Send() call.
    std::string ref_request_id;

    /// @brief Per-message send results in the same order as the sent batch.
    std::vector<QueueSendResult> results;

    /// @brief true if the entire batch failed.
    bool is_error = false;

    /// @brief Error description when is_error is true; empty otherwise.
    std::string error;
};

/// @brief Handle for a streaming queue message publisher.
///
/// Created by Client::CreateQueueUpstreamStream. Maintains a persistent gRPC
/// bidirectional stream for high-throughput queue message publishing with
/// batching support. Results are delivered asynchronously via the result
/// callback.
///
/// @note Thread-safe. Multiple threads may call Send() concurrently.
/// @see Client
/// @see QueueMessage
/// @see QueueUpstreamResult
/// @see ReconnectPolicy
/// @example queues_stream/stream_send/main.cc
class KUBEMQ_API QueueUpstreamHandle {
public:
    /// @brief Sends a batch of queue messages through the open stream.
    /// @param request_id Caller-assigned ID to correlate with QueueUpstreamResult.
    /// @param messages The queue messages to send.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status Send(const std::string& request_id,
                              const std::vector<QueueMessage>& messages);

    /// @brief Closes the stream gracefully.
    ///
    /// Flushes pending writes and tears down the gRPC stream. After Close(),
    /// IsDone() returns true and further Send() calls return an error.
    void Close();

    /// @brief Checks whether the stream has finished.
    /// @return true if the stream was closed or disconnected.
    bool IsDone() const;

    /// @brief Blocks the calling thread until the stream completes or is closed.
    void Wait();

    /// @brief Sets the result callback for send acknowledgements.
    /// @param callback Invoked for each QueueUpstreamResult received from the broker.
    /// @deprecated Provide callbacks at construction time via the Client factory.
    ///   Retained for backward compatibility; mutex-protected internally.
    void SetOnResult(std::function<void(const QueueUpstreamResult&)> callback);

    /// @brief Sets the error callback.
    /// @param callback Invoked when a stream-level error occurs.
    /// @deprecated Provide callbacks at construction time via the Client factory.
    ///   Retained for backward compatibility; mutex-protected internally.
    void SetOnError(std::function<void(const Status&)> callback);

    /// @brief Destructor. Closes the stream if still open.
    ~QueueUpstreamHandle();

    QueueUpstreamHandle(const QueueUpstreamHandle&) = delete;
    QueueUpstreamHandle& operator=(const QueueUpstreamHandle&) = delete;
    QueueUpstreamHandle(QueueUpstreamHandle&&) = delete;
    QueueUpstreamHandle& operator=(QueueUpstreamHandle&&) = delete;

private:
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    QueueUpstreamHandle();
};

}  // namespace v1
}  // namespace kubemq
