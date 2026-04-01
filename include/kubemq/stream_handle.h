/// @file stream_handle.h
/// @brief Streaming handles for publishing events and event-store messages.

// include/kubemq/stream_handle.h
#pragma once

#include <functional>
#include <memory>

#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Handle for a streaming event publisher channel.
///
/// Created by Client::CreateEventStream. Maintains a persistent gRPC
/// bidirectional stream to the broker for high-throughput event publishing.
/// The stream reconnects automatically according to the configured
/// ReconnectPolicy.
///
/// @note Thread-safe. Multiple threads may call Send() concurrently.
/// @see Client
/// @see Event
/// @see ReconnectPolicy
/// @example events/stream_send/main.cc
class KUBEMQ_API EventStreamHandle {
public:
    /// @brief Sends an event through the open stream.
    /// @param event The event to publish.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status Send(const Event& event);

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

    /// @brief Sets the error callback.
    /// @param callback Invoked when a stream-level error occurs.
    /// @deprecated Provide callbacks at construction time via the Client factory.
    ///   Retained for backward compatibility; mutex-protected internally.
    void SetOnError(std::function<void(const Status&)> callback);

    /// @brief Destructor. Closes the stream if still open.
    ~EventStreamHandle();

    EventStreamHandle(const EventStreamHandle&) = delete;
    EventStreamHandle& operator=(const EventStreamHandle&) = delete;
    EventStreamHandle(EventStreamHandle&&) = delete;
    EventStreamHandle& operator=(EventStreamHandle&&) = delete;

private:
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    EventStreamHandle();
};

/// @brief Handle for a streaming event-store publisher channel.
///
/// Created by Client::CreateEventStoreStream. Maintains a persistent gRPC
/// bidirectional stream to the broker for high-throughput event-store
/// publishing with persistence guarantees. Results are delivered
/// asynchronously via the result callback.
///
/// @note Thread-safe. Multiple threads may call Send() concurrently.
/// @see Client
/// @see EventStore
/// @see EventStoreResult
/// @see ReconnectPolicy
/// @example events_store/stream_send/main.cc
class KUBEMQ_API EventStoreStreamHandle {
public:
    /// @brief Sends an event-store message through the open stream.
    /// @param event The event-store message to publish.
    /// @return Status::OK on success, or an error status.
    [[nodiscard]] Status Send(const EventStore& event);

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

    /// @brief Sets the result callback for acknowledgements.
    /// @param callback Invoked for each EventStoreResult received from the broker.
    /// @deprecated Provide callbacks at construction time via the Client factory.
    ///   Retained for backward compatibility; mutex-protected internally.
    void SetOnResult(std::function<void(const EventStoreResult&)> callback);

    /// @brief Sets the error callback.
    /// @param callback Invoked when a stream-level error occurs.
    /// @deprecated Provide callbacks at construction time via the Client factory.
    ///   Retained for backward compatibility; mutex-protected internally.
    void SetOnError(std::function<void(const Status&)> callback);

    /// @brief Destructor. Closes the stream if still open.
    ~EventStoreStreamHandle();

    EventStoreStreamHandle(const EventStoreStreamHandle&) = delete;
    EventStoreStreamHandle& operator=(const EventStoreStreamHandle&) = delete;
    EventStoreStreamHandle(EventStoreStreamHandle&&) = delete;
    EventStoreStreamHandle& operator=(EventStoreStreamHandle&&) = delete;

private:
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    EventStoreStreamHandle();
};

}  // namespace v1
}  // namespace kubemq
