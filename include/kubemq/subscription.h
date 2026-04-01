/// @file subscription.h
/// @brief Active subscription handle for PubSub and CQS message streams.

// include/kubemq/subscription.h
#pragma once

#include <memory>
#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Handle to an active subscription that receives messages from a channel.
///
/// Returned by Client::SubscribeToEvents, Client::SubscribeToEventsStore,
/// Client::SubscribeToCommands, and Client::SubscribeToQueries. The handle
/// manages the lifetime of the underlying gRPC stream. Destroying the object
/// automatically cancels the subscription.
///
/// @note Non-copyable and non-movable. All public methods are thread-safe.
/// @see Client
/// @example events/basic_pubsub/main.cc
/// @example events/cancel_subscription/main.cc
/// @example events_store/persistent_pubsub/main.cc
class KUBEMQ_API Subscription {
public:
    /// @brief Cancels the subscription and tears down the underlying stream.
    ///
    /// Safe to call multiple times; subsequent calls are no-ops. After
    /// cancellation, IsDone() returns true.
    void Cancel();

    /// @brief Blocks the calling thread until the subscription completes or is cancelled.
    void Wait();

    /// @brief Checks whether the subscription has finished.
    /// @return true if the subscription was cancelled or the server closed the stream.
    bool IsDone() const;

    /// @brief Returns the unique identifier for this subscription.
    /// @return The subscription ID string assigned at creation time.
    std::string Id() const;

    /// @brief Destructor. Calls Cancel() to ensure the subscription is torn down.
    ~Subscription();  // calls Cancel()

    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    Subscription(Subscription&&) = delete;
    Subscription& operator=(Subscription&&) = delete;

private:
    friend class Client;
    class Impl;
    std::unique_ptr<Impl> impl_;
    Subscription();
};

}  // namespace v1
}  // namespace kubemq
