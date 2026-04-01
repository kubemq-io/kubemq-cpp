/// @file subscription_option.h
/// @brief Starting point options for Events Store subscriptions.
#pragma once

#include <chrono>
#include <cstdint>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Specifies the starting point for Events Store subscriptions.
///
/// Use one of the static factory methods to create an option, then pass
/// to Client::SubscribeToEventsStore().
///
/// @see Client::SubscribeToEventsStore
/// @example events_store/replay_from_sequence/main.cc
class KUBEMQ_API SubscriptionOption {
public:
    /// @brief Start receiving only new events (default).
    /// @return A SubscriptionOption for new events only.
    static SubscriptionOption StartFromNewEvents();
    /// @brief Replay all events from the beginning.
    /// @return A SubscriptionOption to start from the first event.
    static SubscriptionOption StartFromFirstEvent();
    /// @brief Start from the most recent event.
    /// @return A SubscriptionOption to start from the last event.
    static SubscriptionOption StartFromLastEvent();
    /// @brief Start from a specific sequence number.
    /// @param sequence Sequence number to start from.
    /// @return A SubscriptionOption for the given sequence.
    static SubscriptionOption StartFromSequence(uint64_t sequence);
    /// @brief Start from a specific point in time.
    /// @param time Time point to start from.
    /// @return A SubscriptionOption for the given time.
    static SubscriptionOption StartFromTime(std::chrono::system_clock::time_point time);
    /// @brief Start from a relative time offset (seconds ago).
    /// @param delta Duration in seconds before now.
    /// @return A SubscriptionOption for the given time delta.
    static SubscriptionOption StartFromTimeDelta(
        std::chrono::seconds delta);  // Proto uses seconds (int64)

    /// @brief Internal accessor for proto conversion.
    /// @return The subscription start type.
    int type() const { return type_; }
    /// @brief Internal accessor for proto conversion.
    /// @return The subscription start value.
    int64_t value() const { return value_; }

private:
    int type_ = 0;
    int64_t value_ = 0;
};

}  // namespace v1
}  // namespace kubemq
