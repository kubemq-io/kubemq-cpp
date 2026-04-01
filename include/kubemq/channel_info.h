/// @file channel_info.h
/// @brief Channel statistics and metadata returned by management queries.

// include/kubemq/channel_info.h
#pragma once

#include <cstdint>
#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Aggregated message statistics for one direction of a channel.
///
/// Represents either incoming (producer-side) or outgoing (consumer-side)
/// statistics for a channel as reported by the broker.
///
/// @see ChannelInfo
struct KUBEMQ_API ChannelStats {
    /// @brief Total number of messages processed.
    int64_t messages = 0;

    /// @brief Total byte volume of messages processed.
    int64_t volume = 0;

    /// @brief Total number of responses (applicable to CQS channels).
    int64_t responses = 0;

    /// @brief Number of messages currently waiting for delivery or acknowledgement.
    int64_t waiting = 0;

    /// @brief Number of messages that expired before delivery.
    int64_t expired = 0;

    /// @brief Number of messages with a delayed delivery schedule.
    int64_t delayed = 0;
};

/// @brief Metadata and statistics for a single KubeMQ channel.
///
/// Returned by Client::ListChannels and related management queries. Contains
/// the channel name, type, activity status, and directional statistics.
///
/// @see Client
/// @see ChannelStats
/// @example management/main.cc
struct KUBEMQ_API ChannelInfo {
    /// @brief Channel name.
    std::string name;

    /// @brief Channel type (e.g., "events", "events_store", "queues", "commands", "queries").
    std::string type;

    /// @brief Unix timestamp (seconds) of the last activity on this channel.
    int64_t last_activity = 0;

    /// @brief Whether the channel currently has active producers or consumers.
    bool is_active = false;

    /// @brief Producer-side (incoming) statistics.
    ChannelStats incoming;

    /// @brief Consumer-side (outgoing) statistics.
    ChannelStats outgoing;
};

}  // namespace v1
}  // namespace kubemq
