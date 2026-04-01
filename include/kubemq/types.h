/// @file types.h
/// @brief SDK version, channel type constants, and default configuration values.
#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief SDK version string.
constexpr const char* kVersion = "1.0.0";

/// @brief Channel type constant for fire-and-forget events.
constexpr const char* kChannelTypeEvents = "events";
/// @brief Channel type constant for persistent events with replay.
constexpr const char* kChannelTypeEventsStore = "events_store";
/// @brief Channel type constant for synchronous command RPC.
constexpr const char* kChannelTypeCommands = "commands";
/// @brief Channel type constant for synchronous query RPC.
constexpr const char* kChannelTypeQueries = "queries";
/// @brief Channel type constant for guaranteed-delivery queues.
constexpr const char* kChannelTypeQueues = "queues";

/// @brief Default connection timeout (10 seconds).
constexpr auto kDefaultConnectionTimeout = std::chrono::seconds(10);
/// @brief Default send operation timeout (5 seconds).
constexpr auto kDefaultSendTimeout = std::chrono::seconds(5);
/// @brief Default subscribe operation timeout (10 seconds).
constexpr auto kDefaultSubscribeTimeout = std::chrono::seconds(10);
/// @brief Default RPC (command/query) timeout (10 seconds).
constexpr auto kDefaultRPCTimeout = std::chrono::seconds(10);
/// @brief Default queue receive timeout (10 seconds).
constexpr auto kDefaultQueueRecvTimeout = std::chrono::seconds(10);
/// @brief Default queue poll timeout (30 seconds).
constexpr auto kDefaultQueuePollTimeout = std::chrono::seconds(30);
/// @brief Default drain timeout on close (5 seconds).
constexpr auto kDefaultDrainTimeout = std::chrono::seconds(5);
/// @brief Default gRPC keepalive ping interval (10 seconds).
constexpr auto kDefaultKeepaliveTime = std::chrono::seconds(10);
/// @brief Default gRPC keepalive timeout (20 seconds).
constexpr auto kDefaultKeepaliveTimeout = std::chrono::seconds(20);
/// @brief Default maximum callback invocation duration (30 seconds).
constexpr auto kDefaultCallbackTimeout = std::chrono::seconds(30);

/// @brief Default per-subscription delivery buffer size (10 messages).
constexpr int kDefaultSubscriptionBufferSize =
    10;  // Per-subscription delivery buffer (matches Go SDK)
/// @brief Default max inbound message size (4 MB).
constexpr int kDefaultMaxReceiveMessageSize = 4 * 1024 * 1024;  // 4 MB
/// @brief Default max outbound message size (100 MB).
constexpr int kDefaultMaxSendMessageSize = 100 * 1024 * 1024;  // 100 MB
/// @brief Default max concurrent subscription callback invocations (1).
constexpr int kDefaultMaxConcurrentCallbacks = 1;

}  // namespace v1
}  // namespace kubemq
