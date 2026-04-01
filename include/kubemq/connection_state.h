/// @file connection_state.h
/// @brief Connection lifecycle states for the KubeMQ client.
#pragma once

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Represents the lifecycle state of a Client connection.
/// @see Client::State
enum class ConnectionState {
    /// @brief Created but not connected.
    kIdle,
    /// @brief Initial connection in progress.
    kConnecting,
    /// @brief Connected and operational.
    kReady,
    /// @brief Disconnected, attempting reconnection.
    kReconnecting,
    /// @brief Permanently closed.
    kClosed,
};

/// @brief Convert a ConnectionState to a human-readable string.
/// @param state The connection state to convert.
/// @return A null-terminated C string name for the state.
KUBEMQ_API const char* ConnectionStateToString(ConnectionState state);

}  // namespace v1
}  // namespace kubemq
