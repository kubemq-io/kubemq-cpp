/// @file server_info.h
/// @brief Broker server information returned by ping operations.

// include/kubemq/server_info.h
#pragma once

#include <cstdint>
#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Information about the connected KubeMQ broker server.
///
/// Returned by Client::Ping(). Contains the broker hostname, version,
/// and uptime information.
///
/// @see Client
/// @example connection/ping/main.cc
struct KUBEMQ_API ServerInfo {
    /// @brief Hostname or address of the broker server.
    std::string host;

    /// @brief KubeMQ server version string.
    std::string version;

    /// @brief Unix timestamp (seconds) when the server was started.
    int64_t server_start_time = 0;

    /// @brief Number of seconds the server has been running.
    int64_t server_up_time_seconds = 0;
};

}  // namespace v1
}  // namespace kubemq
