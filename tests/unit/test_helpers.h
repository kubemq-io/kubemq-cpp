#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>

#include "kubemq/connection_state.h"
#include "kubemq/error_code.h"
#include "kubemq/server_info.h"
#include "kubemq/status.h"

namespace kubemq {
namespace testing {

// MockTransport intercepts all transport calls, allowing unit tests to
// verify interactions without a live gRPC connection.
class MockTransport {
public:
    // Lifecycle
    MOCK_METHOD(Status, Connect, (const std::string& address), ());
    MOCK_METHOD(Status, Close, (), ());
    MOCK_METHOD(ConnectionState, State, (), (const));

    // Ping
    MOCK_METHOD(StatusOr<ServerInfo>, Ping, (), ());

    // Events
    MOCK_METHOD(Status, SendEvent, (const std::string& channel,
                                    const std::string& body,
                                    const std::string& metadata), ());

    // Queues
    MOCK_METHOD(Status, SendQueueMessage, (const std::string& channel,
                                           const std::string& body), ());

    // Commands
    MOCK_METHOD(Status, SendCommand, (const std::string& channel,
                                      const std::string& body,
                                      int timeout_ms), ());

    // Queries
    MOCK_METHOD(Status, SendQuery, (const std::string& channel,
                                    const std::string& body,
                                    int timeout_ms), ());

    // Channels
    MOCK_METHOD(Status, CreateChannel, (const std::string& name,
                                        const std::string& type), ());
    MOCK_METHOD(Status, DeleteChannel, (const std::string& name,
                                        const std::string& type), ());
};

// Helper to create an OK Status
inline Status MakeOkStatus() {
    return Status();
}

// Helper to create an error Status
inline Status MakeErrorStatus(ErrorCode code, const std::string& message) {
    return Status(code, message);
}

// Helper to create a test ServerInfo
inline ServerInfo MakeTestServerInfo() {
    ServerInfo info;
    info.host = "localhost";
    info.version = "1.0.0";
    info.server_start_time = 1000000;
    info.server_up_time_seconds = 3600;
    return info;
}

// Unique channel name generator for test isolation
inline std::string UniqueChannel(const std::string& prefix) {
    static int counter = 0;
    return prefix + ".test." + std::to_string(++counter);
}

}  // namespace testing
}  // namespace kubemq
