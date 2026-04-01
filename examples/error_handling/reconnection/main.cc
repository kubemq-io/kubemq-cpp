// Example: error_handling/reconnection
//
// Demonstrates configuring automatic reconnection with state callbacks.
// The client registers callbacks for connection state transitions
// (connected, disconnected, reconnecting, reconnected, closed).
//
// Channel: cpp-error_handling.reconnection
// Client ID: cpp-error-handling-reconnection-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>

int main() {
    std::cout << "[1] Configuring reconnection policy and state callbacks" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-error-handling-reconnection-client");

    // Custom reconnect policy with exponential backoff.
    kubemq::ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.backoff_multiplier = 2.0;
    policy.max_attempts = 0;  // 0 = unlimited
    policy.jitter = kubemq::JitterMode::kFull;
    options.set_reconnect_policy(policy);

    // State callbacks to monitor connection lifecycle.
    options.set_on_connected(
        []() { std::cout << "[State] Connected to KubeMQ server" << std::endl; });
    options.set_on_disconnected(
        []() { std::cout << "[State] Disconnected from KubeMQ server" << std::endl; });
    options.set_on_reconnecting(
        []() { std::cout << "[State] Reconnecting to KubeMQ server..." << std::endl; });
    options.set_on_reconnected(
        []() { std::cout << "[State] Reconnected to KubeMQ server" << std::endl; });
    options.set_on_closed([]() { std::cout << "[State] Connection closed" << std::endl; });

    std::cout << "[2] Connecting to localhost:50000" << std::endl;
    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Check connection state.
    auto state = client->State();
    std::cout << "[3] Current state: " << kubemq::ConnectionStateToString(state) << std::endl;

    // Verify connectivity.
    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        auto close_status = client->Close();
        if (!close_status.ok()) {
            std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        }
        return 1;
    }
    std::cout << "[4] Connected: host=" << ping_result->host << " version=" << ping_result->version
              << std::endl;
    std::cout << "[5] Client is configured with automatic reconnection" << std::endl;

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
