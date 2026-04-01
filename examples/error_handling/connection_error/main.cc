// Example: error_handling/connection_error
//
// Demonstrates handling connection errors when the KubeMQ server is
// unreachable. Shows how to use set_check_connection(true) to fail fast
// on startup, and how to handle connection failures gracefully.
//
// Channel: cpp-error_handling.connection_error
// Client ID: cpp-error-handling-connection-error-client
//
// This example intentionally connects to a non-existent server.

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>

int main() {
    std::cout << "[1] Attempting connection to non-existent server localhost:59999" << std::endl;

    // Configure client to connect to a non-existent server with
    // check_connection enabled. This causes Create to fail fast
    // if the server is unreachable.
    kubemq::ClientOptions options;
    options.set_address("localhost", 59999);
    options.set_client_id("cpp-error-handling-connection-error-client");
    options.set_check_connection(true);
    options.set_connection_timeout(std::chrono::seconds(3));

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cout << "[2] Connection failed (expected): " << client_result.status().message()
                  << std::endl;
        std::cout << "[3] Tip: Use set_check_connection(true) to detect "
                  << "unreachable servers at startup" << std::endl;
        return 0;
    }
    auto& client = *client_result;

    // If we get here unexpectedly, verify with a ping.
    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        auto close_status = client->Close();
        if (!close_status.ok()) {
            std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        }
        return 1;
    }
    std::cout << "[2] Connected (unexpected): host=" << ping_result->host << std::endl;

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }

    return 0;
}
