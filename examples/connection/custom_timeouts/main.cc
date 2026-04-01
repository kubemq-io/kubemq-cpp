// Example: connection/custom_timeouts
//
// Demonstrates how to configure custom timeouts for the KubeMQ client
// including connection timeout, keepalive, drain timeout, and message sizes.
//
// Channel: cpp-connection.custom_timeouts
// Client ID: cpp-connection-custom-timeouts-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>

int main() {
    std::cout << "[1] Configuring custom timeouts" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-connection-custom-timeouts-client");
    // Custom connection timeout (default: 10s)
    options.set_connection_timeout(std::chrono::seconds(15));
    // Custom keepalive settings (default: 10s interval, 20s timeout)
    options.set_keepalive_time(std::chrono::seconds(30));
    options.set_keepalive_timeout(std::chrono::seconds(10));
    // Custom drain timeout for Close() (default: 5s)
    options.set_drain_timeout(std::chrono::seconds(10));
    // Max message sizes (50 MB)
    options.set_max_receive_message_size(50 * 1024 * 1024);
    options.set_max_send_message_size(50 * 1024 * 1024);

    std::cout << "[2] Connecting to localhost:50000" << std::endl;

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[3] Connected with custom timeouts: host=" << ping_result->host
              << " version=" << ping_result->version << std::endl;

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[4] Client closed" << std::endl;

    return 0;
}
