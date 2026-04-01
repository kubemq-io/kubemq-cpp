// Example: connection/close
//
// Demonstrates how to gracefully close a KubeMQ client connection.
// Close drains in-flight operations before shutting down.
//
// Channel: cpp-connection.close
// Client ID: cpp-connection-close-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-connection-close-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Verify connectivity before closing.
    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[2] Connected: host=" << ping_result->host << " version=" << ping_result->version
              << std::endl;

    // Close the client gracefully, draining in-flight operations.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed successfully" << std::endl;

    return 0;
}
