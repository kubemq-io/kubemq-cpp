// Example: connection/connect
//
// Demonstrates how to create a basic KubeMQ client connection.
// The client connects to a KubeMQ server on localhost:50000 and verifies
// connectivity with a Ping.
//
// Channel: cpp-connection.connect
// Client ID: cpp-connection-connect-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    // Create a KubeMQ client with basic configuration.
    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-connection-connect-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Verify the connection by pinging the server.
    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[2] Connected successfully: host=" << ping_result->host
              << " version=" << ping_result->version
              << " uptime=" << ping_result->server_up_time_seconds << "s" << std::endl;

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed" << std::endl;

    return 0;
}
