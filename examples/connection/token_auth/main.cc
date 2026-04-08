// Example: connection/token_auth
//
// Demonstrates connecting to a KubeMQ server with JWT/token authentication.
// The auth token is set on the client options and included in every gRPC request.
//
// Channel: cpp-connection.token_auth
// Client ID: cpp-connection-token-auth-client
//
// Run with a KubeMQ server configured for token authentication.

#include <kubemq/kubemq.h>

#include <cstdlib>
#include <iostream>

int main() {
    // Read auth token from environment or use a placeholder
    const char* env_token = std::getenv("KUBEMQ_AUTH_TOKEN");
    std::string token = env_token ? env_token : "your-auth-token-here";

    std::cout << "[1] Connecting with auth token to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-connection-token-auth-client");
    options.set_auth_token(token);

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
    std::cout << "[2] Connected with auth token. Server version: " << ping_result->version
              << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed" << std::endl;

    return 0;
}
