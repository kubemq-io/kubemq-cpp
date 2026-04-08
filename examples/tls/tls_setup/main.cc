// Example: tls/tls_setup
//
// Demonstrates connecting to a KubeMQ server over TLS using a CA certificate.
// The client verifies the server's identity via the CA certificate without
// providing client credentials (server-side TLS only).
//
// Channel: cpp-tls.tls_setup
// Client ID: cpp-tls-tls-setup-client
//
// Run with a KubeMQ server configured for TLS.
// Update the CA certificate path before running.

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting with TLS to localhost:50000" << std::endl;

    // Create TLS config with CA certificate file
    auto tls = kubemq::TlsConfig::FromCertFile("/path/to/ca.pem");

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-tls-tls-setup-client");
    options.set_tls_config(tls);

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        std::cout << "[INFO] Ensure the KubeMQ server has TLS enabled and the CA cert path is correct."
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[2] Connected with TLS. Server version: " << ping_result->version << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed" << std::endl;

    return 0;
}
