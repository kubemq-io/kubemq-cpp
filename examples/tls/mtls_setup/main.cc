// Example: tls/mtls_setup
//
// Demonstrates mutual TLS (mTLS) where both the server and client present
// certificates. Requires a CA certificate, client certificate, and client
// private key.
//
// Channel: cpp-tls.mtls_setup
// Client ID: cpp-tls-mtls-setup-client
//
// Run with a KubeMQ server configured for mutual TLS.
// Update the certificate file paths before running.

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting with mTLS to localhost:50000" << std::endl;

    // Create mutual TLS config with client cert, key, and CA cert
    auto tls = kubemq::TlsConfig::FromMTLS(
        "/path/to/client.pem",     // client certificate
        "/path/to/client-key.pem", // client private key
        "/path/to/ca.pem"          // CA certificate
    );

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-tls-mtls-setup-client");
    options.set_tls_config(tls);

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        std::cout << "[INFO] Ensure the KubeMQ server has mTLS enabled and all cert paths are correct."
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    auto ping_result = client->Ping();
    if (!ping_result.ok()) {
        std::cerr << "[ERROR] Ping failed: " << ping_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[2] Connected with mTLS. Server version: " << ping_result->version << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed" << std::endl;

    return 0;
}
