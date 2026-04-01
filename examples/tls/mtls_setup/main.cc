// Example: tls/mtls_setup
//
// Demonstrates mutual TLS connection with client and CA certificates.
// SKIP: Requires a KubeMQ server with mutual TLS enabled and cert/key files.
//
// Channel: cpp-tls.mtls_setup
// Client ID: cpp-tls-mtls-setup-client
//
// Run with a KubeMQ server configured for mTLS.

#include <iostream>

int main() {
    std::cout << "[SKIP] This example requires a KubeMQ server with mutual TLS enabled. "
              << "See Go equivalent for reference." << std::endl;
    return 0;
}
