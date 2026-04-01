// Example: commands/command_timeout
//
// Demonstrates command timeout handling. When no handler responds
// within the timeout period, the SendCommand call returns an error.
//
// Channel: cpp-commands.command_timeout
// Client ID: cpp-commands-command-timeout-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-commands-command-timeout-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Send a command to a channel with no handler -- it will timeout.
    std::cout << "[2] Sending command to channel with no handler (2s timeout)" << std::endl;
    auto cmd_result = kubemq::Command::Builder()
                          .SetChannel("cpp-commands.command_timeout")
                          .SetBody("will timeout")
                          .SetTimeout(std::chrono::seconds(2))
                          .Build();
    if (!cmd_result.ok()) {
        std::cerr << "[ERROR] Build command: " << cmd_result.status().message() << std::endl;
        return 1;
    }

    auto resp_result = client->SendCommand(*cmd_result);
    if (!resp_result.ok()) {
        std::cout << "[3] Command timed out as expected: " << resp_result.status().message()
                  << std::endl;
    } else {
        std::cout << "[3] Unexpected: command succeeded without a handler" << std::endl;
    }

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[4] Client closed" << std::endl;

    return 0;
}
