// Example: management/delete_channel
//
// Demonstrates deleting a channel after creating it, including
// typed convenience methods.
//
// Channel: cpp-management.delete_channel
// Client ID: cpp-management-delete-channel-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>
#include <string>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-management-delete-channel-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel_name = "cpp-management.delete_channel.temp";

    // Create a channel to demonstrate deletion.
    auto status = client->CreateChannel(channel_name, kubemq::kChannelTypeEvents);
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[2] Created channel: " << channel_name << std::endl;
    }

    // Delete the channel.
    status = client->DeleteChannel(channel_name, kubemq::kChannelTypeEvents);
    if (!status.ok()) {
        std::cerr << "[ERROR] DeleteChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[3] Deleted channel: " << channel_name << std::endl;
    }

    // Typed convenience method.
    std::string typed_name = "cpp-management.delete_channel.typed";
    client->CreateEventsChannel(typed_name);
    status = client->DeleteEventsChannel(typed_name);
    if (!status.ok()) {
        std::cerr << "[ERROR] DeleteEventsChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[4] Deleted events channel (typed): " << typed_name << std::endl;
    }

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[5] Client closed" << std::endl;

    return 0;
}
