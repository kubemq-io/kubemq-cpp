// Example: management/create_channel
//
// Demonstrates creating channels of different types (events, queues, etc.)
// using both generic and typed convenience methods.
//
// Channel: cpp-management.create_channel
// Client ID: cpp-management-create-channel-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-management-create-channel-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Create an events channel using generic method.
    auto status =
        client->CreateChannel("cpp-management.create_channel.events", kubemq::kChannelTypeEvents);
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateChannel events: " << status.message() << std::endl;
    } else {
        std::cout << "[2] Created events channel: cpp-management.create_channel.events"
                  << std::endl;
    }

    // Create a queues channel using generic method.
    status =
        client->CreateChannel("cpp-management.create_channel.queues", kubemq::kChannelTypeQueues);
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateChannel queues: " << status.message() << std::endl;
    } else {
        std::cout << "[3] Created queue channel: cpp-management.create_channel.queues" << std::endl;
    }

    // Create an events store channel using generic method.
    status =
        client->CreateChannel("cpp-management.create_channel.es", kubemq::kChannelTypeEventsStore);
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateChannel events_store: " << status.message() << std::endl;
    } else {
        std::cout << "[4] Created events store channel: cpp-management.create_channel.es"
                  << std::endl;
    }

    // Typed convenience methods.
    status = client->CreateEventsChannel("cpp-management.typed.events");
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateEventsChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[5] Created events channel (typed): cpp-management.typed.events" << std::endl;
    }

    status = client->CreateQueuesChannel("cpp-management.typed.queues");
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateQueuesChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[6] Created queues channel (typed): cpp-management.typed.queues" << std::endl;
    }

    status = client->CreateCommandsChannel("cpp-management.typed.commands");
    if (!status.ok()) {
        std::cerr << "[ERROR] CreateCommandsChannel: " << status.message() << std::endl;
    } else {
        std::cout << "[7] Created commands channel (typed): cpp-management.typed.commands"
                  << std::endl;
    }

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
