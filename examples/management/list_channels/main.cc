// Example: management/list_channels
//
// Demonstrates listing channels with optional search filter, using both
// generic and typed convenience methods.
//
// Channel: cpp-management.list_channels
// Client ID: cpp-management-list-channels-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-management-list-channels-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // List all events channels.
    auto channels_result = client->ListChannels(kubemq::kChannelTypeEvents, "");
    if (!channels_result.ok()) {
        std::cerr << "[ERROR] ListChannels: " << channels_result.status().message() << std::endl;
    } else {
        std::cout << "[2] All events channels: " << channels_result->size() << " found"
                  << std::endl;
        for (const auto& ch : *channels_result) {
            std::cout << "  - name=" << ch.name << " active=" << ch.is_active << std::endl;
        }
    }

    // List channels with a search filter.
    auto filtered_result = client->ListChannels(kubemq::kChannelTypeQueues, "cpp-");
    if (!filtered_result.ok()) {
        std::cerr << "[ERROR] ListChannels filtered: " << filtered_result.status().message()
                  << std::endl;
    } else {
        std::cout << "[3] Queue channels matching 'cpp-': " << filtered_result->size() << " found"
                  << std::endl;
        for (const auto& ch : *filtered_result) {
            std::cout << "  - name=" << ch.name << std::endl;
        }
    }

    // Typed convenience methods.
    auto events_result = client->ListEventsChannels("");
    if (!events_result.ok()) {
        std::cerr << "[ERROR] ListEventsChannels: " << events_result.status().message()
                  << std::endl;
    } else {
        std::cout << "[4] Events channels (typed): " << events_result->size() << " found"
                  << std::endl;
        for (const auto& ch : *events_result) {
            std::cout << "  - name=" << ch.name << " active=" << ch.is_active << std::endl;
        }
    }

    auto queues_result = client->ListQueuesChannels("cpp-");
    if (!queues_result.ok()) {
        std::cerr << "[ERROR] ListQueuesChannels: " << queues_result.status().message()
                  << std::endl;
    } else {
        std::cout << "[5] Queue channels (typed, matching 'cpp-'): " << queues_result->size()
                  << " found" << std::endl;
        for (const auto& ch : *queues_result) {
            std::cout << "  - name=" << ch.name << std::endl;
        }
    }

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
