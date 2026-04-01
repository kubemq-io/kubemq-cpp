// Example: events/consumer_group
//
// Demonstrates load-balanced event consumption using consumer groups.
// When multiple subscribers share the same group on the same channel,
// each event is delivered to exactly one subscriber in the group.
//
// Channel: cpp-events.consumer_group
// Client ID: cpp-events-consumer-group-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-events-consumer-group-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events.consumer_group";
    std::string group = "cpp-events-worker-group";

    // Subscribe with a consumer group for load-balanced delivery.
    std::cout << "[2] Subscribing with group=" << group << std::endl;
    auto sub_result = client->SubscribeToEvents(
        channel, group,
        [](const kubemq::EventReceive& event) {
            std::cout << "[4] Consumer group received: channel=" << event.channel
                      << " body=" << event.body << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Publish an event to the group channel.
    auto event_result = kubemq::Event::Builder()
                            .SetChannel(channel)
                            .SetBody("hello consumer group")
                            .SetMetadata("group-demo")
                            .Build();
    if (!event_result.ok()) {
        std::cerr << "[ERROR] Build event: " << event_result.status().message() << std::endl;
        return 1;
    }
    auto send_status = client->SendEvent(*event_result);
    if (!send_status.ok()) {
        std::cerr << "[ERROR] SendEvent: " << send_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Event published to consumer group channel" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Consumer group demo complete" << std::endl;

    // Cancel the subscription explicitly.
    // Note: Subscription destructor would also handle cleanup (RAII)
    sub->Cancel();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
