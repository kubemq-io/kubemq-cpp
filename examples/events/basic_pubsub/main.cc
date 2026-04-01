// Example: events/basic_pubsub
//
// Demonstrates basic fire-and-forget event publish/subscribe.
// A subscriber listens on a channel, then a publisher sends an event.
//
// Channel: cpp-events.basic_pubsub
// Client ID: cpp-events-basic-pubsub-client
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
    options.set_client_id("cpp-events-basic-pubsub-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events.basic_pubsub";

    // Subscribe to events on the channel.
    std::cout << "[2] Subscribing to channel " << channel << std::endl;
    auto sub_result = client->SubscribeToEvents(
        channel, "",
        [](const kubemq::EventReceive& event) {
            std::cout << "[4] Received: channel=" << event.channel << " body=" << event.body
                      << " metadata=" << event.metadata << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to fully establish before publishing.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Publish an event to the channel.
    auto event_result = kubemq::Event::Builder()
                            .SetChannel(channel)
                            .SetBody("hello from C++ SDK")
                            .SetMetadata("greeting")
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
    std::cout << "[3] Event published" << std::endl;

    // Wait for the event to be received.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Event received successfully" << std::endl;

    // Cancel the subscription explicitly.
    // Note: Subscription destructor would also handle cleanup (RAII)
    sub->Cancel();
    std::cout << "[6] Subscription cancelled" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
