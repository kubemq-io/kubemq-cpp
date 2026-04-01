// Example: events/wildcard_subscription
//
// Demonstrates subscribing to events using a wildcard pattern.
// Wildcard "cpp-events.wildcard.*" matches channels like
// "cpp-events.wildcard.a" and "cpp-events.wildcard.b".
//
// Channel: cpp-events.wildcard_subscription
// Client ID: cpp-events-wildcard-subscription-client
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
    options.set_client_id("cpp-events-wildcard-subscription-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Subscribe with a wildcard pattern to match multiple channels.
    std::cout << "[2] Subscribing to cpp-events.wildcard.*" << std::endl;
    auto sub_result = client->SubscribeToEvents(
        "cpp-events.wildcard.*", "",
        [](const kubemq::EventReceive& event) {
            std::cout << "[4] Wildcard received: channel=" << event.channel
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

    // Publish to two channels that match the wildcard pattern.
    for (const auto& ch : {"cpp-events.wildcard.a", "cpp-events.wildcard.b"}) {
        auto ev_result = kubemq::Event::Builder()
                             .SetChannel(ch)
                             .SetBody("wildcard-msg")
                             .SetMetadata("wildcard-demo")
                             .Build();
        if (!ev_result.ok()) {
            std::cerr << "[ERROR] Build event: " << ev_result.status().message() << std::endl;
            return 1;
        }
        auto send_status = client->SendEvent(*ev_result);
        if (!send_status.ok()) {
            std::cerr << "[ERROR] SendEvent to " << ch << ": " << send_status.message()
                      << std::endl;
            return 1;
        }
        std::cout << "[3] Published to " << ch << std::endl;
    }

    // Wait for at least one event.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Wildcard subscription demo complete" << std::endl;

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
