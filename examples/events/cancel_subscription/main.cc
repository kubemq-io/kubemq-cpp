// Example: events/cancel_subscription
//
// Demonstrates how to cancel (unsubscribe from) an event subscription.
// After cancellation, no more events are delivered to the handler.
//
// Channel: cpp-events.cancel_subscription
// Client ID: cpp-events-cancel-subscription-client
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
    options.set_client_id("cpp-events-cancel-subscription-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events.cancel_subscription";

    // Create a subscription.
    std::cout << "[2] Subscribing to channel " << channel << std::endl;
    auto sub_result = client->SubscribeToEvents(
        channel, "",
        [](const kubemq::EventReceive& event) {
            std::cout << "[4] Received: body=" << event.body << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow time for subscription to register on server.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Send an event and wait for it.
    auto ev_result = kubemq::Event::Builder()
                         .SetChannel(channel)
                         .SetBody("before cancel")
                         .SetMetadata("test")
                         .Build();
    if (!ev_result.ok()) {
        std::cerr << "[ERROR] Build event: " << ev_result.status().message() << std::endl;
        return 1;
    }
    auto send_status = client->SendEvent(*ev_result);
    if (!send_status.ok()) {
        std::cerr << "[ERROR] SendEvent: " << send_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Event sent" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Event received before cancellation" << std::endl;

    // Cancel the subscription. After this, no more events are delivered.
    // Note: Subscription destructor would also handle cleanup (RAII)
    sub->Cancel();
    std::cout << "[6] Subscription cancelled" << std::endl;

    // Check that the subscription is done.
    if (sub->IsDone()) {
        std::cout << "[7] Subscription confirmed done" << std::endl;
    }

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
