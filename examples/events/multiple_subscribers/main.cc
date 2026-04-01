// Example: events/multiple_subscribers
//
// Demonstrates multiple subscribers receiving the same event.
// Without a consumer group, each subscriber gets every event (fan-out).
//
// Channel: cpp-events.multiple_subscribers
// Client ID: cpp-events-multiple-subscribers-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-events-multiple-subscribers-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events.multiple_subscribers";
    std::atomic<int> count{0};

    // Create two subscribers on the same channel without a group.
    // Both subscribers should receive every event (fan-out).
    std::cout << "[2] Creating subscriber 1" << std::endl;
    auto sub1_result = client->SubscribeToEvents(
        channel, "",
        [&count](const kubemq::EventReceive& event) {
            std::cout << "[5] Subscriber 1 received: body=" << event.body << std::endl;
            count.fetch_add(1);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Sub1 error: " << err.message() << std::endl;
        });
    if (!sub1_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents sub1: " << sub1_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& sub1 = *sub1_result;

    std::cout << "[3] Creating subscriber 2" << std::endl;
    auto sub2_result = client->SubscribeToEvents(
        channel, "",
        [&count](const kubemq::EventReceive& event) {
            std::cout << "[5] Subscriber 2 received: body=" << event.body << std::endl;
            count.fetch_add(1);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Sub2 error: " << err.message() << std::endl;
        });
    if (!sub2_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents sub2: " << sub2_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& sub2 = *sub2_result;

    // Allow subscriptions to fully establish before publishing.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Publish an event -- both subscribers should receive it.
    auto event_result = kubemq::Event::Builder()
                            .SetChannel(channel)
                            .SetBody("hello to all subscribers")
                            .SetMetadata("fan-out")
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
    std::cout << "[4] Event published" << std::endl;

    // Wait briefly for both subscribers to receive the event.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[6] Total deliveries: " << count.load() << " (expected 2 for fan-out)"
              << std::endl;

    // Cancel subscriptions explicitly.
    // Note: Subscription destructor would also handle cleanup (RAII)
    sub1->Cancel();
    sub2->Cancel();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
