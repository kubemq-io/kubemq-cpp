// Example: patterns/fan_out
//
// Demonstrates the fan-out pattern using events.
// A single publisher sends events that are delivered to all subscribers
// on the channel. Each subscriber receives every event independently
// because no consumer group is used.
//
// Channel: cpp-patterns.fan_out
// Client ID: cpp-patterns-fan-out-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-patterns-fan-out-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-patterns.fan_out";
    std::atomic<int> deliveries{0};

    // Create 3 independent subscribers (no consumer group = fan-out).
    std::cout << "[2] Creating 3 fan-out subscribers" << std::endl;
    std::vector<std::unique_ptr<kubemq::Subscription>> subs;
    for (int i = 1; i <= 3; i++) {
        int subscriber_id = i;
        auto sub_result = client->SubscribeToEvents(
            channel, "",
            [subscriber_id, &deliveries](const kubemq::EventReceive& event) {
                std::cout << "[4] Subscriber " << subscriber_id << " received: body=" << event.body
                          << std::endl;
                deliveries.fetch_add(1);
            },
            [subscriber_id](const kubemq::Status& err) {
                std::cerr << "[ERROR] Subscriber " << subscriber_id << " error: " << err.message()
                          << std::endl;
            });
        if (!sub_result.ok()) {
            std::cerr << "[ERROR] SubscribeToEvents (subscriber " << i
                      << "): " << sub_result.status().message() << std::endl;
            return 1;
        }
        subs.push_back(std::move(*sub_result));
    }

    // Allow subscriptions to fully establish.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Publish a single event -- all 3 subscribers should receive it.
    std::cout << "[3] Publishing event to all subscribers" << std::endl;
    auto event_result = kubemq::Event::Builder()
                            .SetChannel(channel)
                            .SetBody("broadcast message")
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

    // Wait for all deliveries.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Total deliveries: " << deliveries.load() << " (expected 3 for fan-out)"
              << std::endl;

    // Cancel all subscriptions explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    for (auto& sub : subs) {
        sub->Cancel();
    }
    std::cout << "[6] All subscriptions cancelled" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
