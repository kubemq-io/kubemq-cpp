// Example: events_store/start_at_time_delta
//
// Demonstrates subscribing to event store with StartFromTimeDelta.
// Events are replayed from (now - delta). For example, 30 minutes ago.
//
// Channel: cpp-events_store.start_at_time_delta
// Client ID: cpp-events-store-start-at-time-delta-client
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
    options.set_client_id("cpp-events-store-start-at-time-delta-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events_store.start_at_time_delta";
    std::atomic<int> received_count{0};

    // Subscribe starting from 30 minutes ago (1800 seconds).
    std::cout << "[2] Subscribing with StartFromTimeDelta(30 minutes)" << std::endl;
    auto sub_result = client->SubscribeToEventsStore(
        channel, "", kubemq::SubscriptionOption::StartFromTimeDelta(std::chrono::seconds(1800)),
        [&received_count](const kubemq::EventStoreReceive& e) {
            std::cout << "[4] [StartFromTimeDelta] seq=" << e.sequence << " body=" << e.body
                      << std::endl;
            received_count.fetch_add(1);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEventsStore: " << sub_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to establish.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Send an event that falls within the time delta window.
    std::cout << "[3] Sending event within delta window" << std::endl;
    auto ev_result =
        kubemq::EventStore::Builder().SetChannel(channel).SetBody("recent event").Build();
    if (!ev_result.ok()) {
        std::cerr << "[ERROR] Build: " << ev_result.status().message() << std::endl;
        return 1;
    }
    auto send_result = client->SendEventStore(*ev_result);
    if (!send_result.ok()) {
        std::cerr << "[ERROR] SendEventStore: " << send_result.status().message() << std::endl;
        return 1;
    }

    // Wait for events to arrive.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Received " << received_count.load() << " event(s)" << std::endl;
    std::cout << "[6] Start at time delta demo complete" << std::endl;

    // Cancel the subscription explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    sub->Cancel();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
