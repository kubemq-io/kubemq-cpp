// Example: events_store/replay_from_time
//
// Demonstrates subscribing to event store with StartFromTime.
// Events are replayed starting from a specific point in time.
//
// Channel: cpp-events_store.replay_from_time
// Client ID: cpp-events-store-replay-from-time-client
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
    options.set_client_id("cpp-events-store-replay-from-time-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events_store.replay_from_time";
    std::atomic<int> received_count{0};

    // Subscribe starting from 1 hour ago -- replays events stored in the last hour.
    auto since = std::chrono::system_clock::now() - std::chrono::hours(1);
    std::cout << "[2] Subscribing with StartFromTime(now - 1 hour)" << std::endl;
    auto sub_result = client->SubscribeToEventsStore(
        channel, "", kubemq::SubscriptionOption::StartFromTime(since),
        [&received_count](const kubemq::EventStoreReceive& e) {
            std::cout << "[4] [StartFromTime] seq=" << e.sequence << " body=" << e.body
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

    // Send a new event that should be received.
    std::cout << "[3] Sending event within time window" << std::endl;
    auto ev_result = kubemq::EventStore::Builder()
                         .SetChannel(channel)
                         .SetBody("event within time window")
                         .Build();
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
    std::cout << "[6] Replay from time demo complete" << std::endl;

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
