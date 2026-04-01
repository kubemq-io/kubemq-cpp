// Example: events_store/start_from_first
//
// Demonstrates subscribing to event store with StartFromFirstEvent.
// All stored events from the beginning are replayed, then new events
// continue to be delivered.
//
// Channel: cpp-events_store.start_from_first
// Client ID: cpp-events-store-start-from-first-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-events-store-start-from-first-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events_store.start_from_first";

    // First, send some events so there is data to replay.
    for (int i = 1; i <= 3; i++) {
        auto ev_result = kubemq::EventStore::Builder()
                             .SetChannel(channel)
                             .SetBody("stored-msg-" + std::to_string(i))
                             .SetMetadata("replay-test")
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
    }
    std::cout << "[2] Sent 3 events to store" << std::endl;

    std::atomic<int> received_count{0};

    // Subscribe with StartFromFirstEvent -- replays all stored events.
    std::cout << "[3] Subscribing with StartFromFirstEvent" << std::endl;
    auto sub_result = client->SubscribeToEventsStore(
        channel, "", kubemq::SubscriptionOption::StartFromFirstEvent(),
        [&received_count](const kubemq::EventStoreReceive& e) {
            std::cout << "[4] [StartFromFirst] seq=" << e.sequence << " body=" << e.body
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

    // Wait for events to arrive.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::cout << "[5] Received " << received_count.load() << " events from replay" << std::endl;

    // Cancel the subscription explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    sub->Cancel();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
