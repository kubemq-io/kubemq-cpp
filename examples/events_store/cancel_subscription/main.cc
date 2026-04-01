// Example: events_store/cancel_subscription
//
// Demonstrates cancelling an event store subscription.
// After Cancel is called, no more events are delivered.
//
// Channel: cpp-events_store.cancel_subscription
// Client ID: cpp-events-store-cancel-subscription-client
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
    options.set_client_id("cpp-events-store-cancel-subscription-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events_store.cancel_subscription";
    std::atomic<int> received_count{0};

    // Create an event store subscription.
    std::cout << "[2] Subscribing with StartFromNewEvents" << std::endl;
    auto sub_result = client->SubscribeToEventsStore(
        channel, "", kubemq::SubscriptionOption::StartFromNewEvents(),
        [&received_count](const kubemq::EventStoreReceive& e) {
            std::cout << "[4] Received: seq=" << e.sequence << " body=" << e.body << std::endl;
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

    // Allow time for subscription to register on server.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Send an event and wait for it.
    std::cout << "[3] Sending event before cancel" << std::endl;
    auto ev_result =
        kubemq::EventStore::Builder().SetChannel(channel).SetBody("before cancel").Build();
    if (!ev_result.ok()) {
        std::cerr << "[ERROR] Build: " << ev_result.status().message() << std::endl;
        return 1;
    }
    auto send_result = client->SendEventStore(*ev_result);
    if (!send_result.ok()) {
        std::cerr << "[ERROR] SendEventStore: " << send_result.status().message() << std::endl;
        return 1;
    }

    // Wait for the event to be received.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Event received before cancellation, count=" << received_count.load()
              << std::endl;

    // Cancel the subscription.
    sub->Cancel();
    std::cout << "[6] Subscription cancelled" << std::endl;

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
