// Example: events_store/stream_send
//
// Demonstrates high-throughput event store publishing using SendEventStoreStream.
// Each sent event receives a confirmation result via the result callback.
//
// Channel: cpp-events_store.stream_send
// Client ID: cpp-events-store-stream-send-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-events-store-stream-send-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events_store.stream_send";

    // Open a bidirectional stream for event store publishing.
    std::cout << "[2] Opening event store stream" << std::endl;
    auto handle_result = client->SendEventStoreStream(
        [](const kubemq::EventStoreResult& r) {
            std::cout << "[4] Stream result: eventId=" << r.id << " sent=" << r.sent
                      << " err=" << r.error << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Stream error: " << err.message() << std::endl;
        });
    if (!handle_result.ok()) {
        std::cerr << "[ERROR] SendEventStoreStream: " << handle_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& handle = *handle_result;

    // Send multiple events via the stream.
    for (int i = 0; i < 5; i++) {
        auto ev_result = kubemq::EventStore::Builder()
                             .SetChannel(channel)
                             .SetBody("stream-data-" + std::to_string(i))
                             .SetMetadata("stream-meta")
                             .Build();
        if (!ev_result.ok()) {
            std::cerr << "[ERROR] Build event store: " << ev_result.status().message() << std::endl;
            return 1;
        }
        auto send_status = handle->Send(*ev_result);
        if (!send_status.ok()) {
            std::cerr << "[ERROR] handle.Send: " << send_status.message() << std::endl;
            return 1;
        }
        std::cout << "[3] Stream sent event " << (i + 1) << std::endl;
    }

    // Allow time for results to arrive.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Event store stream send demo complete" << std::endl;

    // Close the stream handle explicitly.
    // Note: The EventStoreStreamHandle destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    handle->Close();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
