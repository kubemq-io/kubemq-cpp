// Example: events/stream_send
//
// Demonstrates high-throughput event publishing using SendEventStream.
// A bidirectional stream is opened for sending multiple events efficiently.
//
// Channel: cpp-events.stream_send
// Client ID: cpp-events-stream-send-client
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
    options.set_client_id("cpp-events-stream-send-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-events.stream_send";

    // Subscribe to verify events arrive.
    std::cout << "[2] Subscribing to channel " << channel << std::endl;
    auto sub_result = client->SubscribeToEvents(
        channel, "",
        [](const kubemq::EventReceive& event) {
            std::cout << "[5] Stream received: channel=" << event.channel << " body=" << event.body
                      << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Open a stream for high-throughput publishing.
    std::cout << "[3] Opening event stream" << std::endl;
    auto handle_result = client->SendEventStream([](const kubemq::Status& err) {
        std::cerr << "[ERROR] Stream send error: " << err.message() << std::endl;
    });
    if (!handle_result.ok()) {
        std::cerr << "[ERROR] SendEventStream: " << handle_result.status().message() << std::endl;
        return 1;
    }
    auto& handle = *handle_result;

    // Send multiple events via the stream.
    for (int i = 0; i < 10; i++) {
        auto ev_result = kubemq::Event::Builder()
                             .SetChannel(channel)
                             .SetBody("stream-msg-" + std::to_string(i))
                             .SetMetadata("stream-demo")
                             .Build();
        if (!ev_result.ok()) {
            std::cerr << "[ERROR] Build event: " << ev_result.status().message() << std::endl;
            return 1;
        }
        auto send_status = handle->Send(*ev_result);
        if (!send_status.ok()) {
            std::cerr << "[ERROR] handle.Send: " << send_status.message() << std::endl;
            return 1;
        }
        std::cout << "[4] Stream sent event " << (i + 1) << std::endl;
    }

    // Wait for at least one event to be received.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[6] Stream send demo complete" << std::endl;

    // Close the stream handle explicitly.
    // Note: EventStreamHandle destructor would also handle cleanup (RAII)
    handle->Close();
    sub->Cancel();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
