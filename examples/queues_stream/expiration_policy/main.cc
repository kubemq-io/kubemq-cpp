// Example: queues_stream/expiration_policy
//
// Demonstrates sending queue messages with an expiration (TTL) policy.
// Messages that are not consumed before the expiration time are automatically
// removed from the queue.
//
// Channel: cpp-queues_stream.expiration_policy
// Client ID: cpp-queues-stream-expiration-policy-client
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
    options.set_client_id("cpp-queues-stream-expiration-policy-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.expiration_policy";
    std::atomic<bool> result_received{false};

    // Open an upstream stream for sending messages with expiration.
    std::cout << "[2] Opening upstream stream" << std::endl;
    auto upstream_result = client->QueueUpstream(
        [&result_received](const kubemq::QueueUpstreamResult& res) {
            if (res.is_error) {
                std::cerr << "[ERROR] Upstream result: " << res.error << std::endl;
            } else {
                std::cout << "[4] Sent message with 60s expiration policy" << std::endl;
            }
            result_received.store(true);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Upstream error: " << err.message() << std::endl;
        });
    if (!upstream_result.ok()) {
        std::cerr << "[ERROR] QueueUpstream: " << upstream_result.status().message() << std::endl;
        return 1;
    }
    auto& upstream = *upstream_result;

    // Build a message with a 60-second expiration.
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(channel)
                          .SetBody("expires in 60s")
                          .SetExpirationSeconds(60)
                          .Build();
    if (!msg_result.ok()) {
        std::cerr << "[ERROR] Build message: " << msg_result.status().message() << std::endl;
        return 1;
    }

    std::vector<kubemq::QueueMessage> messages;
    messages.push_back(std::move(*msg_result));

    std::cout << "[3] Sending message with expiration_seconds=60" << std::endl;
    auto send_status = upstream->Send("req-expiration", messages);
    if (!send_status.ok()) {
        std::cerr << "[ERROR] Send: " << send_status.message() << std::endl;
        return 1;
    }

    // Wait for the result callback.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    if (!result_received.load()) {
        std::cout << "[4] Sent message (no result confirmation within timeout)" << std::endl;
    }

    // Close the upstream stream and client explicitly.
    // Note: Destructors also handle cleanup, but explicit calls are shown
    // for clarity and to match Go's defer pattern.
    upstream->Close();
    std::cout << "[5] Upstream stream closed" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
