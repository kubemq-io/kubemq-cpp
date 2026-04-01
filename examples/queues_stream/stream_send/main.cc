// Example: queues_stream/stream_send
//
// Demonstrates high-throughput queue message publishing using QueueUpstream.
// The bidirectional stream allows sending multiple messages efficiently
// with per-batch result confirmations via callback.
//
// Channel: cpp-queues_stream.stream_send
// Client ID: cpp-queues-stream-stream-send-client
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
    options.set_client_id("cpp-queues-stream-stream-send-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.stream_send";
    std::atomic<int> batches_confirmed{0};

    // Open a bidirectional upstream stream for publishing with callbacks.
    std::cout << "[2] Opening upstream stream" << std::endl;
    auto upstream_result = client->QueueUpstream(
        [&batches_confirmed](const kubemq::QueueUpstreamResult& res) {
            if (res.is_error) {
                std::cerr << "[ERROR] Upstream result error: " << res.error << std::endl;
            } else {
                std::cout << "[4] Batch confirmed: ref_request_id=" << res.ref_request_id
                          << " messages=" << res.results.size() << std::endl;
            }
            batches_confirmed.fetch_add(1);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Upstream stream error: " << err.message() << std::endl;
        });
    if (!upstream_result.ok()) {
        std::cerr << "[ERROR] QueueUpstream: " << upstream_result.status().message() << std::endl;
        return 1;
    }
    auto& upstream = *upstream_result;

    // Send 5 batches of 3 messages each.
    std::cout << "[3] Sending 5 batches of 3 messages each" << std::endl;
    for (int batch = 1; batch <= 5; batch++) {
        std::vector<kubemq::QueueMessage> messages;
        for (int i = 1; i <= 3; i++) {
            auto msg_result =
                kubemq::QueueMessage::Builder()
                    .SetChannel(channel)
                    .SetBody("batch-" + std::to_string(batch) + "-msg-" + std::to_string(i))
                    .Build();
            if (!msg_result.ok()) {
                std::cerr << "[ERROR] Build message batch=" << batch << " msg=" << i << ": "
                          << msg_result.status().message() << std::endl;
                return 1;
            }
            messages.push_back(std::move(*msg_result));
        }

        auto send_status = upstream->Send("req-batch-" + std::to_string(batch), messages);
        if (!send_status.ok()) {
            std::cerr << "[ERROR] Upstream Send batch " << batch << ": " << send_status.message()
                      << std::endl;
            return 1;
        }
    }

    // Wait for all batch result callbacks.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "[5] Batches confirmed: " << batches_confirmed.load() << " of 5" << std::endl;

    // Close the upstream stream and client explicitly.
    // Note: Destructors also handle cleanup, but explicit calls are shown
    // for clarity and to match Go's defer pattern.
    upstream->Close();
    std::cout << "[6] Upstream stream closed" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
