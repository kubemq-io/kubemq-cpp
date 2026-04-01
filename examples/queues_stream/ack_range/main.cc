// Example: queues_stream/ack_range
//
// Demonstrates selectively acknowledging specific messages using individual
// message Ack/Nack methods. This allows fine-grained control over which
// messages in a transaction are acknowledged.
//
// Channel: cpp-queues_stream.ack_range
// Client ID: cpp-queues-stream-ack-range-client
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
    options.set_client_id("cpp-queues-stream-ack-range-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.ack_range";
    std::atomic<int> results_received{0};

    // Send 3 messages via upstream stream, one at a time.
    std::cout << "[2] Sending 3 messages via upstream stream" << std::endl;
    auto upstream_result = client->QueueUpstream(
        [&results_received](const kubemq::QueueUpstreamResult& res) {
            if (res.is_error) {
                std::cerr << "[ERROR] Upstream result: " << res.error << std::endl;
            }
            results_received.fetch_add(1);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Upstream error: " << err.message() << std::endl;
        });
    if (!upstream_result.ok()) {
        std::cerr << "[ERROR] QueueUpstream: " << upstream_result.status().message() << std::endl;
        return 1;
    }
    auto& upstream = *upstream_result;

    for (int i = 0; i < 3; i++) {
        auto msg_result = kubemq::QueueMessage::Builder()
                              .SetChannel(channel)
                              .SetBody("msg-" + std::to_string(i))
                              .Build();
        if (!msg_result.ok()) {
            std::cerr << "[ERROR] Build message " << i << ": " << msg_result.status().message()
                      << std::endl;
            return 1;
        }
        std::vector<kubemq::QueueMessage> batch;
        batch.push_back(std::move(*msg_result));
        auto send_status = upstream->Send("req-" + std::to_string(i), batch);
        if (!send_status.ok()) {
            std::cerr << "[ERROR] Send " << i << ": " << send_status.message() << std::endl;
            return 1;
        }
        // Wait for result callback.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    std::cout << "[3] Sent 3 messages" << std::endl;

    // Allow messages to be committed to the queue.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Receive messages via downstream receiver with manual ack.
    std::cout << "[4] Opening downstream receiver" << std::endl;
    auto receiver_result = client->NewQueueDownstreamReceiver();
    if (!receiver_result.ok()) {
        std::cerr << "[ERROR] NewQueueDownstreamReceiver: " << receiver_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& receiver = *receiver_result;

    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 10;
    poll_req.wait_timeout_seconds = 5;
    poll_req.auto_ack = false;

    auto poll_result = receiver->Poll(poll_req);
    if (!poll_result.ok()) {
        std::cerr << "[ERROR] Poll: " << poll_result.status().message() << std::endl;
        return 1;
    }
    if (poll_result->is_error()) {
        std::cerr << "[ERROR] Poll response: " << poll_result->error() << std::endl;
        return 1;
    }

    std::cout << "[5] Received " << poll_result->messages().size() << " messages" << std::endl;
    for (const auto& dm : poll_result->messages()) {
        std::cout << "  body=" << dm.message().body() << " seq=" << dm.sequence() << std::endl;
    }

    // Selectively ack only the first message.
    if (!poll_result->messages().empty()) {
        auto& first_msg = poll_result->messages()[0];
        std::cout << "[6] Acking sequence " << first_msg.sequence()
                  << " from tx=" << first_msg.transaction_id() << std::endl;
        auto ack_status = first_msg.Ack();
        if (!ack_status.ok()) {
            std::cerr << "[ERROR] Ack: " << ack_status.message() << std::endl;
            return 1;
        }
        std::cout << "[7] Selective ack complete" << std::endl;
    }

    // Close resources explicitly.
    // Note: Destructors also handle cleanup, but explicit calls are shown
    // for clarity and to match Go's defer pattern.
    receiver->Close();
    upstream->Close();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
