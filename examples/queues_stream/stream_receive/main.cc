// Example: queues_stream/stream_receive
//
// Demonstrates receiving queue messages using NewQueueDownstreamReceiver + Poll.
// The receiver manages a persistent downstream stream with automatic reconnection.
//
// Channel: cpp-queues_stream.stream_receive
// Client ID: cpp-queues-stream-stream-receive-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-stream-stream-receive-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.stream_receive";

    // Send a message to the queue so we have something to receive.
    std::cout << "[2] Sending message to queue " << channel << std::endl;
    auto msg_result =
        kubemq::QueueMessage::Builder().SetChannel(channel).SetBody("message to receive").Build();
    if (!msg_result.ok()) {
        std::cerr << "[ERROR] Build message: " << msg_result.status().message() << std::endl;
        return 1;
    }
    auto send_result = client->SendQueueMessage(*msg_result);
    if (!send_result.ok()) {
        std::cerr << "[ERROR] SendQueueMessage: " << send_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[3] Message sent" << std::endl;

    // Allow message to be committed to the queue.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Open a downstream receiver.
    std::cout << "[4] Opening downstream receiver" << std::endl;
    auto receiver_result = client->NewQueueDownstreamReceiver();
    if (!receiver_result.ok()) {
        std::cerr << "[ERROR] NewQueueDownstreamReceiver: " << receiver_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& receiver = *receiver_result;

    // Poll for messages (manual ack).
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
        std::cerr << "[ERROR] Poll response error: " << poll_result->error() << std::endl;
        return 1;
    }

    std::cout << "[5] Received " << poll_result->messages().size() << " messages" << std::endl;
    for (const auto& dm : poll_result->messages()) {
        std::cout << "  body=" << dm.message().body() << " tx=" << dm.transaction_id() << std::endl;
    }

    // Ack all received messages.
    if (!poll_result->messages().empty()) {
        auto ack_status = poll_result->AckAll();
        if (!ack_status.ok()) {
            std::cerr << "[ERROR] AckAll: " << ack_status.message() << std::endl;
            return 1;
        }
        std::cout << "[6] Messages acknowledged" << std::endl;
    }

    // Close the receiver and client explicitly.
    // Note: Destructors also handle cleanup, but explicit calls are shown
    // for clarity and to match Go's defer pattern.
    receiver->Close();
    std::cout << "[7] Downstream receiver closed" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
