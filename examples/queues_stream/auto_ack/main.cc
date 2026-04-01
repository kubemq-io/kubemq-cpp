// Example: queues_stream/auto_ack
//
// Demonstrates receiving queue messages with automatic acknowledgment.
// When AutoAck is true, messages are automatically acknowledged upon receipt.
//
// Channel: cpp-queues_stream.auto_ack
// Client ID: cpp-queues-stream-auto-ack-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-stream-auto-ack-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.auto_ack";

    // Send a message to the queue.
    std::cout << "[2] Sending message to queue " << channel << std::endl;
    auto msg_result =
        kubemq::QueueMessage::Builder().SetChannel(channel).SetBody("auto-ack message").Build();
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

    // Poll with auto-ack -- messages are acknowledged automatically on receipt.
    std::cout << "[4] Polling with auto_ack=true" << std::endl;
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 10;
    poll_req.wait_timeout_seconds = 5;
    poll_req.auto_ack = true;

    auto poll_result = client->PollQueue(poll_req);
    if (!poll_result.ok()) {
        std::cerr << "[ERROR] PollQueue: " << poll_result.status().message() << std::endl;
        return 1;
    }
    if (poll_result->is_error()) {
        std::cerr << "[ERROR] Poll response: " << poll_result->error() << std::endl;
        return 1;
    }

    std::cout << "[5] Auto-ack: received " << poll_result->messages().size()
              << " messages (automatically acknowledged)" << std::endl;
    for (const auto& dm : poll_result->messages()) {
        std::cout << "  body=" << dm.message().body() << std::endl;
    }

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Client closed" << std::endl;

    return 0;
}
