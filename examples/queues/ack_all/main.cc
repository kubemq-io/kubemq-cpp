// Example: queues/ack_all
//
// Demonstrates acknowledging all messages in a queue at once.
// This is useful for purging or bulk-acknowledging queue messages.
//
// Channel: cpp-queues.ack_all
// Client ID: cpp-queues-ack-all-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>
#include <string>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-ack-all-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.ack_all";

    // Send some messages to the queue.
    std::cout << "[2] Sending 3 messages to queue " << channel << std::endl;
    for (int i = 1; i <= 3; i++) {
        auto msg_result = kubemq::QueueMessage::Builder()
                              .SetChannel(channel)
                              .SetBody("msg-" + std::to_string(i))
                              .Build();
        if (!msg_result.ok()) {
            std::cerr << "[ERROR] Build message " << i << ": " << msg_result.status().message()
                      << std::endl;
            return 1;
        }
        auto send_result = client->SendQueueMessage(*msg_result);
        if (!send_result.ok()) {
            std::cerr << "[ERROR] SendQueueMessage: " << send_result.status().message()
                      << std::endl;
            return 1;
        }
        if (send_result->is_error) {
            std::cerr << "[ERROR] Send failed: " << send_result->error << std::endl;
            return 1;
        }
    }
    std::cout << "[3] Sent 3 messages" << std::endl;

    // Acknowledge all messages in the queue.
    std::cout << "[4] AckAllQueueMessages on channel " << channel << std::endl;
    kubemq::AckAllQueueMessagesRequest ack_req;
    ack_req.channel = channel;
    ack_req.wait_time_seconds = 5;

    auto ack_result = client->AckAllQueueMessages(ack_req);
    if (!ack_result.ok()) {
        std::cerr << "[ERROR] AckAllQueueMessages: " << ack_result.status().message() << std::endl;
        return 1;
    }
    if (ack_result->is_error) {
        std::cerr << "[ERROR] Ack warning: " << ack_result->error << std::endl;
    }
    std::cout << "[5] Acknowledged " << ack_result->affected_messages << " messages" << std::endl;

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
