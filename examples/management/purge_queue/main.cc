// Example: management/purge_queue
//
// Demonstrates purging all messages from a queue using AckAllQueueMessages.
// This effectively removes all pending messages from the queue.
//
// Channel: cpp-management.purge_queue
// Client ID: cpp-management-purge-queue-client
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
    options.set_client_id("cpp-management-purge-queue-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-management.purge_queue";

    // Send some messages to the queue.
    for (int i = 1; i <= 5; i++) {
        auto msg_result = kubemq::QueueMessage::Builder()
                              .SetChannel(channel)
                              .SetBody("to-purge-" + std::to_string(i))
                              .Build();
        if (!msg_result.ok()) {
            std::cerr << "[ERROR] Build message: " << msg_result.status().message() << std::endl;
            return 1;
        }
        auto send_result = client->SendQueueMessage(*msg_result);
        if (!send_result.ok()) {
            std::cerr << "[ERROR] SendQueueMessage: " << send_result.status().message()
                      << std::endl;
            return 1;
        }
    }
    std::cout << "[2] Sent 5 messages to queue" << std::endl;

    // Purge the queue by acknowledging all messages.
    kubemq::AckAllQueueMessagesRequest req;
    req.channel = channel;
    req.wait_time_seconds = 5;
    auto purge_result = client->AckAllQueueMessages(req);
    if (!purge_result.ok()) {
        std::cerr << "[ERROR] AckAllQueueMessages: " << purge_result.status().message()
                  << std::endl;
        return 1;
    }
    if (purge_result->is_error) {
        std::cerr << "[ERROR] Purge warning: " << purge_result->error << std::endl;
    } else {
        std::cout << "[3] Purged " << purge_result->affected_messages << " messages from queue"
                  << std::endl;
    }

    // Close the client explicitly.
    // Note: Client destructor would also handle cleanup (RAII)
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[4] Client closed" << std::endl;

    return 0;
}
