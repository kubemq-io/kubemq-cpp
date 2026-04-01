// Example: queues/batch_send
//
// Demonstrates sending multiple queue messages in a single batch operation.
// Batch send reduces round trips for high-throughput scenarios.
//
// Channel: cpp-queues.batch_send
// Client ID: cpp-queues-batch-send-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>
#include <string>
#include <vector>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-batch-send-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.batch_send";

    // Create a batch of queue messages.
    std::cout << "[2] Building batch of 3 messages" << std::endl;
    std::vector<kubemq::QueueMessage> batch;
    for (int i = 1; i <= 3; i++) {
        auto msg_result = kubemq::QueueMessage::Builder()
                              .SetChannel(channel)
                              .SetBody("batch-msg-" + std::to_string(i))
                              .Build();
        if (!msg_result.ok()) {
            std::cerr << "[ERROR] Build message " << i << ": " << msg_result.status().message()
                      << std::endl;
            return 1;
        }
        batch.push_back(std::move(*msg_result));
    }

    // Send all messages in a single batch operation.
    std::cout << "[3] Sending batch" << std::endl;
    auto results = client->SendQueueMessages(batch);
    if (!results.ok()) {
        std::cerr << "[ERROR] SendQueueMessages: " << results.status().message() << std::endl;
        return 1;
    }
    for (size_t i = 0; i < results->size(); i++) {
        std::cout << "[4] Batch[" << i << "]: id=" << (*results)[i].message_id
                  << " error=" << std::boolalpha << (*results)[i].is_error << std::endl;
    }
    std::cout << "[5] Batch send complete" << std::endl;

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
