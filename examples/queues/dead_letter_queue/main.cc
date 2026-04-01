// Example: queues/dead_letter_queue
//
// Demonstrates configuring a dead-letter queue (DLQ) for messages that
// exceed the maximum receive count. After the max attempts, messages
// are moved to the specified DLQ channel.
//
// Channel: cpp-queues.dead_letter_queue
// Client ID: cpp-queues-dead-letter-queue-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-dead-letter-queue-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.dead_letter_queue";
    std::string dlq_channel = channel + ".dlq";

    // Send a message with dead-letter queue configuration.
    // After 3 failed receive attempts, the message is moved to the DLQ.
    std::cout << "[2] Sending message with DLQ config (max_receives=3, dlq=" << dlq_channel << ")"
              << std::endl;
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(channel)
                          .SetBody("message with DLQ")
                          .SetMaxReceiveCount(3)
                          .SetMaxReceiveQueue(dlq_channel)
                          .Build();
    if (!msg_result.ok()) {
        std::cerr << "[ERROR] Build message: " << msg_result.status().message() << std::endl;
        return 1;
    }
    auto send_result = client->SendQueueMessage(*msg_result);
    if (!send_result.ok()) {
        std::cerr << "[ERROR] SendQueueMessage: " << send_result.status().message() << std::endl;
        return 1;
    }
    if (send_result->is_error) {
        std::cerr << "[ERROR] Send failed: " << send_result->error << std::endl;
        return 1;
    }
    std::cout << "[3] DLQ message sent: id=" << send_result->message_id
              << " (max_receives=3, dlq=" << dlq_channel << ")" << std::endl;

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[4] Client closed" << std::endl;

    return 0;
}
