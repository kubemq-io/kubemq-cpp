// Example: queues/delayed_messages
//
// Demonstrates sending a delayed queue message. The message becomes
// available for consumption only after the specified delay period.
//
// Channel: cpp-queues.delayed_messages
// Client ID: cpp-queues-delayed-messages-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-delayed-messages-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.delayed_messages";

    // Send a message with a 3-second delay.
    std::cout << "[2] Sending delayed message (3s) to queue " << channel << std::endl;
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(channel)
                          .SetBody("delayed message")
                          .SetDelaySeconds(3)
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
    std::cout << "[3] Delayed message sent: id=" << send_result->message_id
              << " delayed_to=" << send_result->delayed_to << std::endl;
    std::cout << "[4] Message will be available after 3 seconds" << std::endl;

    // Close the client explicitly.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[5] Client closed" << std::endl;

    return 0;
}
