// Example: queues/send_receive
//
// Demonstrates basic queue send and receive operations.
// A message is sent to a queue and then consumed (pulled) with auto-ack.
//
// Channel: cpp-queues.send_receive
// Client ID: cpp-queues-send-receive-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-send-receive-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.send_receive";

    // Build and send a single queue message.
    std::cout << "[2] Sending message to queue " << channel << std::endl;
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(channel)
                          .SetBody("hello queue")
                          .SetMetadata("greeting")
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
    std::cout << "[3] Sent: id=" << send_result->message_id << std::endl;

    // Receive (consume) messages from the queue via PollQueue with auto-ack.
    std::cout << "[4] Polling queue " << channel << std::endl;
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
        std::cerr << "[ERROR] Poll failed: " << poll_result->error() << std::endl;
        return 1;
    }
    std::cout << "[5] Received: " << poll_result->messages().size() << " messages" << std::endl;
    for (const auto& dm : poll_result->messages()) {
        std::cout << "  body=" << dm.message().body() << " metadata=" << dm.message().metadata()
                  << std::endl;
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
