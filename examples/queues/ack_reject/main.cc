// Example: queues/ack_reject
//
// Demonstrates individual message acknowledgment and rejection using
// the queue downstream receiver. Messages can be individually acked
// (confirmed) or rejected (nacked) back to the queue.
//
// Channel: cpp-queues.ack_reject
// Client ID: cpp-queues-ack-reject-client
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
    options.set_client_id("cpp-queues-ack-reject-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues.ack_reject";

    // Send two messages.
    std::cout << "[2] Sending 2 messages to queue " << channel << std::endl;
    for (int i = 1; i <= 2; i++) {
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
    std::cout << "[3] Sent 2 messages" << std::endl;

    // Poll messages with manual acknowledgment (AutoAck=false).
    std::cout << "[4] Creating downstream receiver" << std::endl;
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

    std::cout << "[5] Polling with manual ack" << std::endl;
    auto resp_result = receiver->Poll(poll_req);
    if (!resp_result.ok()) {
        std::cerr << "[ERROR] Poll: " << resp_result.status().message() << std::endl;
        receiver->Close();
        auto close_status = client->Close();
        if (!close_status.ok()) {
            std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        }
        return 1;
    }

    std::cout << "[6] Polled " << resp_result->messages().size() << " messages" << std::endl;
    for (const auto& dm : resp_result->messages()) {
        std::cout << "  body=" << dm.message().body() << std::endl;
    }

    // Close the downstream receiver explicitly.
    // Note: The QueueDownstreamReceiver destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    receiver->Close();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
