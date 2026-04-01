// Example: queues_stream/nack_all
//
// Demonstrates rejecting all messages in a transaction using NackAll.
// Rejected messages are returned to the queue for reprocessing (up to
// the MaxReceiveCount limit, after which they are discarded).
//
// Channel: cpp-queues_stream.nack_all
// Client ID: cpp-queues-stream-nack-all-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-stream-nack-all-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queues_stream.nack_all";

    // Send a message with a receive policy so the server knows how to
    // handle it after rejection (re-deliver up to 3 times).
    std::cout << "[2] Sending message with max_receive_count=3" << std::endl;
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(channel)
                          .SetBody("will be nacked")
                          .SetMaxReceiveCount(3)
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
    std::cout << "[3] Message sent" << std::endl;

    // Receive the message via downstream receiver.
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
        std::cout << "  body=" << dm.message().body() << " tx=" << dm.transaction_id() << std::endl;
    }

    // NackAll: reject all messages in the transaction (return to queue).
    if (!poll_result->messages().empty()) {
        std::cout << "[6] NackAll: rejecting all messages" << std::endl;
        auto nack_status = poll_result->NackAll();
        if (!nack_status.ok()) {
            std::cerr << "[ERROR] NackAll: " << nack_status.message() << std::endl;
            return 1;
        }
        std::cout << "[7] All messages rejected (returned to queue)" << std::endl;
    }

    // Close resources explicitly.
    // Note: Destructors also handle cleanup, but explicit calls are shown
    // for clarity and to match Go's defer pattern.
    receiver->Close();

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
