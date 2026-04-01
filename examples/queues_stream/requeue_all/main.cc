// Example: queues_stream/requeue_all
//
// Demonstrates moving all messages from one queue to another using ReQueueAll.
// This is useful for routing messages to different processing pipelines.
//
// Channel: cpp-queues_stream.requeue_all
// Client ID: cpp-queues-stream-requeue-all-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queues-stream-requeue-all-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string src_channel = "cpp-queues_stream.requeue_all";
    std::string dst_channel = "cpp-queues_stream.requeue_all.dest";

    // Send a message to the source queue with a receive policy.
    std::cout << "[2] Sending message to source queue " << src_channel << std::endl;
    auto msg_result = kubemq::QueueMessage::Builder()
                          .SetChannel(src_channel)
                          .SetBody("will be requeued")
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
    std::cout << "[3] Message sent to source queue" << std::endl;

    // Receive from source queue via downstream receiver.
    std::cout << "[4] Opening downstream receiver" << std::endl;
    auto receiver_result = client->NewQueueDownstreamReceiver();
    if (!receiver_result.ok()) {
        std::cerr << "[ERROR] NewQueueDownstreamReceiver: " << receiver_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& receiver = *receiver_result;

    kubemq::PollRequest poll_req;
    poll_req.channel = src_channel;
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

    // ReQueueAll: move all messages to the destination queue.
    if (!poll_result->messages().empty()) {
        std::cout << "[6] ReQueueAll: moving messages to " << dst_channel << std::endl;
        auto requeue_status = poll_result->ReQueueAll(dst_channel);
        if (!requeue_status.ok()) {
            std::cerr << "[ERROR] ReQueueAll: " << requeue_status.message() << std::endl;
            return 1;
        }
        std::cout << "[7] Messages requeued to destination" << std::endl;
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
