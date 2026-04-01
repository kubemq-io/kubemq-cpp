// Example: patterns/work_queue
//
// Demonstrates the work queue pattern using queues.
// Multiple messages are sent to a queue and consumed by workers.
// Each message is processed by exactly one worker (competing consumers).
//
// Channel: cpp-patterns.work_queue
// Client ID: cpp-patterns-work-queue-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <iostream>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-patterns-work-queue-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-patterns.work_queue";

    // Producer: send 5 work items to the queue.
    std::cout << "[2] Enqueuing 5 tasks" << std::endl;
    for (int i = 1; i <= 5; i++) {
        auto msg_result = kubemq::QueueMessage::Builder()
                              .SetChannel(channel)
                              .SetBody("task-" + std::to_string(i))
                              .SetMetadata("priority-" + std::to_string(i))
                              .Build();
        if (!msg_result.ok()) {
            std::cerr << "[ERROR] Build message " << i << ": " << msg_result.status().message()
                      << std::endl;
            return 1;
        }
        auto send_result = client->SendQueueMessage(*msg_result);
        if (!send_result.ok()) {
            std::cerr << "[ERROR] SendQueueMessage " << i << ": " << send_result.status().message()
                      << std::endl;
            return 1;
        }
        if (send_result->is_error) {
            std::cerr << "[ERROR] Send error: " << send_result->error << std::endl;
            return 1;
        }
        std::cout << "  Enqueued: task-" << i << " (id=" << send_result->message_id << ")"
                  << std::endl;
    }

    // Worker: consume and process work items via PollQueue.
    std::cout << "[3] Worker polling for tasks" << std::endl;
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

    std::cout << "[4] Worker processed " << poll_result->messages().size()
              << " tasks:" << std::endl;
    for (const auto& dm : poll_result->messages()) {
        std::cout << "  - body=" << dm.message().body() << " metadata=" << dm.message().metadata()
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
    std::cout << "[5] Client closed" << std::endl;

    return 0;
}
