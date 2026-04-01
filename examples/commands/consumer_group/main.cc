// Example: commands/consumer_group
//
// Demonstrates load-balanced command handling with consumer groups.
// Multiple handlers in the same group share the command workload,
// with each command delivered to exactly one handler.
//
// Channel: cpp-commands.consumer_group
// Client ID: cpp-commands-consumer-group-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-commands-consumer-group-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-commands.consumer_group";
    std::string group = "cpp-commands-worker-group";
    std::atomic<bool> command_handled{false};

    // Subscribe with a consumer group for load-balanced command handling.
    std::cout << "[2] Subscribing to commands with group " << group << std::endl;
    auto sub_result = client->SubscribeToCommands(
        channel, group,
        [&client, &command_handled](const kubemq::CommandReceive& cmd) {
            std::cout << "[4] Worker received: body=" << cmd.body << std::endl;

            auto now_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            auto reply_result = kubemq::CommandReply::Builder()
                                    .SetRequestId(cmd.id)
                                    .SetResponseTo(cmd.response_to)
                                    .SetExecuted(true)
                                    .SetExecutedAt(now_epoch)
                                    .Build();
            if (!reply_result.ok()) {
                std::cerr << "[ERROR] Build reply: " << reply_result.status().message()
                          << std::endl;
                return;
            }
            auto send_status = client->SendCommandResponse(*reply_result);
            if (!send_status.ok()) {
                std::cerr << "[ERROR] SendCommandResponse: " << send_status.message() << std::endl;
                return;
            }
            command_handled.store(true);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Group error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToCommands: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to fully establish before sending.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Send a command to the group.
    std::cout << "[3] Sending command to group " << group << std::endl;
    auto cmd_result = kubemq::Command::Builder()
                          .SetChannel(channel)
                          .SetBody("group-task")
                          .SetTimeout(std::chrono::seconds(10))
                          .Build();
    if (!cmd_result.ok()) {
        std::cerr << "[ERROR] Build command: " << cmd_result.status().message() << std::endl;
        return 1;
    }
    auto resp_result = client->SendCommand(*cmd_result);
    if (!resp_result.ok()) {
        std::cerr << "[ERROR] SendCommand: " << resp_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[5] Group response: executed=" << std::boolalpha << resp_result->executed
              << std::endl;

    // Wait for the handler to finish processing.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Cancel the subscription explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    sub->Cancel();
    std::cout << "[6] Subscription cancelled" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
