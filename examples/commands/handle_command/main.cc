// Example: commands/handle_command
//
// Demonstrates subscribing to commands and handling them with business logic.
// The handler processes incoming commands and sends back responses,
// logging all fields (id, channel, metadata, body) for observability.
//
// Channel: cpp-commands.handle_command
// Client ID: cpp-commands-handle-command-client
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
    options.set_client_id("cpp-commands-handle-command-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-commands.handle_command";
    std::atomic<bool> command_handled{false};

    // Register a command handler that processes incoming commands
    // and logs all fields for comprehensive observability.
    std::cout << "[2] Subscribing to commands on channel " << channel << std::endl;
    auto sub_result = client->SubscribeToCommands(
        channel, "",
        [&client, &command_handled](const kubemq::CommandReceive& cmd) {
            std::cout << "[4] Handling command: id=" << cmd.id << " channel=" << cmd.channel
                      << " body=" << cmd.body << " metadata=" << cmd.metadata << std::endl;

            // Process the command (business logic goes here).
            // Then send back a response.
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
            std::cerr << "[ERROR] Handler error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToCommands: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to fully establish before sending.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Send a command to trigger the handler.
    std::cout << "[3] Sending command to channel " << channel << std::endl;
    auto cmd_result = kubemq::Command::Builder()
                          .SetChannel(channel)
                          .SetBody("process-order")
                          .SetMetadata("order-123")
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
    std::cout << "[5] Response: executed=" << std::boolalpha << resp_result->executed << std::endl;

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
