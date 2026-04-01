// Example: error_handling/graceful_shutdown
//
// Demonstrates graceful shutdown with OS signal handling.
// The client properly drains in-flight operations and closes
// subscriptions when receiving SIGINT or SIGTERM.
//
// Channel: cpp-error_handling.graceful_shutdown
// Client ID: cpp-error-handling-graceful-shutdown-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

// Global flag for signal handler.
volatile std::sig_atomic_t g_shutdown_requested = 0;

void signal_handler(int signal) {
    (void)signal;
    g_shutdown_requested = 1;
}

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-error-handling-graceful-shutdown-client");
    options.set_drain_timeout(std::chrono::seconds(10));
    options.set_on_closed([]() { std::cout << "[Shutdown] Connection closed" << std::endl; });

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-error_handling.graceful_shutdown";

    // Start a subscription that runs until shutdown.
    std::cout << "[2] Subscribing to events on channel " << channel << std::endl;
    auto sub_result = client->SubscribeToEvents(
        channel, "",
        [](const kubemq::EventReceive& event) {
            std::cout << "Received: body=" << event.body << std::endl;
        },
        [](const kubemq::Status& err) {
            std::cerr << "Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToEvents: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Install signal handlers for graceful shutdown.
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "[3] Running... Press Ctrl+C to initiate graceful shutdown" << std::endl;

    // Wait for shutdown signal (poll every 100ms, exit after 5s for example mode).
    auto start = std::chrono::steady_clock::now();
    while (!g_shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // Auto-exit after 5 seconds when running as an automated example.
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(5)) {
            std::cout << "\n[4] Auto-shutdown after 5s (example mode)" << std::endl;
            break;
        }
    }

    if (g_shutdown_requested) {
        std::cout << "\n[4] Shutdown signal received, cleaning up..." << std::endl;
    }

    // Cancel subscription first.
    sub->Cancel();
    std::cout << "[5] Subscription cancelled" << std::endl;

    // Close the client, draining in-flight operations.
    // Note: The Client destructor also calls Close(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close error: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[6] Graceful shutdown complete" << std::endl;

    return 0;
}
