// KubeMQ C++ SDK - Burn-In Application
// Entry point with signal handling, environment variable parsing, and HTTP server.

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "config/config.h"
#include "config/defaults.h"
#include "engine/engine.h"
#include "server/http_server.h"

static std::atomic<bool> g_shutdown{false};

void SignalHandler(int signum) {
    std::cout << "\n[main] Received signal " << signum << ", shutting down...\n";
    g_shutdown.store(true);
}

// Read environment variable with a default value.
static std::string GetEnv(const char* name, const std::string& default_value) {
    const char* val = std::getenv(name);
    if (val != nullptr && val[0] != '\0') {
        return std::string(val);
    }
    return default_value;
}

static int GetEnvInt(const char* name, int default_value) {
    const char* val = std::getenv(name);
    if (val != nullptr && val[0] != '\0') {
        try {
            return std::stoi(val);
        } catch (...) {
            std::cerr << "[main] Warning: invalid integer for " << name
                      << ", using default " << default_value << "\n";
        }
    }
    return default_value;
}

int main(int argc, char* argv[]) {
    std::cout << "=== KubeMQ C++ SDK Burn-In Application ===\n\n";

    // Register signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // Parse configuration from environment variables
    std::string broker_address = GetEnv("KUBEMQ_BROKER_ADDRESS",
                                        burnin::defaults::kDefaultBrokerAddress);
    int http_port = GetEnvInt("PORT", burnin::defaults::kDefaultHttpPort);

    std::cout << "[main] Configuration:\n"
              << "  Broker:    " << broker_address << "\n"
              << "  HTTP Port: " << http_port << "\n"
              << "  SDK:       kubemq-cpp v1.0.0\n\n";

    // Create engine
    burnin::Engine engine(broker_address);
    std::cout << "[main] Engine created (state=" << engine.State() << ")\n";

    // Create and start HTTP server in a background thread
    burnin::HttpServer http_server(http_port, engine);

    std::thread server_thread([&http_server]() {
        http_server.Start();
    });

    std::cout << "[main] HTTP server started on port " << http_port << "\n";
    std::cout << "[main] Ready for API-driven burn-in runs.\n";
    std::cout << "[main] Endpoints:\n"
              << "  GET  /health       - Health check\n"
              << "  GET  /ready        - Readiness check\n"
              << "  GET  /metrics      - Prometheus metrics\n"
              << "  GET  /info         - SDK and broker info\n"
              << "  GET  /broker/status - Broker connectivity\n"
              << "  POST /run/start    - Start a run (JSON config body)\n"
              << "  POST /run/stop     - Stop current run\n"
              << "  GET  /run          - Full run status + config\n"
              << "  GET  /run/status   - Run metrics snapshot\n"
              << "  GET  /run/config   - Run configuration\n"
              << "  GET  /run/report   - Final test report\n"
              << "  POST /cleanup      - Delete channels\n"
              << "  GET  /status       - Legacy alias\n"
              << "  GET  /summary      - Legacy alias\n\n";

    // Wait for shutdown signal
    while (!g_shutdown.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\n[main] Shutting down...\n";

    // Stop any running burn-in
    std::string state = engine.State();
    if (state == burnin::kStateRunning || state == burnin::kStateStarting) {
        std::cout << "[main] Stopping active run...\n";
        engine.StopRun();
    }

    // Stop HTTP server
    http_server.Stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }

    std::cout << "[main] Shutdown complete.\n";
    return 0;
}
