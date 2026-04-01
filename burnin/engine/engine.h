#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "config/config.h"
#include "engine/metrics.h"

namespace burnin {

// Engine states
constexpr const char* kStateIdle = "idle";
constexpr const char* kStateStarting = "starting";
constexpr const char* kStateRunning = "running";
constexpr const char* kStateStopping = "stopping";
constexpr const char* kStateStopped = "stopped";
constexpr const char* kStateError = "error";

// Worker is the base interface for all pattern workers.
class Worker {
public:
    virtual ~Worker() = default;
    virtual void Start() = 0;
    virtual void Stop() = 0;
    virtual std::string PatternName() const = 0;
};

// Engine orchestrates burn-in runs: idle -> running -> stopped.
class Engine {
public:
    explicit Engine(const std::string& broker_address);
    ~Engine();

    // State machine
    std::string State() const;

    // Start a run with the given configuration. Returns run_id and enabled count.
    struct StartResult {
        std::string run_id;
        int enabled_count = 0;
        std::string error;
    };
    StartResult StartRun(const Config& config);

    // Stop the current run.
    std::string StopRun();

    // Information endpoints
    nlohmann::json GetInfo() const;
    nlohmann::json GetBrokerStatus() const;
    nlohmann::json GetRunFull() const;
    nlohmann::json GetRunStatus() const;
    std::pair<nlohmann::json, bool> GetRunConfig() const;
    std::pair<nlohmann::json, bool> GetRunReport() const;
    nlohmann::json CleanupChannels();

    // Run metadata
    std::string RunId() const;
    std::string RunStartedAt() const;
    std::string RunError() const;

private:
    void SetState(const std::string& state);
    void RunLoop();
    void CreateWorkers();
    void StopWorkers();
    void CleanupWorkerChannels();

    mutable std::mutex mu_;
    std::string state_;
    std::string broker_address_;

    // Current run
    Config config_;
    std::string run_id_;
    std::string run_started_at_;
    std::string run_error_;
    std::atomic<bool> stop_requested_{false};
    std::thread run_thread_;

    // Workers
    std::vector<std::unique_ptr<Worker>> workers_;

    // Metrics
    AggregateMetrics metrics_;

    // Last completed run report
    nlohmann::json last_report_;
    bool has_report_ = false;
};

}  // namespace burnin
