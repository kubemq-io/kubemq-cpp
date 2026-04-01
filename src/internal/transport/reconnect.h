// src/internal/transport/reconnect.h
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include "kubemq/reconnect_policy.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// ReconnectLoop manages reconnection attempts with exponential backoff and jitter.
// It runs a background thread that repeatedly calls a user-provided connect function
// until the connection succeeds or the loop is stopped.
class ReconnectLoop {
public:
    explicit ReconnectLoop(const ReconnectPolicy& policy);
    ~ReconnectLoop();

    // Start the reconnection loop. The connect_fn is called on each attempt.
    // Returns Status::OK if started, or an error if already running.
    // connect_fn should return OK on success, or an error to retry.
    void Start(std::function<Status()> connect_fn);

    // Stop the reconnection loop. Blocks until the background thread exits.
    void Stop();

    // Returns true if the reconnection loop is currently running.
    bool IsRunning() const;

    // Non-copyable, non-movable.
    ReconnectLoop(const ReconnectLoop&) = delete;
    ReconnectLoop& operator=(const ReconnectLoop&) = delete;
    ReconnectLoop(ReconnectLoop&&) = delete;
    ReconnectLoop& operator=(ReconnectLoop&&) = delete;

private:
    // Calculate backoff delay for a given attempt number.
    std::chrono::milliseconds CalculateBackoff(int attempt) const;

    // The background reconnection thread function.
    void Run(std::function<Status()> connect_fn);

    ReconnectPolicy policy_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::thread thread_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

// Free function for testability: compute backoff for a given policy and attempt.
std::chrono::milliseconds CalculateBackoff(const ReconnectPolicy& policy, int attempt);

}  // namespace internal
}  // namespace kubemq
