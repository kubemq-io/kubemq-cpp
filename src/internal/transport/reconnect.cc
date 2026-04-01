// src/internal/transport/reconnect.cc
#include "internal/transport/reconnect.h"

#include <algorithm>
#include <cmath>

#include "internal/types/jitter.h"

namespace kubemq {
namespace internal {

ReconnectLoop::ReconnectLoop(const ReconnectPolicy& policy) : policy_(policy) {}

ReconnectLoop::~ReconnectLoop() {
    Stop();
}

void ReconnectLoop::Start(std::function<Status()> connect_fn) {
    std::lock_guard<std::mutex> lock(mu_);
    if (running_.load(std::memory_order_acquire))
        return;
    stop_requested_.store(false, std::memory_order_release);
    if (thread_.joinable())
        thread_.join();  // clean up leftover
    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this, fn = std::move(connect_fn)]() { Run(fn); });
}

void ReconnectLoop::Stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_.load(std::memory_order_acquire))
            return;
        stop_requested_.store(true, std::memory_order_release);
        cv_.notify_all();
    }
    if (thread_.joinable())
        thread_.join();
}

bool ReconnectLoop::IsRunning() const {
    return running_.load();
}

std::chrono::milliseconds ReconnectLoop::CalculateBackoff(int attempt) const {
    // backoff = initial_delay * multiplier^attempt, capped at max_delay
    auto base_ms = static_cast<double>(policy_.initial_delay.count());
    auto computed = base_ms * std::pow(policy_.backoff_multiplier, static_cast<double>(attempt));
    auto max_ms = static_cast<double>(policy_.max_delay.count());
    auto capped = std::min(computed, max_ms);
    auto delay = std::chrono::milliseconds(static_cast<int64_t>(capped));

    // Apply jitter.
    return ApplyJitter(delay, policy_.jitter);
}

void ReconnectLoop::Run(std::function<Status()> connect_fn) {
    int attempt = 0;

    while (!stop_requested_.load()) {
        // Check max attempts (0 = infinite).
        if (policy_.max_attempts > 0 && attempt >= policy_.max_attempts) {
            break;
        }

        // Attempt connection.
        auto status = connect_fn();
        if (status.ok()) {
            break;  // Connected successfully.
        }

        // Calculate backoff with jitter.
        auto backoff = CalculateBackoff(attempt);
        ++attempt;

        // Wait for backoff duration or until stopped.
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, backoff, [this]() { return stop_requested_.load(); });
        }
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        running_.store(false, std::memory_order_release);
    }
}

std::chrono::milliseconds CalculateBackoff(const ReconnectPolicy& policy, int attempt) {
    auto base_ms = static_cast<double>(policy.initial_delay.count());
    auto computed = base_ms * std::pow(policy.backoff_multiplier, static_cast<double>(attempt));
    auto max_ms = static_cast<double>(policy.max_delay.count());
    auto capped = std::min(computed, max_ms);
    auto delay = std::chrono::milliseconds(static_cast<int64_t>(capped));
    return ApplyJitter(delay, policy.jitter);
}

}  // namespace internal
}  // namespace kubemq
