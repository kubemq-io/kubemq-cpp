// src/internal/transport/state_machine.cc
#include "internal/transport/state_machine.h"

namespace kubemq {
namespace internal {

StateMachine::StateMachine(ConnectionState initial) : state_(initial) {}

ConnectionState StateMachine::Current() const {
    std::lock_guard<std::mutex> lock(mu_);
    return state_;
}

bool StateMachine::Transition(ConnectionState to) {
    std::function<void()> callback;

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!IsValidTransition(state_, to)) {
            return false;
        }
        ConnectionState from = state_;
        state_ = to;

        // Track whether we are in a reconnection cycle.
        if (to == ConnectionState::kReconnecting) {
            reconnecting_ = true;
        }

        // Determine which callback to fire based on the transition.
        // We capture it under the lock but invoke it outside.
        if (to == ConnectionState::kReady && from == ConnectionState::kConnecting) {
            if (reconnecting_) {
                reconnecting_ = false;
                callback = on_reconnected_;
            } else {
                callback = on_connected_;
            }
        } else if (to == ConnectionState::kReconnecting) {
            // First fire disconnected, then reconnecting
            if (on_disconnected_) {
                auto disconnected_cb = on_disconnected_;
                auto reconnecting_cb = on_reconnecting_;
                // Fire both callbacks in sequence outside main lock
                // We need a compound callback here
                callback = [disconnected_cb, reconnecting_cb]() {
                    disconnected_cb();
                    if (reconnecting_cb) {
                        reconnecting_cb();
                    }
                };
            } else {
                callback = on_reconnecting_;
            }
        } else if (to == ConnectionState::kClosed) {
            callback = on_closed_;
        }
    }

    cv_.notify_all();

    // Fire callback outside the lock to avoid deadlocks.
    if (callback) {
        callback();
    }

    return true;
}

void StateMachine::WaitForState(ConnectionState target) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this, target]() { return state_ == target; });
}

bool StateMachine::WaitForStateFor(ConnectionState target, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this, target]() { return state_ == target; });
}

void StateMachine::SetOnConnected(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    on_connected_ = std::move(cb);
}

void StateMachine::SetOnDisconnected(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    on_disconnected_ = std::move(cb);
}

void StateMachine::SetOnReconnecting(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    on_reconnecting_ = std::move(cb);
}

void StateMachine::SetOnReconnected(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    on_reconnected_ = std::move(cb);
}

void StateMachine::SetOnClosed(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(mu_);
    on_closed_ = std::move(cb);
}

bool StateMachine::IsValidTransition(ConnectionState from, ConnectionState to) const {
    // Valid transitions:
    //   kIdle -> kConnecting
    //   kConnecting -> kReady
    //   kConnecting -> kClosed
    //   kReady -> kReconnecting
    //   kReady -> kClosed
    //   kReconnecting -> kConnecting
    //   kReconnecting -> kClosed
    switch (from) {
        case ConnectionState::kIdle:
            return to == ConnectionState::kConnecting || to == ConnectionState::kClosed;

        case ConnectionState::kConnecting:
            return to == ConnectionState::kReady || to == ConnectionState::kClosed;

        case ConnectionState::kReady:
            return to == ConnectionState::kReconnecting || to == ConnectionState::kClosed;

        case ConnectionState::kReconnecting:
            return to == ConnectionState::kConnecting || to == ConnectionState::kClosed;

        case ConnectionState::kClosed:
            return false;  // Terminal state, no transitions allowed.
    }
    return false;
}

void StateMachine::FireCallback(ConnectionState from, ConnectionState to) {
    // This method is kept for potential future use but the actual callback
    // firing is done inline in Transition() for efficiency.
    (void)from;
    (void)to;
}

}  // namespace internal
}  // namespace kubemq
