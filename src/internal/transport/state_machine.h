// src/internal/transport/state_machine.h
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>

#include "kubemq/connection_state.h"

namespace kubemq {
namespace internal {

class StateMachine {
public:
    explicit StateMachine(ConnectionState initial = ConnectionState::kIdle);

    ConnectionState Current() const;
    bool Transition(ConnectionState to);
    void WaitForState(ConnectionState target);
    bool WaitForStateFor(ConnectionState target, std::chrono::milliseconds timeout);

    // State callbacks
    void SetOnConnected(std::function<void()> cb);
    void SetOnDisconnected(std::function<void()> cb);
    void SetOnReconnecting(std::function<void()> cb);
    void SetOnReconnected(std::function<void()> cb);
    void SetOnClosed(std::function<void()> cb);

private:
    bool IsValidTransition(ConnectionState from, ConnectionState to) const;
    void FireCallback(ConnectionState from, ConnectionState to);

    mutable std::mutex mu_;
    std::condition_variable cv_;
    ConnectionState state_;
    bool reconnecting_ = false;  // Tracks if we came through kReconnecting

    std::function<void()> on_connected_;
    std::function<void()> on_disconnected_;
    std::function<void()> on_reconnecting_;
    std::function<void()> on_reconnected_;
    std::function<void()> on_closed_;
};

}  // namespace internal
}  // namespace kubemq
