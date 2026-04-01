#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>

#include "internal/transport/state_machine.h"

namespace kubemq {
namespace internal {
namespace {

// --- Initial state ---

TEST(StateMachineTest, DefaultInitialStateIsIdle) {
    StateMachine sm;
    EXPECT_EQ(sm.Current(), ConnectionState::kIdle);
}

TEST(StateMachineTest, CustomInitialState) {
    StateMachine sm(ConnectionState::kConnecting);
    EXPECT_EQ(sm.Current(), ConnectionState::kConnecting);
}

// --- Valid transitions ---
// kIdle -> kConnecting
TEST(StateMachineTest, IdleToConnecting) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kConnecting);
}

// kConnecting -> kReady
TEST(StateMachineTest, ConnectingToReady) {
    StateMachine sm(ConnectionState::kIdle);
    ASSERT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_TRUE(sm.Transition(ConnectionState::kReady));
    EXPECT_EQ(sm.Current(), ConnectionState::kReady);
}

// kReady -> kReconnecting
TEST(StateMachineTest, ReadyToReconnecting) {
    StateMachine sm(ConnectionState::kIdle);
    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    EXPECT_TRUE(sm.Transition(ConnectionState::kReconnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kReconnecting);
}

// kReconnecting -> kConnecting
TEST(StateMachineTest, ReconnectingToConnecting) {
    StateMachine sm(ConnectionState::kIdle);
    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    sm.Transition(ConnectionState::kReconnecting);
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kConnecting);
}

// Any state -> kClosed
TEST(StateMachineTest, IdleToClosed) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

TEST(StateMachineTest, ConnectingToClosed) {
    StateMachine sm(ConnectionState::kConnecting);
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

TEST(StateMachineTest, ReadyToClosed) {
    StateMachine sm(ConnectionState::kReady);
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

TEST(StateMachineTest, ReconnectingToClosed) {
    StateMachine sm(ConnectionState::kReconnecting);
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

// --- Invalid transitions ---

TEST(StateMachineTest, IdleToReadyRejected) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_FALSE(sm.Transition(ConnectionState::kReady));
    EXPECT_EQ(sm.Current(), ConnectionState::kIdle);  // unchanged
}

TEST(StateMachineTest, IdleToReconnectingRejected) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_FALSE(sm.Transition(ConnectionState::kReconnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kIdle);
}

TEST(StateMachineTest, ConnectingToReconnectingRejected) {
    StateMachine sm(ConnectionState::kConnecting);
    EXPECT_FALSE(sm.Transition(ConnectionState::kReconnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kConnecting);
}

TEST(StateMachineTest, ReadyToIdleRejected) {
    StateMachine sm(ConnectionState::kReady);
    EXPECT_FALSE(sm.Transition(ConnectionState::kIdle));
    EXPECT_EQ(sm.Current(), ConnectionState::kReady);
}

TEST(StateMachineTest, ClosedToAnythingRejected) {
    StateMachine sm(ConnectionState::kClosed);
    EXPECT_FALSE(sm.Transition(ConnectionState::kIdle));
    EXPECT_FALSE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_FALSE(sm.Transition(ConnectionState::kReady));
    EXPECT_FALSE(sm.Transition(ConnectionState::kReconnecting));
    EXPECT_FALSE(sm.Transition(ConnectionState::kClosed));  // closed to closed also rejected
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

TEST(StateMachineTest, ReconnectingToReadyRejected) {
    StateMachine sm(ConnectionState::kReconnecting);
    EXPECT_FALSE(sm.Transition(ConnectionState::kReady));
    EXPECT_EQ(sm.Current(), ConnectionState::kReconnecting);
}

TEST(StateMachineTest, SameStateTransitionRejected) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_FALSE(sm.Transition(ConnectionState::kIdle));
}

// --- Callbacks fire correctly ---

TEST(StateMachineTest, OnConnectedCallback) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnConnected([&]() { count++; });

    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    EXPECT_EQ(count.load(), 1);
}

TEST(StateMachineTest, OnDisconnectedCallback) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnDisconnected([&]() { count++; });

    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    sm.Transition(ConnectionState::kReconnecting);
    EXPECT_EQ(count.load(), 1);
}

TEST(StateMachineTest, OnReconnectingCallback) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnReconnecting([&]() { count++; });

    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    sm.Transition(ConnectionState::kReconnecting);
    EXPECT_EQ(count.load(), 1);
}

TEST(StateMachineTest, OnReconnectedCallback) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnReconnected([&]() { count++; });

    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    sm.Transition(ConnectionState::kReconnecting);
    sm.Transition(ConnectionState::kConnecting);
    sm.Transition(ConnectionState::kReady);
    EXPECT_EQ(count.load(), 1);
}

TEST(StateMachineTest, OnClosedCallback) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnClosed([&]() { count++; });

    sm.Transition(ConnectionState::kClosed);
    EXPECT_EQ(count.load(), 1);
}

TEST(StateMachineTest, CallbackNotFiredOnInvalidTransition) {
    StateMachine sm(ConnectionState::kIdle);
    std::atomic<int> count{0};
    sm.SetOnConnected([&]() { count++; });

    // Try invalid transition: kIdle -> kReady (should fail)
    sm.Transition(ConnectionState::kReady);
    EXPECT_EQ(count.load(), 0);
}

TEST(StateMachineTest, NoCallbackSetDoesNotCrash) {
    StateMachine sm(ConnectionState::kIdle);
    // No callbacks set - transitions should still work
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_TRUE(sm.Transition(ConnectionState::kReady));
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
}

// --- WaitForState with timeout ---

TEST(StateMachineTest, WaitForStateAlreadyInState) {
    StateMachine sm(ConnectionState::kIdle);
    bool reached = sm.WaitForStateFor(ConnectionState::kIdle,
                                       std::chrono::milliseconds(100));
    EXPECT_TRUE(reached);
}

TEST(StateMachineTest, WaitForStateTimeout) {
    StateMachine sm(ConnectionState::kIdle);
    bool reached = sm.WaitForStateFor(ConnectionState::kReady,
                                       std::chrono::milliseconds(50));
    EXPECT_FALSE(reached);
}

// --- ConnectionStateToString ---

TEST(ConnectionStateToStringTest, AllStates) {
    EXPECT_STREQ(ConnectionStateToString(ConnectionState::kIdle), "IDLE");
    EXPECT_STREQ(ConnectionStateToString(ConnectionState::kConnecting), "CONNECTING");
    EXPECT_STREQ(ConnectionStateToString(ConnectionState::kReady), "READY");
    EXPECT_STREQ(ConnectionStateToString(ConnectionState::kReconnecting), "RECONNECTING");
    EXPECT_STREQ(ConnectionStateToString(ConnectionState::kClosed), "CLOSED");
}

// --- Full lifecycle callback verification ---

TEST(StateMachineTest, TransitionsFireCallbacks_FullLifecycle) {
    StateMachine sm(ConnectionState::kIdle);
    bool connected = false, disconnected = false, reconnecting = false,
         reconnected = false, closed = false;

    sm.SetOnConnected([&]() { connected = true; });
    sm.SetOnDisconnected([&]() { disconnected = true; });
    sm.SetOnReconnecting([&]() { reconnecting = true; });
    sm.SetOnReconnected([&]() { reconnected = true; });
    sm.SetOnClosed([&]() { closed = true; });

    // kIdle -> kConnecting -> kReady (fires OnConnected)
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_TRUE(sm.Transition(ConnectionState::kReady));
    EXPECT_TRUE(connected);

    // kReady -> kReconnecting (fires OnDisconnected and OnReconnecting)
    EXPECT_TRUE(sm.Transition(ConnectionState::kReconnecting));
    EXPECT_TRUE(disconnected);
    EXPECT_TRUE(reconnecting);

    // kReconnecting -> kConnecting -> kReady (fires OnReconnected)
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_TRUE(sm.Transition(ConnectionState::kReady));
    EXPECT_TRUE(reconnected);

    // kReady -> kClosed (fires OnClosed)
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    EXPECT_TRUE(closed);
}

// --- Invalid transition after close ---

TEST(StateMachineTest, InvalidTransition_ClosedToReady_Rejected) {
    StateMachine sm(ConnectionState::kIdle);
    // kIdle -> kClosed is valid
    EXPECT_TRUE(sm.Transition(ConnectionState::kClosed));
    // kClosed -> kReady should be invalid
    EXPECT_FALSE(sm.Transition(ConnectionState::kReady));
    EXPECT_EQ(sm.Current(), ConnectionState::kClosed);
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
