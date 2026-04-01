#include <gtest/gtest.h>
#include "internal/transport/state_machine.h"
#include "kubemq/connection_state.h"

namespace kubemq {
namespace internal {
namespace {

TEST(GrpcTransportTest, HasStateMachine) {
    StateMachine sm(ConnectionState::kIdle);
    EXPECT_EQ(sm.Current(), ConnectionState::kIdle);
    EXPECT_TRUE(sm.Transition(ConnectionState::kConnecting));
    EXPECT_EQ(sm.Current(), ConnectionState::kConnecting);
    EXPECT_TRUE(sm.Transition(ConnectionState::kReady));
    EXPECT_EQ(sm.Current(), ConnectionState::kReady);
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
