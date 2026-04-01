#include <gtest/gtest.h>
#include <stdexcept>
#include "internal/callback_guard.h"

namespace kubemq {
namespace internal {
namespace {

TEST(SafeInvokeTest, CatchesStdException) {
    Status captured;
    bool error_fired = false;
    auto on_error = [&](const Status& s) { captured = s; error_fired = true; };

    SafeInvoke(
        []() { throw std::runtime_error("boom"); },
        on_error, "TestOp");

    EXPECT_TRUE(error_fired);
    EXPECT_EQ(captured.code(), ErrorCode::kFatal);
    EXPECT_NE(captured.message().find("callback threw: boom"), std::string::npos);
}

TEST(SafeInvokeTest, CatchesUnknownException) {
    Status captured;
    bool error_fired = false;
    auto on_error = [&](const Status& s) { captured = s; error_fired = true; };

    SafeInvoke(
        []() { throw 42; },
        on_error, "TestOp");

    EXPECT_TRUE(error_fired);
    EXPECT_NE(captured.message().find("unknown exception"), std::string::npos);
}

TEST(SafeInvokeTest, OnErrorThrows_NoTerminate) {
    auto on_error = [](const Status&) { throw std::runtime_error("on_error boom"); };

    SafeInvoke(
        []() { throw std::runtime_error("original"); },
        on_error, "TestOp");
}

TEST(SafeInvokeTest, NullCallback_NoOp) {
    bool error_fired = false;
    auto on_error = [&](const Status&) { error_fired = true; };

    SafeInvoke(
        []() { /* no-op */ },
        on_error, "TestOp");

    EXPECT_FALSE(error_fired);
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
