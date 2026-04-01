#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <thread>

#include "kubemq/reconnect_policy.h"
#include "internal/transport/reconnect.h"

// Internal reconnect utilities
// The backoff calculation function is expected to be in internal/transport/reconnect.h
namespace kubemq {
namespace internal {

// Calculate backoff delay for attempt n using the reconnect policy.
// delay = min(initial_delay * multiplier^attempt, max_delay)
// Then apply jitter based on the JitterMode.
std::chrono::milliseconds CalculateBackoff(const ReconnectPolicy& policy, int attempt);

}  // namespace internal
}  // namespace kubemq

namespace kubemq {
namespace internal {
namespace {

// --- Backoff calculation: initial * multiplier^n ---

TEST(ReconnectBackoffTest, FirstAttemptIsInitialDelay) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kNone;

    auto delay = CalculateBackoff(policy, 0);
    EXPECT_EQ(delay, std::chrono::milliseconds(1000));
}

TEST(ReconnectBackoffTest, SecondAttemptMultiplied) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kNone;

    auto delay = CalculateBackoff(policy, 1);
    EXPECT_EQ(delay, std::chrono::milliseconds(2000));
}

TEST(ReconnectBackoffTest, ThirdAttemptMultipliedTwice) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kNone;

    auto delay = CalculateBackoff(policy, 2);
    EXPECT_EQ(delay, std::chrono::milliseconds(4000));
}

TEST(ReconnectBackoffTest, ExponentialGrowth) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(100);
    policy.backoff_multiplier = 3.0;
    policy.max_delay = std::chrono::milliseconds(100000);
    policy.jitter = JitterMode::kNone;

    // attempt 0: 100ms
    // attempt 1: 300ms
    // attempt 2: 900ms
    // attempt 3: 2700ms
    EXPECT_EQ(CalculateBackoff(policy, 0), std::chrono::milliseconds(100));
    EXPECT_EQ(CalculateBackoff(policy, 1), std::chrono::milliseconds(300));
    EXPECT_EQ(CalculateBackoff(policy, 2), std::chrono::milliseconds(900));
    EXPECT_EQ(CalculateBackoff(policy, 3), std::chrono::milliseconds(2700));
}

// --- max_delay cap ---

TEST(ReconnectBackoffTest, MaxDelayCap) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(5000);
    policy.jitter = JitterMode::kNone;

    // attempt 0: 1000ms
    // attempt 1: 2000ms
    // attempt 2: 4000ms
    // attempt 3: 8000ms -> capped to 5000ms
    auto delay = CalculateBackoff(policy, 3);
    EXPECT_EQ(delay, std::chrono::milliseconds(5000));
}

TEST(ReconnectBackoffTest, MaxDelayCappedOnHighAttempts) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(100);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(3000);
    policy.jitter = JitterMode::kNone;

    // attempt 10: 100 * 2^10 = 102400ms -> capped to 3000ms
    auto delay = CalculateBackoff(policy, 10);
    EXPECT_EQ(delay, std::chrono::milliseconds(3000));
}

// --- Jitter modes ---

TEST(ReconnectBackoffTest, JitterNoneExact) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kNone;

    // With no jitter, result should be exactly deterministic
    auto d1 = CalculateBackoff(policy, 0);
    auto d2 = CalculateBackoff(policy, 0);
    EXPECT_EQ(d1, d2);
    EXPECT_EQ(d1, std::chrono::milliseconds(1000));
}

TEST(ReconnectBackoffTest, JitterFullWithinRange) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kFull;

    // Full jitter: random in [0, delay)
    // Run multiple times and verify all within range
    for (int i = 0; i < 100; ++i) {
        auto delay = CalculateBackoff(policy, 0);
        EXPECT_GE(delay.count(), 0) << "Iteration " << i;
        EXPECT_LT(delay.count(), 1000) << "Iteration " << i;
    }
}

TEST(ReconnectBackoffTest, JitterEqualWithinRange) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(1000);
    policy.backoff_multiplier = 2.0;
    policy.max_delay = std::chrono::milliseconds(30000);
    policy.jitter = JitterMode::kEqual;

    // Equal jitter: delay/2 + random [0, delay/2)
    // So range is [500, 1000) for attempt 0 with 1000ms initial
    for (int i = 0; i < 100; ++i) {
        auto delay = CalculateBackoff(policy, 0);
        EXPECT_GE(delay.count(), 500) << "Iteration " << i;
        EXPECT_LT(delay.count(), 1000) << "Iteration " << i;
    }
}

// --- max_attempts limit ---

TEST(ReconnectPolicyTest, DefaultMaxAttemptsZeroMeansInfinite) {
    ReconnectPolicy policy;
    EXPECT_EQ(policy.max_attempts, 0);
}

TEST(ReconnectPolicyTest, DefaultValues) {
    ReconnectPolicy policy;
    EXPECT_EQ(policy.initial_delay, std::chrono::milliseconds(1000));
    EXPECT_EQ(policy.max_delay, std::chrono::milliseconds(30000));
    EXPECT_EQ(policy.backoff_multiplier, 2.0);
    EXPECT_EQ(policy.jitter, JitterMode::kFull);
    EXPECT_EQ(policy.buffer_size, 1024);
}

TEST(ReconnectPolicyTest, CustomMaxAttempts) {
    ReconnectPolicy policy;
    policy.max_attempts = 5;
    EXPECT_EQ(policy.max_attempts, 5);
}

// --- JitterMode enum ---

TEST(JitterModeTest, AllValues) {
    EXPECT_NE(static_cast<int>(JitterMode::kNone), static_cast<int>(JitterMode::kFull));
    EXPECT_NE(static_cast<int>(JitterMode::kNone), static_cast<int>(JitterMode::kEqual));
    EXPECT_NE(static_cast<int>(JitterMode::kFull), static_cast<int>(JitterMode::kEqual));
}

// --- ReconnectLoop start/stop stress ---

TEST(ReconnectLoopTest, StartStop_NoCrash) {
    ReconnectPolicy policy;
    policy.initial_delay = std::chrono::milliseconds(10);
    policy.max_delay = std::chrono::milliseconds(100);
    ReconnectLoop loop(policy);

    // Rapid start+stop should not crash
    for (int i = 0; i < 10; ++i) {
        loop.Start([]() -> Status { return Status(ErrorCode::kTransient, "fail"); });
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        loop.Stop();
    }
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
