#include <gtest/gtest.h>

#include <atomic>
#include <functional>

#include "kubemq/error_code.h"
#include "kubemq/retry_policy.h"
#include "kubemq/status.h"

// Internal retry function declared in src/internal/middleware/retry.h
namespace kubemq {
namespace internal {

Status RetryUnary(const RetryPolicy& policy, bool is_idempotent,
                  const std::function<Status()>& operation);

}  // namespace internal
}  // namespace kubemq

namespace kubemq {
namespace internal {
namespace {

// --- Retryable codes do retry ---

TEST(RetryTest, TransientCodeRetries) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);  // fast tests
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        if (attempts < 3) {
            return Status(ErrorCode::kTransient, "unavailable");
        }
        return Status();  // OK on 3rd attempt
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 3);
}

TEST(RetryTest, TimeoutCodeRetries) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        if (attempts < 2) {
            return Status(ErrorCode::kTimeout, "deadline exceeded");
        }
        return Status();
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 2);
}

TEST(RetryTest, ThrottlingCodeRetries) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        if (attempts < 2) {
            return Status(ErrorCode::kThrottling, "rate limited");
        }
        return Status();
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 2);
}

// --- Non-retryable codes do NOT retry ---

TEST(RetryTest, ValidationDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kValidation, "bad input");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ErrorCode::kValidation);
    EXPECT_EQ(attempts.load(), 1);  // No retry
}

TEST(RetryTest, AuthenticationDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kAuthentication, "unauthenticated");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

TEST(RetryTest, AuthorizationDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kAuthorization, "permission denied");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

TEST(RetryTest, FatalDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kFatal, "internal error");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

TEST(RetryTest, NotFoundDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kNotFound, "not found");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

TEST(RetryTest, CancellationDoesNotRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kCancellation, "cancelled");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

// --- Idempotent vs non-idempotent behavior ---

TEST(RetryTest, NonIdempotentSkipsTimeoutRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/false, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kTimeout, "deadline exceeded");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ErrorCode::kTimeout);
    EXPECT_EQ(attempts.load(), 1);  // No retry for non-idempotent + timeout
}

TEST(RetryTest, NonIdempotentStillRetriesTransient) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/false, [&]() -> Status {
        attempts++;
        if (attempts < 2) {
            return Status(ErrorCode::kTransient, "unavailable");
        }
        return Status();
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 2);
}

TEST(RetryTest, NonIdempotentStillRetriesThrottling) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/false, [&]() -> Status {
        attempts++;
        if (attempts < 2) {
            return Status(ErrorCode::kThrottling, "rate limited");
        }
        return Status();
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 2);
}

// --- max_retries respected ---

TEST(RetryTest, MaxRetriesRespected) {
    RetryPolicy policy;
    policy.max_retries = 2;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kTransient, "always fails");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ErrorCode::kTransient);
    // 1 initial + 2 retries = 3 total attempts
    EXPECT_EQ(attempts.load(), 3);
}

TEST(RetryTest, ZeroRetriesMeansNoRetry) {
    RetryPolicy policy;
    policy.max_retries = 0;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status(ErrorCode::kTransient, "fail");
    });

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

TEST(RetryTest, SuccessOnFirstAttemptNoRetry) {
    RetryPolicy policy;
    policy.max_retries = 3;
    policy.initial_backoff = std::chrono::milliseconds(1);
    policy.jitter = JitterMode::kNone;

    std::atomic<int> attempts{0};
    auto result = RetryUnary(policy, /*is_idempotent=*/true, [&]() -> Status {
        attempts++;
        return Status();  // OK immediately
    });

    EXPECT_TRUE(result.ok());
    EXPECT_EQ(attempts.load(), 1);
}

// --- RetryPolicy defaults ---

TEST(RetryPolicyTest, DefaultValues) {
    RetryPolicy policy;
    EXPECT_EQ(policy.max_retries, 3);
    EXPECT_EQ(policy.initial_backoff, std::chrono::milliseconds(100));
    EXPECT_EQ(policy.max_backoff, std::chrono::milliseconds(10000));
    EXPECT_EQ(policy.multiplier, 2.0);
    EXPECT_EQ(policy.jitter, JitterMode::kFull);
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
