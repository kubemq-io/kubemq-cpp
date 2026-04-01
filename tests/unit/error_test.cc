#include <gtest/gtest.h>

#include "kubemq/error_code.h"
#include "kubemq/status.h"

namespace kubemq {
namespace {

// --- Status construction ---

TEST(StatusTest, DefaultConstructorIsOk) {
    Status s;
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kOk);
    EXPECT_TRUE(s.message().empty());
}

TEST(StatusTest, ConstructWithCodeAndMessage) {
    Status s(ErrorCode::kTransient, "connection lost");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kTransient);
    EXPECT_EQ(s.message(), "connection lost");
}

TEST(StatusTest, ConstructWithAllFields) {
    Status s(ErrorCode::kValidation, "invalid channel", "SendEvent",
             "my-channel", "req-123");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_EQ(s.message(), "invalid channel");
    EXPECT_EQ(s.operation(), "SendEvent");
    EXPECT_EQ(s.channel(), "my-channel");
    EXPECT_EQ(s.request_id(), "req-123");
}

TEST(StatusTest, OkStatusOperationAndChannelEmpty) {
    Status s;
    EXPECT_TRUE(s.operation().empty());
    EXPECT_TRUE(s.channel().empty());
    EXPECT_TRUE(s.request_id().empty());
}

// --- Error codes ---

TEST(ErrorCodeTest, AllCodesHaveStringRepresentation) {
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kOk), "OK");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kTransient), "TRANSIENT");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kTimeout), "TIMEOUT");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kThrottling), "THROTTLING");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kAuthentication), "AUTHENTICATION");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kAuthorization), "AUTHORIZATION");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kValidation), "VALIDATION");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kNotFound), "NOT_FOUND");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kFatal), "FATAL");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kCancellation), "CANCELLATION");
    EXPECT_STREQ(ErrorCodeToString(ErrorCode::kBackpressure), "BACKPRESSURE");
}

TEST(ErrorCodeTest, RetryableCodes) {
    EXPECT_TRUE(IsRetryable(ErrorCode::kTransient));
    EXPECT_TRUE(IsRetryable(ErrorCode::kTimeout));
    EXPECT_TRUE(IsRetryable(ErrorCode::kThrottling));
}

TEST(ErrorCodeTest, NonRetryableCodes) {
    EXPECT_FALSE(IsRetryable(ErrorCode::kOk));
    EXPECT_FALSE(IsRetryable(ErrorCode::kAuthentication));
    EXPECT_FALSE(IsRetryable(ErrorCode::kAuthorization));
    EXPECT_FALSE(IsRetryable(ErrorCode::kValidation));
    EXPECT_FALSE(IsRetryable(ErrorCode::kNotFound));
    EXPECT_FALSE(IsRetryable(ErrorCode::kFatal));
    EXPECT_FALSE(IsRetryable(ErrorCode::kCancellation));
    EXPECT_FALSE(IsRetryable(ErrorCode::kBackpressure));
}

// --- Sentinel errors ---

TEST(SentinelErrorTest, ClientClosed) {
    EXPECT_FALSE(kErrClientClosed.ok());
    EXPECT_EQ(kErrClientClosed.code(), ErrorCode::kFatal);
    EXPECT_FALSE(kErrClientClosed.message().empty());
    EXPECT_NE(kErrClientClosed.message().find("client closed"), std::string::npos);
}

TEST(SentinelErrorTest, NotImplemented) {
    EXPECT_FALSE(kErrNotImplemented.ok());
    EXPECT_EQ(kErrNotImplemented.code(), ErrorCode::kFatal);
    EXPECT_NE(kErrNotImplemented.message().find("not implemented"), std::string::npos);
}

TEST(SentinelErrorTest, Validation) {
    EXPECT_FALSE(kErrValidation.ok());
    EXPECT_EQ(kErrValidation.code(), ErrorCode::kValidation);
    EXPECT_NE(kErrValidation.message().find("validation"), std::string::npos);
}

// --- ToString ---

TEST(StatusTest, ToStringOk) {
    Status s;
    EXPECT_EQ(s.ToString(), "OK");
}

TEST(StatusTest, ToStringWithMessage) {
    Status s(ErrorCode::kTransient, "network error");
    std::string str = s.ToString();
    EXPECT_NE(str.find("TRANSIENT"), std::string::npos);
    EXPECT_NE(str.find("network error"), std::string::npos);
}

TEST(StatusTest, ToStringWithAllFields) {
    Status s(ErrorCode::kValidation, "bad input", "SendEvent", "ch1", "req1");
    std::string str = s.ToString();
    EXPECT_NE(str.find("VALIDATION"), std::string::npos);
    EXPECT_NE(str.find("bad input"), std::string::npos);
    EXPECT_NE(str.find("op=SendEvent"), std::string::npos);
    EXPECT_NE(str.find("ch=ch1"), std::string::npos);
    EXPECT_NE(str.find("req=req1"), std::string::npos);
}

// --- is_retryable ---

TEST(StatusTest, IsRetryableTransient) {
    Status s(ErrorCode::kTransient, "temp failure");
    EXPECT_TRUE(s.is_retryable());
}

TEST(StatusTest, IsRetryableFatal) {
    Status s(ErrorCode::kFatal, "permanent failure");
    EXPECT_FALSE(s.is_retryable());
}

TEST(StatusTest, IsRetryableOk) {
    Status s;
    EXPECT_FALSE(s.is_retryable());
}

// --- Domain-specific error types ---

TEST(BufferFullErrorTest, Construction) {
    BufferFullError err(1024, 500);
    EXPECT_EQ(err.buffer_size(), 1024);
    EXPECT_EQ(err.queued_count(), 500);
}

TEST(BufferFullErrorTest, ToString) {
    BufferFullError err(100, 50);
    std::string str = err.ToString();
    EXPECT_FALSE(str.empty());
}

TEST(StreamBrokenErrorTest, Construction) {
    StreamBrokenError err({"id1", "id2", "id3"});
    EXPECT_EQ(err.unacknowledged_ids().size(), 3);
    EXPECT_EQ(err.unacknowledged_ids()[0], "id1");
    EXPECT_EQ(err.unacknowledged_ids()[2], "id3");
}

TEST(TransportErrorTest, Construction) {
    Status cause(ErrorCode::kTransient, "unavailable");
    TransportError err("SendEvent", cause);
    EXPECT_EQ(err.operation(), "SendEvent");
    EXPECT_EQ(err.cause().code(), ErrorCode::kTransient);
}

TEST(HandlerErrorTest, Construction) {
    Status cause(ErrorCode::kFatal, "handler panic");
    HandlerError err("on_event", cause);
    EXPECT_EQ(err.handler(), "on_event");
    EXPECT_EQ(err.cause().code(), ErrorCode::kFatal);
}

}  // namespace
}  // namespace kubemq
