#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "kubemq/status.h"

// Internal validation functions (declared in src/validation.cc)
namespace kubemq {
namespace internal {
Status ValidateChannel(const std::string& channel, const std::string& operation);
Status ValidateChannelNoWildcard(const std::string& channel, const std::string& operation);
Status ValidateBody(const std::string& body, const std::string& metadata,
                    const std::string& operation);
Status ValidateTimeout(std::chrono::milliseconds timeout, const std::string& operation);
}  // namespace internal
}  // namespace kubemq

namespace kubemq {
namespace {

// --- ValidateChannel ---

TEST(ValidateChannelTest, ValidChannel) {
    auto s = internal::ValidateChannel("my-channel", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateChannelTest, ValidChannelWithDots) {
    auto s = internal::ValidateChannel("events.my.topic", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateChannelTest, EmptyChannelFails) {
    auto s = internal::ValidateChannel("", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("channel"), std::string::npos);
}

TEST(ValidateChannelTest, WhitespaceSpaceFails) {
    auto s = internal::ValidateChannel("my channel", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("whitespace"), std::string::npos);
}

TEST(ValidateChannelTest, WhitespaceTabFails) {
    auto s = internal::ValidateChannel("my\tchannel", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateChannelTest, WhitespaceNewlineFails) {
    auto s = internal::ValidateChannel("my\nchannel", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateChannelTest, WhitespaceCarriageReturnFails) {
    auto s = internal::ValidateChannel("my\rchannel", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateChannelTest, TrailingDotFails) {
    auto s = internal::ValidateChannel("my-channel.", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("."), std::string::npos);
}

TEST(ValidateChannelTest, PreservesOperation) {
    auto s = internal::ValidateChannel("", "SendEvent");
    EXPECT_EQ(s.operation(), "SendEvent");
}

// --- ValidateChannelNoWildcard ---

TEST(ValidateChannelNoWildcardTest, ValidChannel) {
    auto s = internal::ValidateChannelNoWildcard("my-channel", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateChannelNoWildcardTest, AsteriskWildcardFails) {
    auto s = internal::ValidateChannelNoWildcard("my-channel.*", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("wildcard"), std::string::npos);
}

TEST(ValidateChannelNoWildcardTest, GreaterThanWildcardFails) {
    auto s = internal::ValidateChannelNoWildcard("my-channel.>", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("wildcard"), std::string::npos);
}

TEST(ValidateChannelNoWildcardTest, WildcardInMiddleFails) {
    auto s = internal::ValidateChannelNoWildcard("my.*.channel", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateChannelNoWildcardTest, EmptyChannelStillFails) {
    auto s = internal::ValidateChannelNoWildcard("", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateChannelNoWildcardTest, TrailingDotStillFails) {
    auto s = internal::ValidateChannelNoWildcard("channel.", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

// --- ValidateBody ---

TEST(ValidateBodyTest, BodyOnlyIsValid) {
    auto s = internal::ValidateBody("hello", "", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateBodyTest, MetadataOnlyIsValid) {
    auto s = internal::ValidateBody("", "meta", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateBodyTest, BothBodyAndMetadataIsValid) {
    auto s = internal::ValidateBody("hello", "meta", "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateBodyTest, BothEmptyFails) {
    auto s = internal::ValidateBody("", "", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("body"), std::string::npos);
}

TEST(ValidateBodyTest, PreservesOperation) {
    auto s = internal::ValidateBody("", "", "SendEvent");
    EXPECT_EQ(s.operation(), "SendEvent");
}

// --- ValidateTimeout ---

TEST(ValidateTimeoutTest, PositiveTimeoutIsValid) {
    auto s = internal::ValidateTimeout(std::chrono::milliseconds(1000), "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateTimeoutTest, OneMillisecondIsValid) {
    auto s = internal::ValidateTimeout(std::chrono::milliseconds(1), "test");
    EXPECT_TRUE(s.ok());
}

TEST(ValidateTimeoutTest, ZeroTimeoutFails) {
    auto s = internal::ValidateTimeout(std::chrono::milliseconds(0), "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
    EXPECT_NE(s.message().find("timeout"), std::string::npos);
}

TEST(ValidateTimeoutTest, NegativeTimeoutFails) {
    auto s = internal::ValidateTimeout(std::chrono::milliseconds(-100), "test");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), ErrorCode::kValidation);
}

TEST(ValidateTimeoutTest, PreservesOperation) {
    auto s = internal::ValidateTimeout(std::chrono::milliseconds(0), "SendCommand");
    EXPECT_EQ(s.operation(), "SendCommand");
}

}  // namespace
}  // namespace kubemq
