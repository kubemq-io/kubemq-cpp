#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <unordered_map>

#include "kubemq/command.h"
#include "kubemq/error_code.h"

namespace kubemq {
namespace {

// --- Builder success with timeout ---

TEST(CommandBuilderTest, BuildSuccess) {
    auto cmd_or = Command::Builder()
        .SetChannel("cmd-ch")
        .SetBody("execute")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->channel(), "cmd-ch");
    EXPECT_EQ(cmd_or->body(), "execute");
    EXPECT_EQ(cmd_or->timeout(), std::chrono::milliseconds(5000));
    EXPECT_FALSE(cmd_or->id().empty());
}

TEST(CommandBuilderTest, BuildWithMetadata) {
    auto cmd_or = Command::Builder()
        .SetChannel("ch")
        .SetMetadata("command-meta")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->metadata(), "command-meta");
}

// --- Missing channel fails ---

TEST(CommandBuilderTest, MissingChannelFails) {
    auto cmd_or = Command::Builder()
        .SetBody("execute")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_FALSE(cmd_or.ok());
    EXPECT_EQ(cmd_or.status().code(), ErrorCode::kValidation);
}

// --- Missing timeout fails ---

TEST(CommandBuilderTest, MissingTimeoutFails) {
    auto cmd_or = Command::Builder()
        .SetChannel("cmd-ch")
        .SetBody("execute")
        .Build();
    ASSERT_FALSE(cmd_or.ok());
    EXPECT_EQ(cmd_or.status().code(), ErrorCode::kValidation);
}

TEST(CommandBuilderTest, ZeroTimeoutFails) {
    auto cmd_or = Command::Builder()
        .SetChannel("cmd-ch")
        .SetBody("execute")
        .SetTimeout(std::chrono::milliseconds(0))
        .Build();
    ASSERT_FALSE(cmd_or.ok());
    EXPECT_EQ(cmd_or.status().code(), ErrorCode::kValidation);
}

TEST(CommandBuilderTest, NegativeTimeoutFails) {
    auto cmd_or = Command::Builder()
        .SetChannel("cmd-ch")
        .SetBody("execute")
        .SetTimeout(std::chrono::milliseconds(-100))
        .Build();
    ASSERT_FALSE(cmd_or.ok());
    EXPECT_EQ(cmd_or.status().code(), ErrorCode::kValidation);
}

// --- Validation: empty body and metadata ---

TEST(CommandBuilderTest, EmptyBodyAndMetadataFails) {
    auto cmd_or = Command::Builder()
        .SetChannel("cmd-ch")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_FALSE(cmd_or.ok());
    EXPECT_EQ(cmd_or.status().code(), ErrorCode::kValidation);
}

// --- Auto UUID ---

TEST(CommandBuilderTest, AutoUUID) {
    auto cmd1 = Command::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    auto cmd2 = Command::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(cmd1.ok());
    ASSERT_TRUE(cmd2.ok());
    EXPECT_NE(cmd1->id(), cmd2->id());
}

TEST(CommandBuilderTest, CustomId) {
    auto cmd_or = Command::Builder()
        .SetId("cmd-id-123")
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->id(), "cmd-id-123");
}

// --- Tags ---

TEST(CommandBuilderTest, Tags) {
    auto cmd_or = Command::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .AddTag("action", "restart")
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->tags().at("action"), "restart");
}

// --- SetBody move overload ---

TEST(CommandBuilderTest, SetBodyMoveOverload) {
    std::string body = "command payload";
    auto cmd_or = Command::Builder()
        .SetChannel("ch")
        .SetBody(std::move(body))
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->body(), "command payload");
}

// --- Client ID ---

TEST(CommandBuilderTest, ClientId) {
    auto cmd_or = Command::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .SetClientId("cmd-client")
        .Build();
    ASSERT_TRUE(cmd_or.ok());
    EXPECT_EQ(cmd_or->client_id(), "cmd-client");
}

// --- CommandReceive ---

TEST(CommandReceiveTest, DefaultValues) {
    CommandReceive recv;
    EXPECT_TRUE(recv.id.empty());
    EXPECT_TRUE(recv.channel.empty());
    EXPECT_TRUE(recv.body.empty());
    EXPECT_TRUE(recv.response_to.empty());
}

// --- CommandResponse ---

TEST(CommandResponseTest, DefaultValues) {
    CommandResponse resp;
    EXPECT_TRUE(resp.command_id.empty());
    EXPECT_FALSE(resp.executed);
    EXPECT_EQ(resp.executed_at, 0);
    EXPECT_TRUE(resp.error.empty());
}

TEST(CommandResponseTest, FieldAssignment) {
    CommandResponse resp;
    resp.command_id = "cmd-1";
    resp.executed = true;
    resp.executed_at = 1234567890;
    EXPECT_EQ(resp.command_id, "cmd-1");
    EXPECT_TRUE(resp.executed);
}

}  // namespace
}  // namespace kubemq
