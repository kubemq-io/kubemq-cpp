#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "kubemq/kubemq.h"

class CommandsIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-commands-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Send + Subscribe + Respond ---

TEST_F(CommandsIntegrationTest, SendAndRespond) {
    const std::string channel = "test.commands.cpp.send-respond";

    // Subscribe to commands and auto-respond
    auto sub_or = client_->SubscribeToCommands(
        channel, "",
        [&](const kubemq::CommandReceive& cmd) {
            // Respond with success
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-commands-test")
                .Build();
            ASSERT_TRUE(reply_or.ok());
            auto status = client_->SendCommandResponse(*reply_or);
            EXPECT_TRUE(status.ok()) << status.ToString();
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok()) << sub_or.status().ToString();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Send a command
    auto cmd_or = kubemq::Command::Builder()
        .SetChannel(channel)
        .SetBody("restart-service")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());

    auto response = client_->SendCommand(*cmd_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_TRUE(response->executed || response->error.empty());

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Send with error response ---

TEST_F(CommandsIntegrationTest, SendWithErrorResponse) {
    const std::string channel = "test.commands.cpp.error-response";

    auto sub_or = client_->SubscribeToCommands(
        channel, "",
        [&](const kubemq::CommandReceive& cmd) {
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-commands-test")
                .SetError("command failed: invalid parameters")
                .Build();
            ASSERT_TRUE(reply_or.ok());
            auto status = client_->SendCommandResponse(*reply_or);
            EXPECT_TRUE(status.ok()) << status.ToString();
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto cmd_or = kubemq::Command::Builder()
        .SetChannel(channel)
        .SetBody("bad-command")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(cmd_or.ok());

    auto response = client_->SendCommand(*cmd_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_FALSE(response->error.empty());
    EXPECT_NE(response->error.find("command failed"), std::string::npos);

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Send with metadata and tags ---

TEST_F(CommandsIntegrationTest, SendWithMetadataAndTags) {
    const std::string channel = "test.commands.cpp.meta-tags";

    std::promise<kubemq::CommandReceive> received;
    auto sub_or = client_->SubscribeToCommands(
        channel, "",
        [&](const kubemq::CommandReceive& cmd) {
            received.set_value(cmd);
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-commands-test")
                .Build();
            if (reply_or.ok()) {
                client_->SendCommandResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto cmd_or = kubemq::Command::Builder()
        .SetChannel(channel)
        .SetBody("tagged-command")
        .SetMetadata("command-metadata")
        .SetTimeout(std::chrono::milliseconds(5000))
        .AddTag("action", "deploy")
        .AddTag("env", "staging")
        .Build();
    ASSERT_TRUE(cmd_or.ok());

    auto response = client_->SendCommand(*cmd_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto recv = future.get();
    EXPECT_EQ(recv.body, "tagged-command");
    EXPECT_EQ(recv.metadata, "command-metadata");
    EXPECT_EQ(recv.tags.at("action"), "deploy");
    EXPECT_EQ(recv.tags.at("env"), "staging");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- SendCommandSimple convenience ---

TEST_F(CommandsIntegrationTest, SendCommandSimple) {
    const std::string channel = "test.commands.cpp.simple";

    auto sub_or = client_->SubscribeToCommands(
        channel, "",
        [&](const kubemq::CommandReceive& cmd) {
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-commands-test")
                .Build();
            if (reply_or.ok()) {
                client_->SendCommandResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto response = client_->SendCommandSimple(
        channel, "simple-body", std::chrono::milliseconds(5000));
    ASSERT_TRUE(response.ok()) << response.status().ToString();

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Subscribe with group ---

TEST_F(CommandsIntegrationTest, SubscribeWithGroup) {
    const std::string channel = "test.commands.cpp.group";

    auto sub_or = client_->SubscribeToCommands(
        channel, "cmd-group",
        [&](const kubemq::CommandReceive& cmd) {
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-commands-test")
                .Build();
            if (reply_or.ok()) {
                client_->SendCommandResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto response = client_->SendCommandSimple(
        channel, "group-body", std::chrono::milliseconds(5000));
    ASSERT_TRUE(response.ok()) << response.status().ToString();

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}
