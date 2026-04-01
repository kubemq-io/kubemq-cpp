#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "kubemq/kubemq.h"

class QueuesIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-queues-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Simple send and receive ---

TEST_F(QueuesIntegrationTest, SimpleSendAndReceive) {
    const std::string channel = "test.queues.cpp.simple";

    // Send a queue message
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("queue-body")
        .SetMetadata("queue-meta")
        .Build();
    ASSERT_TRUE(msg_or.ok());

    auto send_result = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_result.ok()) << send_result.status().ToString();
    EXPECT_FALSE(send_result->is_error) << send_result->error;
    EXPECT_FALSE(send_result->message_id.empty());

    // Receive the message
    kubemq::ReceiveQueueMessagesRequest recv_req;
    recv_req.channel = channel;
    recv_req.max_number_of_messages = 1;
    recv_req.wait_time_seconds = 5;

    auto recv_result = client_->ReceiveQueueMessages(recv_req);
    ASSERT_TRUE(recv_result.ok()) << recv_result.status().ToString();
    EXPECT_FALSE(recv_result->is_error) << recv_result->error;
    ASSERT_GE(recv_result->messages_received, 1);
    ASSERT_FALSE(recv_result->messages.empty());
    EXPECT_EQ(recv_result->messages[0].body(), "queue-body");
    EXPECT_EQ(recv_result->messages[0].metadata(), "queue-meta");
}

// --- Batch send ---

TEST_F(QueuesIntegrationTest, BatchSend) {
    const std::string channel = "test.queues.cpp.batch";

    std::vector<kubemq::QueueMessage> messages;
    for (int i = 0; i < 3; ++i) {
        auto msg_or = kubemq::QueueMessage::Builder()
            .SetChannel(channel)
            .SetBody("batch-" + std::to_string(i))
            .Build();
        ASSERT_TRUE(msg_or.ok());
        messages.push_back(std::move(*msg_or));
    }

    auto results = client_->SendQueueMessages(messages);
    ASSERT_TRUE(results.ok()) << results.status().ToString();
    EXPECT_EQ(results->size(), 3);
    for (const auto& r : *results) {
        EXPECT_FALSE(r.is_error) << r.error;
    }

    // Receive all
    kubemq::ReceiveQueueMessagesRequest recv_req;
    recv_req.channel = channel;
    recv_req.max_number_of_messages = 10;
    recv_req.wait_time_seconds = 5;

    auto recv_result = client_->ReceiveQueueMessages(recv_req);
    ASSERT_TRUE(recv_result.ok()) << recv_result.status().ToString();
    EXPECT_GE(recv_result->messages_received, 3);
}

// --- Ack all ---

TEST_F(QueuesIntegrationTest, AckAll) {
    const std::string channel = "test.queues.cpp.ackall";

    // Send messages
    for (int i = 0; i < 3; ++i) {
        auto msg_or = kubemq::QueueMessage::Builder()
            .SetChannel(channel)
            .SetBody("ack-" + std::to_string(i))
            .Build();
        ASSERT_TRUE(msg_or.ok());
        auto r = client_->SendQueueMessage(*msg_or);
        ASSERT_TRUE(r.ok()) << r.status().ToString();
    }

    // Ack all
    kubemq::AckAllQueueMessagesRequest ack_req;
    ack_req.channel = channel;
    ack_req.wait_time_seconds = 5;

    auto ack_result = client_->AckAllQueueMessages(ack_req);
    ASSERT_TRUE(ack_result.ok()) << ack_result.status().ToString();
    EXPECT_FALSE(ack_result->is_error) << ack_result->error;
    EXPECT_GE(ack_result->affected_messages, 3u);
}

// --- Peek (view without consuming) ---

TEST_F(QueuesIntegrationTest, Peek) {
    const std::string channel = "test.queues.cpp.peek";

    // Send a message
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("peek-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    auto send_r = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_r.ok()) << send_r.status().ToString();

    // Peek (should see message without consuming)
    kubemq::ReceiveQueueMessagesRequest peek_req;
    peek_req.channel = channel;
    peek_req.max_number_of_messages = 1;
    peek_req.wait_time_seconds = 5;
    peek_req.is_peek = true;

    auto peek_result = client_->ReceiveQueueMessages(peek_req);
    ASSERT_TRUE(peek_result.ok()) << peek_result.status().ToString();
    EXPECT_TRUE(peek_result->is_peek);
    EXPECT_GE(peek_result->messages_received, 1);

    // Message should still be available for regular receive
    kubemq::ReceiveQueueMessagesRequest recv_req;
    recv_req.channel = channel;
    recv_req.max_number_of_messages = 1;
    recv_req.wait_time_seconds = 5;
    recv_req.is_peek = false;

    auto recv_result = client_->ReceiveQueueMessages(recv_req);
    ASSERT_TRUE(recv_result.ok()) << recv_result.status().ToString();
    EXPECT_GE(recv_result->messages_received, 1);
}

// --- SendQueueMessageSimple convenience ---

TEST_F(QueuesIntegrationTest, SendQueueMessageSimple) {
    const std::string channel = "test.queues.cpp.convenience";

    auto result = client_->SendQueueMessageSimple(channel, "simple-body");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_FALSE(result->is_error) << result->error;

    // Verify receipt
    kubemq::ReceiveQueueMessagesRequest recv_req;
    recv_req.channel = channel;
    recv_req.max_number_of_messages = 1;
    recv_req.wait_time_seconds = 5;

    auto recv_result = client_->ReceiveQueueMessages(recv_req);
    ASSERT_TRUE(recv_result.ok()) << recv_result.status().ToString();
    EXPECT_GE(recv_result->messages_received, 1);
    EXPECT_EQ(recv_result->messages[0].body(), "simple-body");
}

// --- Queue with expiration ---

TEST_F(QueuesIntegrationTest, QueueMessageWithExpiration) {
    const std::string channel = "test.queues.cpp.expiration";

    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("expiring-body")
        .SetExpirationSeconds(60)
        .Build();
    ASSERT_TRUE(msg_or.ok());

    auto result = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_FALSE(result->is_error);
    EXPECT_GT(result->expiration_at, 0);
}

// --- Queue with delay ---

TEST_F(QueuesIntegrationTest, QueueMessageWithDelay) {
    const std::string channel = "test.queues.cpp.delay";

    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("delayed-body")
        .SetDelaySeconds(1)
        .Build();
    ASSERT_TRUE(msg_or.ok());

    auto result = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_FALSE(result->is_error);
    EXPECT_GT(result->delayed_to, 0);
}
