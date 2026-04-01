#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "kubemq/kubemq.h"

class QueuesStreamIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-queues-stream-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Upstream send ---

TEST_F(QueuesStreamIntegrationTest, UpstreamSend) {
    const std::string channel = "test.qs.cpp.upstream";

    auto upstream_or = client_->QueueUpstream();
    ASSERT_TRUE(upstream_or.ok()) << upstream_or.status().ToString();
    auto& upstream = *upstream_or;

    std::promise<kubemq::QueueUpstreamResult> result_promise;
    upstream->SetOnResult([&](const kubemq::QueueUpstreamResult& result) {
        result_promise.set_value(result);
    });

    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("upstream-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());

    std::vector<kubemq::QueueMessage> messages;
    messages.push_back(std::move(*msg_or));

    auto status = upstream->Send("req-1", messages);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = result_promise.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "Timed out waiting for upstream result";
    auto result = future.get();
    EXPECT_FALSE(result.is_error) << result.error;
    EXPECT_FALSE(result.results.empty());

    upstream->Close();
}

// --- Downstream poll with ack ---

TEST_F(QueuesStreamIntegrationTest, DownstreamPollAndAck) {
    const std::string channel = "test.qs.cpp.downstream-ack";

    // First, send a message via simple API
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("poll-ack-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    auto send_r = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_r.ok()) << send_r.status().ToString();

    // Poll using downstream
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 1;
    poll_req.wait_timeout_seconds = 5;

    auto poll_result = client_->PollQueue(poll_req);
    ASSERT_TRUE(poll_result.ok()) << poll_result.status().ToString();
    EXPECT_FALSE(poll_result->is_error()) << poll_result->error();
    ASSERT_FALSE(poll_result->messages().empty());
    EXPECT_EQ(poll_result->messages()[0].message().body(), "poll-ack-body");

    // Ack the message
    auto ack_status = poll_result->AckAll();
    EXPECT_TRUE(ack_status.ok()) << ack_status.ToString();
}

// --- Downstream poll with nack ---

TEST_F(QueuesStreamIntegrationTest, DownstreamPollAndNack) {
    const std::string channel = "test.qs.cpp.downstream-nack";

    // Send a message
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("poll-nack-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    auto send_r = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_r.ok()) << send_r.status().ToString();

    // Poll and nack
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 1;
    poll_req.wait_timeout_seconds = 5;

    auto poll_result = client_->PollQueue(poll_req);
    ASSERT_TRUE(poll_result.ok()) << poll_result.status().ToString();
    ASSERT_FALSE(poll_result->messages().empty());

    auto nack_status = poll_result->NackAll();
    EXPECT_TRUE(nack_status.ok()) << nack_status.ToString();
}

// --- Downstream poll with requeue ---

TEST_F(QueuesStreamIntegrationTest, DownstreamPollAndRequeue) {
    const std::string channel = "test.qs.cpp.downstream-requeue";
    const std::string requeue_channel = "test.qs.cpp.downstream-requeue-target";

    // Send a message
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("requeue-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    auto send_r = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_r.ok()) << send_r.status().ToString();

    // Poll and requeue to a different channel
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 1;
    poll_req.wait_timeout_seconds = 5;

    auto poll_result = client_->PollQueue(poll_req);
    ASSERT_TRUE(poll_result.ok()) << poll_result.status().ToString();
    ASSERT_FALSE(poll_result->messages().empty());

    auto requeue_status = poll_result->ReQueueAll(requeue_channel);
    EXPECT_TRUE(requeue_status.ok()) << requeue_status.ToString();

    // Verify the message arrived at the requeue target
    kubemq::ReceiveQueueMessagesRequest recv_req;
    recv_req.channel = requeue_channel;
    recv_req.max_number_of_messages = 1;
    recv_req.wait_time_seconds = 5;

    auto recv_result = client_->ReceiveQueueMessages(recv_req);
    ASSERT_TRUE(recv_result.ok()) << recv_result.status().ToString();
    EXPECT_GE(recv_result->messages_received, 1);
    EXPECT_EQ(recv_result->messages[0].body(), "requeue-body");
}

// --- Downstream poll with auto-ack ---

TEST_F(QueuesStreamIntegrationTest, DownstreamPollAutoAck) {
    const std::string channel = "test.qs.cpp.downstream-autoack";

    // Send a message
    auto msg_or = kubemq::QueueMessage::Builder()
        .SetChannel(channel)
        .SetBody("auto-ack-body")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    auto send_r = client_->SendQueueMessage(*msg_or);
    ASSERT_TRUE(send_r.ok()) << send_r.status().ToString();

    // Poll with auto_ack
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 1;
    poll_req.wait_timeout_seconds = 5;
    poll_req.auto_ack = true;

    auto poll_result = client_->PollQueue(poll_req);
    ASSERT_TRUE(poll_result.ok()) << poll_result.status().ToString();
    EXPECT_FALSE(poll_result->is_error()) << poll_result->error();
    ASSERT_FALSE(poll_result->messages().empty());
    EXPECT_EQ(poll_result->messages()[0].message().body(), "auto-ack-body");
}

// --- Individual message ack ---

TEST_F(QueuesStreamIntegrationTest, IndividualMessageAck) {
    const std::string channel = "test.qs.cpp.downstream-individual";

    // Send multiple messages
    for (int i = 0; i < 2; ++i) {
        auto msg_or = kubemq::QueueMessage::Builder()
            .SetChannel(channel)
            .SetBody("individual-" + std::to_string(i))
            .Build();
        ASSERT_TRUE(msg_or.ok());
        auto r = client_->SendQueueMessage(*msg_or);
        ASSERT_TRUE(r.ok()) << r.status().ToString();
    }

    // Poll multiple
    kubemq::PollRequest poll_req;
    poll_req.channel = channel;
    poll_req.max_items = 2;
    poll_req.wait_timeout_seconds = 5;

    auto poll_result = client_->PollQueue(poll_req);
    ASSERT_TRUE(poll_result.ok()) << poll_result.status().ToString();
    ASSERT_GE(poll_result->messages().size(), 2u);

    // Ack each individual message
    for (auto& msg : poll_result->messages()) {
        auto ack_status = msg.Ack();
        EXPECT_TRUE(ack_status.ok()) << ack_status.ToString();
    }
}
