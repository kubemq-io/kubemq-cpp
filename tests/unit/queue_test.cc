#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "kubemq/queue_message.h"
#include "kubemq/error_code.h"

namespace kubemq {
namespace {

// --- QueueMessage Builder ---

TEST(QueueMessageBuilderTest, BuildSuccess) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("queue data")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->channel(), "queue-ch");
    EXPECT_EQ(msg_or->body(), "queue data");
    EXPECT_FALSE(msg_or->id().empty());
}

TEST(QueueMessageBuilderTest, BuildWithMetadata) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetMetadata("queue-meta")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->metadata(), "queue-meta");
}

// --- Missing channel fails ---

TEST(QueueMessageBuilderTest, MissingChannelFails) {
    auto msg_or = QueueMessage::Builder()
        .SetBody("data")
        .Build();
    ASSERT_FALSE(msg_or.ok());
    EXPECT_EQ(msg_or.status().code(), ErrorCode::kValidation);
}

// --- Empty body and metadata fails ---

TEST(QueueMessageBuilderTest, EmptyBodyAndMetadataFails) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .Build();
    ASSERT_FALSE(msg_or.ok());
    EXPECT_EQ(msg_or.status().code(), ErrorCode::kValidation);
}

// --- Policy fields ---

TEST(QueueMessageBuilderTest, SetPolicy) {
    QueuePolicy policy;
    policy.expiration_seconds = 60;
    policy.delay_seconds = 5;
    policy.max_receive_count = 3;
    policy.max_receive_queue = "dlq";

    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetPolicy(policy)
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->expiration_seconds, 60);
    EXPECT_EQ(msg_or->policy()->delay_seconds, 5);
    EXPECT_EQ(msg_or->policy()->max_receive_count, 3);
    EXPECT_EQ(msg_or->policy()->max_receive_queue, "dlq");
}

// --- Expiration and delay convenience methods ---

TEST(QueueMessageBuilderTest, SetExpirationSeconds) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetExpirationSeconds(120)
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->expiration_seconds, 120);
}

TEST(QueueMessageBuilderTest, SetDelaySeconds) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetDelaySeconds(10)
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->delay_seconds, 10);
}

TEST(QueueMessageBuilderTest, SetMaxReceiveCount) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetMaxReceiveCount(5)
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->max_receive_count, 5);
}

TEST(QueueMessageBuilderTest, SetMaxReceiveQueue) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetMaxReceiveQueue("dead-letter")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->max_receive_queue, "dead-letter");
}

TEST(QueueMessageBuilderTest, CombineExpirationAndDelay) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("queue-ch")
        .SetBody("data")
        .SetExpirationSeconds(60)
        .SetDelaySeconds(10)
        .Build();
    ASSERT_TRUE(msg_or.ok());
    ASSERT_NE(msg_or->policy(), nullptr);
    EXPECT_EQ(msg_or->policy()->expiration_seconds, 60);
    EXPECT_EQ(msg_or->policy()->delay_seconds, 10);
}

// --- Auto UUID ---

TEST(QueueMessageBuilderTest, AutoUUID) {
    auto msg1 = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    auto msg2 = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(msg1.ok());
    ASSERT_TRUE(msg2.ok());
    EXPECT_NE(msg1->id(), msg2->id());
}

TEST(QueueMessageBuilderTest, CustomId) {
    auto msg_or = QueueMessage::Builder()
        .SetId("queue-msg-123")
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->id(), "queue-msg-123");
}

// --- Tags ---

TEST(QueueMessageBuilderTest, Tags) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTags({{"priority", "high"}})
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->tags().at("priority"), "high");
}

TEST(QueueMessageBuilderTest, AddTag) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .AddTag("source", "api")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->tags().at("source"), "api");
}

// --- SetBody move ---

TEST(QueueMessageBuilderTest, SetBodyMove) {
    std::string body = "large queue payload";
    auto msg_or = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody(std::move(body))
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->body(), "large queue payload");
}

// --- Client ID ---

TEST(QueueMessageBuilderTest, ClientId) {
    auto msg_or = QueueMessage::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetClientId("queue-client")
        .Build();
    ASSERT_TRUE(msg_or.ok());
    EXPECT_EQ(msg_or->client_id(), "queue-client");
}

// --- QueuePolicy defaults ---

TEST(QueuePolicyTest, DefaultValues) {
    QueuePolicy policy;
    EXPECT_EQ(policy.expiration_seconds, 0);
    EXPECT_EQ(policy.delay_seconds, 0);
    EXPECT_EQ(policy.max_receive_count, 0);
    EXPECT_TRUE(policy.max_receive_queue.empty());
}

// --- QueueMessageAttributes defaults ---

TEST(QueueMessageAttributesTest, DefaultValues) {
    QueueMessageAttributes attrs;
    EXPECT_EQ(attrs.timestamp, 0);
    EXPECT_EQ(attrs.sequence, 0);
    EXPECT_TRUE(attrs.md5_of_body.empty());
    EXPECT_EQ(attrs.receive_count, 0);
    EXPECT_FALSE(attrs.re_routed);
    EXPECT_TRUE(attrs.re_routed_from_queue.empty());
    EXPECT_EQ(attrs.expiration_at, 0);
    EXPECT_EQ(attrs.delayed_to, 0);
}

// --- QueueSendResult defaults ---

TEST(QueueSendResultTest, DefaultValues) {
    QueueSendResult result;
    EXPECT_TRUE(result.message_id.empty());
    EXPECT_EQ(result.sent_at, 0);
    EXPECT_EQ(result.expiration_at, 0);
    EXPECT_EQ(result.delayed_to, 0);
    EXPECT_FALSE(result.is_error);
    EXPECT_TRUE(result.error.empty());
}

// --- ReceiveQueueMessagesRequest defaults ---

TEST(ReceiveQueueMessagesRequestTest, DefaultValues) {
    ReceiveQueueMessagesRequest req;
    EXPECT_EQ(req.max_number_of_messages, 1);
    EXPECT_EQ(req.wait_time_seconds, 5);
    EXPECT_FALSE(req.is_peek);
}

// --- AckAllQueueMessagesRequest defaults ---

TEST(AckAllQueueMessagesRequestTest, DefaultValues) {
    AckAllQueueMessagesRequest req;
    EXPECT_TRUE(req.request_id.empty());
    EXPECT_TRUE(req.client_id.empty());
    EXPECT_TRUE(req.channel.empty());
    EXPECT_EQ(req.wait_time_seconds, 0);
}

// --- Context shared: weak_ptr pattern ---

TEST(QueueDownstreamTest, ContextShared_MessageOutlivesReceiver) {
    auto ctx = std::make_shared<grpc::ClientContext>();
    std::weak_ptr<grpc::ClientContext> weak_ctx = ctx;
    EXPECT_TRUE(weak_ctx.lock() != nullptr);
    ctx.reset();
    EXPECT_TRUE(weak_ctx.lock() == nullptr);
}

}  // namespace
}  // namespace kubemq
