#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "kubemq/event_store.h"
#include "kubemq/error_code.h"

namespace kubemq {
namespace {

// --- Builder success ---

TEST(EventStoreBuilderTest, BuildSuccess) {
    auto event_or = EventStore::Builder()
        .SetChannel("store-ch")
        .SetBody("persistent data")
        .SetMetadata("meta")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->channel(), "store-ch");
    EXPECT_EQ(event_or->body(), "persistent data");
    EXPECT_EQ(event_or->metadata(), "meta");
    EXPECT_FALSE(event_or->id().empty());
}

TEST(EventStoreBuilderTest, BuildWithBodyOnly) {
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->body(), "data");
    EXPECT_TRUE(event_or->metadata().empty());
}

TEST(EventStoreBuilderTest, BuildWithMetadataOnly) {
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetMetadata("meta-only")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_TRUE(event_or->body().empty());
    EXPECT_EQ(event_or->metadata(), "meta-only");
}

// --- Missing channel fails ---

TEST(EventStoreBuilderTest, MissingChannelFails) {
    auto event_or = EventStore::Builder()
        .SetBody("hello")
        .Build();
    ASSERT_FALSE(event_or.ok());
    EXPECT_EQ(event_or.status().code(), ErrorCode::kValidation);
}

// --- Empty body + metadata fails ---

TEST(EventStoreBuilderTest, EmptyBodyAndMetadataFails) {
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .Build();
    ASSERT_FALSE(event_or.ok());
    EXPECT_EQ(event_or.status().code(), ErrorCode::kValidation);
}

// --- Auto UUID generation ---

TEST(EventStoreBuilderTest, AutoUUIDGeneration) {
    auto event1 = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    auto event2 = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event1.ok());
    ASSERT_TRUE(event2.ok());
    EXPECT_FALSE(event1->id().empty());
    EXPECT_FALSE(event2->id().empty());
    EXPECT_NE(event1->id(), event2->id());
}

TEST(EventStoreBuilderTest, CustomIdPreserved) {
    auto event_or = EventStore::Builder()
        .SetId("custom-store-id")
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->id(), "custom-store-id");
}

// --- Tags ---

TEST(EventStoreBuilderTest, SetTags) {
    std::unordered_map<std::string, std::string> tags = {
        {"priority", "high"},
        {"source", "sensor-1"},
    };
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTags(tags)
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->tags().size(), 2);
    EXPECT_EQ(event_or->tags().at("priority"), "high");
}

TEST(EventStoreBuilderTest, AddTag) {
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .AddTag("key", "value")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->tags().size(), 1);
    EXPECT_EQ(event_or->tags().at("key"), "value");
}

// --- SetBody(string&&) move overload ---

TEST(EventStoreBuilderTest, SetBodyMoveOverload) {
    std::string body = "payload to move";
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetBody(std::move(body))
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->body(), "payload to move");
}

// --- Client ID ---

TEST(EventStoreBuilderTest, SetClientId) {
    auto event_or = EventStore::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetClientId("store-client")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->client_id(), "store-client");
}

// --- EventStoreResult ---

TEST(EventStoreResultTest, DefaultValues) {
    EventStoreResult result;
    EXPECT_TRUE(result.id.empty());
    EXPECT_FALSE(result.sent);
    EXPECT_TRUE(result.error.empty());
}

TEST(EventStoreResultTest, FieldAssignment) {
    EventStoreResult result;
    result.id = "result-123";
    result.sent = true;
    result.error = "";
    EXPECT_EQ(result.id, "result-123");
    EXPECT_TRUE(result.sent);
    EXPECT_TRUE(result.error.empty());
}

// --- EventStoreReceive ---

TEST(EventStoreReceiveTest, DefaultValues) {
    EventStoreReceive recv;
    EXPECT_TRUE(recv.id.empty());
    EXPECT_EQ(recv.sequence, 0);
    EXPECT_EQ(recv.timestamp, 0);
    EXPECT_TRUE(recv.channel.empty());
    EXPECT_TRUE(recv.body.empty());
    EXPECT_TRUE(recv.tags.empty());
}

TEST(EventStoreReceiveTest, FieldAssignment) {
    EventStoreReceive recv;
    recv.id = "event-456";
    recv.sequence = 42;
    recv.timestamp = 1234567890;
    recv.channel = "store-ch";
    recv.body = "data";
    recv.tags["key"] = "val";
    EXPECT_EQ(recv.sequence, 42);
    EXPECT_EQ(recv.channel, "store-ch");
}

}  // namespace
}  // namespace kubemq
