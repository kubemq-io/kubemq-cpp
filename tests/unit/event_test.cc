#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "kubemq/event.h"
#include "kubemq/error_code.h"

namespace kubemq {
namespace {

// --- Builder success ---

TEST(EventBuilderTest, BuildSuccess) {
    auto event_or = Event::Builder()
        .SetChannel("test-ch")
        .SetBody("hello")
        .SetMetadata("meta")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->channel(), "test-ch");
    EXPECT_EQ(event_or->body(), "hello");
    EXPECT_EQ(event_or->metadata(), "meta");
    EXPECT_FALSE(event_or->id().empty());  // auto-generated UUID
}

TEST(EventBuilderTest, BuildWithBodyOnly) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->body(), "data");
    EXPECT_TRUE(event_or->metadata().empty());
}

TEST(EventBuilderTest, BuildWithMetadataOnly) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetMetadata("meta-only")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_TRUE(event_or->body().empty());
    EXPECT_EQ(event_or->metadata(), "meta-only");
}

// --- Missing channel fails ---

TEST(EventBuilderTest, MissingChannelFails) {
    auto event_or = Event::Builder()
        .SetBody("hello")
        .Build();
    ASSERT_FALSE(event_or.ok());
    EXPECT_EQ(event_or.status().code(), ErrorCode::kValidation);
}

// --- Empty body + metadata fails ---

TEST(EventBuilderTest, EmptyBodyAndMetadataFails) {
    auto event_or = Event::Builder()
        .SetChannel("test-ch")
        .Build();
    ASSERT_FALSE(event_or.ok());
    EXPECT_EQ(event_or.status().code(), ErrorCode::kValidation);
}

// --- Auto UUID generation ---

TEST(EventBuilderTest, AutoUUIDGeneration) {
    auto event1 = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    auto event2 = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event1.ok());
    ASSERT_TRUE(event2.ok());
    EXPECT_FALSE(event1->id().empty());
    EXPECT_FALSE(event2->id().empty());
    EXPECT_NE(event1->id(), event2->id());
}

TEST(EventBuilderTest, CustomIdPreserved) {
    auto event_or = Event::Builder()
        .SetId("my-custom-id")
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->id(), "my-custom-id");
}

// --- Tags ---

TEST(EventBuilderTest, SetTags) {
    std::unordered_map<std::string, std::string> tags = {
        {"key1", "val1"},
        {"key2", "val2"},
    };
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTags(tags)
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->tags().size(), 2);
    EXPECT_EQ(event_or->tags().at("key1"), "val1");
    EXPECT_EQ(event_or->tags().at("key2"), "val2");
}

TEST(EventBuilderTest, AddTag) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .AddTag("env", "test")
        .AddTag("region", "us-east")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->tags().size(), 2);
    EXPECT_EQ(event_or->tags().at("env"), "test");
    EXPECT_EQ(event_or->tags().at("region"), "us-east");
}

TEST(EventBuilderTest, EmptyTagsDefault) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_TRUE(event_or->tags().empty());
}

// --- SetBody(string&&) move overload ---

TEST(EventBuilderTest, SetBodyMoveOverload) {
    std::string body = "large payload that should be moved";
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody(std::move(body))
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->body(), "large payload that should be moved");
}

// --- Client ID ---

TEST(EventBuilderTest, SetClientId) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetClientId("my-client")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_EQ(event_or->client_id(), "my-client");
}

TEST(EventBuilderTest, DefaultClientIdEmpty) {
    auto event_or = Event::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(event_or.ok());
    EXPECT_TRUE(event_or->client_id().empty());
}

}  // namespace
}  // namespace kubemq
