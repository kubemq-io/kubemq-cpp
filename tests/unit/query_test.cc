#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <unordered_map>

#include "kubemq/query.h"
#include "kubemq/error_code.h"

namespace kubemq {
namespace {

// --- Builder success ---

TEST(QueryBuilderTest, BuildSuccess) {
    auto query_or = Query::Builder()
        .SetChannel("query-ch")
        .SetBody("select *")
        .SetTimeout(std::chrono::milliseconds(10000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->channel(), "query-ch");
    EXPECT_EQ(query_or->body(), "select *");
    EXPECT_EQ(query_or->timeout(), std::chrono::milliseconds(10000));
    EXPECT_FALSE(query_or->id().empty());
}

// --- Cache key and cache TTL ---

TEST(QueryBuilderTest, CacheKey) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetCacheKey("my-cache-key")
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->cache_key(), "my-cache-key");
}

TEST(QueryBuilderTest, CacheTTL) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetCacheTTL(std::chrono::milliseconds(60000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->cache_ttl(), std::chrono::milliseconds(60000));
}

TEST(QueryBuilderTest, CacheKeyAndTTLTogether) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetCacheKey("cache-1")
        .SetCacheTTL(std::chrono::milliseconds(30000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->cache_key(), "cache-1");
    EXPECT_EQ(query_or->cache_ttl(), std::chrono::milliseconds(30000));
}

TEST(QueryBuilderTest, DefaultCacheFieldsEmpty) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_TRUE(query_or->cache_key().empty());
    EXPECT_EQ(query_or->cache_ttl(), std::chrono::milliseconds(0));
}

// --- Missing channel fails ---

TEST(QueryBuilderTest, MissingChannelFails) {
    auto query_or = Query::Builder()
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_FALSE(query_or.ok());
    EXPECT_EQ(query_or.status().code(), ErrorCode::kValidation);
}

// --- Missing timeout fails ---

TEST(QueryBuilderTest, MissingTimeoutFails) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .Build();
    ASSERT_FALSE(query_or.ok());
    EXPECT_EQ(query_or.status().code(), ErrorCode::kValidation);
}

TEST(QueryBuilderTest, ZeroTimeoutFails) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(0))
        .Build();
    ASSERT_FALSE(query_or.ok());
    EXPECT_EQ(query_or.status().code(), ErrorCode::kValidation);
}

// --- Empty body and metadata fails ---

TEST(QueryBuilderTest, EmptyBodyAndMetadataFails) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_FALSE(query_or.ok());
    EXPECT_EQ(query_or.status().code(), ErrorCode::kValidation);
}

// --- Auto UUID ---

TEST(QueryBuilderTest, AutoUUID) {
    auto q1 = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    auto q2 = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(q1.ok());
    ASSERT_TRUE(q2.ok());
    EXPECT_NE(q1->id(), q2->id());
}

TEST(QueryBuilderTest, CustomId) {
    auto query_or = Query::Builder()
        .SetId("query-custom-id")
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->id(), "query-custom-id");
}

// --- Tags ---

TEST(QueryBuilderTest, Tags) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .SetTags({{"type", "select"}, {"db", "users"}})
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->tags().size(), 2);
    EXPECT_EQ(query_or->tags().at("type"), "select");
}

TEST(QueryBuilderTest, AddTag) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .AddTag("key", "value")
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->tags().at("key"), "value");
}

// --- SetBody move ---

TEST(QueryBuilderTest, SetBodyMove) {
    std::string body = "query payload";
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody(std::move(body))
        .SetTimeout(std::chrono::milliseconds(1000))
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->body(), "query payload");
}

// --- Client ID ---

TEST(QueryBuilderTest, ClientId) {
    auto query_or = Query::Builder()
        .SetChannel("ch")
        .SetBody("data")
        .SetTimeout(std::chrono::milliseconds(1000))
        .SetClientId("query-client")
        .Build();
    ASSERT_TRUE(query_or.ok());
    EXPECT_EQ(query_or->client_id(), "query-client");
}

// --- QueryReceive ---

TEST(QueryReceiveTest, DefaultValues) {
    QueryReceive recv;
    EXPECT_TRUE(recv.id.empty());
    EXPECT_TRUE(recv.channel.empty());
    EXPECT_TRUE(recv.body.empty());
    EXPECT_TRUE(recv.response_to.empty());
}

// --- QueryResponse ---

TEST(QueryResponseTest, DefaultValues) {
    QueryResponse resp;
    EXPECT_TRUE(resp.query_id.empty());
    EXPECT_FALSE(resp.executed);
    EXPECT_EQ(resp.executed_at, 0);
    EXPECT_FALSE(resp.cache_hit);
    EXPECT_TRUE(resp.error.empty());
    EXPECT_TRUE(resp.body.empty());
}

TEST(QueryResponseTest, FieldAssignment) {
    QueryResponse resp;
    resp.query_id = "q-1";
    resp.executed = true;
    resp.cache_hit = true;
    resp.body = "result data";
    EXPECT_EQ(resp.query_id, "q-1");
    EXPECT_TRUE(resp.executed);
    EXPECT_TRUE(resp.cache_hit);
    EXPECT_EQ(resp.body, "result data");
}

}  // namespace
}  // namespace kubemq
