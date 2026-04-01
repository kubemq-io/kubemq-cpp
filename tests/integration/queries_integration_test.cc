#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "kubemq/kubemq.h"

class QueriesIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-queries-test");
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

TEST_F(QueriesIntegrationTest, SendAndRespond) {
    const std::string channel = "test.queries.cpp.send-respond";

    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetBody("query-result-data")
                .SetMetadata("result-meta")
                .Build();
            ASSERT_TRUE(reply_or.ok());
            auto status = client_->SendQueryResponse(*reply_or);
            EXPECT_TRUE(status.ok()) << status.ToString();
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok()) << sub_or.status().ToString();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto query_or = kubemq::Query::Builder()
        .SetChannel(channel)
        .SetBody("select * from users")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(query_or.ok());

    auto response = client_->SendQuery(*query_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_EQ(response->body, "query-result-data");
    EXPECT_EQ(response->metadata, "result-meta");
    EXPECT_TRUE(response->executed);

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Send with error response ---

TEST_F(QueriesIntegrationTest, SendWithErrorResponse) {
    const std::string channel = "test.queries.cpp.error";

    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetError("query failed: table not found")
                .Build();
            ASSERT_TRUE(reply_or.ok());
            auto status = client_->SendQueryResponse(*reply_or);
            EXPECT_TRUE(status.ok()) << status.ToString();
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto query_or = kubemq::Query::Builder()
        .SetChannel(channel)
        .SetBody("select * from nonexistent")
        .SetTimeout(std::chrono::milliseconds(5000))
        .Build();
    ASSERT_TRUE(query_or.ok());

    auto response = client_->SendQuery(*query_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_FALSE(response->error.empty());
    EXPECT_NE(response->error.find("table not found"), std::string::npos);

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Query with metadata and tags ---

TEST_F(QueriesIntegrationTest, SendWithMetadataAndTags) {
    const std::string channel = "test.queries.cpp.meta-tags";

    std::promise<kubemq::QueryReceive> received;
    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            received.set_value(query);
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetBody("result")
                .Build();
            if (reply_or.ok()) {
                client_->SendQueryResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto query_or = kubemq::Query::Builder()
        .SetChannel(channel)
        .SetBody("tagged-query")
        .SetMetadata("query-metadata")
        .SetTimeout(std::chrono::milliseconds(5000))
        .AddTag("db", "users")
        .AddTag("format", "json")
        .Build();
    ASSERT_TRUE(query_or.ok());

    auto response = client_->SendQuery(*query_or);
    ASSERT_TRUE(response.ok()) << response.status().ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto recv = future.get();
    EXPECT_EQ(recv.body, "tagged-query");
    EXPECT_EQ(recv.metadata, "query-metadata");
    EXPECT_EQ(recv.tags.at("db"), "users");
    EXPECT_EQ(recv.tags.at("format"), "json");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Query with cache ---

TEST_F(QueriesIntegrationTest, QueryWithCache) {
    const std::string channel = "test.queries.cpp.cache";

    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetBody("cached-result")
                .Build();
            if (reply_or.ok()) {
                client_->SendQueryResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // First query (populates cache)
    auto query1 = kubemq::Query::Builder()
        .SetChannel(channel)
        .SetBody("cache-query")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetCacheKey("test-cache-key")
        .SetCacheTTL(std::chrono::milliseconds(60000))
        .Build();
    ASSERT_TRUE(query1.ok());

    auto resp1 = client_->SendQuery(*query1);
    ASSERT_TRUE(resp1.ok()) << resp1.status().ToString();
    EXPECT_EQ(resp1->body, "cached-result");
    EXPECT_FALSE(resp1->cache_hit);  // First query should not be cache hit

    // Second query with same cache key (should be cache hit)
    auto query2 = kubemq::Query::Builder()
        .SetChannel(channel)
        .SetBody("cache-query")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetCacheKey("test-cache-key")
        .SetCacheTTL(std::chrono::milliseconds(60000))
        .Build();
    ASSERT_TRUE(query2.ok());

    auto resp2 = client_->SendQuery(*query2);
    ASSERT_TRUE(resp2.ok()) << resp2.status().ToString();
    EXPECT_EQ(resp2->body, "cached-result");
    EXPECT_TRUE(resp2->cache_hit);  // Second query should be cache hit

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- SendQuerySimple convenience ---

TEST_F(QueriesIntegrationTest, SendQuerySimple) {
    const std::string channel = "test.queries.cpp.simple";

    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetBody("simple-result")
                .Build();
            if (reply_or.ok()) {
                client_->SendQueryResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto response = client_->SendQuerySimple(
        channel, "simple-query", std::chrono::milliseconds(5000));
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_EQ(response->body, "simple-result");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Subscribe with group ---

TEST_F(QueriesIntegrationTest, SubscribeWithGroup) {
    const std::string channel = "test.queries.cpp.group";

    auto sub_or = client_->SubscribeToQueries(
        channel, "query-group",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-queries-test")
                .SetBody("group-result")
                .Build();
            if (reply_or.ok()) {
                client_->SendQueryResponse(*reply_or);
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto response = client_->SendQuerySimple(
        channel, "group-query", std::chrono::milliseconds(5000));
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_EQ(response->body, "group-result");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}
