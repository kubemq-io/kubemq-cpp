#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "kubemq/kubemq.h"

class EventsStoreIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-events-store-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Send and Receive with StartFromFirst ---

TEST_F(EventsStoreIntegrationTest, SendAndReceiveStartFromFirst) {
    const std::string channel = "test.es.cpp.first";

    // Send an event first
    auto result_or = client_->PublishEventStore(channel, "stored-body-first");
    ASSERT_TRUE(result_or.ok()) << result_or.status().ToString();
    EXPECT_TRUE(result_or->sent);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Subscribe with StartFromFirst to replay
    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromFirstEvent(),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok()) << sub_or.status().ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "Timed out waiting for stored event";
    auto event = future.get();
    EXPECT_EQ(event.body, "stored-body-first");
    EXPECT_GT(event.sequence, 0u);

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- StartFromNewEvents ---

TEST_F(EventsStoreIntegrationTest, StartFromNewEvents) {
    const std::string channel = "test.es.cpp.new";

    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromNewEvents(),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto result_or = client_->PublishEventStore(channel, "new-event-body");
    ASSERT_TRUE(result_or.ok()) << result_or.status().ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto event = future.get();
    EXPECT_EQ(event.body, "new-event-body");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- StartFromLast ---

TEST_F(EventsStoreIntegrationTest, StartFromLast) {
    const std::string channel = "test.es.cpp.last";

    // Send multiple events
    for (int i = 0; i < 3; ++i) {
        auto r = client_->PublishEventStore(channel, "evt-" + std::to_string(i));
        ASSERT_TRUE(r.ok()) << r.status().ToString();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromLastEvent(),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto event = future.get();
    EXPECT_EQ(event.body, "evt-2");  // Should get the last event

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- StartFromSequence ---

TEST_F(EventsStoreIntegrationTest, StartFromSequence) {
    const std::string channel = "test.es.cpp.seq";

    // Send events and track sequences
    uint64_t target_seq = 0;
    for (int i = 0; i < 3; ++i) {
        auto r = client_->PublishEventStore(channel, "seq-evt-" + std::to_string(i));
        ASSERT_TRUE(r.ok()) << r.status().ToString();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Subscribe from sequence 2
    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromSequence(2),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto event = future.get();
    EXPECT_GE(event.sequence, 2u);

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- StartFromTimeDelta ---

TEST_F(EventsStoreIntegrationTest, StartFromTimeDelta) {
    const std::string channel = "test.es.cpp.timedelta";

    // Send an event
    auto r = client_->PublishEventStore(channel, "time-delta-body");
    ASSERT_TRUE(r.ok()) << r.status().ToString();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Subscribe with time delta of 60 seconds (should catch recent events)
    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromTimeDelta(std::chrono::seconds(60)),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto event = future.get();
    EXPECT_EQ(event.body, "time-delta-body");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- SendEventStore with Builder ---

TEST_F(EventsStoreIntegrationTest, SendWithBuilder) {
    const std::string channel = "test.es.cpp.builder";

    auto es_or = kubemq::EventStore::Builder()
        .SetChannel(channel)
        .SetBody("builder-stored")
        .SetMetadata("builder-meta")
        .AddTag("source", "test")
        .Build();
    ASSERT_TRUE(es_or.ok());

    auto result_or = client_->SendEventStore(*es_or);
    ASSERT_TRUE(result_or.ok()) << result_or.status().ToString();
    EXPECT_TRUE(result_or->sent);
    EXPECT_FALSE(result_or->id.empty());
}

// --- EventStore stream ---

TEST_F(EventsStoreIntegrationTest, StreamSendAndReceive) {
    const std::string channel = "test.es.cpp.stream";

    std::promise<kubemq::EventStoreReceive> received;
    auto sub_or = client_->SubscribeToEventsStore(
        channel, "",
        kubemq::SubscriptionOption::StartFromNewEvents(),
        [&](const kubemq::EventStoreReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto stream_or = client_->SendEventStoreStream();
    ASSERT_TRUE(stream_or.ok()) << stream_or.status().ToString();

    auto es_or = kubemq::EventStore::Builder()
        .SetChannel(channel)
        .SetBody("stream-stored")
        .Build();
    ASSERT_TRUE(es_or.ok());

    auto status = (*stream_or)->Send(*es_or);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto event = future.get();
    EXPECT_EQ(event.body, "stream-stored");

    (*stream_or)->Close();
    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}
