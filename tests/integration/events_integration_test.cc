#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "kubemq/kubemq.h"

class EventsIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-events-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Send and Receive ---

TEST_F(EventsIntegrationTest, SendAndReceive) {
    std::promise<kubemq::EventReceive> received;
    auto sub_or = client_->SubscribeToEvents(
        "test.events.cpp.send-recv", "",
        [&](const kubemq::EventReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok()) << sub_or.status().ToString();

    // Allow subscription to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto status = client_->PublishEvent("test.events.cpp.send-recv", "hello-cpp");
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "Timed out waiting for event";
    auto event = future.get();
    EXPECT_EQ(event.body, "hello-cpp");
    EXPECT_EQ(event.channel, "test.events.cpp.send-recv");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Send with Builder ---

TEST_F(EventsIntegrationTest, SendWithBuilder) {
    std::promise<kubemq::EventReceive> received;
    auto sub_or = client_->SubscribeToEvents(
        "test.events.cpp.builder", "",
        [&](const kubemq::EventReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto event_or = kubemq::Event::Builder()
        .SetChannel("test.events.cpp.builder")
        .SetBody("builder-body")
        .SetMetadata("builder-meta")
        .AddTag("env", "test")
        .Build();
    ASSERT_TRUE(event_or.ok());

    auto status = client_->SendEvent(*event_or);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto recv = future.get();
    EXPECT_EQ(recv.body, "builder-body");
    EXPECT_EQ(recv.metadata, "builder-meta");
    EXPECT_EQ(recv.tags.at("env"), "test");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Event Stream ---

TEST_F(EventsIntegrationTest, StreamSendAndReceive) {
    std::promise<kubemq::EventReceive> received;
    auto sub_or = client_->SubscribeToEvents(
        "test.events.cpp.stream", "",
        [&](const kubemq::EventReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto stream_or = client_->SendEventStream();
    ASSERT_TRUE(stream_or.ok()) << stream_or.status().ToString();
    auto& stream = *stream_or;

    auto event_or = kubemq::Event::Builder()
        .SetChannel("test.events.cpp.stream")
        .SetBody("stream-body")
        .Build();
    ASSERT_TRUE(event_or.ok());

    auto status = stream->Send(*event_or);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto recv = future.get();
    EXPECT_EQ(recv.body, "stream-body");

    stream->Close();
    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Multiple Events ---

TEST_F(EventsIntegrationTest, SendMultipleEvents) {
    constexpr int kNumEvents = 5;
    std::atomic<int> received_count{0};
    std::promise<void> all_received;

    auto sub_or = client_->SubscribeToEvents(
        "test.events.cpp.multi", "",
        [&](const kubemq::EventReceive& event) {
            if (++received_count >= kNumEvents) {
                all_received.set_value();
            }
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (int i = 0; i < kNumEvents; ++i) {
        auto status = client_->PublishEvent(
            "test.events.cpp.multi",
            "event-" + std::to_string(i));
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    auto future = all_received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "Timed out waiting for all events, received: " << received_count.load();

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}

// --- Subscription with Group ---

TEST_F(EventsIntegrationTest, SubscribeWithGroup) {
    std::promise<kubemq::EventReceive> received;
    auto sub_or = client_->SubscribeToEvents(
        "test.events.cpp.group", "test-group",
        [&](const kubemq::EventReceive& event) {
            received.set_value(event);
        },
        [](const kubemq::Status& err) {
            FAIL() << "Subscription error: " << err.ToString();
        });
    ASSERT_TRUE(sub_or.ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto status = client_->PublishEvent("test.events.cpp.group", "group-body");
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto future = received.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto recv = future.get();
    EXPECT_EQ(recv.body, "group-body");

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
}
