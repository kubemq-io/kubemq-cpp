#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>

#include "kubemq/kubemq.h"

class ChannelsIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-channels-test");
        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Events channel ---

TEST_F(ChannelsIntegrationTest, CreateAndDeleteEventsChannel) {
    const std::string name = "test-cpp-events-ch";

    auto create_status = client_->CreateEventsChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListEventsChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Channel not found in list after create";

    auto delete_status = client_->DeleteEventsChannel(name);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- Events Store channel ---

TEST_F(ChannelsIntegrationTest, CreateAndDeleteEventsStoreChannel) {
    const std::string name = "test-cpp-es-ch";

    auto create_status = client_->CreateEventsStoreChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListEventsStoreChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Events Store channel not found in list";

    auto delete_status = client_->DeleteEventsStoreChannel(name);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- Commands channel ---

TEST_F(ChannelsIntegrationTest, CreateAndDeleteCommandsChannel) {
    const std::string name = "test-cpp-cmd-ch";

    auto create_status = client_->CreateCommandsChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListCommandsChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Commands channel not found in list";

    auto delete_status = client_->DeleteCommandsChannel(name);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- Queries channel ---

TEST_F(ChannelsIntegrationTest, CreateAndDeleteQueriesChannel) {
    const std::string name = "test-cpp-query-ch";

    auto create_status = client_->CreateQueriesChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListQueriesChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Queries channel not found in list";

    auto delete_status = client_->DeleteQueriesChannel(name);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- Queues channel ---

TEST_F(ChannelsIntegrationTest, CreateAndDeleteQueuesChannel) {
    const std::string name = "test-cpp-queue-ch";

    auto create_status = client_->CreateQueuesChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListQueuesChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Queues channel not found in list";

    auto delete_status = client_->DeleteQueuesChannel(name);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- Generic create/delete/list ---

TEST_F(ChannelsIntegrationTest, GenericCreateDeleteList) {
    const std::string name = "test-cpp-generic-ch";

    auto create_status = client_->CreateChannel(name, kubemq::kChannelTypeEvents);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListChannels(kubemq::kChannelTypeEvents, name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    bool found = false;
    for (const auto& ch : *list_result) {
        if (ch.name == name) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);

    auto delete_status = client_->DeleteChannel(name, kubemq::kChannelTypeEvents);
    EXPECT_TRUE(delete_status.ok()) << delete_status.ToString();
}

// --- List with search filter ---

TEST_F(ChannelsIntegrationTest, ListWithSearch) {
    const std::string prefix = "test-cpp-search";

    // Create a couple channels
    client_->CreateEventsChannel(prefix + "-alpha");
    client_->CreateEventsChannel(prefix + "-beta");

    auto list_result = client_->ListEventsChannels(prefix);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();

    // Should find at least our channels
    int found_count = 0;
    for (const auto& ch : *list_result) {
        if (ch.name.find(prefix) == 0) {
            found_count++;
        }
    }
    EXPECT_GE(found_count, 2);

    // Cleanup
    client_->DeleteEventsChannel(prefix + "-alpha");
    client_->DeleteEventsChannel(prefix + "-beta");
}

// --- ChannelInfo fields ---

TEST_F(ChannelsIntegrationTest, ChannelInfoFields) {
    const std::string name = "test-cpp-info-ch";

    auto create_status = client_->CreateEventsChannel(name);
    EXPECT_TRUE(create_status.ok()) << create_status.ToString();

    auto list_result = client_->ListEventsChannels(name);
    ASSERT_TRUE(list_result.ok()) << list_result.status().ToString();
    ASSERT_FALSE(list_result->empty());

    const auto& info = (*list_result)[0];
    EXPECT_FALSE(info.name.empty());
    EXPECT_FALSE(info.type.empty());
    // last_activity, is_active, incoming/outgoing stats may vary

    client_->DeleteEventsChannel(name);
}
