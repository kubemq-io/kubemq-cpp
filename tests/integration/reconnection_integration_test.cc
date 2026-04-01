#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "kubemq/kubemq.h"

class ReconnectionIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        kubemq::ClientOptions opts;
        opts.set_address("localhost", 50000);
        opts.set_client_id("cpp-reconnect-test");
        opts.set_check_connection(true);

        kubemq::ReconnectPolicy reconnect;
        reconnect.max_attempts = 3;
        reconnect.initial_delay = std::chrono::milliseconds(500);
        reconnect.max_delay = std::chrono::milliseconds(5000);
        reconnect.backoff_multiplier = 2.0;
        opts.set_reconnect_policy(reconnect);

        auto client_or = kubemq::Client::Create(opts);
        ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();
        client_ = std::move(*client_or);
    }

    void TearDown() override {
        if (client_) client_->Close();
    }

    std::unique_ptr<kubemq::Client> client_;
};

// --- Basic connectivity ---

TEST_F(ReconnectionIntegrationTest, BasicConnectivity) {
    // Verify the client is connected and operational
    EXPECT_EQ(client_->State(), kubemq::ConnectionState::kReady);
}

// --- Ping after connect ---

TEST_F(ReconnectionIntegrationTest, PingAfterConnect) {
    auto ping_result = client_->Ping();
    ASSERT_TRUE(ping_result.ok()) << ping_result.status().ToString();
    EXPECT_FALSE(ping_result->host.empty());
    EXPECT_FALSE(ping_result->version.empty());
    EXPECT_GT(ping_result->server_start_time, 0);
}

// --- Graceful close ---

TEST_F(ReconnectionIntegrationTest, GracefulClose) {
    auto status = client_->Close();
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(client_->State(), kubemq::ConnectionState::kClosed);
}

// --- Double close is safe ---

TEST_F(ReconnectionIntegrationTest, DoubleCloseIsSafe) {
    auto status1 = client_->Close();
    EXPECT_TRUE(status1.ok()) << status1.ToString();

    auto status2 = client_->Close();
    // Second close should either succeed or return client closed error
    // It should not crash
    EXPECT_EQ(client_->State(), kubemq::ConnectionState::kClosed);
}

// --- State callbacks ---

TEST_F(ReconnectionIntegrationTest, StateCallbacks) {
    bool connected_called = false;

    kubemq::ClientOptions opts;
    opts.set_address("localhost", 50000);
    opts.set_client_id("cpp-reconnect-callbacks");
    opts.set_on_connected([&]() { connected_called = true; });

    auto client_or = kubemq::Client::Create(opts);
    ASSERT_TRUE(client_or.ok()) << client_or.status().ToString();

    // The connected callback should have been called during Create
    // (or shortly after, depending on implementation)
    EXPECT_TRUE(connected_called);

    (*client_or)->Close();
}

// --- Connection to invalid address fails ---
// Note: Full reconnection testing (disconnect/reconnect/buffer drain)
// requires broker restart capability which is deferred.

TEST_F(ReconnectionIntegrationTest, InvalidAddressFails) {
    kubemq::ClientOptions opts;
    opts.set_address("localhost", 59999);  // Wrong port
    opts.set_client_id("cpp-invalid-addr");
    opts.set_connection_timeout(std::chrono::milliseconds(2000));
    opts.set_check_connection(true);

    kubemq::ReconnectPolicy reconnect;
    reconnect.max_attempts = 1;  // Fail fast
    reconnect.initial_delay = std::chrono::milliseconds(100);
    opts.set_reconnect_policy(reconnect);

    auto client_or = kubemq::Client::Create(opts);
    // Should fail because the address is unreachable
    EXPECT_FALSE(client_or.ok());
}
