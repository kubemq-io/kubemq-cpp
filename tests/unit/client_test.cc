#include <gtest/gtest.h>

#include <string>

#include "kubemq/client_options.h"
#include "kubemq/error_code.h"
#include "kubemq/status.h"

namespace kubemq {
namespace {

// --- ClientOptions validation ---

TEST(ClientOptionsTest, DefaultValues) {
    ClientOptions opts;
    // Default address is localhost:50000
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST(ClientOptionsTest, EmptyAddressFails) {
    ClientOptions opts;
    opts.set_address("");
    auto status = opts.Validate();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::kValidation);
}

TEST(ClientOptionsTest, ValidAddressHostPort) {
    ClientOptions opts;
    opts.set_address("kubemq.example.com", 50000);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST(ClientOptionsTest, ValidAddressString) {
    ClientOptions opts;
    opts.set_address("kubemq.example.com:50000");
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok()) << status.ToString();
}

// --- Client ID ---

TEST(ClientOptionsTest, SetClientId) {
    ClientOptions opts;
    opts.set_client_id("test-client");
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Auth token ---

TEST(ClientOptionsTest, SetAuthToken) {
    ClientOptions opts;
    opts.set_auth_token("my-secret-token");
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Connection timeout ---

TEST(ClientOptionsTest, SetConnectionTimeout) {
    ClientOptions opts;
    opts.set_connection_timeout(std::chrono::milliseconds(5000));
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Reconnect policy ---

TEST(ClientOptionsTest, SetReconnectPolicy) {
    ClientOptions opts;
    ReconnectPolicy policy;
    policy.max_attempts = 5;
    policy.initial_delay = std::chrono::milliseconds(500);
    opts.set_reconnect_policy(policy);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Retry policy ---

TEST(ClientOptionsTest, SetRetryPolicy) {
    ClientOptions opts;
    RetryPolicy policy;
    policy.max_retries = 5;
    policy.initial_backoff = std::chrono::milliseconds(200);
    opts.set_retry_policy(policy);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- TLS config ---

TEST(ClientOptionsTest, SetTlsConfig) {
    ClientOptions opts;
    auto tls = TlsConfig::FromCertFile("/path/to/ca.pem");
    opts.set_tls_config(tls);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Message size limits ---

TEST(ClientOptionsTest, SetMaxReceiveMessageSize) {
    ClientOptions opts;
    opts.set_max_receive_message_size(8 * 1024 * 1024);  // 8MB
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

TEST(ClientOptionsTest, SetMaxSendMessageSize) {
    ClientOptions opts;
    opts.set_max_send_message_size(200 * 1024 * 1024);  // 200MB
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Subscription buffer size ---

TEST(ClientOptionsTest, SetSubscriptionBufferSize) {
    ClientOptions opts;
    opts.set_subscription_buffer_size(100);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Keepalive settings ---

TEST(ClientOptionsTest, SetKeepaliveSettings) {
    ClientOptions opts;
    opts.set_keepalive_time(std::chrono::milliseconds(5000));
    opts.set_keepalive_timeout(std::chrono::milliseconds(10000));
    opts.set_permit_keepalive_without_stream(true);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Default channel ---

TEST(ClientOptionsTest, SetDefaultChannel) {
    ClientOptions opts;
    opts.set_default_channel("my-default-channel");
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Drain timeout ---

TEST(ClientOptionsTest, SetDrainTimeout) {
    ClientOptions opts;
    opts.set_drain_timeout(std::chrono::milliseconds(10000));
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Wait for ready ---

TEST(ClientOptionsTest, SetWaitForReady) {
    ClientOptions opts;
    opts.set_wait_for_ready(false);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Check connection ---

TEST(ClientOptionsTest, SetCheckConnection) {
    ClientOptions opts;
    opts.set_check_connection(false);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- State callbacks (just verify no crash) ---

TEST(ClientOptionsTest, SetStateCallbacks) {
    ClientOptions opts;
    opts.set_on_connected([]() {});
    opts.set_on_disconnected([]() {});
    opts.set_on_reconnecting([]() {});
    opts.set_on_reconnected([]() {});
    opts.set_on_closed([]() {});
    opts.set_on_buffer_drain([](int) {});
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

// --- Callback settings ---

TEST(ClientOptionsTest, SetMaxConcurrentCallbacks) {
    ClientOptions opts;
    opts.set_max_concurrent_callbacks(4);
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

TEST(ClientOptionsTest, SetCallbackTimeout) {
    ClientOptions opts;
    opts.set_callback_timeout(std::chrono::milliseconds(60000));
    auto status = opts.Validate();
    EXPECT_TRUE(status.ok());
}

}  // namespace
}  // namespace kubemq
