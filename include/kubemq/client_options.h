/// @file client_options.h
/// @brief Configuration options for connecting to a KubeMQ broker.
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include "kubemq/connection_state.h"
#include "kubemq/credential_provider.h"
#include "kubemq/export.h"
#include "kubemq/logger.h"
#include "kubemq/reconnect_policy.h"
#include "kubemq/retry_policy.h"
#include "kubemq/status.h"
#include "kubemq/tls_config.h"
#include "kubemq/types.h"

namespace kubemq {
inline namespace v1 {

/// @brief Configuration options for connecting to a KubeMQ broker.
///
/// Controls address, authentication, TLS, timeouts, reconnection, keepalive,
/// callbacks, and logging. Pass to Client::Create() to establish a connection.
///
/// @see Client::Create
/// @example connection/connect/main.cc
class KUBEMQ_API ClientOptions {
public:
    /// @brief Construct default options (localhost:50000, no auth, no TLS).
    ClientOptions();

    /// @brief Set broker address as host and port.
    /// @param host Hostname or IP address.
    /// @param port Port number (default: 50000).
    void set_address(const std::string& host, int port);
    /// @brief Set broker address as "host:port" string.
    /// @param address Address in "host:port" format.
    void set_address(const std::string& address);  // "host:port"

    /// @brief Set unique client identifier.
    /// @param id Client ID (auto-generated if empty).
    void set_client_id(const std::string& id);
    /// @brief Set static authentication token.
    /// @param token JWT or API key.
    void set_auth_token(const std::string& token);
    /// @brief Set dynamic credential provider for token refresh.
    /// @param provider Shared pointer to a CredentialProvider implementation.
    /// @see CredentialProvider
    void set_credential_provider(std::shared_ptr<CredentialProvider> provider);

    /// @brief Enable TLS with the given configuration.
    /// @param config TLS/mTLS settings.
    /// @see TlsConfig
    void set_tls_config(const TlsConfig& config);

    /// @brief Set initial connection timeout (default: 10s).
    /// @param timeout Maximum wait duration for initial connect.
    void set_connection_timeout(std::chrono::milliseconds timeout);
    /// @brief Set reconnection behavior after disconnect.
    /// @param policy Reconnection parameters.
    /// @see ReconnectPolicy
    void set_reconnect_policy(const ReconnectPolicy& policy);
    /// @brief Set max inbound message size (default: 4MB).
    /// @param bytes Maximum size in bytes.
    void set_max_receive_message_size(int bytes);
    /// @brief Set max outbound message size (default: 100MB).
    /// @param bytes Maximum size in bytes.
    void set_max_send_message_size(int bytes);
    /// @brief Enable gRPC wait-for-ready semantics (default: true).
    /// @param wait true to queue RPCs until connected.
    void set_wait_for_ready(bool wait);
    /// @brief Ping broker on connect to verify connectivity (default: true).
    /// @param check true to verify connectivity on connect.
    void set_check_connection(bool check);

    /// @brief Set gRPC keepalive ping interval (default: 10s).
    /// @param time Interval between keepalive pings.
    void set_keepalive_time(std::chrono::milliseconds time);
    /// @brief Set gRPC keepalive timeout (default: 20s).
    /// @param timeout Maximum wait for ping response.
    void set_keepalive_timeout(std::chrono::milliseconds timeout);
    /// @brief Allow keepalive pings with no active streams (default: true).
    /// @param permit true to allow keepalive without streams.
    void set_permit_keepalive_without_stream(bool permit);

    /// @brief Set timeout for draining pending operations on close (default: 5s).
    /// @param timeout Drain timeout duration.
    void set_drain_timeout(std::chrono::milliseconds timeout);

    /// @brief Set per-subscription delivery buffer size (default: 10).
    /// @param size Buffer capacity in messages.
    void set_subscription_buffer_size(int size);  // default: 10 (per-subscription delivery buffer)

    /// @brief Set retry policy for failed operations.
    /// @param policy Retry parameters.
    /// @see RetryPolicy
    void set_retry_policy(const RetryPolicy& policy);

    /// @brief Set default channel for operations that omit channel.
    /// @param channel Default channel name.
    void set_default_channel(const std::string& channel);
    /// @brief Set default cache TTL for queries (default: 0 = disabled).
    /// @param ttl Cache duration.
    void set_default_cache_ttl(std::chrono::milliseconds ttl);

    /// @brief Set callback invoked when connection is established.
    /// @param callback Connection established callback.
    void set_on_connected(std::function<void()> callback);
    /// @brief Set callback invoked when connection is lost.
    /// @param callback Disconnection callback.
    void set_on_disconnected(std::function<void()> callback);
    /// @brief Set callback invoked when reconnection attempt starts.
    /// @param callback Reconnecting callback.
    void set_on_reconnecting(std::function<void()> callback);
    /// @brief Set callback invoked when reconnection succeeds.
    /// @param callback Reconnected callback.
    void set_on_reconnected(std::function<void()> callback);
    /// @brief Set callback invoked when client is permanently closed.
    /// @param callback Closed callback.
    void set_on_closed(std::function<void()> callback);
    /// @brief Set callback invoked when reconnect buffer overflows.
    /// @param callback Receives the count of discarded messages.
    void set_on_buffer_drain(std::function<void(int discarded_count)> callback);

    /// @brief Set max concurrent subscription callback invocations (default: 1).
    /// @param n Concurrency limit.
    void set_max_concurrent_callbacks(int n);
    /// @brief Set max duration for a single callback invocation (default: 30s).
    /// @param timeout Callback timeout duration.
    void set_callback_timeout(std::chrono::milliseconds timeout);

    /// @brief Set custom logger implementation.
    /// @param logger Shared pointer to a Logger implementation.
    /// @see Logger
    void set_logger(std::shared_ptr<Logger> logger);

    /// @brief Set OpenTelemetry TracerProvider (requires KUBEMQ_ENABLE_OTEL).
    /// @param provider Pointer to a TracerProvider instance.
    void set_tracer_provider(void* provider);
    /// @brief Set OpenTelemetry MeterProvider (requires KUBEMQ_ENABLE_OTEL).
    /// @param provider Pointer to a MeterProvider instance.
    void set_meter_provider(void* provider);

    /// @brief Validate all options and return error status if invalid.
    /// @return OK Status if valid, or an error describing the validation failure.
    [[nodiscard]] Status Validate() const;

    // -- Accessors (internal use) --
    /// @brief Get the configured host.
    const std::string& host() const { return host_; }
    /// @brief Get the configured port.
    int port() const { return port_; }
    /// @brief Get the configured client ID.
    const std::string& client_id() const { return client_id_; }
    /// @brief Get a mutable reference to the client ID.
    std::string& mutable_client_id() { return client_id_; }
    /// @brief Get the configured auth token.
    const std::string& auth_token() const { return auth_token_; }
    /// @brief Get the configured credential provider.
    const std::shared_ptr<CredentialProvider>& credential_provider() const {
        return credential_provider_;
    }
    /// @brief Get the configured TLS config.
    const TlsConfig& tls_config() const { return tls_config_; }
    /// @brief Get the configured connection timeout.
    std::chrono::milliseconds connection_timeout() const { return connection_timeout_; }
    /// @brief Get the configured reconnect policy.
    const ReconnectPolicy& reconnect_policy() const { return reconnect_policy_; }
    /// @brief Get the configured max receive message size.
    int max_receive_message_size() const { return max_receive_message_size_; }
    /// @brief Get the configured max send message size.
    int max_send_message_size() const { return max_send_message_size_; }
    /// @brief Get the configured wait-for-ready flag.
    bool wait_for_ready() const { return wait_for_ready_; }
    /// @brief Get the configured check-connection flag.
    bool check_connection() const { return check_connection_; }
    /// @brief Get the configured keepalive time.
    std::chrono::milliseconds keepalive_time() const { return keepalive_time_; }
    /// @brief Get the configured keepalive timeout.
    std::chrono::milliseconds keepalive_timeout() const { return keepalive_timeout_; }
    /// @brief Get the configured permit-keepalive-without-stream flag.
    bool permit_keepalive_without_stream() const { return permit_keepalive_without_stream_; }
    /// @brief Get the configured drain timeout.
    std::chrono::milliseconds drain_timeout() const { return drain_timeout_; }
    /// @brief Get the configured subscription buffer size.
    int subscription_buffer_size() const { return subscription_buffer_size_; }
    /// @brief Get the configured retry policy.
    const RetryPolicy& retry_policy() const { return retry_policy_; }
    /// @brief Get the configured default channel.
    const std::string& default_channel() const { return default_channel_; }
    /// @brief Get the configured default cache TTL.
    std::chrono::milliseconds default_cache_ttl() const { return default_cache_ttl_; }
    /// @brief Get the configured on-connected callback.
    const std::function<void()>& on_connected() const { return on_connected_; }
    /// @brief Get the configured on-disconnected callback.
    const std::function<void()>& on_disconnected() const { return on_disconnected_; }
    /// @brief Get the configured on-reconnecting callback.
    const std::function<void()>& on_reconnecting() const { return on_reconnecting_; }
    /// @brief Get the configured on-reconnected callback.
    const std::function<void()>& on_reconnected() const { return on_reconnected_; }
    /// @brief Get the configured on-closed callback.
    const std::function<void()>& on_closed() const { return on_closed_; }
    /// @brief Get the configured on-buffer-drain callback.
    const std::function<void(int)>& on_buffer_drain() const { return on_buffer_drain_; }
    /// @brief Get the configured max concurrent callbacks.
    int max_concurrent_callbacks() const { return max_concurrent_callbacks_; }
    /// @brief Get the configured callback timeout.
    std::chrono::milliseconds callback_timeout() const { return callback_timeout_; }
    /// @brief Get the configured logger.
    const std::shared_ptr<Logger>& logger() const { return logger_; }
    /// @brief Get the configured tracer provider.
    void* tracer_provider() const { return tracer_provider_; }
    /// @brief Get the configured meter provider.
    void* meter_provider() const { return meter_provider_; }

private:
    std::string host_ = "localhost";
    int port_ = 50000;
    std::string client_id_;
    std::string auth_token_;
    std::shared_ptr<CredentialProvider> credential_provider_;
    TlsConfig tls_config_;
    std::chrono::milliseconds connection_timeout_{kDefaultConnectionTimeout};
    ReconnectPolicy reconnect_policy_;
    int max_receive_message_size_ = kDefaultMaxReceiveMessageSize;
    int max_send_message_size_ = kDefaultMaxSendMessageSize;
    bool wait_for_ready_ = true;
    bool check_connection_ = true;
    std::chrono::milliseconds keepalive_time_{kDefaultKeepaliveTime};
    std::chrono::milliseconds keepalive_timeout_{kDefaultKeepaliveTimeout};
    bool permit_keepalive_without_stream_ = true;
    std::chrono::milliseconds drain_timeout_{kDefaultDrainTimeout};
    int subscription_buffer_size_ = kDefaultSubscriptionBufferSize;
    RetryPolicy retry_policy_;
    std::string default_channel_;
    std::chrono::milliseconds default_cache_ttl_{0};

    // State callbacks
    std::function<void()> on_connected_;
    std::function<void()> on_disconnected_;
    std::function<void()> on_reconnecting_;
    std::function<void()> on_reconnected_;
    std::function<void()> on_closed_;
    std::function<void(int)> on_buffer_drain_;

    int max_concurrent_callbacks_ = kDefaultMaxConcurrentCallbacks;
    std::chrono::milliseconds callback_timeout_{kDefaultCallbackTimeout};

    std::shared_ptr<Logger> logger_;
    void* tracer_provider_ = nullptr;
    void* meter_provider_ = nullptr;
};

}  // namespace v1
}  // namespace kubemq
