// src/client_options.cc
#include "kubemq/client_options.h"

#include <sstream>

namespace kubemq {
inline namespace v1 {

ClientOptions::ClientOptions() = default;

// --- Address ---

void ClientOptions::set_address(const std::string& host, int port) {
    host_ = host;
    port_ = port;
}

void ClientOptions::set_address(const std::string& address) {
    auto pos = address.rfind(':');
    if (pos != std::string::npos && pos < address.size() - 1) {
        host_ = address.substr(0, pos);
        try {
            port_ = std::stoi(address.substr(pos + 1));
        } catch (...) {
            port_ = 0;
        }
    } else {
        host_ = address;
        port_ = 0;
    }
}

// --- Identity ---

void ClientOptions::set_client_id(const std::string& id) {
    client_id_ = id;
}

void ClientOptions::set_auth_token(const std::string& token) {
    auth_token_ = token;
}

void ClientOptions::set_credential_provider(std::shared_ptr<CredentialProvider> provider) {
    credential_provider_ = std::move(provider);
}

// --- TLS ---

void ClientOptions::set_tls_config(const TlsConfig& config) {
    tls_config_ = config;
}

// --- Connection ---

void ClientOptions::set_connection_timeout(std::chrono::milliseconds timeout) {
    connection_timeout_ = timeout;
}

void ClientOptions::set_reconnect_policy(const ReconnectPolicy& policy) {
    reconnect_policy_ = policy;
}

void ClientOptions::set_max_receive_message_size(int bytes) {
    max_receive_message_size_ = bytes;
}

void ClientOptions::set_max_send_message_size(int bytes) {
    max_send_message_size_ = bytes;
}

void ClientOptions::set_wait_for_ready(bool wait) {
    wait_for_ready_ = wait;
}

void ClientOptions::set_check_connection(bool check) {
    check_connection_ = check;
}

// --- Keepalive ---

void ClientOptions::set_keepalive_time(std::chrono::milliseconds time) {
    keepalive_time_ = time;
}

void ClientOptions::set_keepalive_timeout(std::chrono::milliseconds timeout) {
    keepalive_timeout_ = timeout;
}

void ClientOptions::set_permit_keepalive_without_stream(bool permit) {
    permit_keepalive_without_stream_ = permit;
}

// --- Drain ---

void ClientOptions::set_drain_timeout(std::chrono::milliseconds timeout) {
    drain_timeout_ = timeout;
}

// --- Subscription buffering ---

void ClientOptions::set_subscription_buffer_size(int size) {
    subscription_buffer_size_ = size;
}

// --- Retry ---

void ClientOptions::set_retry_policy(const RetryPolicy& policy) {
    retry_policy_ = policy;
}

// --- Defaults ---

void ClientOptions::set_default_channel(const std::string& channel) {
    default_channel_ = channel;
}

void ClientOptions::set_default_cache_ttl(std::chrono::milliseconds ttl) {
    default_cache_ttl_ = ttl;
}

// --- State callbacks ---

void ClientOptions::set_on_connected(std::function<void()> callback) {
    on_connected_ = std::move(callback);
}

void ClientOptions::set_on_disconnected(std::function<void()> callback) {
    on_disconnected_ = std::move(callback);
}

void ClientOptions::set_on_reconnecting(std::function<void()> callback) {
    on_reconnecting_ = std::move(callback);
}

void ClientOptions::set_on_reconnected(std::function<void()> callback) {
    on_reconnected_ = std::move(callback);
}

void ClientOptions::set_on_closed(std::function<void()> callback) {
    on_closed_ = std::move(callback);
}

void ClientOptions::set_on_buffer_drain(std::function<void(int discarded_count)> callback) {
    on_buffer_drain_ = std::move(callback);
}

// --- Callback concurrency ---

void ClientOptions::set_max_concurrent_callbacks(int n) {
    max_concurrent_callbacks_ = n;
}

void ClientOptions::set_callback_timeout(std::chrono::milliseconds timeout) {
    callback_timeout_ = timeout;
}

// --- Logging ---

void ClientOptions::set_logger(std::shared_ptr<Logger> logger) {
    logger_ = std::move(logger);
}

// --- Observability ---

void ClientOptions::set_tracer_provider(void* provider) {
    tracer_provider_ = provider;
}

void ClientOptions::set_meter_provider(void* provider) {
    meter_provider_ = provider;
}

// --- Validation ---

Status ClientOptions::Validate() const {
    if (host_.empty()) {
        return Status(ErrorCode::kValidation, "host is required", "ClientOptions::Validate", "",
                      "");
    }
    if (port_ <= 0) {
        return Status(ErrorCode::kValidation, "port must be > 0", "ClientOptions::Validate", "",
                      "");
    }
    if (port_ > 65535) {
        return Status(ErrorCode::kValidation, "port must be <= 65535", "ClientOptions::Validate",
                      "", "");
    }
    if (subscription_buffer_size_ <= 0) {
        return Status(ErrorCode::kValidation, "subscription_buffer_size must be > 0",
                      "ClientOptions::Validate", "", "");
    }
    if (max_receive_message_size_ <= 0) {
        return Status(ErrorCode::kValidation, "max_receive_message_size must be > 0",
                      "ClientOptions::Validate", "", "");
    }
    if (max_send_message_size_ <= 0) {
        return Status(ErrorCode::kValidation, "max_send_message_size must be > 0",
                      "ClientOptions::Validate", "", "");
    }
    if (connection_timeout_.count() <= 0) {
        return Status(ErrorCode::kValidation, "connection_timeout must be > 0",
                      "ClientOptions::Validate", "", "");
    }
    if (max_concurrent_callbacks_ <= 0) {
        return Status(ErrorCode::kValidation, "max_concurrent_callbacks must be > 0",
                      "ClientOptions::Validate", "", "");
    }
    if (retry_policy_.max_retries < 0 || retry_policy_.max_retries > 10) {
        return Status(ErrorCode::kValidation, "retry_policy max_retries must be 0-10",
                      "ClientOptions::Validate", "", "");
    }
    return Status();
}

}  // namespace v1
}  // namespace kubemq
