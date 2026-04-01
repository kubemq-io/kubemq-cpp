// src/internal/transport/tls.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "kubemq/status.h"
#include "kubemq/tls_config.h"

namespace kubemq {
namespace internal {

// Build gRPC channel credentials from a TlsConfig.
// Returns SslCredentials for TLS/mTLS or InsecureChannelCredentials if TLS is disabled.
// Returns an error Status if TLS files cannot be read or configuration is invalid.
StatusOr<std::shared_ptr<grpc::ChannelCredentials>> BuildChannelCredentials(
    const TlsConfig& config);

}  // namespace internal
}  // namespace kubemq
