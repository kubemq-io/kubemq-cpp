// src/internal/transport/keepalive.h
#pragma once

#include <grpcpp/grpcpp.h>

#include "kubemq/client_options.h"

namespace kubemq {
namespace internal {

// Apply keepalive settings from ClientOptions to gRPC ChannelArguments.
// Sets GRPC_ARG_KEEPALIVE_TIME_MS, GRPC_ARG_KEEPALIVE_TIMEOUT_MS, and
// GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS.
void ApplyKeepalive(grpc::ChannelArguments& args, const ClientOptions& opts);

}  // namespace internal
}  // namespace kubemq
