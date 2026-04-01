// src/internal/transport/keepalive.cc
#include "internal/transport/keepalive.h"

namespace kubemq {
namespace internal {

void ApplyKeepalive(grpc::ChannelArguments& args, const ClientOptions& opts) {
    // GRPC_ARG_KEEPALIVE_TIME_MS: time between keepalive pings.
    auto keepalive_time_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(opts.keepalive_time()).count());
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, keepalive_time_ms);

    // GRPC_ARG_KEEPALIVE_TIMEOUT_MS: time to wait for keepalive ping ack.
    auto keepalive_timeout_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(opts.keepalive_timeout()).count());
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, keepalive_timeout_ms);

    // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS: allow pings when no active RPCs.
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                opts.permit_keepalive_without_stream() ? 1 : 0);
}

}  // namespace internal
}  // namespace kubemq
