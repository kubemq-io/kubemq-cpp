// src/internal/middleware/auth.h
#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

#include "kubemq/credential_provider.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// Inject the auth token into a gRPC ClientContext's metadata.
// Uses the static token if set, otherwise queries the credential provider.
// Does nothing if neither is configured.
Status InjectAuthToken(grpc::ClientContext* context, const std::string& auth_token,
                       const std::shared_ptr<CredentialProvider>& provider);

// Create a gRPC metadata credentials processor that automatically injects
// the auth token into every RPC call.
std::shared_ptr<grpc::MetadataCredentialsPlugin> MakeAuthPlugin(
    const std::string& auth_token, const std::shared_ptr<CredentialProvider>& provider);

}  // namespace internal
}  // namespace kubemq
