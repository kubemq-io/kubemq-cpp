// src/internal/middleware/auth.cc
#include "internal/middleware/auth.h"

namespace kubemq {
namespace internal {

Status InjectAuthToken(grpc::ClientContext* context, const std::string& auth_token,
                       const std::shared_ptr<CredentialProvider>& provider) {
    if (context == nullptr) {
        return Status(ErrorCode::kValidation, "ClientContext is null");
    }

    // Try static token first.
    if (!auth_token.empty()) {
        context->AddMetadata("authorization", auth_token);
        return Status();
    }

    // Try credential provider.
    if (provider) {
        auto token_result = provider->GetToken();
        if (!token_result.ok()) {
            return token_result.status();
        }
        if (!token_result->empty()) {
            context->AddMetadata("authorization", *token_result);
        }
        return Status();
    }

    // No auth configured -- nothing to inject.
    return Status();
}

// AuthPlugin implements grpc::MetadataCredentialsPlugin to automatically
// attach an auth token to outgoing RPCs.
class AuthPlugin : public grpc::MetadataCredentialsPlugin {
public:
    AuthPlugin(std::string auth_token, std::shared_ptr<CredentialProvider> provider)
        : auth_token_(std::move(auth_token)), provider_(std::move(provider)) {}

    grpc::Status GetMetadata(grpc::string_ref /*service_url*/, grpc::string_ref /*method_name*/,
                             const grpc::AuthContext& /*channel_auth_context*/,
                             std::multimap<grpc::string, grpc::string>* metadata) override {
        std::string token;

        if (!auth_token_.empty()) {
            token = auth_token_;
        } else if (provider_) {
            auto result = provider_->GetToken();
            if (result.ok()) {
                token = *result;
            }
        }

        if (!token.empty() && metadata != nullptr) {
            metadata->insert(std::make_pair("authorization", token));
        }

        return grpc::Status::OK;
    }

    bool IsBlocking() const override {
        // If using a credential provider, it might block.
        return provider_ != nullptr;
    }

    grpc::string DebugString() override { return "KubeMQ Auth Plugin"; }

private:
    std::string auth_token_;
    std::shared_ptr<CredentialProvider> provider_;
};

std::shared_ptr<grpc::MetadataCredentialsPlugin> MakeAuthPlugin(
    const std::string& auth_token, const std::shared_ptr<CredentialProvider>& provider) {
    return std::make_shared<AuthPlugin>(auth_token, provider);
}

}  // namespace internal
}  // namespace kubemq
