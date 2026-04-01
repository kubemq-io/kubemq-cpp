/// @file credential_provider.h
/// @brief Authentication token providers for broker connections.

// include/kubemq/credential_provider.h
#pragma once

#include <memory>
#include <string>

#include "kubemq/export.h"
#include "kubemq/status.h"

namespace kubemq {
inline namespace v1 {

/// @brief Abstract interface for providing authentication tokens.
///
/// Implement this interface to supply dynamic credentials (e.g., from a vault
/// or token rotation service). Pass an instance to
/// ClientOptions::set_credential_provider() before connecting.
///
/// @note Implementations must be thread-safe; GetToken() may be called from
///   multiple threads during reconnection.
/// @see ClientOptions
/// @see StaticTokenProvider
/// @example connection/token_auth/main.cc
class KUBEMQ_API CredentialProvider {
public:
    /// @brief Virtual destructor.
    virtual ~CredentialProvider() = default;

    /// @brief Retrieves the current authentication token.
    /// @return The token string on success, or an error status if the token
    ///   cannot be obtained.
    virtual StatusOr<std::string> GetToken() = 0;
};

/// @brief A credential provider that returns a fixed token.
///
/// Suitable for environments where the authentication token does not change
/// during the client lifetime (e.g., development or static API keys).
///
/// @see CredentialProvider
/// @example connection/token_auth/main.cc
class KUBEMQ_API StaticTokenProvider : public CredentialProvider {
public:
    /// @brief Constructs a provider with the given static token.
    /// @param token The authentication token string.
    explicit StaticTokenProvider(const std::string& token);

    /// @brief Returns the static token.
    /// @return The token string provided at construction time.
    StatusOr<std::string> GetToken() override;

private:
    std::string token_;
};

}  // namespace v1
}  // namespace kubemq
