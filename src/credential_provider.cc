// src/credential_provider.cc
#include "kubemq/credential_provider.h"

namespace kubemq {
inline namespace v1 {

StaticTokenProvider::StaticTokenProvider(const std::string& token) : token_(token) {}

StatusOr<std::string> StaticTokenProvider::GetToken() {
    return token_;
}

}  // namespace v1
}  // namespace kubemq
