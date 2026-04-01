// src/internal/transport/tls.cc
#include "internal/transport/tls.h"

#include <fstream>
#include <string>

#include "internal/types/error.h"

namespace kubemq {
namespace internal {

namespace {

// Read an entire file into a string. Returns an error Status on failure.
StatusOr<std::string> ReadFile(const std::string& path) {
    if (path.empty()) {
        return MakeError(ErrorCode::kValidation, "TLS file path is empty", "ReadFile");
    }
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return MakeError(ErrorCode::kValidation, "failed to open TLS file: " + path, "ReadFile");
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    if (content.empty()) {
        return MakeError(ErrorCode::kValidation, "TLS file is empty: " + path, "ReadFile");
    }
    return content;
}

}  // namespace

StatusOr<std::shared_ptr<grpc::ChannelCredentials>> BuildChannelCredentials(
    const TlsConfig& config) {
    if (!config.is_enabled()) {
        return grpc::InsecureChannelCredentials();
    }

    if (config.insecure_skip_verify()) {
        return MakeError(ErrorCode::kValidation,
                         "insecure_skip_verify is not supported in the C++ SDK. "
                         "Use a custom root CA certificate instead.",
                         "TlsConfig::Validate");
    }

    grpc::SslCredentialsOptions ssl_opts;

    // CA certificate: prefer PEM content, fall back to file path.
    if (!config.ca_pem().empty()) {
        ssl_opts.pem_root_certs = config.ca_pem();
    } else if (!config.ca_cert_file().empty()) {
        auto ca_or = ReadFile(config.ca_cert_file());
        if (!ca_or.ok())
            return ca_or.status();
        ssl_opts.pem_root_certs = std::move(*ca_or);
    }

    // mTLS: client certificate and key.
    if (config.is_mtls()) {
        if (!config.cert_pem().empty()) {
            ssl_opts.pem_cert_chain = config.cert_pem();
        } else if (!config.cert_file().empty()) {
            auto cert_or = ReadFile(config.cert_file());
            if (!cert_or.ok())
                return cert_or.status();
            ssl_opts.pem_cert_chain = std::move(*cert_or);
        }

        if (!config.key_pem().empty()) {
            ssl_opts.pem_private_key = config.key_pem();
        } else if (!config.key_file().empty()) {
            auto key_or = ReadFile(config.key_file());
            if (!key_or.ok())
                return key_or.status();
            ssl_opts.pem_private_key = std::move(*key_or);
        }
    }

    return grpc::SslCredentials(ssl_opts);
}

}  // namespace internal
}  // namespace kubemq
