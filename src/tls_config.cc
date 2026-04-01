// src/tls_config.cc
#include "kubemq/tls_config.h"

namespace kubemq {
inline namespace v1 {

TlsConfig TlsConfig::FromCertFile(const std::string& ca_cert_file) {
    TlsConfig config;
    config.ca_cert_file_ = ca_cert_file;
    config.enabled_ = true;
    return config;
}

TlsConfig TlsConfig::FromPem(const std::string& ca_pem) {
    TlsConfig config;
    config.ca_pem_ = ca_pem;
    config.enabled_ = true;
    return config;
}

TlsConfig TlsConfig::FromMTLS(const std::string& cert_file, const std::string& key_file,
                              const std::string& ca_file) {
    TlsConfig config;
    config.cert_file_ = cert_file;
    config.key_file_ = key_file;
    config.ca_cert_file_ = ca_file;
    config.enabled_ = true;
    config.mtls_ = true;
    return config;
}

TlsConfig TlsConfig::FromMTLSPem(const std::string& cert_pem, const std::string& key_pem,
                                 const std::string& ca_pem) {
    TlsConfig config;
    config.cert_pem_ = cert_pem;
    config.key_pem_ = key_pem;
    config.ca_pem_ = ca_pem;
    config.enabled_ = true;
    config.mtls_ = true;
    return config;
}

void TlsConfig::set_server_name_override(const std::string& name) {
    server_name_override_ = name;
}

void TlsConfig::set_insecure_skip_verify(bool skip) {
    insecure_skip_verify_ = skip;
}

const std::string& TlsConfig::ca_cert_file() const {
    return ca_cert_file_;
}
const std::string& TlsConfig::cert_file() const {
    return cert_file_;
}
const std::string& TlsConfig::key_file() const {
    return key_file_;
}
const std::string& TlsConfig::ca_pem() const {
    return ca_pem_;
}
const std::string& TlsConfig::cert_pem() const {
    return cert_pem_;
}
const std::string& TlsConfig::key_pem() const {
    return key_pem_;
}
const std::string& TlsConfig::server_name_override() const {
    return server_name_override_;
}
bool TlsConfig::insecure_skip_verify() const {
    return insecure_skip_verify_;
}
bool TlsConfig::is_mtls() const {
    return mtls_;
}
bool TlsConfig::is_enabled() const {
    return enabled_;
}

}  // namespace v1
}  // namespace kubemq
