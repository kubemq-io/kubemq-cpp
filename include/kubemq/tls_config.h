/// @file tls_config.h
/// @brief TLS and mTLS configuration for secure broker connections.

// include/kubemq/tls_config.h
#pragma once

#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Configures TLS or mutual-TLS (mTLS) for the broker connection.
///
/// Use the static factory methods to create a TlsConfig instance:
/// - FromCertFile() / FromPem() for server-side TLS (client verifies server).
/// - FromMTLS() / FromMTLSPem() for mutual TLS (both sides verify each other).
///
/// Pass the resulting object to ClientOptions::set_tls() before connecting.
///
/// @see ClientOptions
/// @example tls/tls_setup/main.cc
/// @example tls/mtls_setup/main.cc
class KUBEMQ_API TlsConfig {
public:
    /// @brief Default constructor. Creates a disabled (plaintext) configuration.
    TlsConfig() = default;

    // -----------------------------------------------------------------------
    // Factory methods
    // -----------------------------------------------------------------------

    /// @brief Creates a TLS config from a CA certificate file on disk.
    /// @param ca_cert_file Path to the PEM-encoded CA certificate file.
    /// @return A TlsConfig with server-side TLS enabled.
    static TlsConfig FromCertFile(const std::string& ca_cert_file);

    /// @brief Creates a TLS config from a PEM-encoded CA certificate string.
    /// @param ca_pem The CA certificate in PEM format.
    /// @return A TlsConfig with server-side TLS enabled.
    static TlsConfig FromPem(const std::string& ca_pem);

    /// @brief Creates a mutual-TLS config from certificate files on disk.
    /// @param cert_file Path to the client certificate PEM file.
    /// @param key_file Path to the client private key PEM file.
    /// @param ca_file Path to the CA certificate PEM file.
    /// @return A TlsConfig with mutual TLS enabled.
    static TlsConfig FromMTLS(const std::string& cert_file, const std::string& key_file,
                              const std::string& ca_file);

    /// @brief Creates a mutual-TLS config from PEM-encoded strings.
    /// @param cert_pem Client certificate in PEM format.
    /// @param key_pem Client private key in PEM format.
    /// @param ca_pem CA certificate in PEM format.
    /// @return A TlsConfig with mutual TLS enabled.
    static TlsConfig FromMTLSPem(const std::string& cert_pem, const std::string& key_pem,
                                 const std::string& ca_pem);

    // -----------------------------------------------------------------------
    // Options
    // -----------------------------------------------------------------------

    /// @brief Overrides the server name used for TLS verification.
    /// @param name The expected server name (SNI). Use when the broker
    ///   certificate CN does not match the connection address.
    void set_server_name_override(const std::string& name);

    /// @brief Skips server certificate verification.
    /// @param skip true to disable verification (insecure; for testing only).
    void set_insecure_skip_verify(bool skip);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// @brief Returns the CA certificate file path.
    /// @return File path, or empty string if not set.
    const std::string& ca_cert_file() const;

    /// @brief Returns the client certificate file path.
    /// @return File path, or empty string if not using mTLS files.
    const std::string& cert_file() const;

    /// @brief Returns the client private key file path.
    /// @return File path, or empty string if not using mTLS files.
    const std::string& key_file() const;

    /// @brief Returns the CA certificate PEM string.
    /// @return PEM string, or empty string if not set.
    const std::string& ca_pem() const;

    /// @brief Returns the client certificate PEM string.
    /// @return PEM string, or empty string if not using mTLS PEM.
    const std::string& cert_pem() const;

    /// @brief Returns the client private key PEM string.
    /// @return PEM string, or empty string if not using mTLS PEM.
    const std::string& key_pem() const;

    /// @brief Returns the server name override.
    /// @return Server name override, or empty string if not set.
    const std::string& server_name_override() const;

    /// @brief Returns whether server certificate verification is skipped.
    /// @return true if verification is disabled.
    bool insecure_skip_verify() const;

    /// @brief Returns whether mutual TLS is configured.
    /// @return true if mTLS client credentials are present.
    bool is_mtls() const;

    /// @brief Returns whether TLS is enabled.
    /// @return true if any TLS configuration has been applied.
    bool is_enabled() const;

private:
    std::string ca_cert_file_;
    std::string cert_file_;
    std::string key_file_;
    std::string ca_pem_;
    std::string cert_pem_;
    std::string key_pem_;
    std::string server_name_override_;
    bool insecure_skip_verify_ = false;
    bool enabled_ = false;
    bool mtls_ = false;
};

}  // namespace v1
}  // namespace kubemq
