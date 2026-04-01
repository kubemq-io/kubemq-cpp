#include <gtest/gtest.h>

#include <string>

#include "kubemq/tls_config.h"
#include "internal/transport/tls.h"

namespace kubemq {
namespace {

// --- Default construction ---

TEST(TlsConfigTest, DefaultNotEnabled) {
    TlsConfig config;
    EXPECT_FALSE(config.is_enabled());
    EXPECT_FALSE(config.is_mtls());
    EXPECT_FALSE(config.insecure_skip_verify());
}

TEST(TlsConfigTest, DefaultAccessorsEmpty) {
    TlsConfig config;
    EXPECT_TRUE(config.ca_cert_file().empty());
    EXPECT_TRUE(config.cert_file().empty());
    EXPECT_TRUE(config.key_file().empty());
    EXPECT_TRUE(config.ca_pem().empty());
    EXPECT_TRUE(config.cert_pem().empty());
    EXPECT_TRUE(config.key_pem().empty());
    EXPECT_TRUE(config.server_name_override().empty());
}

// --- FromCertFile ---

TEST(TlsConfigTest, FromCertFile) {
    auto config = TlsConfig::FromCertFile("/path/to/ca.pem");
    EXPECT_TRUE(config.is_enabled());
    EXPECT_FALSE(config.is_mtls());
    EXPECT_EQ(config.ca_cert_file(), "/path/to/ca.pem");
    EXPECT_TRUE(config.cert_file().empty());
    EXPECT_TRUE(config.key_file().empty());
}

// --- FromPem ---

TEST(TlsConfigTest, FromPem) {
    std::string ca_pem = "-----BEGIN CERTIFICATE-----\nFAKEDATA\n-----END CERTIFICATE-----";
    auto config = TlsConfig::FromPem(ca_pem);
    EXPECT_TRUE(config.is_enabled());
    EXPECT_FALSE(config.is_mtls());
    EXPECT_EQ(config.ca_pem(), ca_pem);
    EXPECT_TRUE(config.ca_cert_file().empty());
}

// --- FromMTLS ---

TEST(TlsConfigTest, FromMTLS) {
    auto config = TlsConfig::FromMTLS("/path/cert.pem", "/path/key.pem", "/path/ca.pem");
    EXPECT_TRUE(config.is_enabled());
    EXPECT_TRUE(config.is_mtls());
    EXPECT_EQ(config.cert_file(), "/path/cert.pem");
    EXPECT_EQ(config.key_file(), "/path/key.pem");
    EXPECT_EQ(config.ca_cert_file(), "/path/ca.pem");
}

// --- FromMTLSPem ---

TEST(TlsConfigTest, FromMTLSPem) {
    std::string cert_pem = "CERT_PEM_DATA";
    std::string key_pem = "KEY_PEM_DATA";
    std::string ca_pem = "CA_PEM_DATA";
    auto config = TlsConfig::FromMTLSPem(cert_pem, key_pem, ca_pem);
    EXPECT_TRUE(config.is_enabled());
    EXPECT_TRUE(config.is_mtls());
    EXPECT_EQ(config.cert_pem(), cert_pem);
    EXPECT_EQ(config.key_pem(), key_pem);
    EXPECT_EQ(config.ca_pem(), ca_pem);
}

// --- Accessors ---

TEST(TlsConfigTest, ServerNameOverride) {
    auto config = TlsConfig::FromCertFile("/path/ca.pem");
    config.set_server_name_override("kubemq.example.com");
    EXPECT_EQ(config.server_name_override(), "kubemq.example.com");
}

TEST(TlsConfigTest, InsecureSkipVerify) {
    auto config = TlsConfig::FromCertFile("/path/ca.pem");
    EXPECT_FALSE(config.insecure_skip_verify());
    config.set_insecure_skip_verify(true);
    EXPECT_TRUE(config.insecure_skip_verify());
}

TEST(TlsConfigTest, InsecureSkipVerifyCanBeReset) {
    auto config = TlsConfig::FromCertFile("/path/ca.pem");
    config.set_insecure_skip_verify(true);
    config.set_insecure_skip_verify(false);
    EXPECT_FALSE(config.insecure_skip_verify());
}

// --- is_mtls() ---

TEST(TlsConfigTest, IsMtlsFalseForCertFileOnly) {
    auto config = TlsConfig::FromCertFile("/path/ca.pem");
    EXPECT_FALSE(config.is_mtls());
}

TEST(TlsConfigTest, IsMtlsFalseForPemOnly) {
    auto config = TlsConfig::FromPem("CA_PEM");
    EXPECT_FALSE(config.is_mtls());
}

TEST(TlsConfigTest, IsMtlsTrueForMTLS) {
    auto config = TlsConfig::FromMTLS("cert", "key", "ca");
    EXPECT_TRUE(config.is_mtls());
}

TEST(TlsConfigTest, IsMtlsTrueForMTLSPem) {
    auto config = TlsConfig::FromMTLSPem("cert", "key", "ca");
    EXPECT_TRUE(config.is_mtls());
}

// --- is_enabled() ---

TEST(TlsConfigTest, IsEnabledFalseByDefault) {
    TlsConfig config;
    EXPECT_FALSE(config.is_enabled());
}

TEST(TlsConfigTest, IsEnabledTrueAfterFromCertFile) {
    auto config = TlsConfig::FromCertFile("/path/ca.pem");
    EXPECT_TRUE(config.is_enabled());
}

TEST(TlsConfigTest, IsEnabledTrueAfterFromPem) {
    auto config = TlsConfig::FromPem("CA_PEM");
    EXPECT_TRUE(config.is_enabled());
}

TEST(TlsConfigTest, IsEnabledTrueAfterFromMTLS) {
    auto config = TlsConfig::FromMTLS("cert", "key", "ca");
    EXPECT_TRUE(config.is_enabled());
}

TEST(TlsConfigTest, IsEnabledTrueAfterFromMTLSPem) {
    auto config = TlsConfig::FromMTLSPem("cert", "key", "ca");
    EXPECT_TRUE(config.is_enabled());
}

// --- BuildChannelCredentials: StatusOr returns ---

TEST(TlsConfigTest, BuildCredentials_NotFoundFile_ReturnsError) {
    // TLS enabled with a non-existent CA file should return a validation error
    auto config = TlsConfig::FromCertFile("/nonexistent/path/ca.pem");

    auto result = internal::BuildChannelCredentials(config);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::kValidation);
}

TEST(TlsConfigTest, BuildCredentials_InsecureSkipVerify_Rejected) {
    auto config = TlsConfig::FromCertFile("/some/ca.pem");
    config.set_insecure_skip_verify(true);

    auto result = internal::BuildChannelCredentials(config);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::kValidation);
    EXPECT_NE(result.status().message().find("not supported"), std::string::npos);
}

TEST(TlsConfigTest, BuildCredentials_DisabledTls_ReturnsInsecure) {
    // Default TlsConfig (not enabled) should return insecure credentials successfully
    TlsConfig config;
    auto result = internal::BuildChannelCredentials(config);
    EXPECT_TRUE(result.ok());
}

TEST(TlsConfigTest, BuildCredentials_EmptyCertFile_StillCreatesCredentials) {
    // TLS enabled via FromCertFile with empty path: the implementation skips
    // the file read (guards with !empty()), so SslCredentials is created with
    // default options.  This verifies the guard logic does not crash.
    auto config = TlsConfig::FromCertFile("");
    EXPECT_TRUE(config.is_enabled());
    EXPECT_TRUE(config.ca_cert_file().empty());

    auto result = internal::BuildChannelCredentials(config);
    // gRPC SslCredentials with empty options succeeds (uses system roots).
    EXPECT_TRUE(result.ok());
}

TEST(TlsConfigTest, ReadFile_EmptyPath_ReturnsError) {
    // Directly verify that an mTLS config where cert_file is empty but
    // key_file points to a nonexistent file triggers a validation error.
    auto config = TlsConfig::FromMTLS("", "/nonexistent/key.pem", "/nonexistent/ca.pem");

    auto result = internal::BuildChannelCredentials(config);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::kValidation);
}

}  // namespace
}  // namespace kubemq
