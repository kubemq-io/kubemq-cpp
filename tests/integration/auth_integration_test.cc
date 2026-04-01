#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "kubemq/kubemq.h"

// --- CredentialProvider token injection ---

TEST(AuthIntegrationTest, CredentialProvider_TokenInjected) {
    // Requires live broker with auth enabled.
    // Verifies that CredentialProvider token appears in RPC metadata
    // and that Ping succeeds with valid token.

    auto provider = std::make_shared<kubemq::StaticTokenProvider>("valid-token");

    kubemq::ClientOptions opts;
    opts.set_address("localhost", 50000);
    opts.set_client_id("cpp-test-auth");
    opts.set_credential_provider(provider);

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        GTEST_SKIP() << "Broker not available: " << client_or.status().ToString();
    }

    auto ping = (*client_or)->Ping();
    EXPECT_TRUE(ping.ok()) << ping.status().ToString();

    (*client_or)->Close();
}

// --- Error mapping for unauthenticated ---

TEST(AuthIntegrationTest, ErrorMapping_Unauthenticated) {
    // Connect with invalid token; expect kAuthentication error.

    kubemq::ClientOptions opts;
    opts.set_address("localhost", 50000);
    opts.set_client_id("cpp-test-bad-auth");
    opts.set_auth_token("invalid-token");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        // Connection-level rejection with bad auth
        EXPECT_EQ(client_or.status().code(), kubemq::ErrorCode::kAuthentication)
            << "Expected kAuthentication error, got: " << client_or.status().ToString();
        return;  // test passed -- broker rejected at connect time
    }

    // Broker accepted connection; try an RPC that should fail with UNAUTHENTICATED
    auto ping = (*client_or)->Ping();
    if (ping.ok()) {
        (*client_or)->Close();
        GTEST_SKIP() << "Broker does not enforce auth; cannot test unauthenticated error mapping";
    }

    EXPECT_EQ(ping.status().code(), kubemq::ErrorCode::kAuthentication)
        << "Expected kAuthentication error from Ping, got: " << ping.status().ToString();
    (*client_or)->Close();
}
