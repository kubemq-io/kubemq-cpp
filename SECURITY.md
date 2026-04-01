# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability in the KubeMQ C++ SDK, please report it responsibly.

### How to Report

1. **Do NOT** create a public GitHub issue for security vulnerabilities.
2. Email security reports to: **security@kubemq.io**
3. Include the following information:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt within 48 hours.
- **Assessment**: We will assess the vulnerability and determine its severity within 5 business days.
- **Fix**: Critical vulnerabilities will be patched within 7 days. Other issues will be addressed in the next release.
- **Disclosure**: We will coordinate disclosure timing with the reporter.

### Security Best Practices

When using the KubeMQ C++ SDK:

- **Use TLS/mTLS** for production deployments to encrypt traffic between clients and the broker.
- **Use authentication tokens** to restrict access to the broker.
- **Keep dependencies updated**, especially gRPC and Protobuf, to receive security patches.
- **Do not hardcode credentials** in source code. Use environment variables or secure credential providers.
- **Review the `CredentialProvider` interface** for implementing custom token rotation.
- **Set appropriate timeouts** to prevent resource exhaustion attacks.
- **Validate channel names** to prevent injection of control characters.

### Dependency Security

The SDK depends on:
- **gRPC** (Apache-2.0): Follow [gRPC security advisories](https://github.com/grpc/grpc/security/advisories)
- **Protobuf** (BSD-3): Follow [Protobuf security advisories](https://github.com/protocolbuffers/protobuf/security/advisories)
- **nlohmann/json** (MIT): Header-only, minimal attack surface

### TLS Configuration

For secure deployments, always configure TLS:

```cpp
// Server-side TLS
auto tls = kubemq::TlsConfig::FromCertFile("/path/to/ca.pem");

// Mutual TLS (recommended for production)
auto tls = kubemq::TlsConfig::FromMTLS(
    "/path/to/client.pem",
    "/path/to/client.key",
    "/path/to/ca.pem"
);
```

Avoid using `set_insecure_skip_verify(true)` in production.
