# Troubleshooting

Common issues and solutions when building or running the KubeMQ C++ SDK.

## Build Issues

### 1. gRPC/Protobuf Not Found

**Error:**
```
CMake Error: Could not find a package configuration file provided by "gRPC"
```

**Solution:**
```bash
# Install via vcpkg
vcpkg install grpc protobuf

# Make sure CMAKE_TOOLCHAIN_FILE is set
cmake -B build -DCMAKE_TOOLCHAIN_FILE=[vcpkg-root]/scripts/buildsystems/vcpkg.cmake
```

### 2. Protobuf Version Mismatch

**Error:**
```
error: "PROTOBUF_MIN_PROTOC_VERSION" is not satisfied
```

**Solution:** Ensure protobuf and grpc are built from the same vcpkg installation. Do not mix system-installed protobuf with vcpkg-installed grpc.

```bash
vcpkg remove grpc protobuf
vcpkg install grpc  # This pulls the matching protobuf
```

### 3. Linker Errors on macOS

**Error:**
```
ld: symbol(s) not found for architecture arm64
```

**Solution:** Ensure all dependencies are built for the same architecture:

```bash
# For Apple Silicon
vcpkg install grpc:arm64-osx

# For Intel Mac
vcpkg install grpc:x64-osx
```

### 4. nlohmann/json Not Found

**Error:**
```
CMake Error: Could not find a package configuration file provided by "nlohmann_json"
```

**Solution:**
```bash
vcpkg install nlohmann-json
```

### 5. cpp-httplib Not Found (Burn-In Build)

**Error:**
```
CMake Error: Could not find a package configuration file provided by "httplib"
```

**Solution:**
```bash
vcpkg install cpp-httplib

# Or disable burn-in build:
cmake -B build -DKUBEMQ_BUILD_BURNIN=OFF
```

### 6. Proto Code Generation Fails

**Error:**
```
protoc: not found
```

**Solution:** Ensure protoc is in your PATH or let CMake find it through vcpkg:

```bash
which protoc
# If not found, vcpkg provides it
ls [vcpkg-root]/installed/x64-linux/tools/protobuf/protoc
```

### 7. C++17 Not Supported

**Error:**
```
error: 'variant' is not a member of 'std'
```

**Solution:** Upgrade your compiler:
- GCC: minimum version 9
- Clang: minimum version 9
- MSVC: minimum Visual Studio 2019

```bash
# Check version
g++ --version
clang++ --version
```

## Runtime Issues

### 8. Connection Refused

**Error:**
```
TRANSIENT: failed to connect to all addresses
```

**Solution:**
1. Verify the KubeMQ server is running:
   ```bash
   docker run -d -p 50000:50000 kubemq/kubemq
   ```
2. Check the address and port:
   ```cpp
   opts.set_address("localhost", 50000);
   ```
3. Check firewall rules

### 9. Authentication Failed

**Error:**
```
AUTHENTICATION: token invalid
```

**Solution:**
```cpp
opts.set_auth_token("your-valid-token");
// Or use a credential provider:
opts.set_credential_provider(std::make_shared<kubemq::StaticTokenProvider>("token"));
```

### 10. TLS Handshake Failed

**Error:**
```
TRANSIENT: SSL_ERROR_SSL: error:... certificate verify failed
```

**Solution:**
1. Verify the CA certificate matches the server certificate:
   ```cpp
   auto tls = kubemq::TlsConfig::FromCertFile("/correct/path/to/ca.pem");
   ```
2. For testing, skip verification (not for production):
   ```cpp
   tls.set_insecure_skip_verify(true);
   ```
3. For mTLS, ensure client cert and key match:
   ```cpp
   auto tls = kubemq::TlsConfig::FromMTLS("client.pem", "client.key", "ca.pem");
   ```

### 11. Subscription Not Receiving Messages

**Possible causes:**
1. Subscription not established before sending -- add a small delay:
   ```cpp
   std::this_thread::sleep_for(std::chrono::milliseconds(500));
   ```
2. Channel name mismatch between sender and subscriber
3. Group name causing load balancing -- use empty string for broadcast:
   ```cpp
   client->SubscribeToEvents("channel", "", on_event, on_error);
   ```

### 12. Queue Messages Not Being Received

**Possible causes:**
1. Messages expired before consumption -- increase expiration:
   ```cpp
   builder.SetExpirationSeconds(3600);
   ```
2. Messages consumed by another client -- check for competing consumers
3. Wait timeout too short:
   ```cpp
   req.wait_time_seconds = 30;
   ```

### 13. Command/Query Timeout

**Error:**
```
TIMEOUT: deadline exceeded
```

**Solution:**
1. Increase timeout:
   ```cpp
   builder.SetTimeout(std::chrono::milliseconds(10000));  // 10s
   ```
2. Ensure responder is subscribed and responding
3. Check broker logs for routing issues

### 14. Memory Growth During Reconnection

The SDK buffers messages during reconnection (default: 1024 messages). If the buffer fills, oldest messages are evicted.

**Solution:**
```cpp
kubemq::ReconnectPolicy policy;
policy.buffer_size = 512;  // Reduce buffer if memory is constrained
opts.set_reconnect_policy(policy);

// Monitor buffer drain events:
opts.set_on_buffer_drain([](int discarded) {
    std::cerr << "Discarded " << discarded << " buffered messages\n";
});
```

### 15. Burn-In Application Not Starting

1. Check environment variables:
   ```bash
   export KUBEMQ_BROKER_ADDRESS=localhost:50000
   export PORT=9090
   ```
2. Verify broker connectivity:
   ```bash
   curl http://localhost:9090/broker/status
   ```
3. Check logs for error messages

## Getting Help

If your issue is not listed above:

1. Check the [examples/](examples/) directory for working code samples
2. Open an issue on [GitHub](https://github.com/kubemq-io/kubemq-cpp/issues)
3. Contact support at [kubemq.io](https://kubemq.io)
