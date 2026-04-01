# KubeMQ C++ SDK

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![CMake](https://img.shields.io/badge/CMake-3.16%2B-green.svg)](https://cmake.org/)
[![KubeMQ](https://img.shields.io/badge/KubeMQ-v4-purple.svg)](https://kubemq.io)

The KubeMQ C++ SDK provides a high-performance, type-safe C++17 client library for [KubeMQ](https://kubemq.io) message broker. It supports all KubeMQ messaging patterns with a modern, idiomatic C++ API.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Concepts](#concepts)
- [API Overview](#api-overview)
- [Building](#building)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Compatibility](#compatibility)
- [Contributing](#contributing)
- [Resources](#resources)
- [License](#license)

## Features

- **Events (Pub/Sub)** -- Fire-and-forget event publishing and subscription
- **Events Store** -- Persistent events with replay capabilities (StartFromSequence, StartFromTime, etc.)
- **Commands (RPC)** -- Synchronous command/response with timeout
- **Queries (RPC)** -- Synchronous query/response with optional caching
- **Queues (Simple)** -- Send, receive, batch, peek, and ack-all operations
- **Queues (Stream)** -- Upstream/downstream streaming with per-message settlement (Ack/Nack/ReQueue)
- **Channel Management** -- Create, delete, and list channels for all types
- **Connection Management** -- Automatic reconnection with exponential backoff and jitter
- **TLS/mTLS** -- Full TLS and mutual TLS support
- **Error Handling** -- Explicit Status/StatusOr return types (no exceptions in public API)
- **PIMPL** -- ABI-stable public interface with inline namespace versioning

## Requirements

- C++17 compiler (GCC 9+, Clang 9+, MSVC 2019+)
- CMake 3.16+
- gRPC v1.78+
- Protobuf v5.29+

## Installation

### vcpkg (Recommended)

```bash
vcpkg install grpc protobuf nlohmann-json

cmake -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=[vcpkg-root]/scripts/buildsystems/vcpkg.cmake
cmake --build build --parallel
```

### Conan 2.x

```bash
conan install . --build=missing
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
cmake --build build --parallel
```

### CMake FetchContent

```cmake
include(FetchContent)
FetchContent_Declare(
    kubemq-cpp
    GIT_REPOSITORY https://github.com/kubemq-io/kubemq-cpp.git
    GIT_TAG v1.0.0
)
FetchContent_MakeAvailable(kubemq-cpp)

target_link_libraries(your_app PRIVATE kubemq_static)
```

### Source Build

```bash
git clone https://github.com/kubemq-io/kubemq-cpp.git
cd kubemq-cpp
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
cmake --install build --prefix /usr/local
```

## Quick Start

```cpp
#include <iostream>
#include "kubemq/kubemq.h"

int main() {
    // Connect
    kubemq::ClientOptions opts;
    opts.set_address("localhost", 50000);
    opts.set_client_id("my-app");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "Connect failed: " << client_or.status().ToString() << "\n";
        return 1;
    }
    auto& client = *client_or;

    // Ping
    auto ping_or = client->Ping();
    if (ping_or.ok()) {
        std::cout << "Connected to: " << ping_or->host
                  << " v" << ping_or->version << "\n";
    }

    // Send an event
    auto status = client->PublishEvent("my-channel", "Hello, KubeMQ!");
    if (!status.ok()) {
        std::cerr << "Send failed: " << status.ToString() << "\n";
    }

    client->Close();
    return 0;
}
```

## Concepts

KubeMQ supports five messaging patterns, organized into three categories:

### Pub/Sub -- Events
Fire-and-forget message delivery. Publishers send events to a channel; all active subscribers receive them in real time. Events are **not persisted** -- if no subscriber is connected, the event is lost.
- **Use case:** Real-time notifications, telemetry, logs
- **Delivery:** At-most-once

### Pub/Sub -- Events Store
Persistent event delivery with replay. Like Events, but the broker stores every message. Subscribers can start from: the beginning, a sequence number, a timestamp, or new events only.
- **Use case:** Event sourcing, audit logs, data replication
- **Delivery:** At-least-once (with replay)

### RPC -- Commands
Synchronous request/response with a timeout. The sender blocks until a responder processes the command or the timeout expires. Commands carry no response body -- only success/failure.
- **Use case:** Remote procedure calls, configuration changes
- **Delivery:** Exactly-once (within timeout)

### RPC -- Queries
Synchronous request/response with a response body. Like Commands, but the responder can return data. Supports server-side caching via cache_key and cache_ttl.
- **Use case:** Data retrieval, search, lookups
- **Delivery:** Exactly-once (within timeout, with optional caching)

### Queues
Guaranteed message delivery with acknowledgment. Messages are stored until consumed and acknowledged. Supports delayed delivery, expiration, dead-letter queues, and batch operations. Two APIs are available:
- **Simple API:** SendQueueMessage / ReceiveQueueMessages -- one-shot operations
- **Stream API:** QueueUpstream / QueueDownstreamReceiver -- persistent streams with per-message Ack/Nack/ReQueue
- **Use case:** Task distribution, work queues, reliable messaging
- **Delivery:** At-least-once (with manual acknowledgment)

### Channels and Consumer Groups
- **Channel:** A named endpoint for a specific message type. Each messaging pattern has its own channel namespace.
- **Consumer Group:** A named group of subscribers that share load. Only one member of the group receives each message (for Events/Events Store round-robin; for Commands/Queries, one responder handles each request).

## API Overview

### Connection

```cpp
kubemq::ClientOptions opts;
opts.set_address("localhost", 50000);
opts.set_client_id("my-client");
opts.set_auth_token("your-token");              // Optional
opts.set_tls_config(kubemq::TlsConfig::FromCertFile("ca.pem"));  // Optional

auto client_or = kubemq::Client::Create(opts);
auto& client = *client_or;

auto ping = client->Ping();     // Health check
auto state = client->State();   // ConnectionState enum
client->Close();                // Graceful shutdown
```

### Events (Pub/Sub)

```cpp
// Subscribe
auto sub = client->SubscribeToEvents("channel", "group",
    [](const kubemq::EventReceive& e) { /* handle */ },
    [](const kubemq::Status& err) { /* handle */ });

// Send
client->PublishEvent("channel", "body", "metadata", {{"key", "value"}});

// Stream
auto stream = client->SendEventStream();
stream.value()->Send(event);
```

### Events Store

```cpp
// Subscribe with replay
auto sub = client->SubscribeToEventsStore("channel", "",
    kubemq::SubscriptionOption::StartFromSequence(1),
    on_event, on_error);

// Send
auto result = client->SendEventStore(event_store);
```

### Commands

```cpp
// Subscribe and respond
auto sub = client->SubscribeToCommands("channel", "", on_command, on_error);
client->SendCommandResponse(reply);

// Send
auto resp = client->SendCommand(command);
```

### Queries

```cpp
// Subscribe and respond
auto sub = client->SubscribeToQueries("channel", "", on_query, on_error);
client->SendQueryResponse(reply);

// Send with caching
auto resp = client->SendQuery(query);
```

### Queues (Simple API)

```cpp
auto result = client->SendQueueMessage(message);
auto response = client->ReceiveQueueMessages(request);
auto batch = client->SendQueueMessages(messages);
```

### Queues (Stream API)

```cpp
// Upstream (send)
auto upstream = client->QueueUpstream();
upstream.value()->Send("req-id", messages);

// Downstream (poll + settle)
auto poll = client->PollQueue(poll_request);
poll.value()->AckAll();
```

### Channel Management

```cpp
client->CreateEventsChannel("my-channel");
client->DeleteEventsChannel("my-channel");
auto channels = client->ListEventsChannels("search-prefix");
```

## Building

```bash
# Configure
cmake -B build -S . \
    -DCMAKE_BUILD_TYPE=Release \
    -DKUBEMQ_BUILD_TESTS=ON \
    -DKUBEMQ_BUILD_EXAMPLES=ON \
    -DKUBEMQ_BUILD_BURNIN=ON

# Build
cmake --build build --parallel

# Run unit tests
cd build && ctest --label-exclude integration --output-on-failure

# Run integration tests (requires live broker on localhost:50000)
cd build && ctest --label-regex integration --output-on-failure
```

### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `KUBEMQ_BUILD_TESTS` | ON | Build unit and integration tests |
| `KUBEMQ_BUILD_EXAMPLES` | ON | Build example programs |
| `KUBEMQ_BUILD_BURNIN` | ON | Build burn-in soak test application |
| `KUBEMQ_BUILD_BENCHMARKS` | OFF | Build performance benchmarks |
| `KUBEMQ_ENABLE_OTEL` | OFF | Enable OpenTelemetry integration |
| `KUBEMQ_BUILD_SHARED` | ON | Build shared library |
| `KUBEMQ_BUILD_STATIC` | ON | Build static library |

## Examples

See the [examples/](examples/) directory for complete, runnable programs:

| Example | Pattern |
|---------|---------|
| `connection/basic_connect` | Connect + Ping |
| `connection/tls` | TLS/mTLS connection |
| `events/send_receive` | Events pub/sub |
| `events/stream` | Event streaming |
| `events_store/replay_from_sequence` | Persistent events + replay |
| `queues/send_receive_simple` | Simple queue API |
| `queues/send_receive_stream` | Stream queue API |
| `commands/send_receive` | Command RPC |
| `queries/send_receive` | Query RPC |
| `management/create_delete_channels` | Channel CRUD |
| `error_handling/reconnection` | Reconnection + state callbacks |
| `error_handling/graceful_shutdown` | Proper cleanup |

## API Reference

Generate HTML API documentation from the Doxygen comments:

```bash
cd kubemq-cpp
doxygen Doxyfile
open docs/html/index.html
```

See the [generated API docs](docs/html/index.html) for complete class and method reference.

## Compatibility

See [COMPATIBILITY.md](COMPATIBILITY.md) for the full compiler/OS/architecture support matrix.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, and PR guidelines.

## Resources

- [KubeMQ Documentation](https://docs.kubemq.io)
- [KubeMQ Website](https://kubemq.io)
- [GitHub Issues](https://github.com/kubemq-io/kubemq-cpp/issues)

## License

Apache-2.0 License. See [LICENSE](LICENSE) for details.
