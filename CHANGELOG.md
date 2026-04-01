# Changelog

All notable changes to the KubeMQ C++ SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-01

### Added

- Initial release of the KubeMQ C++ SDK
- **Core**
  - Client with PIMPL pattern and `inline namespace v1` for ABI stability
  - `Status` and `StatusOr<T>` error handling (no exceptions in public API)
  - 10 error codes with gRPC status code mapping
  - 18 validation rules for channels, bodies, timeouts
  - UUID v4 auto-generation for message IDs
  - ClientID auto-population from options
- **Connection Management**
  - 5-state connection FSM (Idle, Connecting, Ready, Reconnecting, Closed)
  - Automatic reconnection with exponential backoff and jitter
  - Ring buffer (1024) for message buffering during reconnection
  - Subscription auto-recovery on reconnect
  - TLS and mutual TLS (mTLS) support
  - Keepalive configuration
  - State change callbacks (connected, disconnected, reconnecting, reconnected, closed)
- **Events (Pub/Sub)**
  - `SendEvent` -- fire-and-forget event publishing
  - `SendEventStream` -- bidirectional event streaming
  - `SubscribeToEvents` -- event subscription with callback delivery
  - `PublishEvent` -- convenience method
- **Events Store (Persistent)**
  - `SendEventStore` -- persistent event publishing with result confirmation
  - `SendEventStoreStream` -- persistent event streaming
  - `SubscribeToEventsStore` -- subscription with replay options
    - StartFromNewEvents, StartFromFirstEvent, StartFromLastEvent
    - StartFromSequence, StartFromTime, StartFromTimeDelta
  - `PublishEventStore` -- convenience method
- **Commands (RPC)**
  - `SendCommand` -- synchronous command with timeout
  - `SubscribeToCommands` -- command subscription
  - `SendCommandResponse` -- command response
  - `SendCommandSimple` -- convenience method
- **Queries (RPC)**
  - `SendQuery` -- synchronous query with timeout
  - `SubscribeToQueries` -- query subscription
  - `SendQueryResponse` -- query response
  - `SendQuerySimple` -- convenience method
  - Cache key and TTL support
- **Queues (Simple API)**
  - `SendQueueMessage` -- single message send
  - `SendQueueMessages` -- batch send
  - `ReceiveQueueMessages` -- receive with wait timeout
  - `AckAllQueueMessages` -- acknowledge all
  - `SendQueueMessageSimple` -- convenience method
  - Message policies (expiration, delay, max receive count, dead letter queue)
- **Queues (Stream API)**
  - `QueueUpstream` -- streaming upstream with batch send
  - `PollQueue` -- poll with per-message settlement
  - `QueueDownstreamReceiver` -- reusable downstream receiver
  - Settlement: Ack, Nack, ReQueue (individual and bulk)
- **Channel Management**
  - `CreateChannel` / `DeleteChannel` / `ListChannels` -- generic operations
  - 15 convenience aliases (Create/Delete/List for Events, EventsStore, Commands, Queries, Queues)
- **Build System**
  - CMake 3.16+ with vcpkg, Conan 2.x, and FetchContent support
  - Shared and static library targets
  - Proto codegen with LITE_RUNTIME and mock stubs
  - Google Test integration
- **Examples**
  - 12 example programs covering all messaging patterns
- **Burn-In Application**
  - REST API with 13 endpoints for soak testing
  - 6 pattern workers (events, events_store, queue_simple, queue_stream, commands, queries)
  - JSON configuration with per-pattern settings
- **Documentation**
  - README with quick start and API overview
  - CONTRIBUTING guide
  - COMPATIBILITY matrix
  - SECURITY policy
  - BENCHMARKS documentation
  - TROUBLESHOOTING guide
  - Doxyfile for API documentation generation
