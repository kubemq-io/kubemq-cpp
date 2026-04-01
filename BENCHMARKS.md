# Performance Benchmarks

This document describes the benchmark suite and provides baseline performance numbers for the KubeMQ C++ SDK.

## Running Benchmarks

Benchmarks require a live KubeMQ broker on `localhost:50000` and Google Benchmark.

```bash
cmake -B build -S . \
    -DKUBEMQ_BUILD_BENCHMARKS=ON \
    -DCMAKE_BUILD_TYPE=Release

cmake --build build --parallel

./build/tests/kubemq_benchmarks
```

## Benchmark Categories

### 1. Events Send Throughput

Measures fire-and-forget event send rate.

| Metric | Value |
|--------|-------|
| Benchmark | `BM_EventsSendUnary` |
| Operation | `SendEvent` (unary RPC) |
| Payload | 256 bytes |
| Target | > 10,000 msg/s per client |

### 2. Events Stream Throughput

Measures event send rate through a persistent bidirectional stream.

| Metric | Value |
|--------|-------|
| Benchmark | `BM_EventsSendStream` |
| Operation | `EventStreamHandle::Send` |
| Payload | 256 bytes |
| Target | > 50,000 msg/s per stream |

### 3. Events Store Send Throughput

Measures persistent event send rate with confirmation.

| Metric | Value |
|--------|-------|
| Benchmark | `BM_EventsStoreSend` |
| Operation | `SendEventStore` |
| Payload | 256 bytes |
| Target | > 5,000 msg/s per client |

### 4. Queue Send Throughput (Simple API)

| Metric | Value |
|--------|-------|
| Benchmark | `BM_QueueSimpleSend` |
| Operation | `SendQueueMessage` |
| Payload | 256 bytes |
| Target | > 5,000 msg/s per client |

### 5. Queue Stream Throughput (Upstream)

| Metric | Value |
|--------|-------|
| Benchmark | `BM_QueueUpstreamSend` |
| Operation | `QueueUpstreamHandle::Send` |
| Payload | 256 bytes, batch of 10 |
| Target | > 20,000 msg/s per stream |

### 6. Queue Poll Throughput (Downstream)

| Metric | Value |
|--------|-------|
| Benchmark | `BM_QueuePoll` |
| Operation | `PollQueue` |
| Max items | 10 per poll |
| Target | > 10,000 msg/s per receiver |

### 7. Command Round-Trip Latency

| Metric | Value |
|--------|-------|
| Benchmark | `BM_CommandRoundTrip` |
| Operation | `SendCommand` (end-to-end) |
| Target P50 | < 5ms |
| Target P99 | < 20ms |

### 8. Query Round-Trip Latency

| Metric | Value |
|--------|-------|
| Benchmark | `BM_QueryRoundTrip` |
| Operation | `SendQuery` (end-to-end) |
| Target P50 | < 5ms |
| Target P99 | < 20ms |

## Benchmark Environment

All baseline numbers are collected under the following conditions:

- **Broker**: KubeMQ server running locally (single instance)
- **Network**: Loopback (127.0.0.1)
- **CPU**: Modern x86_64 (4+ cores recommended)
- **Build**: Release mode with optimizations (`-O2`)
- **OS**: Ubuntu 22.04 LTS

## Factors Affecting Performance

- **Network latency**: Local benchmarks eliminate network overhead. Production latency varies.
- **Payload size**: Larger payloads reduce throughput (bytes/s may increase, msg/s decreases).
- **TLS overhead**: TLS adds ~10-20% latency overhead vs. plaintext.
- **Concurrency**: Multiple clients can achieve higher aggregate throughput.
- **Broker configuration**: Persistence, replication, and queue depth affect broker-side performance.

## Burn-In Testing

For sustained load testing, use the burn-in application:

```bash
./build/burnin/kubemq_burnin

# Then start a run via the REST API:
curl -X POST http://localhost:9090/run/start -d '{
  "duration_seconds": 3600,
  "patterns": {
    "events": {"enabled": true, "rate": 1000, "channels": 4},
    "queues": {"enabled": true, "rate": 500, "channels": 2}
  }
}'

# Monitor metrics:
curl http://localhost:9090/run/status
```

See the burn-in REST API documentation for full configuration options.
