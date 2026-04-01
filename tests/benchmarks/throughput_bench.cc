// Throughput benchmark for KubeMQ C++ SDK.
// Requires a live broker on localhost:50000.
// Gated behind KUBEMQ_BUILD_BENCHMARKS=ON.

#include <benchmark/benchmark.h>

#include <memory>
#include <string>

#include "kubemq/kubemq.h"

namespace {

// Global client shared across benchmarks (created once)
class BenchmarkFixture : public benchmark::Fixture {
public:
    void SetUp(benchmark::State& state) override {
        if (!client_) {
            kubemq::ClientOptions opts;
            opts.set_address("localhost", 50000);
            opts.set_client_id("cpp-benchmark");
            opts.set_check_connection(true);
            auto client_or = kubemq::Client::Create(opts);
            if (!client_or.ok()) {
                state.SkipWithError(
                    ("Failed to connect: " + client_or.status().ToString()).c_str());
                return;
            }
            client_ = std::move(*client_or);
        }
    }

    void TearDown(benchmark::State& /*state*/) override {
        // Client stays alive across benchmarks
    }

protected:
    static std::unique_ptr<kubemq::Client> client_;
};

std::unique_ptr<kubemq::Client> BenchmarkFixture::client_ = nullptr;

// --- Event send throughput (unary) ---

BENCHMARK_DEFINE_F(BenchmarkFixture, EventSendUnary)(benchmark::State& state) {
    if (!client_) {
        state.SkipWithError("No client");
        return;
    }

    const std::string channel = "bench.events.unary";
    const std::string body(state.range(0), 'x');  // Payload size from range

    for (auto _ : state) {
        auto status = client_->PublishEvent(channel, body);
        if (!status.ok()) {
            state.SkipWithError(("SendEvent failed: " + status.ToString()).c_str());
            return;
        }
    }

    state.SetBytesProcessed(state.iterations() * state.range(0));
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(BenchmarkFixture, EventSendUnary)
    ->Arg(64)      // 64 bytes
    ->Arg(1024)    // 1 KB
    ->Arg(4096)    // 4 KB
    ->Arg(16384);  // 16 KB

// --- Event send throughput (stream) ---

BENCHMARK_DEFINE_F(BenchmarkFixture, EventSendStream)(benchmark::State& state) {
    if (!client_) {
        state.SkipWithError("No client");
        return;
    }

    auto stream_or = client_->SendEventStream();
    if (!stream_or.ok()) {
        state.SkipWithError(("Stream creation failed: " + stream_or.status().ToString()).c_str());
        return;
    }
    auto& stream = *stream_or;

    const std::string channel = "bench.events.stream";
    const std::string body(state.range(0), 'x');

    for (auto _ : state) {
        auto event_or = kubemq::Event::Builder()
            .SetChannel(channel)
            .SetBody(body)
            .Build();
        if (!event_or.ok()) {
            state.SkipWithError("Event build failed");
            return;
        }
        auto status = (*stream)->Send(*event_or);
        if (!status.ok()) {
            state.SkipWithError(("Stream send failed: " + status.ToString()).c_str());
            return;
        }
    }

    (*stream)->Close();
    state.SetBytesProcessed(state.iterations() * state.range(0));
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(BenchmarkFixture, EventSendStream)
    ->Arg(64)
    ->Arg(1024)
    ->Arg(4096);

// --- Queue message throughput ---

BENCHMARK_DEFINE_F(BenchmarkFixture, QueueSendThroughput)(benchmark::State& state) {
    if (!client_) {
        state.SkipWithError("No client");
        return;
    }

    const std::string channel = "bench.queues.throughput";
    const std::string body(state.range(0), 'q');

    for (auto _ : state) {
        auto result = client_->SendQueueMessageSimple(channel, body);
        if (!result.ok()) {
            state.SkipWithError(("Queue send failed: " + result.status().ToString()).c_str());
            return;
        }
    }

    state.SetBytesProcessed(state.iterations() * state.range(0));
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(BenchmarkFixture, QueueSendThroughput)
    ->Arg(64)
    ->Arg(1024)
    ->Arg(4096);

// --- Command round-trip latency ---

BENCHMARK_DEFINE_F(BenchmarkFixture, CommandRoundTrip)(benchmark::State& state) {
    if (!client_) {
        state.SkipWithError("No client");
        return;
    }

    const std::string channel = "bench.commands.roundtrip";

    // Set up subscriber to auto-respond
    auto sub_or = client_->SubscribeToCommands(
        channel, "",
        [&](const kubemq::CommandReceive& cmd) {
            auto reply_or = kubemq::CommandReply::Builder()
                .SetRequestId(cmd.id)
                .SetResponseTo(cmd.response_to)
                .SetClientId("cpp-benchmark")
                .Build();
            if (reply_or.ok()) {
                client_->SendCommandResponse(*reply_or);
            }
        },
        [](const kubemq::Status&) {});

    if (!sub_or.ok()) {
        state.SkipWithError(("Subscribe failed: " + sub_or.status().ToString()).c_str());
        return;
    }

    // Wait for subscription to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto _ : state) {
        auto response = client_->SendCommandSimple(
            channel, "bench-body", std::chrono::milliseconds(5000));
        if (!response.ok()) {
            state.SkipWithError(("Command failed: " + response.status().ToString()).c_str());
            (*sub_or)->Cancel();
            return;
        }
    }

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(BenchmarkFixture, CommandRoundTrip);

// --- Query round-trip latency ---

BENCHMARK_DEFINE_F(BenchmarkFixture, QueryRoundTrip)(benchmark::State& state) {
    if (!client_) {
        state.SkipWithError("No client");
        return;
    }

    const std::string channel = "bench.queries.roundtrip";

    auto sub_or = client_->SubscribeToQueries(
        channel, "",
        [&](const kubemq::QueryReceive& query) {
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId("cpp-benchmark")
                .SetBody("bench-result")
                .Build();
            if (reply_or.ok()) {
                client_->SendQueryResponse(*reply_or);
            }
        },
        [](const kubemq::Status&) {});

    if (!sub_or.ok()) {
        state.SkipWithError(("Subscribe failed: " + sub_or.status().ToString()).c_str());
        return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto _ : state) {
        auto response = client_->SendQuerySimple(
            channel, "bench-query", std::chrono::milliseconds(5000));
        if (!response.ok()) {
            state.SkipWithError(("Query failed: " + response.status().ToString()).c_str());
            (*sub_or)->Cancel();
            return;
        }
    }

    (*sub_or)->Cancel();
    (*sub_or)->Wait();
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(BenchmarkFixture, QueryRoundTrip);

}  // namespace
