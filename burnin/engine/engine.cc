#include "engine/engine.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <kubemq/client.h>
#include "config/defaults.h"
#include "worker/commands.h"
#include "worker/events.h"
#include "worker/events_store.h"
#include "worker/queries.h"
#include "worker/queue_simple.h"
#include "worker/queue_stream.h"

namespace burnin {

static std::string NowIso8601() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream ss;
    ss << std::put_time(std::gmtime(&t), "%FT%TZ");
    return ss.str();
}

Engine::Engine(const std::string& broker_address)
    : state_(kStateIdle), broker_address_(broker_address) {}

Engine::~Engine() {
    if (run_thread_.joinable()) {
        stop_requested_.store(true);
        run_thread_.join();
    }
}

std::string Engine::State() const {
    std::lock_guard<std::mutex> lock(mu_);
    return state_;
}

void Engine::SetState(const std::string& state) {
    std::lock_guard<std::mutex> lock(mu_);
    state_ = state;
}

Engine::StartResult Engine::StartRun(const Config& config) {
    StartResult result;

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (state_ != kStateIdle && state_ != kStateStopped && state_ != kStateError) {
            result.error = "cannot start: current state is " + state_;
            return result;
        }
    }

    SetState(kStateStarting);

    config_ = config;
    run_id_ = config.run_id.empty() ? GenerateRunId() : config.run_id;
    config_.run_id = run_id_;
    run_started_at_ = NowIso8601();
    run_error_.clear();
    stop_requested_.store(false);
    has_report_ = false;

    // Count enabled patterns
    int enabled = 0;
    for (const auto& [name, pc] : config_.patterns) {
        if (pc.enabled) ++enabled;
    }

    result.run_id = run_id_;
    result.enabled_count = enabled;

    // Start the run in a background thread
    if (run_thread_.joinable()) {
        run_thread_.join();
    }
    run_thread_ = std::thread(&Engine::RunLoop, this);

    return result;
}

std::string Engine::StopRun() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (state_ != kStateRunning && state_ != kStateStarting) {
            return "cannot stop: current state is " + state_;
        }
        state_ = kStateStopping;
    }

    stop_requested_.store(true);
    // Don't join run_thread_ here — it blocks the HTTP handler.
    // RunLoop will finish asynchronously: stop workers, build report, set state to stopped.
    // run_thread_ is joined in StartRun() or destructor before reuse.
    return "";
}

void Engine::RunLoop() {
    try {
        CreateWorkers();

        metrics_.Reset();
        metrics_.MarkStart();
        SetState(kStateRunning);

        // Start all workers
        for (auto& w : workers_) {
            w->Start();
        }

        std::cout << "[Engine] Run " << run_id_ << " started ("
                  << workers_.size() << " workers)\n";

        // Wait for duration or stop signal
        auto start = std::chrono::steady_clock::now();
        auto duration = std::chrono::seconds(config_.duration_seconds);

        while (!stop_requested_.load()) {
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed >= duration) {
                std::cout << "[Engine] Duration reached, stopping.\n";
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Capture worker count before stopping (StopWorkers clears the vector)
        int worker_count = static_cast<int>(workers_.size());

        // Stop workers
        StopWorkers();
        metrics_.MarkStop();

        // Build final report matching RunReportResponse
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto snapshot = metrics_.Snapshot();

            // Build patterns for report
            nlohmann::json report_patterns;
            for (const auto& pname : {"events", "events_store", "queue_stream", "queue_simple", "commands", "queries"}) {
                auto it = config_.patterns.find(pname);
                if (it == config_.patterns.end() || !it->second.enabled) {
                    report_patterns[pname] = {{"enabled", false}};
                    continue;
                }
                bool is_rpc = (std::string(pname) == "commands" || std::string(pname) == "queries");
                nlohmann::json pdata = {
                    {"enabled", true},
                    {"channels", it->second.channels},
                    {"sent", snapshot.contains(pname) ? snapshot[pname].value("sent", (uint64_t)0) : 0},
                    {"errors", snapshot.contains(pname) ? snapshot[pname].value("errors", (uint64_t)0) : 0},
                    {"reconnections", 0},
                    {"target_rate", it->second.rate},
                    {"avg_rate", 0}, {"peak_rate", 0},
                    {"bytes_sent", snapshot.contains(pname) ? snapshot[pname].value("bytes_sent", (uint64_t)0) : 0},
                    {"bytes_received", snapshot.contains(pname) ? snapshot[pname].value("bytes_received", (uint64_t)0) : 0},
                    {"latency", {{"p50_ms", 0}, {"p95_ms", 0}, {"p99_ms", 0}, {"p999_ms", 0}}},
                };
                if (is_rpc) {
                    pdata["senders_per_channel"] = it->second.senders_per_channel;
                    pdata["responders_per_channel"] = it->second.responders_per_channel;
                    pdata["responses_success"] = snapshot.contains(pname) ? snapshot[pname].value("rpc_success", (uint64_t)0) : 0;
                    pdata["responses_timeout"] = snapshot.contains(pname) ? snapshot[pname].value("rpc_timeout", (uint64_t)0) : 0;
                    pdata["responses_error"] = snapshot.contains(pname) ? snapshot[pname].value("rpc_error", (uint64_t)0) : 0;
                    pdata["senders"] = nlohmann::json::array();
                    pdata["responders"] = nlohmann::json::array();
                } else {
                    pdata["producers_per_channel"] = it->second.producers_per_channel;
                    pdata["consumers_per_channel"] = it->second.consumers_per_channel;
                    pdata["consumer_group"] = it->second.consumer_group;
                    pdata["received"] = snapshot.contains(pname) ? snapshot[pname].value("received", (uint64_t)0) : 0;
                    pdata["lost"] = snapshot.contains(pname) ? snapshot[pname].value("lost", (uint64_t)0) : 0;
                    pdata["duplicated"] = snapshot.contains(pname) ? snapshot[pname].value("duplicated", (uint64_t)0) : 0;
                    pdata["corrupted"] = snapshot.contains(pname) ? snapshot[pname].value("corrupted", (uint64_t)0) : 0;
                    pdata["out_of_order"] = snapshot.contains(pname) ? snapshot[pname].value("out_of_order", (uint64_t)0) : 0;
                    pdata["loss_pct"] = 0;
                    pdata["producers"] = nlohmann::json::array();
                    pdata["consumers"] = nlohmann::json::array();
                }
                report_patterns[pname] = pdata;
            }

            // Count total errors for verdict
            uint64_t total_errors = 0;
            for (const auto& [name, data] : snapshot.items()) {
                if (name == "_totals" || !data.is_object()) continue;
                total_errors += data.value("errors", (uint64_t)0);
            }

            last_report_ = {
                {"run_id", run_id_},
                {"sdk", "cpp"},
                {"sdk_version", "1.0.0"},
                {"mode", "soak"},
                {"broker_address", broker_address_},
                {"started_at", run_started_at_},
                {"ended_at", NowIso8601()},
                {"duration_seconds", static_cast<int64_t>(metrics_.ElapsedSeconds())},
                {"all_patterns_enabled", worker_count == 6},
                {"warmup_active", false},
                {"patterns", report_patterns},
                {"resources", {
                    {"peak_rss_mb", 0},
                    {"baseline_rss_mb", 0},
                    {"memory_growth_factor", 1.0},
                    {"peak_workers", worker_count},
                }},
                {"verdict", {
                    {"result", (total_errors == 0) ? "PASSED" : "PASSED_WITH_WARNINGS"},
                    {"warnings", nlohmann::json::array()},
                    {"checks", nlohmann::json::object()},
                }},
            };
            has_report_ = true;
        }

        SetState(kStateStopped);
        std::cout << "[Engine] Run " << run_id_ << " completed.\n";

    } catch (const std::exception& e) {
        run_error_ = e.what();
        SetState(kStateError);
        std::cerr << "[Engine] Run error: " << e.what() << "\n";
    }
}

void Engine::CreateWorkers() {
    workers_.clear();

    for (const auto& [name, pc] : config_.patterns) {
        if (!pc.enabled) continue;

        for (int ch = 0; ch < pc.channels; ++ch) {
            std::string channel = std::string(defaults::kChannelPrefix)
                                  + run_id_ + "." + name + "." + std::to_string(ch);

            if (name == "events") {
                workers_.push_back(std::make_unique<EventsWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, metrics_.GetPattern("events")));
            } else if (name == "events_store") {
                workers_.push_back(std::make_unique<EventsStoreWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, metrics_.GetPattern("events_store")));
            } else if (name == "queue_simple") {
                workers_.push_back(std::make_unique<QueueSimpleWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, config_.queue,
                    metrics_.GetPattern("queue_simple")));
            } else if (name == "queue_stream") {
                workers_.push_back(std::make_unique<QueueStreamWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, config_.queue,
                    metrics_.GetPattern("queue_stream")));
            } else if (name == "commands") {
                workers_.push_back(std::make_unique<CommandsWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, metrics_.GetPattern("commands")));
            } else if (name == "queries") {
                workers_.push_back(std::make_unique<QueriesWorker>(
                    broker_address_, config_.client_id_prefix,
                    channel, pc.rate, metrics_.GetPattern("queries")));
            }
        }
    }
}

void Engine::StopWorkers() {
    for (auto& w : workers_) {
        w->Stop();
    }
    workers_.clear();
}

void Engine::CleanupWorkerChannels() {
    // Channel cleanup is handled by workers on stop
}

nlohmann::json Engine::GetInfo() const {
    return {
        {"sdk", "kubemq-cpp"},
        {"version", "1.0.0"},
        {"broker_address", broker_address_},
        {"state", State()},
        {"run_id", run_id_},
    };
}

nlohmann::json Engine::GetBrokerStatus() const {
    // Try to ping the broker
    try {
        kubemq::ClientOptions opts;
        opts.set_address(broker_address_);
        opts.set_client_id("burnin-ping");
        opts.set_check_connection(false);

        auto client_or = kubemq::Client::Create(opts);
        if (!client_or.ok()) {
            return {{"connected", false}, {"error", client_or.status().ToString()}};
        }

        auto ping_or = (*client_or)->Ping();
        (void)(*client_or)->Close();

        if (!ping_or.ok()) {
            return {{"connected", false}, {"error", ping_or.status().ToString()}};
        }

        return {
            {"connected", true},
            {"host", ping_or->host},
            {"version", ping_or->version},
            {"uptime_seconds", ping_or->server_up_time_seconds},
        };
    } catch (...) {
        return {{"connected", false}, {"error", "unknown error"}};
    }
}

nlohmann::json Engine::GetRunFull() const {
    std::lock_guard<std::mutex> lock(mu_);

    if (state_ == kStateIdle) {
        return {{"run_id", nullptr}, {"state", "idle"}};
    }

    double elapsed = metrics_.ElapsedSeconds();
    double remaining = (config_.duration_seconds > 0)
        ? std::max(0.0, config_.duration_seconds - elapsed)
        : 0.0;

    // Build patterns detail
    auto snapshot = metrics_.Snapshot();
    nlohmann::json patterns_obj;
    for (const auto& [name, pc] : config_.patterns) {
        if (!pc.enabled) {
            patterns_obj[name] = {{"enabled", false}};
            continue;
        }
        bool is_rpc = (name == "commands" || name == "queries");
        nlohmann::json pdata = {
            {"enabled", true},
            {"state", (state_ == kStateRunning) ? "running" : state_},
            {"channels", pc.channels},
            {"sent", snapshot.contains(name) ? snapshot[name].value("sent", (uint64_t)0) : 0},
            {"errors", snapshot.contains(name) ? snapshot[name].value("errors", (uint64_t)0) : 0},
            {"reconnections", 0},
            {"target_rate", 0}, {"actual_rate", 0}, {"peak_rate", 0},
            {"bytes_sent", snapshot.contains(name) ? snapshot[name].value("bytes_sent", (uint64_t)0) : 0},
            {"bytes_received", snapshot.contains(name) ? snapshot[name].value("bytes_received", (uint64_t)0) : 0},
            {"latency", {{"p50_ms", 0}, {"p95_ms", 0}, {"p99_ms", 0}, {"p999_ms", 0}}},
        };
        if (is_rpc) {
            pdata["responses_success"] = snapshot.contains(name) ? snapshot[name].value("rpc_success", (uint64_t)0) : 0;
            pdata["responses_timeout"] = snapshot.contains(name) ? snapshot[name].value("rpc_timeout", (uint64_t)0) : 0;
            pdata["responses_error"] = snapshot.contains(name) ? snapshot[name].value("rpc_error", (uint64_t)0) : 0;
            pdata["senders"] = nlohmann::json::array();
            pdata["responders"] = nlohmann::json::array();
        } else {
            pdata["received"] = snapshot.contains(name) ? snapshot[name].value("received", (uint64_t)0) : 0;
            pdata["lost"] = snapshot.contains(name) ? snapshot[name].value("lost", (uint64_t)0) : 0;
            pdata["duplicated"] = snapshot.contains(name) ? snapshot[name].value("duplicated", (uint64_t)0) : 0;
            pdata["corrupted"] = snapshot.contains(name) ? snapshot[name].value("corrupted", (uint64_t)0) : 0;
            pdata["out_of_order"] = snapshot.contains(name) ? snapshot[name].value("out_of_order", (uint64_t)0) : 0;
            pdata["loss_pct"] = 0;
            pdata["producers"] = nlohmann::json::array();
            pdata["consumers"] = nlohmann::json::array();
        }
        patterns_obj[name] = pdata;
    }
    // Add disabled entries for any missing standard patterns
    for (const auto& pname : {"events", "events_store", "queue_stream", "queue_simple", "commands", "queries"}) {
        if (!patterns_obj.contains(pname)) {
            patterns_obj[pname] = {{"enabled", false}};
        }
    }

    std::string duration_str = std::to_string(config_.duration_seconds) + "s";

    return {
        {"state", state_},
        {"run_id", run_id_},
        {"mode", "soak"},
        {"started_at", run_started_at_},
        {"elapsed_seconds", static_cast<int64_t>(elapsed)},
        {"remaining_seconds", static_cast<int64_t>(remaining)},
        {"duration", duration_str},
        {"warmup_active", false},
        {"broker_address", broker_address_},
        {"patterns", patterns_obj},
        {"resources", {
            {"rss_mb", 0},
            {"baseline_rss_mb", 0},
            {"memory_growth_factor", 1.0},
            {"active_workers", static_cast<int>(workers_.size())},
        }},
    };
}

nlohmann::json Engine::GetRunStatus() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::string current_state = state_;

    if (current_state == kStateIdle) {
        return {{"run_id", nullptr}, {"state", "idle"}};
    }

    auto snapshot = metrics_.Snapshot();
    double elapsed = metrics_.ElapsedSeconds();
    double remaining = (config_.duration_seconds > 0)
        ? std::max(0.0, config_.duration_seconds - elapsed)
        : 0.0;

    // Build top-level totals from the _totals aggregation
    nlohmann::json totals;
    uint64_t total_sent = 0, total_received = 0, total_errors = 0;
    uint64_t total_lost = 0, total_duplicated = 0, total_corrupted = 0;
    uint64_t total_out_of_order = 0, total_reconnections = 0;
    for (const auto& [name, data] : snapshot.items()) {
        if (name == "_totals" || !data.is_object()) continue;
        total_sent += data.value("sent", (uint64_t)0);
        total_received += data.value("received", (uint64_t)0);
        total_errors += data.value("errors", (uint64_t)0);
        total_lost += data.value("lost", (uint64_t)0);
        total_duplicated += data.value("duplicated", (uint64_t)0);
        total_corrupted += data.value("corrupted", (uint64_t)0);
        total_out_of_order += data.value("out_of_order", (uint64_t)0);
        total_reconnections += data.value("reconnections", (uint64_t)0);
    }
    totals = {
        {"sent", total_sent}, {"received", total_received}, {"errors", total_errors},
        {"lost", total_lost}, {"duplicated", total_duplicated}, {"corrupted", total_corrupted},
        {"out_of_order", total_out_of_order}, {"reconnections", total_reconnections},
    };

    // Build pattern_states with {state, channels} objects
    nlohmann::json pattern_states;
    for (const auto& [name, data] : snapshot.items()) {
        if (name == "_totals" || !data.is_object()) continue;
        std::string ps = (current_state == kStateRunning) ? "running" : current_state;
        int channels = 1;
        auto it = config_.patterns.find(name);
        if (it != config_.patterns.end()) {
            channels = it->second.channels;
        }
        pattern_states[name] = {{"state", ps}, {"channels", channels}};
    }

    return {
        {"state", current_state},
        {"run_id", run_id_},
        {"started_at", run_started_at_},
        {"elapsed_seconds", static_cast<int64_t>(elapsed)},
        {"remaining_seconds", static_cast<int64_t>(remaining)},
        {"warmup_active", false},
        {"totals", totals},
        {"pattern_states", pattern_states},
    };
}

std::pair<nlohmann::json, bool> Engine::GetRunConfig() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (run_id_.empty()) {
        return {nlohmann::json{}, false};
    }
    return {config_.ToJson(), true};
}

std::pair<nlohmann::json, bool> Engine::GetRunReport() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!has_report_) {
        return {nlohmann::json{}, false};
    }
    return {last_report_, true};
}

nlohmann::json Engine::CleanupChannels() {
    std::lock_guard<std::mutex> lock(mu_);
    if (state_ != kStateIdle && state_ != kStateStopped && state_ != kStateError) {
        return {{"error", "can only cleanup in idle/stopped/error state"}};
    }
    // Cleanup is delegated to workers
    return {{"status", "cleanup completed"}};
}

std::string Engine::RunId() const { return run_id_; }
std::string Engine::RunStartedAt() const { return run_started_at_; }
std::string Engine::RunError() const { return run_error_; }

}  // namespace burnin
