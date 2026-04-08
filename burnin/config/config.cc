#include "config/config.h"
#include "config/defaults.h"

#include <cctype>
#include <cstdio>
#include <random>

#include <yaml-cpp/yaml.h>

namespace burnin {

// Parse a duration string like "5m", "1h", "30s", "2d" to seconds.
static int ParseDurationToSeconds(const std::string& s) {
    if (s.empty()) return 0;
    int value = 0;
    size_t i = 0;
    while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) {
        value = value * 10 + (s[i] - '0');
        ++i;
    }
    if (i < s.size()) {
        switch (s[i]) {
            case 's': return value;
            case 'm': return value * 60;
            case 'h': return value * 3600;
            case 'd': return value * 86400;
            default: return value;
        }
    }
    return value;  // bare number treated as seconds
}

// --- PatternConfig JSON ---

void to_json(nlohmann::json& j, const PatternConfig& p) {
    j = nlohmann::json{
        {"enabled", p.enabled},
        {"channels", p.channels},
        {"producers_per_channel", p.producers_per_channel},
        {"consumers_per_channel", p.consumers_per_channel},
        {"consumer_group", p.consumer_group},
        {"senders_per_channel", p.senders_per_channel},
        {"responders_per_channel", p.responders_per_channel},
        {"rate", p.rate},
    };
}

void from_json(const nlohmann::json& j, PatternConfig& p) {
    if (j.contains("enabled")) j.at("enabled").get_to(p.enabled);
    if (j.contains("channels")) j.at("channels").get_to(p.channels);
    if (j.contains("producers_per_channel")) j.at("producers_per_channel").get_to(p.producers_per_channel);
    if (j.contains("consumers_per_channel")) j.at("consumers_per_channel").get_to(p.consumers_per_channel);
    if (j.contains("consumer_group")) j.at("consumer_group").get_to(p.consumer_group);
    if (j.contains("senders_per_channel")) j.at("senders_per_channel").get_to(p.senders_per_channel);
    if (j.contains("responders_per_channel")) j.at("responders_per_channel").get_to(p.responders_per_channel);
    if (j.contains("rate")) j.at("rate").get_to(p.rate);
}

// --- QueueConfig JSON ---

void to_json(nlohmann::json& j, const QueueConfig& q) {
    j = nlohmann::json{
        {"poll_max_messages", q.poll_max_messages},
        {"poll_wait_timeout_seconds", q.poll_wait_timeout_seconds},
        {"auto_ack", q.auto_ack},
    };
}

void from_json(const nlohmann::json& j, QueueConfig& q) {
    if (j.contains("poll_max_messages")) j.at("poll_max_messages").get_to(q.poll_max_messages);
    if (j.contains("poll_wait_timeout_seconds")) j.at("poll_wait_timeout_seconds").get_to(q.poll_wait_timeout_seconds);
    if (j.contains("auto_ack")) j.at("auto_ack").get_to(q.auto_ack);
}

// --- Config ---

void Config::SetDefaults() {
    broker_address = defaults::kDefaultBrokerAddress;
    client_id_prefix = defaults::kDefaultClientIdPrefix;
    duration_seconds = defaults::kDefaultDurationSeconds;
    warmup_seconds = defaults::kDefaultWarmupSeconds;
    payload_size = defaults::kDefaultPayloadSize;

    patterns["events"] = PatternConfig{
        true,
        defaults::kDefaultEventsChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultEventsRate,
    };

    patterns["events_store"] = PatternConfig{
        true,
        defaults::kDefaultEventsStoreChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultEventsStoreRate,
    };

    patterns["queue_simple"] = PatternConfig{
        true,
        defaults::kDefaultQueueSimpleChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultQueueSimpleRate,
    };

    patterns["queue_stream"] = PatternConfig{
        true,
        defaults::kDefaultQueueStreamChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultQueueStreamRate,
    };

    patterns["commands"] = PatternConfig{
        true,
        defaults::kDefaultCommandsChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultCommandsRate,
    };

    patterns["queries"] = PatternConfig{
        true,
        defaults::kDefaultQueriesChannels,
        defaults::kDefaultProducersPerChannel,
        defaults::kDefaultConsumersPerChannel,
        false,
        defaults::kDefaultSendersPerChannel,
        defaults::kDefaultRespondersPerChannel,
        defaults::kDefaultQueriesRate,
    };

    queue.poll_max_messages = defaults::kDefaultPollMaxMessages;
    queue.poll_wait_timeout_seconds = defaults::kDefaultPollWaitTimeoutSeconds;
    queue.auto_ack = defaults::kDefaultAutoAck;
}

Config Config::FromJson(const std::string& json_str) {
    Config cfg;
    cfg.SetDefaults();

    if (json_str.empty()) {
        return cfg;
    }

    try {
        auto j = nlohmann::json::parse(json_str);
        cfg = j.get<Config>();
    } catch (const nlohmann::json::exception& e) {
        cfg.warnings.push_back(std::string("JSON parse warning: ") + e.what());
    }

    return cfg;
}

nlohmann::json Config::ToJson() const {
    nlohmann::json j;
    to_json(j, *this);
    return j;
}

void to_json(nlohmann::json& j, const Config& c) {
    j = nlohmann::json{
        {"broker_address", c.broker_address},
        {"client_id_prefix", c.client_id_prefix},
        {"duration_seconds", c.duration_seconds},
        {"warmup_seconds", c.warmup_seconds},
        {"run_id", c.run_id},
        {"payload_size", c.payload_size},
        {"patterns", c.patterns},
        {"queue", c.queue},
    };
}

void from_json(const nlohmann::json& j, Config& c) {
    c.SetDefaults();

    // v2 format: "broker": {"address": "host:port"}
    if (j.contains("broker") && j["broker"].is_object() && j["broker"].contains("address")) {
        c.broker_address = j["broker"]["address"].get<std::string>();
    }
    // v1 format: "broker_address"
    if (j.contains("broker_address")) j.at("broker_address").get_to(c.broker_address);

    if (j.contains("client_id_prefix")) j.at("client_id_prefix").get_to(c.client_id_prefix);

    // v2 format: "duration": "5m" (string with unit suffix)
    if (j.contains("duration") && j["duration"].is_string()) {
        c.duration_seconds = ParseDurationToSeconds(j["duration"].get<std::string>());
    }
    // v1 format: "duration_seconds" (int) — overrides v2 if both present
    if (j.contains("duration_seconds")) j.at("duration_seconds").get_to(c.duration_seconds);

    // v2 format: "warmup": {"warmup_duration": "10s"}
    if (j.contains("warmup") && j["warmup"].is_object() && j["warmup"].contains("warmup_duration")) {
        c.warmup_seconds = ParseDurationToSeconds(j["warmup"]["warmup_duration"].get<std::string>());
    }
    if (j.contains("warmup_seconds")) j.at("warmup_seconds").get_to(c.warmup_seconds);

    // v2 format: "message": {"size_bytes": 1024}
    if (j.contains("message") && j["message"].is_object() && j["message"].contains("size_bytes")) {
        c.payload_size = j["message"]["size_bytes"].get<int>();
    }
    if (j.contains("payload_size")) j.at("payload_size").get_to(c.payload_size);

    if (j.contains("run_id")) j.at("run_id").get_to(c.run_id);
    if (j.contains("patterns")) j.at("patterns").get_to(c.patterns);
    if (j.contains("queue")) j.at("queue").get_to(c.queue);
}

std::string GenerateRunId() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dist;
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%08x", dist(gen));
    return std::string(buf);
}

// --- YAML startup config loader ---

Config Config::LoadFromYaml(const std::string& path,
                             std::vector<std::string>& warnings) {
    Config cfg;
    // Do NOT call SetDefaults() here — metrics_port stays 0 so the caller can
    // distinguish "not set in YAML" from an explicit value.

    try {
        YAML::Node root = YAML::LoadFile(path);

        // metrics.port
        if (root["metrics"] && root["metrics"]["port"]) {
            cfg.metrics_port = root["metrics"]["port"].as<int>();
        }

        // broker.address (optional)
        if (root["broker"] && root["broker"]["address"]) {
            cfg.broker_address = root["broker"]["address"].as<std::string>();
        }

    } catch (const YAML::Exception& e) {
        warnings.push_back(std::string("[config] YAML load failed for '") +
                           path + "': " + e.what() +
                           " — falling back to defaults");
    }

    return cfg;
}

}  // namespace burnin
