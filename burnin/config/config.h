#pragma once

#include <map>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace burnin {

// PatternConfig holds per-pattern configuration.
struct PatternConfig {
    bool enabled = true;
    int channels = 1;
    int producers_per_channel = 1;
    int consumers_per_channel = 1;
    bool consumer_group = false;
    int senders_per_channel = 1;
    int responders_per_channel = 1;
    int rate = 50;
};

void to_json(nlohmann::json& j, const PatternConfig& p);
void from_json(const nlohmann::json& j, PatternConfig& p);

// QueueConfig holds queue-specific settings.
struct QueueConfig {
    int poll_max_messages = 10;
    int poll_wait_timeout_seconds = 5;
    bool auto_ack = false;
};

void to_json(nlohmann::json& j, const QueueConfig& q);
void from_json(const nlohmann::json& j, QueueConfig& q);

// Config is the full burn-in configuration.
struct Config {
    // Broker
    std::string broker_address = "localhost:50000";
    std::string client_id_prefix = "cpp-burnin";

    // Run parameters
    int duration_seconds = 900;
    int warmup_seconds = 10;
    std::string run_id;
    int payload_size = 256;

    // Per-pattern configs
    std::map<std::string, PatternConfig> patterns;

    // Queue-specific settings
    QueueConfig queue;

    // Warnings from parsing
    std::vector<std::string> warnings;

    // Initialize with defaults for all 6 patterns
    void SetDefaults();

    // Parse from JSON string
    static Config FromJson(const std::string& json_str);

    // Serialize to JSON
    nlohmann::json ToJson() const;
};

void to_json(nlohmann::json& j, const Config& c);
void from_json(const nlohmann::json& j, Config& c);

// Generate an 8-character hex run ID.
std::string GenerateRunId();

}  // namespace burnin
