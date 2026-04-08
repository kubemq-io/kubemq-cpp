#pragma once

#include <string>

namespace burnin {
namespace defaults {

// Broker defaults
constexpr const char* kDefaultBrokerAddress = "localhost:50000";
constexpr const char* kDefaultClientIdPrefix = "cpp-burnin";

// Run defaults
constexpr int kDefaultDurationSeconds = 900;   // 15 minutes
constexpr int kDefaultWarmupSeconds = 10;

// Rate defaults (messages per second per channel)
constexpr int kDefaultEventsRate = 100;
constexpr int kDefaultEventsStoreRate = 50;
constexpr int kDefaultQueueSimpleRate = 50;
constexpr int kDefaultQueueStreamRate = 50;
constexpr int kDefaultCommandsRate = 20;
constexpr int kDefaultQueriesRate = 20;

// Channel defaults
constexpr int kDefaultEventsChannels = 1;
constexpr int kDefaultEventsStoreChannels = 1;
constexpr int kDefaultQueueSimpleChannels = 1;
constexpr int kDefaultQueueStreamChannels = 1;
constexpr int kDefaultCommandsChannels = 1;
constexpr int kDefaultQueriesChannels = 1;

// Worker defaults
constexpr int kDefaultProducersPerChannel = 1;
constexpr int kDefaultConsumersPerChannel = 1;
constexpr int kDefaultSendersPerChannel = 1;
constexpr int kDefaultRespondersPerChannel = 1;

// Queue polling defaults
constexpr int kDefaultPollMaxMessages = 10;
constexpr int kDefaultPollWaitTimeoutSeconds = 5;
constexpr bool kDefaultAutoAck = false;

// HTTP server default port
constexpr int kDefaultHttpPort = 8888;

// Payload size (bytes)
constexpr int kDefaultPayloadSize = 256;

// Channel prefix for burn-in channels
constexpr const char* kChannelPrefix = "burnin.cpp.";

}  // namespace defaults
}  // namespace burnin
