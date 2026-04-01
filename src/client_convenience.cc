// src/client_convenience.cc
#include "kubemq/client.h"
#include "kubemq/types.h"

namespace kubemq {
inline namespace v1 {

// --- Create ---

Status Client::CreateEventsChannel(const std::string& name) {
    return CreateChannel(name, kChannelTypeEvents);
}

Status Client::CreateEventsStoreChannel(const std::string& name) {
    return CreateChannel(name, kChannelTypeEventsStore);
}

Status Client::CreateCommandsChannel(const std::string& name) {
    return CreateChannel(name, kChannelTypeCommands);
}

Status Client::CreateQueriesChannel(const std::string& name) {
    return CreateChannel(name, kChannelTypeQueries);
}

Status Client::CreateQueuesChannel(const std::string& name) {
    return CreateChannel(name, kChannelTypeQueues);
}

// --- Delete ---

Status Client::DeleteEventsChannel(const std::string& name) {
    return DeleteChannel(name, kChannelTypeEvents);
}

Status Client::DeleteEventsStoreChannel(const std::string& name) {
    return DeleteChannel(name, kChannelTypeEventsStore);
}

Status Client::DeleteCommandsChannel(const std::string& name) {
    return DeleteChannel(name, kChannelTypeCommands);
}

Status Client::DeleteQueriesChannel(const std::string& name) {
    return DeleteChannel(name, kChannelTypeQueries);
}

Status Client::DeleteQueuesChannel(const std::string& name) {
    return DeleteChannel(name, kChannelTypeQueues);
}

// --- List ---

StatusOr<std::vector<ChannelInfo>> Client::ListEventsChannels(const std::string& search) {
    return ListChannels(kChannelTypeEvents, search);
}

StatusOr<std::vector<ChannelInfo>> Client::ListEventsStoreChannels(const std::string& search) {
    return ListChannels(kChannelTypeEventsStore, search);
}

StatusOr<std::vector<ChannelInfo>> Client::ListCommandsChannels(const std::string& search) {
    return ListChannels(kChannelTypeCommands, search);
}

StatusOr<std::vector<ChannelInfo>> Client::ListQueriesChannels(const std::string& search) {
    return ListChannels(kChannelTypeQueries, search);
}

StatusOr<std::vector<ChannelInfo>> Client::ListQueuesChannels(const std::string& search) {
    return ListChannels(kChannelTypeQueues, search);
}

}  // namespace v1
}  // namespace kubemq
