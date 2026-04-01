// src/command.cc
#include "kubemq/command.h"

#include "internal/uuid.h"
#include "internal/validation.h"

namespace kubemq {
inline namespace v1 {

// --- Command accessors ---

const std::string& Command::id() const {
    return id_;
}
const std::string& Command::channel() const {
    return channel_;
}
const std::string& Command::metadata() const {
    return metadata_;
}
const std::string& Command::body() const {
    return body_;
}
std::chrono::milliseconds Command::timeout() const {
    return timeout_;
}
const std::string& Command::client_id() const {
    return client_id_;
}
const std::unordered_map<std::string, std::string>& Command::tags() const {
    return tags_;
}

// --- Command::Builder ---

Command::Builder& Command::Builder::SetId(const std::string& id) {
    id_ = id;
    return *this;
}

Command::Builder& Command::Builder::SetChannel(const std::string& channel) {
    channel_ = channel;
    return *this;
}

Command::Builder& Command::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

Command::Builder& Command::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

Command::Builder& Command::Builder::SetBody(std::string&& body) {
    body_ = std::move(body);
    return *this;
}

Command::Builder& Command::Builder::SetTimeout(std::chrono::milliseconds timeout) {
    timeout_ = timeout;
    return *this;
}

Command::Builder& Command::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

Command::Builder& Command::Builder::SetTags(std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

Command::Builder& Command::Builder::AddTag(const std::string& key, const std::string& value) {
    tags_[key] = value;
    return *this;
}

StatusOr<Command> Command::Builder::Build() {
    if (channel_.empty()) {
        return Status(ErrorCode::kValidation, "channel is required");
    }
    if (body_.empty() && metadata_.empty()) {
        return Status(ErrorCode::kValidation, "body or metadata is required");
    }
    if (timeout_.count() <= 0) {
        return Status(ErrorCode::kValidation, "timeout must be > 0");
    }

    Command command;

    // Auto-generate ID if empty
    command.id_ = id_.empty() ? internal::GenerateUUID() : id_;
    command.channel_ = std::move(channel_);
    command.metadata_ = std::move(metadata_);
    command.body_ = std::move(body_);
    command.timeout_ = timeout_;
    command.client_id_ = std::move(client_id_);
    command.tags_ = std::move(tags_);

    return command;
}

// --- CommandReply::Builder ---

CommandReply::Builder& CommandReply::Builder::SetRequestId(const std::string& id) {
    request_id_ = id;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetResponseTo(const std::string& channel) {
    response_to_ = channel;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetTags(
    std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetError(const std::string& error) {
    error_ = error;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetExecutedAt(int64_t timestamp) {
    executed_at_ = timestamp;
    return *this;
}

CommandReply::Builder& CommandReply::Builder::SetExecuted(bool v) {
    executed_ = v;
    return *this;
}

StatusOr<CommandReply> CommandReply::Builder::Build() {
    if (request_id_.empty()) {
        return Status(ErrorCode::kValidation, "request_id is required", "CommandReply::Build", "",
                      "");
    }
    if (response_to_.empty()) {
        return Status(ErrorCode::kValidation, "response_to is required", "CommandReply::Build", "",
                      "");
    }

    CommandReply reply;
    reply.request_id_ = std::move(request_id_);
    reply.response_to_ = std::move(response_to_);
    reply.metadata_ = std::move(metadata_);
    reply.body_ = std::move(body_);
    reply.client_id_ = std::move(client_id_);
    reply.tags_ = std::move(tags_);
    reply.error_ = std::move(error_);
    reply.executed_at_ = executed_at_;
    reply.executed_ = executed_;

    return reply;
}

}  // namespace v1
}  // namespace kubemq
