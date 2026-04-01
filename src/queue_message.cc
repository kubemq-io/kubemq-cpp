// src/queue_message.cc
#include "kubemq/queue_message.h"

#include "internal/uuid.h"
#include "internal/validation.h"

namespace kubemq {
inline namespace v1 {

// --- QueueMessage accessors ---

const std::string& QueueMessage::id() const {
    return id_;
}
const std::string& QueueMessage::channel() const {
    return channel_;
}
const std::string& QueueMessage::metadata() const {
    return metadata_;
}
const std::string& QueueMessage::body() const {
    return body_;
}
const std::string& QueueMessage::client_id() const {
    return client_id_;
}
const std::unordered_map<std::string, std::string>& QueueMessage::tags() const {
    return tags_;
}

const QueuePolicy* QueueMessage::policy() const {
    // Return pointer to policy; always valid since it's a member
    return &policy_;
}

const QueueMessageAttributes* QueueMessage::attributes() const {
    return &attributes_;
}

// --- QueueMessage::Builder ---

QueueMessage::Builder& QueueMessage::Builder::SetId(const std::string& id) {
    id_ = id;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetChannel(const std::string& channel) {
    channel_ = channel;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetBody(std::string&& body) {
    body_ = std::move(body);
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetTags(
    std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::AddTag(const std::string& key,
                                                     const std::string& value) {
    tags_[key] = value;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetPolicy(const QueuePolicy& policy) {
    policy_ = policy;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetExpirationSeconds(int seconds) {
    policy_.expiration_seconds = seconds;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetDelaySeconds(int seconds) {
    policy_.delay_seconds = seconds;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetMaxReceiveCount(int count) {
    policy_.max_receive_count = count;
    return *this;
}

QueueMessage::Builder& QueueMessage::Builder::SetMaxReceiveQueue(const std::string& queue) {
    policy_.max_receive_queue = queue;
    return *this;
}

StatusOr<QueueMessage> QueueMessage::Builder::Build() {
    if (channel_.empty()) {
        return Status(ErrorCode::kValidation, "channel is required");
    }
    if (body_.empty() && metadata_.empty()) {
        return Status(ErrorCode::kValidation, "body or metadata is required");
    }

    QueueMessage message;

    // Auto-generate ID if empty
    message.id_ = id_.empty() ? internal::GenerateUUID() : id_;
    message.channel_ = std::move(channel_);
    message.metadata_ = std::move(metadata_);
    message.body_ = std::move(body_);
    message.client_id_ = std::move(client_id_);
    message.tags_ = std::move(tags_);
    message.policy_ = std::move(policy_);

    return message;
}

}  // namespace v1
}  // namespace kubemq
