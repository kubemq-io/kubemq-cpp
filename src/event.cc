// src/event.cc
#include "kubemq/event.h"

#include "internal/uuid.h"
#include "internal/validation.h"

namespace kubemq {
inline namespace v1 {

// --- Event accessors ---

const std::string& Event::id() const {
    return id_;
}
const std::string& Event::channel() const {
    return channel_;
}
const std::string& Event::metadata() const {
    return metadata_;
}
const std::string& Event::body() const {
    return body_;
}
const std::string& Event::client_id() const {
    return client_id_;
}
const std::unordered_map<std::string, std::string>& Event::tags() const {
    return tags_;
}

// --- Event::Builder ---

Event::Builder& Event::Builder::SetId(const std::string& id) {
    id_ = id;
    return *this;
}

Event::Builder& Event::Builder::SetChannel(const std::string& channel) {
    channel_ = channel;
    return *this;
}

Event::Builder& Event::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

Event::Builder& Event::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

Event::Builder& Event::Builder::SetBody(std::string&& body) {
    body_ = std::move(body);
    return *this;
}

Event::Builder& Event::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

Event::Builder& Event::Builder::SetTags(std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

Event::Builder& Event::Builder::AddTag(const std::string& key, const std::string& value) {
    tags_[key] = value;
    return *this;
}

StatusOr<Event> Event::Builder::Build() {
    if (channel_.empty()) {
        return Status(ErrorCode::kValidation, "channel is required");
    }
    if (body_.empty() && metadata_.empty()) {
        return Status(ErrorCode::kValidation, "body or metadata is required");
    }

    Event event;

    // Auto-generate ID if empty
    event.id_ = id_.empty() ? internal::GenerateUUID() : id_;
    event.channel_ = std::move(channel_);
    event.metadata_ = std::move(metadata_);
    event.body_ = std::move(body_);
    event.client_id_ = std::move(client_id_);
    event.tags_ = std::move(tags_);

    return event;
}

}  // namespace v1
}  // namespace kubemq
