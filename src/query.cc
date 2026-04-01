// src/query.cc
#include "kubemq/query.h"

#include "internal/uuid.h"
#include "internal/validation.h"

namespace kubemq {
inline namespace v1 {

// --- Query accessors ---

const std::string& Query::id() const {
    return id_;
}
const std::string& Query::channel() const {
    return channel_;
}
const std::string& Query::metadata() const {
    return metadata_;
}
const std::string& Query::body() const {
    return body_;
}
std::chrono::milliseconds Query::timeout() const {
    return timeout_;
}
const std::string& Query::client_id() const {
    return client_id_;
}
const std::string& Query::cache_key() const {
    return cache_key_;
}
std::chrono::milliseconds Query::cache_ttl() const {
    return cache_ttl_;
}
const std::unordered_map<std::string, std::string>& Query::tags() const {
    return tags_;
}

// --- Query::Builder ---

Query::Builder& Query::Builder::SetId(const std::string& id) {
    id_ = id;
    return *this;
}

Query::Builder& Query::Builder::SetChannel(const std::string& channel) {
    channel_ = channel;
    return *this;
}

Query::Builder& Query::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

Query::Builder& Query::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

Query::Builder& Query::Builder::SetBody(std::string&& body) {
    body_ = std::move(body);
    return *this;
}

Query::Builder& Query::Builder::SetTimeout(std::chrono::milliseconds timeout) {
    timeout_ = timeout;
    return *this;
}

Query::Builder& Query::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

Query::Builder& Query::Builder::SetCacheKey(const std::string& key) {
    cache_key_ = key;
    return *this;
}

Query::Builder& Query::Builder::SetCacheTTL(std::chrono::milliseconds ttl) {
    cache_ttl_ = ttl;
    return *this;
}

Query::Builder& Query::Builder::SetTags(std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

Query::Builder& Query::Builder::AddTag(const std::string& key, const std::string& value) {
    tags_[key] = value;
    return *this;
}

StatusOr<Query> Query::Builder::Build() {
    if (channel_.empty()) {
        return Status(ErrorCode::kValidation, "channel is required");
    }
    if (body_.empty() && metadata_.empty()) {
        return Status(ErrorCode::kValidation, "body or metadata is required");
    }
    if (timeout_.count() <= 0) {
        return Status(ErrorCode::kValidation, "timeout must be > 0");
    }

    Query query;

    // Auto-generate ID if empty
    query.id_ = id_.empty() ? internal::GenerateUUID() : id_;
    query.channel_ = std::move(channel_);
    query.metadata_ = std::move(metadata_);
    query.body_ = std::move(body_);
    query.timeout_ = timeout_;
    query.client_id_ = std::move(client_id_);
    query.cache_key_ = std::move(cache_key_);
    query.cache_ttl_ = cache_ttl_;
    query.tags_ = std::move(tags_);

    return query;
}

// --- QueryReply::Builder ---

QueryReply::Builder& QueryReply::Builder::SetRequestId(const std::string& id) {
    request_id_ = id;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetResponseTo(const std::string& channel) {
    response_to_ = channel;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetMetadata(const std::string& metadata) {
    metadata_ = metadata;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetBody(const std::string& body) {
    body_ = body;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetClientId(const std::string& client_id) {
    client_id_ = client_id;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetTags(
    std::unordered_map<std::string, std::string> tags) {
    tags_ = std::move(tags);
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetError(const std::string& error) {
    error_ = error;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetExecutedAt(int64_t timestamp) {
    executed_at_ = timestamp;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetCacheHit(bool hit) {
    cache_hit_ = hit;
    return *this;
}

QueryReply::Builder& QueryReply::Builder::SetExecuted(bool v) {
    executed_ = v;
    return *this;
}

StatusOr<QueryReply> QueryReply::Builder::Build() {
    if (request_id_.empty()) {
        return Status(ErrorCode::kValidation, "request_id is required", "QueryReply::Build", "",
                      "");
    }
    if (response_to_.empty()) {
        return Status(ErrorCode::kValidation, "response_to is required", "QueryReply::Build", "",
                      "");
    }

    QueryReply reply;
    reply.request_id_ = std::move(request_id_);
    reply.response_to_ = std::move(response_to_);
    reply.metadata_ = std::move(metadata_);
    reply.body_ = std::move(body_);
    reply.client_id_ = std::move(client_id_);
    reply.tags_ = std::move(tags_);
    reply.error_ = std::move(error_);
    reply.executed_at_ = executed_at_;
    reply.cache_hit_ = cache_hit_;
    reply.executed_ = executed_;

    return reply;
}

}  // namespace v1
}  // namespace kubemq
