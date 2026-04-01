// src/client_queries.cc
#include <thread>

#include "internal/callback_guard.h"
#include "internal/client_impl.h"
#include "internal/subscription_impl.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "internal/validation.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

StatusOr<QueryResponse> Client::SendQuery(const Query& query) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = query.channel();
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Resolve effective client_id
    std::string client_id = query.client_id();
    if (client_id.empty()) {
        client_id = impl_->opts.client_id();
    }

    // 4. Validate
    s = internal::ValidateChannelNoWildcard(channel, "SendQuery");
    if (!s.ok())
        return s;

    auto timeout = query.timeout();
    if (timeout.count() <= 0) {
        timeout = std::chrono::duration_cast<std::chrono::milliseconds>(kDefaultRPCTimeout);
    }
    s = internal::ValidateTimeout(timeout, "SendQuery");
    if (!s.ok())
        return s;

    s = internal::ValidateBody(query.body(), query.metadata(), "SendQuery");
    if (!s.ok())
        return s;

    // 5. Convert to pb::Request (RequestTypeData=Query)
    ::kubemq_pb::Request proto_request;
    proto_request.set_requestid(query.id().empty() ? internal::GenerateUUID() : query.id());
    proto_request.set_requesttypedata(::kubemq_pb::Request::Query);
    proto_request.set_clientid(client_id);
    proto_request.set_channel(channel);
    proto_request.set_metadata(query.metadata());
    proto_request.set_body(query.body());
    proto_request.set_timeout(static_cast<int32_t>(timeout.count()));

    // Query-specific: CacheKey and CacheTTL
    if (!query.cache_key().empty()) {
        proto_request.set_cachekey(query.cache_key());
    }
    auto cache_ttl = query.cache_ttl();
    if (cache_ttl.count() <= 0 && impl_->opts.default_cache_ttl().count() > 0) {
        cache_ttl = impl_->opts.default_cache_ttl();
    }
    if (cache_ttl.count() > 0) {
        // Proto CacheTTL is in seconds
        proto_request.set_cachettl(static_cast<int32_t>(
            std::chrono::duration_cast<std::chrono::seconds>(cache_ttl).count()));
    }

    for (const auto& [k, v] : query.tags()) {
        (*proto_request.mutable_tags())[k] = v;
    }

    // 6. Call transport.SendRequest() (non-idempotent: skip timeout retry)
    auto response_or = impl_->transport->SendRequest(proto_request);
    if (!response_or.ok())
        return response_or.status();

    const auto& proto_resp = *response_or;

    // 7. Check for server-side error
    if (!proto_resp.error().empty()) {
        return Status(ErrorCode::kFatal, proto_resp.error(), "SendQuery", channel,
                      proto_request.requestid());
    }

    // 8. Convert pb::Response -> QueryResponse
    QueryResponse result;
    result.query_id = proto_resp.requestid();
    result.reply_channel = proto_resp.replychannel();
    result.executed = proto_resp.executed();
    result.executed_at = proto_resp.timestamp();
    result.metadata = proto_resp.metadata();
    result.response_client_id = proto_resp.clientid();
    result.body = proto_resp.body();
    result.cache_hit = proto_resp.cachehit();
    result.error = proto_resp.error();
    for (const auto& [k, v] : proto_resp.tags()) {
        result.tags[k] = v;
    }

    return result;
}

StatusOr<std::unique_ptr<Subscription>> Client::SubscribeToQueries(
    const std::string& channel, const std::string& group,
    std::function<void(const QueryReceive&)> on_query,
    std::function<void(const Status&)> on_error) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve channel
    std::string effective_channel = channel;
    if (effective_channel.empty()) {
        effective_channel = impl_->opts.default_channel();
    }

    // 3. Validate channel
    s = internal::ValidateChannel(effective_channel, "SubscribeToQueries");
    if (!s.ok())
        return s;

    // 4. Build Subscribe proto (SubscribeType=Queries)
    ::kubemq_pb::Subscribe sub;
    sub.set_subscribetypedata(::kubemq_pb::Subscribe::Queries);
    sub.set_clientid(impl_->opts.client_id());
    sub.set_channel(effective_channel);
    sub.set_group(group);

    // 5. Call transport SubscribeToRequests (server stream)
    auto ctx = std::make_shared<grpc::ClientContext>();
    auto reader_or = impl_->transport->SubscribeToRequests(sub, ctx);
    if (!reader_or.ok())
        return reader_or.status();

    // 6. Create Subscription handle
    auto subscription = std::unique_ptr<Subscription>(new Subscription());
    subscription->impl_->id = internal::GenerateUUID();
    subscription->impl_->ctx = ctx;

    // H-11: store reconnection state
    subscription->impl_->subscribe_proto = sub;
    subscription->impl_->transport = impl_->transport.get();

    // 7. Start callback dispatch thread with auto-reconnect (H-11)
    auto reader = std::move(*reader_or);
    auto sub_impl = subscription->impl_.get();
    subscription->impl_->reader_thread =
        std::thread([reader, on_query = std::move(on_query), on_error = std::move(on_error),
                     sub_impl]() mutable {
            grpc::Status last_status;

            // Outer loop: survives reconnections
            while (!sub_impl->done.load(std::memory_order_acquire)) {
                // Inner read loop: process messages from current stream
                ::kubemq_pb::Request proto_request;
                while (reader->Read(&proto_request)) {
                    // Filter: only handle Query type
                    if (proto_request.requesttypedata() != ::kubemq_pb::Request::Query) {
                        continue;
                    }
                    QueryReceive qry;
                    qry.id = proto_request.requestid();
                    qry.channel = proto_request.channel();
                    qry.client_id = proto_request.clientid();
                    qry.metadata = proto_request.metadata();
                    qry.body = proto_request.body();
                    qry.response_to = proto_request.replychannel();
                    qry.timeout = std::chrono::seconds(proto_request.timeout());
                    qry.cache_key = proto_request.cachekey();
                    qry.cache_ttl = std::chrono::seconds(proto_request.cachettl());
                    for (const auto& [k, v] : proto_request.tags()) {
                        qry.tags[k] = v;
                    }
                    internal::SafeInvoke(
                        [&]() {
                            if (on_query)
                                on_query(qry);
                        },
                        on_error, "SubscribeToQueries::OnQuery");
                }

                // Stream ended -- get status
                last_status = reader->Finish();

                // Check if intentional close
                if (sub_impl->done.load(std::memory_order_acquire))
                    break;

                // Stream broke unexpectedly -- attempt reconnect with exponential backoff
                auto backoff = std::chrono::seconds(1);
                const auto max_backoff = std::chrono::seconds(30);
                bool reconnected = false;

                while (!sub_impl->done.load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(backoff);
                    if (sub_impl->done.load(std::memory_order_acquire))
                        break;

                    auto new_ctx = std::make_shared<grpc::ClientContext>();
                    auto result = sub_impl->transport->SubscribeToRequests(
                        sub_impl->subscribe_proto, new_ctx);
                    if (result.ok()) {
                        sub_impl->ctx = new_ctx;
                        reader = std::move(*result);
                        reconnected = true;
                        break;
                    }

                    backoff = std::min(backoff * 2, max_backoff);
                }

                if (!reconnected)
                    break;
            }

            // Thread exiting -- fire on_error only for unexpected termination
            if (!sub_impl->done.load(std::memory_order_acquire)) {
                sub_impl->done.store(true, std::memory_order_release);
                internal::SafeInvoke(
                    [&]() {
                        if (on_error) {
                            on_error(Status(
                                ErrorCode::kTransient,
                                "queries subscription stream ended: " + last_status.error_message(),
                                "SubscribeToQueries", "", ""));
                        }
                    },
                    nullptr, "SubscribeToQueries::OnError");
            }
        });

    return subscription;
}

Status Client::SendQueryResponse(const QueryReply& reply) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Convert QueryReply -> pb::Response
    ::kubemq_pb::Response proto_response;
    proto_response.set_clientid(reply.client_id_.empty() ? impl_->opts.client_id()
                                                         : reply.client_id_);
    proto_response.set_requestid(reply.request_id_);
    proto_response.set_replychannel(reply.response_to_);
    proto_response.set_metadata(reply.metadata_);
    proto_response.set_body(reply.body_);
    proto_response.set_cachehit(reply.cache_hit_);
    proto_response.set_executed(reply.executed());
    proto_response.set_timestamp(reply.executed_at_);
    proto_response.set_error(reply.error_);
    for (const auto& [k, v] : reply.tags_) {
        (*proto_response.mutable_tags())[k] = v;
    }

    // 3. Call transport.SendResponse()
    return impl_->transport->SendResponse(proto_response);
}

StatusOr<QueryResponse> Client::SendQuerySimple(const std::string& channel, const std::string& body,
                                                std::chrono::milliseconds timeout) {
    auto qry_or = Query::Builder().SetChannel(channel).SetBody(body).SetTimeout(timeout).Build();
    if (!qry_or.ok())
        return qry_or.status();
    return SendQuery(*qry_or);
}

}  // namespace v1
}  // namespace kubemq
