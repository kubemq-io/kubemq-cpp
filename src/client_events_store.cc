// src/client_events_store.cc
#include <atomic>
#include <thread>

#include "internal/callback_guard.h"
#include "internal/client_impl.h"
#include "internal/stream_handle_impl.h"
#include "internal/subscription_impl.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"
#include "internal/validation.h"
#include "kubemq.grpc.pb.h"
#include "kubemq.pb.h"
#include "kubemq/client.h"

namespace kubemq {
inline namespace v1 {

EventStoreStreamHandle::EventStoreStreamHandle() : impl_(std::make_unique<Impl>()) {}

EventStoreStreamHandle::~EventStoreStreamHandle() {
    Close();
}

Status EventStoreStreamHandle::Send(const EventStore& event) {
    ::kubemq_pb::Event proto_event;
    proto_event.set_eventid(event.id().empty() ? internal::GenerateUUID() : event.id());
    proto_event.set_clientid(event.client_id().empty() ? impl_->opts.client_id()
                                                       : event.client_id());
    proto_event.set_channel(event.channel());
    proto_event.set_metadata(event.metadata());
    proto_event.set_body(event.body());
    proto_event.set_store(true);
    for (const auto& [k, v] : event.tags()) {
        (*proto_event.mutable_tags())[k] = v;
    }

    std::lock_guard<std::mutex> lock(impl_->write_mu);
    if (impl_->done.load(std::memory_order_acquire)) {
        return Status(ErrorCode::kFatal, "event store stream is closed",
                      "EventStoreStreamHandle::Send", event.channel(), "");
    }
    if (!impl_->stream->Write(proto_event)) {
        return Status(ErrorCode::kTransient, "failed to write to event store stream",
                      "EventStoreStreamHandle::Send", event.channel(), "");
    }
    return Status();
}

void EventStoreStreamHandle::Close() {
    {
        std::lock_guard<std::mutex> lock(impl_->write_mu);
        if (impl_->done.exchange(true, std::memory_order_acq_rel))
            return;
        if (impl_->stream) {
            impl_->stream->WritesDone();
        }
    }
    impl_->JoinReader();
}

bool EventStoreStreamHandle::IsDone() const {
    return impl_->done.load(std::memory_order_acquire);
}

void EventStoreStreamHandle::Wait() {
    impl_->JoinReader();
}

void EventStoreStreamHandle::SetOnResult(std::function<void(const EventStoreResult&)> callback) {
    std::lock_guard<std::mutex> lock(impl_->callback_mu);
    impl_->on_result = std::move(callback);
}

void EventStoreStreamHandle::SetOnError(std::function<void(const Status&)> callback) {
    std::lock_guard<std::mutex> lock(impl_->callback_mu);
    impl_->on_error = std::move(callback);
}

// --- Client EventStore Methods ---

StatusOr<EventStoreResult> Client::SendEventStore(const EventStore& event) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Resolve effective channel
    std::string channel = event.channel();
    if (channel.empty()) {
        channel = impl_->opts.default_channel();
    }

    // 3. Resolve effective client_id
    std::string client_id = event.client_id();
    if (client_id.empty()) {
        client_id = impl_->opts.client_id();
    }

    // 4. Validate
    s = internal::ValidateChannelNoWildcard(channel, "SendEventStore");
    if (!s.ok())
        return s;

    s = internal::ValidateBody(event.body(), event.metadata(), "SendEventStore");
    if (!s.ok())
        return s;

    // 5. Convert to proto Event (Store=true)
    ::kubemq_pb::Event proto_event;
    proto_event.set_eventid(event.id().empty() ? internal::GenerateUUID() : event.id());
    proto_event.set_clientid(client_id);
    proto_event.set_channel(channel);
    proto_event.set_metadata(event.metadata());
    proto_event.set_body(event.body());
    proto_event.set_store(true);
    for (const auto& [k, v] : event.tags()) {
        (*proto_event.mutable_tags())[k] = v;
    }

    // 6. Call transport.SendEventStore()
    return impl_->transport->SendEventStore(proto_event);
}

StatusOr<std::unique_ptr<EventStoreStreamHandle>> Client::SendEventStoreStream(
    std::function<void(const EventStoreResult&)> on_result,
    std::function<void(const Status&)> on_error) {
    // 1. Check client not closed
    auto s = impl_->CheckClosed();
    if (!s.ok())
        return s;

    // 2. Open bidi stream via transport
    std::unique_ptr<grpc::ClientContext> ctx;
    auto stream_or = impl_->transport->OpenEventStream(ctx);
    if (!stream_or.ok())
        return stream_or.status();

    // 3. Create EventStoreStreamHandle wrapping the stream
    auto handle = std::unique_ptr<EventStoreStreamHandle>(new EventStoreStreamHandle());
    handle->impl_->ctx = std::move(ctx);
    handle->impl_->stream = std::move(*stream_or);
    handle->impl_->opts = impl_->opts;

    // 4. Store callbacks before starting reader (no race)
    handle->impl_->on_result = std::move(on_result);
    handle->impl_->on_error = std::move(on_error);

    // 5. Start background thread reading results from stream
    handle->impl_->StartReader();

    return handle;
}

StatusOr<std::unique_ptr<EventStoreStreamHandle>> Client::SendEventStoreStream() {
    return SendEventStoreStream(nullptr, nullptr);
}

StatusOr<std::unique_ptr<Subscription>> Client::SubscribeToEventsStore(
    const std::string& channel, const std::string& group, const SubscriptionOption& start_option,
    std::function<void(const EventStoreReceive&)> on_event,
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
    s = internal::ValidateChannel(effective_channel, "SubscribeToEventsStore");
    if (!s.ok())
        return s;

    // 4. Build Subscribe proto with EventsStoreType
    ::kubemq_pb::Subscribe sub;
    sub.set_subscribetypedata(::kubemq_pb::Subscribe::EventsStore);
    sub.set_clientid(impl_->opts.client_id());
    sub.set_channel(effective_channel);
    sub.set_group(group);
    sub.set_eventsstoretypedata(
        static_cast<::kubemq_pb::Subscribe_EventsStoreType>(start_option.type()));
    sub.set_eventsstoretypevalue(start_option.value());

    // 5. Call transport SubscribeToEvents (same RPC for Events and EventsStore)
    auto ctx = std::make_shared<grpc::ClientContext>();
    auto reader_or = impl_->transport->SubscribeToEvents(sub, ctx);
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
        std::thread([reader, on_event = std::move(on_event), on_error = std::move(on_error),
                     sub_impl]() mutable {
            grpc::Status last_status;

            // Outer loop: survives reconnections
            while (!sub_impl->done.load(std::memory_order_acquire)) {
                // Inner read loop: process messages from current stream
                ::kubemq_pb::EventReceive proto_event;
                while (reader->Read(&proto_event)) {
                    EventStoreReceive event;
                    event.id = proto_event.eventid();
                    event.sequence = proto_event.sequence();
                    event.timestamp = proto_event.timestamp();
                    event.channel = proto_event.channel();
                    event.metadata = proto_event.metadata();
                    event.body = proto_event.body();
                    for (const auto& [k, v] : proto_event.tags()) {
                        event.tags[k] = v;
                    }
                    internal::SafeInvoke(
                        [&]() {
                            if (on_event)
                                on_event(event);
                        },
                        on_error, "SubscribeToEventsStore::OnEvent");
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
                    auto result =
                        sub_impl->transport->SubscribeToEvents(sub_impl->subscribe_proto, new_ctx);
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
                            on_error(Status(ErrorCode::kTransient,
                                            "events store subscription stream ended: " +
                                                last_status.error_message(),
                                            "SubscribeToEventsStore", "", ""));
                        }
                    },
                    nullptr, "SubscribeToEventsStore::OnError");
            }
        });

    return subscription;
}

StatusOr<EventStoreResult> Client::PublishEventStore(
    const std::string& channel, const std::string& body, const std::string& metadata,
    const std::unordered_map<std::string, std::string>& tags) {
    auto event_or = EventStore::Builder()
                        .SetChannel(channel)
                        .SetBody(body)
                        .SetMetadata(metadata)
                        .SetTags(tags)
                        .Build();
    if (!event_or.ok())
        return event_or.status();
    return SendEventStore(*event_or);
}

}  // namespace v1
}  // namespace kubemq
