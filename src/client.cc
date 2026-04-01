// src/client.cc
#include "kubemq/client.h"

#include <mutex>

#include "internal/client_impl.h"
#include "internal/transport/grpc_transport.h"
#include "internal/uuid.h"

namespace kubemq {
inline namespace v1 {

// --- Constructors / Destructor ---

Client::Client() : impl_(std::make_unique<Impl>()) {}

Client::~Client() {
    Close();
}

// --- Factory ---

StatusOr<std::unique_ptr<Client>> Client::Create(const ClientOptions& options) {
    // Copy options so we can mutate (auto-generate client_id)
    ClientOptions opts = options;

    // Auto-generate client_id if empty
    if (opts.client_id().empty()) {
        opts.mutable_client_id() = internal::GenerateUUID();
    }

    // Validate options
    auto validation = opts.Validate();
    if (!validation.ok()) {
        return validation;
    }

    // Create transport
    auto transport_or = internal::GrpcTransport::Create(opts);
    if (!transport_or.ok()) {
        return transport_or.status();
    }

    // Assemble client
    auto client = std::unique_ptr<Client>(new Client());
    client->impl_->transport = std::move(*transport_or);
    client->impl_->opts = std::move(opts);

    // H-10: Wire connection state callbacks from options to transport state machine
    if (auto* sm = client->impl_->transport->GetStateMachine()) {
        const auto& co = client->impl_->opts;
        if (co.on_connected())
            sm->SetOnConnected(co.on_connected());
        if (co.on_disconnected())
            sm->SetOnDisconnected(co.on_disconnected());
        if (co.on_reconnecting())
            sm->SetOnReconnecting(co.on_reconnecting());
        if (co.on_reconnected())
            sm->SetOnReconnected(co.on_reconnected());
        if (co.on_closed())
            sm->SetOnClosed(co.on_closed());
    }

    // If check_connection is enabled, verify with Ping
    if (client->impl_->opts.check_connection()) {
        auto ping_or = client->Ping();
        if (!ping_or.ok()) {
            return ping_or.status();
        }
    }

    return client;
}

// --- Connection State ---

ConnectionState Client::State() const {
    if (impl_->closed.load(std::memory_order_acquire)) {
        return ConnectionState::kClosed;
    }
    if (impl_->transport) {
        return impl_->transport->State();
    }
    return ConnectionState::kIdle;
}

// --- Ping ---

StatusOr<ServerInfo> Client::Ping() {
    auto s = impl_->CheckClosed();
    if (!s.ok()) {
        return s;
    }
    return impl_->transport->Ping();
}

// --- Close ---

Status Client::Close() {
    Status result;
    std::call_once(impl_->close_once, [this, &result]() {
        impl_->closed.store(true, std::memory_order_release);
        impl_->DrainAll(impl_->opts.drain_timeout());
        if (impl_->transport) {
            result = impl_->transport->Close();
        }
    });
    return result;
}

}  // namespace v1
}  // namespace kubemq
