#include "worker/queries.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "kubemq/kubemq.h"

namespace burnin {

QueriesWorker::QueriesWorker(const std::string& broker_address,
                             const std::string& client_id_prefix,
                             const std::string& channel,
                             int rate,
                             PatternMetrics& metrics)
    : broker_address_(broker_address),
      client_id_prefix_(client_id_prefix),
      channel_(channel),
      rate_(rate),
      metrics_(metrics) {}

QueriesWorker::~QueriesWorker() {
    Stop();
}

void QueriesWorker::Start() {
    running_.store(true);
    responder_thread_ = std::thread(&QueriesWorker::ResponderLoop, this);
    // Small delay to let the responder subscribe before sending
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    sender_thread_ = std::thread(&QueriesWorker::SenderLoop, this);
}

void QueriesWorker::Stop() {
    running_.store(false);

    if (sender_thread_.joinable()) sender_thread_.join();
    if (responder_thread_.joinable()) responder_thread_.join();

    if (sender_client_) { (void)sender_client_->Close(); sender_client_.reset(); }
    if (responder_client_) { (void)responder_client_->Close(); responder_client_.reset(); }
}

void QueriesWorker::SenderLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qry-sender");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueriesWorker] Sender connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    sender_client_ = std::move(*client_or);

    auto interval = std::chrono::microseconds(1000000 / std::max(rate_, 1));
    auto next_send = std::chrono::steady_clock::now();
    uint64_t seq = 0;

    while (running_.load()) {
        ++seq;
        std::string body = "query-" + std::to_string(seq);

        auto resp_or = sender_client_->SendQuerySimple(
            channel_, body, std::chrono::milliseconds(5000));

        if (resp_or.ok()) {
            metrics_.sent.fetch_add(1, std::memory_order_relaxed);
            metrics_.bytes_sent.fetch_add(body.size(), std::memory_order_relaxed);
            metrics_.rpc_success.fetch_add(1, std::memory_order_relaxed);
            if (resp_or->executed) {
                metrics_.received.fetch_add(1, std::memory_order_relaxed);
                metrics_.bytes_received.fetch_add(resp_or->body.size(), std::memory_order_relaxed);
            }
        } else {
            if (running_.load()) {
                metrics_.errors.fetch_add(1, std::memory_order_relaxed);
                if (resp_or.status().code() == kubemq::ErrorCode::kTimeout) {
                    metrics_.rpc_timeout.fetch_add(1, std::memory_order_relaxed);
                } else {
                    metrics_.rpc_error.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        next_send += interval;
        if (next_send > std::chrono::steady_clock::now()) {
            std::this_thread::sleep_until(next_send);
        } else {
            next_send = std::chrono::steady_clock::now();
        }
    }
}

void QueriesWorker::ResponderLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qry-responder");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueriesWorker] Responder connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    responder_client_ = std::move(*client_or);

    auto sub_or = responder_client_->SubscribeToQueries(
        channel_, "",
        [this](const kubemq::QueryReceive& query) {
            // Build and send response
            auto reply_or = kubemq::QueryReply::Builder()
                .SetRequestId(query.id)
                .SetResponseTo(query.response_to)
                .SetClientId(client_id_prefix_ + "-qry-responder")
                .SetBody("response-to-" + query.body)
                .Build();

            if (reply_or.ok()) {
                auto status = responder_client_->SendQueryResponse(*reply_or);
                if (!status.ok()) {
                    metrics_.errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        },
        [this](const kubemq::Status& err) {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        });

    if (!sub_or.ok()) {
        std::cerr << "[QueriesWorker] Subscribe failed: "
                  << sub_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    (*sub_or)->Cancel();
}

}  // namespace burnin
