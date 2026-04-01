#include "worker/queue_stream.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

#include "kubemq/kubemq.h"

namespace burnin {

QueueStreamWorker::QueueStreamWorker(const std::string& broker_address,
                                     const std::string& client_id_prefix,
                                     const std::string& channel,
                                     int rate,
                                     const QueueConfig& queue_config,
                                     PatternMetrics& metrics)
    : broker_address_(broker_address),
      client_id_prefix_(client_id_prefix),
      channel_(channel),
      rate_(rate),
      queue_config_(queue_config),
      metrics_(metrics) {}

QueueStreamWorker::~QueueStreamWorker() {
    Stop();
}

void QueueStreamWorker::Start() {
    running_.store(true);
    producer_thread_ = std::thread(&QueueStreamWorker::ProducerLoop, this);
    consumer_thread_ = std::thread(&QueueStreamWorker::ConsumerLoop, this);
}

void QueueStreamWorker::Stop() {
    running_.store(false);

    // Close the downstream receiver to unblock Poll() in consumer thread
    if (receiver_) receiver_->Close();

    // Join threads
    if (producer_thread_.joinable()) producer_thread_.join();
    if (consumer_thread_.joinable()) consumer_thread_.join();

    // QueueUpstreamHandle::~QueueUpstreamHandle() calls Close() which
    // blocks on flushing pending writes. Detach cleanup to avoid stalling
    // the shutdown path. The detached thread will clean up when flush
    // completes or the process exits.
    if (upstream_) {
        auto* raw_upstream = upstream_.release();
        auto client = std::move(producer_client_);
        std::thread([raw_upstream, c = std::move(client)]() {
            delete raw_upstream;
            if (c) (void)c->Close();
        }).detach();
    } else {
        if (producer_client_) { (void)producer_client_->Close(); producer_client_.reset(); }
    }

    receiver_.reset();
    if (consumer_client_) { (void)consumer_client_->Close(); consumer_client_.reset(); }
}

void QueueStreamWorker::ProducerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qstream-producer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueueStreamWorker] Producer connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    producer_client_ = std::move(*client_or);

    // Open upstream stream (stored as member so Stop() can close it)
    auto upstream_or = producer_client_->QueueUpstream();
    if (!upstream_or.ok()) {
        std::cerr << "[QueueStreamWorker] Upstream failed: "
                  << upstream_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    upstream_ = std::move(*upstream_or);

    upstream_->SetOnResult([this](const kubemq::QueueUpstreamResult& result) {
        if (!result.is_error) {
            for (const auto& r : result.results) {
                if (!r.is_error) {
                    metrics_.sent.fetch_add(1, std::memory_order_relaxed);
                } else {
                    metrics_.errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        } else {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        }
    });

    upstream_->SetOnError([this](const kubemq::Status& err) {
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
    });

    auto interval = std::chrono::microseconds(1000000 / std::max(rate_, 1));
    auto next_send = std::chrono::steady_clock::now();
    uint64_t seq = 0;

    while (running_.load()) {
        ++seq;
        std::string body = "queue_stream-" + std::to_string(seq);

        std::vector<kubemq::QueueMessage> batch;
        auto msg_or = kubemq::QueueMessage::Builder()
            .SetChannel(channel_)
            .SetBody(body)
            .Build();

        if (msg_or.ok()) {
            batch.push_back(std::move(*msg_or));
            metrics_.bytes_sent.fetch_add(body.size(), std::memory_order_relaxed);

            auto status = upstream_->Send("burnin-" + std::to_string(seq), batch);
            if (!status.ok()) {
                if (running_.load()) {
                    metrics_.errors.fetch_add(1, std::memory_order_relaxed);
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
    // Don't close upstream here — Stop() handles cleanup after joining.
}

void QueueStreamWorker::ConsumerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qstream-consumer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueueStreamWorker] Consumer connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    consumer_client_ = std::move(*client_or);

    // Create a persistent downstream receiver (stored as member so Stop()
    // can close it; also reuses the gRPC stream so acks work reliably).
    auto receiver_or = consumer_client_->NewQueueDownstreamReceiver();
    if (!receiver_or.ok()) {
        std::cerr << "[QueueStreamWorker] Receiver create failed: "
                  << receiver_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    receiver_ = std::move(*receiver_or);

    while (running_.load()) {
        kubemq::PollRequest poll_req;
        poll_req.channel = channel_;
        poll_req.max_items = queue_config_.poll_max_messages;
        poll_req.wait_timeout_seconds = std::min(queue_config_.poll_wait_timeout_seconds, 2);
        poll_req.auto_ack = queue_config_.auto_ack;

        auto poll_or = receiver_->Poll(poll_req);
        if (poll_or.ok() && !poll_or->is_error()) {
            auto& messages = poll_or->messages();
            metrics_.received.fetch_add(messages.size(), std::memory_order_relaxed);

            for (const auto& dmsg : messages) {
                metrics_.bytes_received.fetch_add(
                    dmsg.message().body().size(), std::memory_order_relaxed);
            }

            // Ack all if not auto-ack
            if (!queue_config_.auto_ack && !messages.empty()) {
                auto ack_status = poll_or->AckAll();
                if (!ack_status.ok()) {
                    metrics_.errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        } else if (!poll_or.ok()) {
            if (running_.load()) {
                metrics_.errors.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    // receiver cleanup handled by client Close() in Stop()
}

}  // namespace burnin
