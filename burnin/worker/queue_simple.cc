#include "worker/queue_simple.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

#include "kubemq/kubemq.h"

namespace burnin {

QueueSimpleWorker::QueueSimpleWorker(const std::string& broker_address,
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

QueueSimpleWorker::~QueueSimpleWorker() {
    Stop();
}

void QueueSimpleWorker::Start() {
    running_.store(true);
    producer_thread_ = std::thread(&QueueSimpleWorker::ProducerLoop, this);
    consumer_thread_ = std::thread(&QueueSimpleWorker::ConsumerLoop, this);
}

void QueueSimpleWorker::Stop() {
    running_.store(false);

    if (producer_thread_.joinable()) producer_thread_.join();
    if (consumer_thread_.joinable()) consumer_thread_.join();

    if (producer_client_) { (void)producer_client_->Close(); producer_client_.reset(); }
    if (consumer_client_) { (void)consumer_client_->Close(); consumer_client_.reset(); }
}

void QueueSimpleWorker::ProducerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qs-producer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueueSimpleWorker] Producer connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    producer_client_ = std::move(*client_or);

    auto interval = std::chrono::microseconds(1000000 / std::max(rate_, 1));
    auto next_send = std::chrono::steady_clock::now();
    uint64_t seq = 0;

    while (running_.load()) {
        ++seq;
        std::string body = "queue_simple-" + std::to_string(seq);

        auto result_or = producer_client_->SendQueueMessageSimple(channel_, body);
        if (result_or.ok() && !result_or->is_error) {
            metrics_.sent.fetch_add(1, std::memory_order_relaxed);
            metrics_.bytes_sent.fetch_add(body.size(), std::memory_order_relaxed);
        } else {
            if (running_.load()) {
                metrics_.errors.fetch_add(1, std::memory_order_relaxed);
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

void QueueSimpleWorker::ConsumerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-qs-consumer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[QueueSimpleWorker] Consumer connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    consumer_client_ = std::move(*client_or);

    while (running_.load()) {
        kubemq::ReceiveQueueMessagesRequest req;
        req.channel = channel_;
        req.max_number_of_messages = queue_config_.poll_max_messages;
        req.wait_time_seconds = std::min(queue_config_.poll_wait_timeout_seconds, 2);

        auto resp_or = consumer_client_->ReceiveQueueMessages(req);
        if (resp_or.ok() && !resp_or->is_error) {
            uint64_t count = resp_or->messages_received;
            metrics_.received.fetch_add(count, std::memory_order_relaxed);
            for (const auto& msg : resp_or->messages) {
                metrics_.bytes_received.fetch_add(msg.body().size(), std::memory_order_relaxed);
            }
        } else if (!resp_or.ok()) {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace burnin
