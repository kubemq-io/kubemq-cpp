#include "worker/events.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "kubemq/kubemq.h"

namespace burnin {

EventsWorker::EventsWorker(const std::string& broker_address,
                           const std::string& client_id_prefix,
                           const std::string& channel,
                           int rate,
                           PatternMetrics& metrics)
    : broker_address_(broker_address),
      client_id_prefix_(client_id_prefix),
      channel_(channel),
      rate_(rate),
      metrics_(metrics) {}

EventsWorker::~EventsWorker() {
    Stop();
}

void EventsWorker::Start() {
    running_.store(true);
    consumer_thread_ = std::thread(&EventsWorker::ConsumerLoop, this);
    producer_thread_ = std::thread(&EventsWorker::ProducerLoop, this);
}

void EventsWorker::Stop() {
    running_.store(false);

    if (producer_thread_.joinable()) producer_thread_.join();
    if (consumer_thread_.joinable()) consumer_thread_.join();

    if (producer_client_) { (void)producer_client_->Close(); producer_client_.reset(); }
    if (consumer_client_) { (void)consumer_client_->Close(); consumer_client_.reset(); }
}

void EventsWorker::ProducerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-events-producer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[EventsWorker] Producer connect failed: "
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
        std::string body = "events-" + std::to_string(seq);

        auto status = producer_client_->PublishEvent(channel_, body);
        if (status.ok()) {
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

void EventsWorker::ConsumerLoop() {
    kubemq::ClientOptions opts;
    opts.set_address(broker_address_);
    opts.set_client_id(client_id_prefix_ + "-events-consumer");

    auto client_or = kubemq::Client::Create(opts);
    if (!client_or.ok()) {
        std::cerr << "[EventsWorker] Consumer connect failed: "
                  << client_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    consumer_client_ = std::move(*client_or);

    auto sub_or = consumer_client_->SubscribeToEvents(
        channel_, "",
        [this](const kubemq::EventReceive& event) {
            metrics_.received.fetch_add(1, std::memory_order_relaxed);
            metrics_.bytes_received.fetch_add(event.body.size(), std::memory_order_relaxed);
        },
        [this](const kubemq::Status& err) {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        });

    if (!sub_or.ok()) {
        std::cerr << "[EventsWorker] Subscribe failed: "
                  << sub_or.status().ToString() << "\n";
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // Wait until stopped
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    (*sub_or)->Cancel();
}

}  // namespace burnin
