#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "config/config.h"
#include "engine/engine.h"
#include "engine/metrics.h"

namespace kubemq { inline namespace v1 { class Client; } }

namespace burnin {

// QueueSimpleWorker sends and receives queue messages using the simple API.
class QueueSimpleWorker : public Worker {
public:
    QueueSimpleWorker(const std::string& broker_address,
                      const std::string& client_id_prefix,
                      const std::string& channel,
                      int rate,
                      const QueueConfig& queue_config,
                      PatternMetrics& metrics);
    ~QueueSimpleWorker() override;

    void Start() override;
    void Stop() override;
    std::string PatternName() const override { return "queue_simple"; }

private:
    void ProducerLoop();
    void ConsumerLoop();

    std::string broker_address_;
    std::string client_id_prefix_;
    std::string channel_;
    int rate_;
    QueueConfig queue_config_;
    PatternMetrics& metrics_;

    std::atomic<bool> running_{false};
    std::thread producer_thread_;
    std::thread consumer_thread_;

    std::unique_ptr<kubemq::Client> producer_client_;
    std::unique_ptr<kubemq::Client> consumer_client_;
};

}  // namespace burnin
