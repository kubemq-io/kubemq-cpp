#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "engine/engine.h"
#include "engine/metrics.h"

// Forward declarations for kubemq types
namespace kubemq { inline namespace v1 { class Client; } }

namespace burnin {

// EventsWorker sends and receives events on a single channel.
class EventsWorker : public Worker {
public:
    EventsWorker(const std::string& broker_address,
                 const std::string& client_id_prefix,
                 const std::string& channel,
                 int rate,
                 PatternMetrics& metrics);
    ~EventsWorker() override;

    void Start() override;
    void Stop() override;
    std::string PatternName() const override { return "events"; }

private:
    void ProducerLoop();
    void ConsumerLoop();

    std::string broker_address_;
    std::string client_id_prefix_;
    std::string channel_;
    int rate_;
    PatternMetrics& metrics_;

    std::atomic<bool> running_{false};
    std::thread producer_thread_;
    std::thread consumer_thread_;

    std::unique_ptr<kubemq::Client> producer_client_;
    std::unique_ptr<kubemq::Client> consumer_client_;
};

}  // namespace burnin
