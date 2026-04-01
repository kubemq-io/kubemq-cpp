#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "engine/engine.h"
#include "engine/metrics.h"

namespace kubemq { inline namespace v1 { class Client; } }

namespace burnin {

// CommandsWorker sends commands and responds on a single channel.
class CommandsWorker : public Worker {
public:
    CommandsWorker(const std::string& broker_address,
                   const std::string& client_id_prefix,
                   const std::string& channel,
                   int rate,
                   PatternMetrics& metrics);
    ~CommandsWorker() override;

    void Start() override;
    void Stop() override;
    std::string PatternName() const override { return "commands"; }

private:
    void SenderLoop();
    void ResponderLoop();

    std::string broker_address_;
    std::string client_id_prefix_;
    std::string channel_;
    int rate_;
    PatternMetrics& metrics_;

    std::atomic<bool> running_{false};
    std::thread sender_thread_;
    std::thread responder_thread_;

    std::unique_ptr<kubemq::Client> sender_client_;
    std::unique_ptr<kubemq::Client> responder_client_;
};

}  // namespace burnin
