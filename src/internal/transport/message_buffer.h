// src/internal/transport/message_buffer.h
#pragma once

#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace kubemq {
namespace internal {

// Ring buffer for message buffering during reconnection.
class MessageBuffer {
public:
    explicit MessageBuffer(
        int capacity);  // Capacity from ReconnectPolicy::buffer_size (default: 1024)

    // Push message bytes to buffer. Returns number of evicted messages.
    int Push(std::string message);

    // Drain all buffered messages. Calls sender for each.
    // Returns number of messages drained.
    int Drain(const std::function<bool(std::string&&)>& sender);

    // Discard all buffered messages. Returns count discarded.
    int Discard();

    int Size() const;
    int Capacity() const;
    bool Empty() const;

private:
    mutable std::mutex mu_;
    std::vector<std::string> buffer_;
    int head_ = 0;
    int tail_ = 0;
    int count_ = 0;
    int capacity_;
};

}  // namespace internal
}  // namespace kubemq
