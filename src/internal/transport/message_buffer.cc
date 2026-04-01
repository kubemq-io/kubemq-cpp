// src/internal/transport/message_buffer.cc
#include "internal/transport/message_buffer.h"

#include <utility>

namespace kubemq {
namespace internal {

MessageBuffer::MessageBuffer(int capacity) : capacity_(capacity > 0 ? capacity : 1) {
    buffer_.resize(static_cast<size_t>(capacity_));
}

int MessageBuffer::Push(std::string message) {
    std::lock_guard<std::mutex> lock(mu_);
    int evicted = 0;

    if (count_ == capacity_) {
        // Buffer is full -- evict the oldest entry (overwrite head).
        head_ = (head_ + 1) % capacity_;
        evicted = 1;
    } else {
        ++count_;
    }

    buffer_[static_cast<size_t>(tail_)] = std::move(message);
    tail_ = (tail_ + 1) % capacity_;

    return evicted;
}

int MessageBuffer::Drain(const std::function<bool(std::string&&)>& sender) {
    std::lock_guard<std::mutex> lock(mu_);
    int drained = 0;

    while (count_ > 0) {
        auto idx = static_cast<size_t>(head_);
        bool ok = sender(std::move(buffer_[idx]));
        buffer_[idx].clear();
        head_ = (head_ + 1) % capacity_;
        --count_;
        ++drained;
        if (!ok) {
            // Sender signaled stop -- leave remaining messages in buffer.
            break;
        }
    }

    return drained;
}

int MessageBuffer::Discard() {
    std::lock_guard<std::mutex> lock(mu_);
    int discarded = count_;

    // Clear all entries.
    for (int i = 0; i < count_; ++i) {
        auto idx = static_cast<size_t>((head_ + i) % capacity_);
        buffer_[idx].clear();
    }

    head_ = 0;
    tail_ = 0;
    count_ = 0;

    return discarded;
}

int MessageBuffer::Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return count_;
}

int MessageBuffer::Capacity() const {
    // Capacity is immutable after construction, no lock needed.
    return capacity_;
}

bool MessageBuffer::Empty() const {
    std::lock_guard<std::mutex> lock(mu_);
    return count_ == 0;
}

}  // namespace internal
}  // namespace kubemq
