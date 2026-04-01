// src/subscription_option.cc
#include "kubemq/subscription_option.h"

namespace kubemq {
inline namespace v1 {

// Proto EventsStoreType values:
//   StartNewOnly = 1, StartFromFirst = 2, StartFromLast = 3,
//   StartAtSequence = 4, StartAtTime = 5, StartAtTimeDelta = 6

SubscriptionOption SubscriptionOption::StartFromNewEvents() {
    SubscriptionOption opt;
    opt.type_ = 1;  // StartNewOnly
    opt.value_ = 0;
    return opt;
}

SubscriptionOption SubscriptionOption::StartFromFirstEvent() {
    SubscriptionOption opt;
    opt.type_ = 2;  // StartFromFirst
    opt.value_ = 0;
    return opt;
}

SubscriptionOption SubscriptionOption::StartFromLastEvent() {
    SubscriptionOption opt;
    opt.type_ = 3;  // StartFromLast
    opt.value_ = 0;
    return opt;
}

SubscriptionOption SubscriptionOption::StartFromSequence(uint64_t sequence) {
    SubscriptionOption opt;
    opt.type_ = 4;  // StartAtSequence
    opt.value_ = static_cast<int64_t>(sequence);
    return opt;
}

SubscriptionOption SubscriptionOption::StartFromTime(std::chrono::system_clock::time_point time) {
    SubscriptionOption opt;
    opt.type_ = 5;  // StartAtTime
    // Convert to Unix nanoseconds
    auto epoch = time.time_since_epoch();
    opt.value_ = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count();
    return opt;
}

SubscriptionOption SubscriptionOption::StartFromTimeDelta(std::chrono::seconds delta) {
    SubscriptionOption opt;
    opt.type_ = 6;  // StartAtTimeDelta
    opt.value_ = delta.count();
    return opt;
}

}  // namespace v1
}  // namespace kubemq
