// src/internal/types/jitter.h
#pragma once

#include <chrono>

#include "kubemq/reconnect_policy.h"

namespace kubemq {
namespace internal {

// Apply jitter to a delay value based on the specified mode.
//   kNone:  return delay unchanged
//   kFull:  random in [0, delay)
//   kEqual: delay/2 + random in [0, delay/2)
std::chrono::milliseconds ApplyJitter(std::chrono::milliseconds delay, JitterMode mode);

}  // namespace internal
}  // namespace kubemq
