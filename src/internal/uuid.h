// src/internal/uuid.h
#pragma once

#include <string>

namespace kubemq {
namespace internal {

// Generate RFC 4122 UUID v4 string (xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx)
std::string GenerateUUID();

}  // namespace internal
}  // namespace kubemq
