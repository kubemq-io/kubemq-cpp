/// @file logger.h
/// @brief Abstract logging interface for SDK diagnostic output.

// include/kubemq/logger.h
#pragma once

#include <string>

#include "kubemq/export.h"

namespace kubemq {
inline namespace v1 {

/// @brief Abstract interface for receiving SDK log messages.
///
/// Implement this interface and pass an instance to ClientOptions::set_logger()
/// to integrate KubeMQ SDK logging with your application's logging framework.
/// The SDK calls these methods from internal threads; implementations must be
/// thread-safe.
///
/// @note All methods must be thread-safe.
/// @see ClientOptions
class KUBEMQ_API Logger {
public:
    /// @brief Virtual destructor.
    virtual ~Logger() = default;

    /// @brief Logs a debug-level message.
    /// @param message The log message text.
    virtual void Debug(const std::string& message) = 0;

    /// @brief Logs an informational message.
    /// @param message The log message text.
    virtual void Info(const std::string& message) = 0;

    /// @brief Logs a warning message.
    /// @param message The log message text.
    virtual void Warn(const std::string& message) = 0;

    /// @brief Logs an error message.
    /// @param message The log message text.
    virtual void Error(const std::string& message) = 0;
};

}  // namespace v1
}  // namespace kubemq
