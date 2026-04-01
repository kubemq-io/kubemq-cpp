// src/internal/callback_guard.h
#pragma once

#include <exception>
#include <functional>
#include <string>
#include <utility>

#include "internal/types/error.h"
#include "kubemq/error_code.h"
#include "kubemq/status.h"

namespace kubemq {
namespace internal {

// Invoke a user callback safely. Catches all exceptions and routes them
// to on_error. If on_error itself throws, the exception is silently swallowed
// to prevent std::terminate from background threads.
// Primary overload for std::function (supports bool check for null).
template <typename Fn>
void SafeInvoke(Fn&& fn, const std::function<void(const Status&)>& on_error,
                const char* operation) {
    try {
        std::forward<Fn>(fn)();
    } catch (const std::exception& e) {
        if (on_error)
            try {
                on_error(MakeError(ErrorCode::kFatal, std::string("callback threw: ") + e.what(),
                                   operation));
            } catch (...) {
            }
    } catch (...) {
        if (on_error)
            try {
                on_error(
                    MakeError(ErrorCode::kFatal, "callback threw unknown exception", operation));
            } catch (...) {
            }
    }
}

// Overload for raw callables (lambdas) — always assumed non-null.
template <typename Fn, typename ErrFn,
          typename = std::enable_if_t<
              !std::is_same_v<std::decay_t<ErrFn>, std::function<void(const Status&)>> &&
              !std::is_null_pointer_v<std::decay_t<ErrFn>>>>
void SafeInvoke(Fn&& fn, ErrFn&& on_error, const char* operation) {
    try {
        std::forward<Fn>(fn)();
    } catch (const std::exception& e) {
        try {
            on_error(MakeError(ErrorCode::kFatal, std::string("callback threw: ") + e.what(),
                               operation));
        } catch (...) {
        }
    } catch (...) {
        try {
            on_error(MakeError(ErrorCode::kFatal, "callback threw unknown exception", operation));
        } catch (...) {
        }
    }
}

// Convenience overload for fire-and-forget (no error handler).
template <typename Fn>
void SafeInvoke(Fn&& fn, std::nullptr_t, const char*) {
    try {
        std::forward<Fn>(fn)();
    } catch (...) {
    }
}

}  // namespace internal
}  // namespace kubemq
