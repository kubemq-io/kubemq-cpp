// src/connection_state.cc
#include "kubemq/connection_state.h"

namespace kubemq {
inline namespace v1 {

const char* ConnectionStateToString(ConnectionState state) {
    switch (state) {
        case ConnectionState::kIdle:
            return "IDLE";
        case ConnectionState::kConnecting:
            return "CONNECTING";
        case ConnectionState::kReady:
            return "READY";
        case ConnectionState::kReconnecting:
            return "RECONNECTING";
        case ConnectionState::kClosed:
            return "CLOSED";
    }
    return "UNKNOWN";
}

}  // namespace v1
}  // namespace kubemq
