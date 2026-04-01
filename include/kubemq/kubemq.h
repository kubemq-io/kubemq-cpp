/// @file kubemq.h
/// @brief Master include file for the KubeMQ C++ SDK.
///
/// Including this single header provides access to all public SDK types:
/// Client, ClientOptions, Event, EventStore, Command, Query, QueueMessage,
/// Subscription, stream handles, TLS/reconnect/retry configuration,
/// status types, and management utilities.

// include/kubemq/kubemq.h
#pragma once

#include "kubemq/channel_info.h"
#include "kubemq/client.h"
#include "kubemq/client_options.h"
#include "kubemq/command.h"
#include "kubemq/connection_state.h"
#include "kubemq/credential_provider.h"
#include "kubemq/error_code.h"
#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/export.h"
#include "kubemq/logger.h"
#include "kubemq/query.h"
#include "kubemq/queue_downstream_receiver.h"
#include "kubemq/queue_message.h"
#include "kubemq/queue_upstream_handle.h"
#include "kubemq/reconnect_policy.h"
#include "kubemq/retry_policy.h"
#include "kubemq/server_info.h"
#include "kubemq/status.h"
#include "kubemq/stream_handle.h"
#include "kubemq/subscription.h"
#include "kubemq/subscription_option.h"
#include "kubemq/tls_config.h"
#include "kubemq/types.h"
