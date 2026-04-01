#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "kubemq.pb.h"
#include "internal/transport/converter.h"

#include "kubemq/event.h"
#include "kubemq/event_store.h"
#include "kubemq/command.h"
#include "kubemq/query.h"
#include "kubemq/queue_message.h"
#include "kubemq/server_info.h"

namespace kubemq {
namespace internal {
namespace {

// --- Event: SDK -> Proto ---

TEST(ConverterTest, EventToProtoTest) {
    auto event_or = Event::Builder()
        .SetId("evt-1")
        .SetChannel("events.test")
        .SetBody("hello world")
        .SetMetadata("meta")
        .SetClientId("client-1")
        .AddTag("key1", "val1")
        .Build();
    ASSERT_TRUE(event_or.ok());

    ::kubemq_pb::Event proto;
    EventToProto(*event_or, "client-1", &proto);

    EXPECT_EQ(proto.eventid(), "evt-1");
    EXPECT_EQ(proto.channel(), "events.test");
    EXPECT_EQ(proto.body(), "hello world");
    EXPECT_EQ(proto.metadata(), "meta");
    EXPECT_EQ(proto.clientid(), "client-1");
    EXPECT_FALSE(proto.store());
    EXPECT_EQ(proto.tags().at("key1"), "val1");
}

// --- EventStore: SDK -> Proto ---

TEST(ConverterTest, EventStoreToProtoTest) {
    auto event_or = EventStore::Builder()
        .SetId("es-1")
        .SetChannel("store.test")
        .SetBody("stored data")
        .SetMetadata("store-meta")
        .SetClientId("client-2")
        .Build();
    ASSERT_TRUE(event_or.ok());

    ::kubemq_pb::Event proto;
    EventStoreToProto(*event_or, "client-2", &proto);

    EXPECT_EQ(proto.eventid(), "es-1");
    EXPECT_EQ(proto.channel(), "store.test");
    EXPECT_EQ(proto.body(), "stored data");
    EXPECT_TRUE(proto.store());
}

// --- EventReceive: Proto -> SDK ---

TEST(ConverterTest, EventReceiveFromProtoTest) {
    ::kubemq_pb::EventReceive proto;
    proto.set_eventid("recv-1");
    proto.set_channel("events.ch");
    proto.set_metadata("recv-meta");
    proto.set_body("recv-body");
    proto.set_timestamp(1234567890);
    proto.set_sequence(42);
    (*proto.mutable_tags())["env"] = "test";

    auto sdk = EventReceiveFromProto(proto);

    EXPECT_EQ(sdk.id, "recv-1");
    EXPECT_EQ(sdk.channel, "events.ch");
    EXPECT_EQ(sdk.metadata, "recv-meta");
    EXPECT_EQ(sdk.body, "recv-body");
    EXPECT_EQ(sdk.timestamp, 1234567890);
    EXPECT_EQ(sdk.sequence, 42);
    EXPECT_EQ(sdk.tags.at("env"), "test");
}

// --- Command: SDK -> Proto ---

TEST(ConverterTest, CommandToProtoTest) {
    auto cmd_or = Command::Builder()
        .SetId("cmd-1")
        .SetChannel("commands.test")
        .SetBody("execute")
        .SetMetadata("cmd-meta")
        .SetTimeout(std::chrono::milliseconds(5000))
        .SetClientId("client-3")
        .AddTag("action", "restart")
        .Build();
    ASSERT_TRUE(cmd_or.ok());

    ::kubemq_pb::Request proto;
    CommandToProto(*cmd_or, "client-3", &proto);

    EXPECT_EQ(proto.requestid(), "cmd-1");
    EXPECT_EQ(proto.channel(), "commands.test");
    EXPECT_EQ(proto.body(), "execute");
    EXPECT_EQ(proto.metadata(), "cmd-meta");
    EXPECT_EQ(proto.requesttypedata(), ::kubemq_pb::Request::Command);
    EXPECT_EQ(proto.timeout(), 5000);
    EXPECT_EQ(proto.clientid(), "client-3");
    EXPECT_EQ(proto.tags().at("action"), "restart");
}

// --- Query: SDK -> Proto ---

TEST(ConverterTest, QueryToProtoTest) {
    auto query_or = Query::Builder()
        .SetId("q-1")
        .SetChannel("queries.test")
        .SetBody("select")
        .SetTimeout(std::chrono::milliseconds(10000))
        .SetCacheKey("cache-key-1")
        .SetCacheTTL(std::chrono::milliseconds(60000))
        .SetClientId("client-4")
        .Build();
    ASSERT_TRUE(query_or.ok());

    ::kubemq_pb::Request proto;
    QueryToProto(*query_or, "client-4", &proto);

    EXPECT_EQ(proto.requestid(), "q-1");
    EXPECT_EQ(proto.channel(), "queries.test");
    EXPECT_EQ(proto.body(), "select");
    EXPECT_EQ(proto.requesttypedata(), ::kubemq_pb::Request::Query);
    EXPECT_EQ(proto.timeout(), 10000);
    EXPECT_EQ(proto.cachekey(), "cache-key-1");
    EXPECT_EQ(proto.clientid(), "client-4");
}

// --- CommandResponse: Proto -> SDK ---

TEST(ConverterTest, CommandResponseFromProtoTest) {
    ::kubemq_pb::Response proto;
    proto.set_requestid("cmd-resp-1");
    proto.set_clientid("responder");
    proto.set_replychannel("reply-ch");
    proto.set_executed(true);
    proto.set_timestamp(9876543210);
    proto.set_error("");
    (*proto.mutable_tags())["status"] = "done";

    auto sdk = CommandResponseFromProto(proto);

    EXPECT_EQ(sdk.command_id, "cmd-resp-1");
    EXPECT_EQ(sdk.response_client_id, "responder");
    EXPECT_EQ(sdk.reply_channel, "reply-ch");
    EXPECT_TRUE(sdk.executed);
    EXPECT_EQ(sdk.executed_at, 9876543210);
    EXPECT_TRUE(sdk.error.empty());
    EXPECT_EQ(sdk.tags.at("status"), "done");
}

// --- QueryResponse: Proto -> SDK ---

TEST(ConverterTest, QueryResponseFromProtoTest) {
    ::kubemq_pb::Response proto;
    proto.set_requestid("q-resp-1");
    proto.set_clientid("query-responder");
    proto.set_replychannel("reply-ch");
    proto.set_executed(true);
    proto.set_timestamp(1111111111);
    proto.set_metadata("response-meta");
    proto.set_body("result-body");
    proto.set_cachehit(true);
    proto.set_error("");

    auto sdk = QueryResponseFromProto(proto);

    EXPECT_EQ(sdk.query_id, "q-resp-1");
    EXPECT_EQ(sdk.response_client_id, "query-responder");
    EXPECT_TRUE(sdk.executed);
    EXPECT_TRUE(sdk.cache_hit);
    EXPECT_EQ(sdk.body, "result-body");
    EXPECT_EQ(sdk.metadata, "response-meta");
}

// --- QueueMessage: SDK -> Proto ---

TEST(ConverterTest, QueueMessageToProtoTest) {
    auto msg_or = QueueMessage::Builder()
        .SetId("qm-1")
        .SetChannel("queues.test")
        .SetBody("queue data")
        .SetMetadata("queue-meta")
        .SetClientId("client-5")
        .SetExpirationSeconds(60)
        .SetDelaySeconds(5)
        .SetMaxReceiveCount(3)
        .SetMaxReceiveQueue("dlq")
        .AddTag("priority", "high")
        .Build();
    ASSERT_TRUE(msg_or.ok());

    ::kubemq_pb::QueueMessage proto;
    QueueMessageToProto(*msg_or, "client-5", &proto);

    EXPECT_EQ(proto.messageid(), "qm-1");
    EXPECT_EQ(proto.channel(), "queues.test");
    EXPECT_EQ(proto.body(), "queue data");
    EXPECT_EQ(proto.metadata(), "queue-meta");
    EXPECT_EQ(proto.clientid(), "client-5");
    EXPECT_EQ(proto.tags().at("priority"), "high");

    EXPECT_TRUE(proto.has_policy());
    EXPECT_EQ(proto.policy().expirationseconds(), 60);
    EXPECT_EQ(proto.policy().delayseconds(), 5);
    EXPECT_EQ(proto.policy().maxreceivecount(), 3);
    EXPECT_EQ(proto.policy().maxreceivequeue(), "dlq");
}

// --- QueueSendResult: Proto -> SDK ---

TEST(ConverterTest, QueueSendResultFromProtoTest) {
    ::kubemq_pb::SendQueueMessageResult proto;
    proto.set_messageid("qm-result-1");
    proto.set_sentat(1234567890);
    proto.set_expirationat(1234567950);
    proto.set_delayedto(1234567895);
    proto.set_iserror(false);
    proto.set_error("");

    auto sdk = QueueSendResultFromProto(proto);

    EXPECT_EQ(sdk.message_id, "qm-result-1");
    EXPECT_EQ(sdk.sent_at, 1234567890);
    EXPECT_EQ(sdk.expiration_at, 1234567950);
    EXPECT_EQ(sdk.delayed_to, 1234567895);
    EXPECT_FALSE(sdk.is_error);
}

TEST(ConverterTest, QueueSendResultFromProtoWithError) {
    ::kubemq_pb::SendQueueMessageResult proto;
    proto.set_messageid("qm-err-1");
    proto.set_iserror(true);
    proto.set_error("queue full");

    auto sdk = QueueSendResultFromProto(proto);

    EXPECT_TRUE(sdk.is_error);
    EXPECT_EQ(sdk.error, "queue full");
}

// --- ServerInfo: Proto -> SDK ---

TEST(ConverterTest, ServerInfoFromProtoTest) {
    ::kubemq_pb::PingResult proto;
    proto.set_host("kubemq-server");
    proto.set_version("2.5.0");
    proto.set_serverstarttime(1000000);
    proto.set_serveruptimeseconds(7200);

    auto sdk = ServerInfoFromProto(proto);

    EXPECT_EQ(sdk.host, "kubemq-server");
    EXPECT_EQ(sdk.version, "2.5.0");
    EXPECT_EQ(sdk.server_start_time, 1000000);
    EXPECT_EQ(sdk.server_up_time_seconds, 7200);
}

// --- Round-trip: Event ---

TEST(ConverterTest, EventRoundTrip) {
    auto event_or = Event::Builder()
        .SetId("rt-evt-1")
        .SetChannel("events.round-trip")
        .SetBody("round trip body")
        .SetMetadata("round trip meta")
        .SetClientId("rt-client")
        .AddTag("env", "test")
        .Build();
    ASSERT_TRUE(event_or.ok());

    ::kubemq_pb::Event proto;
    EventToProto(*event_or, "rt-client", &proto);

    EXPECT_EQ(proto.eventid(), event_or->id());
    EXPECT_EQ(proto.channel(), event_or->channel());
    EXPECT_EQ(proto.body(), event_or->body());
    EXPECT_EQ(proto.metadata(), event_or->metadata());
    EXPECT_EQ(proto.clientid(), event_or->client_id());
    EXPECT_FALSE(proto.store());
}

// --- Round-trip: EventStore ---

TEST(ConverterTest, EventStoreRoundTrip) {
    auto es_or = EventStore::Builder()
        .SetId("rt-es-1")
        .SetChannel("store.round-trip")
        .SetBody("stored round trip")
        .Build();
    ASSERT_TRUE(es_or.ok());

    ::kubemq_pb::Event proto;
    EventStoreToProto(*es_or, "", &proto);

    EXPECT_EQ(proto.eventid(), es_or->id());
    EXPECT_TRUE(proto.store());
}

// --- QueueMessage without policy ---

TEST(ConverterTest, QueueMessageToProtoNoPolicy) {
    auto msg_or = QueueMessage::Builder()
        .SetId("qm-nopolicy")
        .SetChannel("queues.test")
        .SetBody("data")
        .Build();
    ASSERT_TRUE(msg_or.ok());

    ::kubemq_pb::QueueMessage proto;
    QueueMessageToProto(*msg_or, "", &proto);

    EXPECT_EQ(proto.messageid(), "qm-nopolicy");
}

// --- QueueMessage attributes from proto ---

TEST(ConverterTest, QueueMessageAttributes_FullyPopulated) {
    ::kubemq_pb::QueueMessage proto;
    proto.set_messageid("msg-attr-test");
    proto.set_channel("queues.test");
    proto.set_body("test-body");
    auto* attrs = proto.mutable_attributes();
    attrs->set_timestamp(1234567890);
    attrs->set_sequence(42);
    attrs->set_md5ofbody("abc123");
    attrs->set_receivecount(3);
    attrs->set_rerouted(true);
    attrs->set_reroutedfromqueue("old-queue");
    attrs->set_expirationat(9999999999);
    attrs->set_delayedto(1111111111);

    auto msg = QueueMessageFromProto(proto);
    ASSERT_NE(msg.attributes(), nullptr);
    EXPECT_EQ(msg.attributes()->timestamp, 1234567890);
    EXPECT_EQ(msg.attributes()->sequence, 42u);
    EXPECT_EQ(msg.attributes()->md5_of_body, "abc123");
    EXPECT_EQ(msg.attributes()->receive_count, 3);
    EXPECT_TRUE(msg.attributes()->re_routed);
    EXPECT_EQ(msg.attributes()->re_routed_from_queue, "old-queue");
    EXPECT_EQ(msg.attributes()->expiration_at, 9999999999);
    EXPECT_EQ(msg.attributes()->delayed_to, 1111111111);
}

// --- CommandReceive timeout ---

TEST(ConverterTest, CommandReceive_HasTimeout) {
    ::kubemq_pb::Request proto;
    proto.set_timeout(30);  // 30 seconds in proto

    auto recv = CommandReceiveFromProto(proto);
    EXPECT_EQ(recv.timeout, std::chrono::seconds(30));
}

// --- QueryReceive cache fields ---

TEST(ConverterTest, QueryReceive_HasCacheFields) {
    ::kubemq_pb::Request proto;
    proto.set_timeout(60);
    proto.set_cachekey("my-cache-key");
    proto.set_cachettl(300);

    auto recv = QueryReceiveFromProto(proto);
    EXPECT_EQ(recv.timeout, std::chrono::seconds(60));
    EXPECT_EQ(recv.cache_key, "my-cache-key");
    EXPECT_EQ(recv.cache_ttl, std::chrono::seconds(300));
}

// --- QueuesDownstreamResponse: TransactionComplete mapping ---

TEST(ConverterTest, TransactionComplete_Mapped) {
    // Verify that the TransactionComplete field from the proto is accessible
    ::kubemq_pb::QueuesDownstreamResponse proto;
    proto.set_transactionid("txn-1");
    proto.set_transactioncomplete(true);
    proto.set_iserror(false);

    EXPECT_TRUE(proto.transactioncomplete());
    EXPECT_EQ(proto.transactionid(), "txn-1");
    EXPECT_FALSE(proto.iserror());

    // Verify false case
    proto.set_transactioncomplete(false);
    EXPECT_FALSE(proto.transactioncomplete());
}

// --- CommandReply executed field ---

TEST(ConverterTest, Reply_ExecutedIndependent) {
    // executed should be set independently of error emptiness
    auto reply_or = kubemq::v1::CommandReply::Builder()
        .SetRequestId("req-1")
        .SetResponseTo("resp-chan")
        .SetExecuted(false)
        .SetError("")  // no error, but executed=false
        .Build();
    ASSERT_TRUE(reply_or.ok());
    EXPECT_FALSE(reply_or->executed());

    ::kubemq_pb::Response proto;
    CommandReplyToProto(*reply_or, &proto);
    EXPECT_FALSE(proto.executed());
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
