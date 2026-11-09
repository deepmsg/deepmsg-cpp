// Pure components of the C2a container: session table, sticky header, the two
// adapters' decoding and the property layer. The driver-facing agent is covered
// by the interop test.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <vector>

#include "cluster/service/cluster.h"
#include "cluster/service/consensus_module_proxy.h"
#include "cluster/service/container_config.h"
#include "cluster/service/log_adapter.h"
#include "cluster/service/service_adapter.h"
#include "io_aeron_cluster_codecs/JoinLog.h"
#include "io_aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "io_aeron_cluster_codecs/RequestServiceAck.h"
#include "io_aeron_cluster_codecs/ServiceTerminationPosition.h"
#include "io_aeron_cluster_codecs/SessionCloseEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SessionOpenEvent.h"
#include "io_aeron_cluster_codecs/TimerEvent.h"

namespace
{

using namespace deepmsg::cluster::service;

TEST(SessionTableTest, sessionsAreInsertedInIdOrder)
{
    SessionTable table;
    table.add(ContainerClientSession{3, 102, "aeron:ipc", {}});
    table.add(ContainerClientSession{1, 102, "aeron:ipc", {}});
    table.add(ContainerClientSession{2, 102, "aeron:ipc", {}});

    ASSERT_EQ(3U, table.size());
    EXPECT_EQ(1, table.sessions()[0].id());
    EXPECT_EQ(2, table.sessions()[1].id());
    EXPECT_EQ(3, table.sessions()[2].id());
    ASSERT_NE(nullptr, table.get(2));
    EXPECT_EQ(2, table.get(2)->id());
    EXPECT_EQ(nullptr, table.get(4));
}

TEST(SessionTableTest, removingASessionReindexesTheTail)
{
    SessionTable table;
    for (std::int64_t id : {1, 2, 3})
    {
        table.add(ContainerClientSession{id, 102, "aeron:ipc", {}});
    }

    auto removed = table.remove(2);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(2, removed->id());
    ASSERT_NE(nullptr, table.get(1));
    ASSERT_NE(nullptr, table.get(3));
    EXPECT_FALSE(table.remove(2).has_value());

    table.add(ContainerClientSession{2, 102, "aeron:ipc", {}});
    EXPECT_EQ(1, table.sessions()[0].id());
    EXPECT_EQ(2, table.sessions()[1].id());
    EXPECT_EQ(3, table.sessions()[2].id());
}

TEST(ConsensusModuleProxyTest, aFollowerClaimCarriesTheStickySessionHeader)
{
    ConsensusModuleProxy proxy{4};
    proxy.set_leadership_term_id(9);

    auto claimed = proxy.mock_session_message(21, 5'000, 6);
    ASSERT_TRUE(claimed) << claimed.error().message.view();
    EXPECT_EQ(MOCKED_OFFER, claimed->position());
    ASSERT_EQ(6U, claimed->payload().size());
    std::memcpy(claimed->payload().data(), "abcdef", 6);
    auto committed = claimed->commit();
    ASSERT_TRUE(committed) << committed.error().message.view();
    EXPECT_EQ(MOCKED_OFFER, *committed);

    const std::uint8_t* header = claimed->payload().data() - SESSION_HEADER_LENGTH;
    const auto i64_at = [header](std::size_t offset)
    {
        std::int64_t value = 0;
        std::memcpy(&value, header + offset, sizeof(value));
        return value;
    };
    const auto u16_at = [header](std::size_t offset)
    {
        std::uint16_t value = 0;
        std::memcpy(&value, header + offset, sizeof(value));
        return value;
    };

    EXPECT_EQ(24, u16_at(0));
    EXPECT_EQ(1, u16_at(2));
    EXPECT_EQ(111, u16_at(4));
    EXPECT_EQ(9, i64_at(8));
    EXPECT_EQ(21, i64_at(16));
    EXPECT_EQ(5'000, i64_at(24));
    EXPECT_EQ(0, std::memcmp(header + SESSION_HEADER_LENGTH, "abcdef", 6));
}

TEST(ServiceAdapterTest, dispatchDecodesJoinLog)
{
    std::vector<std::uint8_t> buffer(256, 0);
    ::io::aeron::cluster::codecs::JoinLog encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.logPosition(1000)
        .maxLogPosition(2000)
        .memberId(2)
        .logSessionId(11)
        .logStreamId(103)
        .isStartup(::io::aeron::cluster::codecs::BooleanType::TRUE)
        .role(2)
        .isStandby(::io::aeron::cluster::codecs::BooleanType::FALSE)
        .putLogChannel(std::string_view{"aeron:ipc?alias=log"});

    ServiceEvents events;
    auto dispatched = ServiceAdapter::dispatch_message(events, buffer);
    ASSERT_TRUE(dispatched) << dispatched.error().message.view();
    ASSERT_TRUE(events.join_log.has_value());
    EXPECT_EQ(1000, events.join_log->log_position);
    EXPECT_EQ(2000, events.join_log->max_log_position);
    EXPECT_EQ(2, events.join_log->member_id);
    EXPECT_EQ(11, events.join_log->log_session_id);
    EXPECT_EQ(103, events.join_log->log_stream_id);
    EXPECT_TRUE(events.join_log->is_startup);
    EXPECT_EQ(2, events.join_log->role);
    EXPECT_FALSE(events.join_log->is_standby);
    EXPECT_EQ("aeron:ipc?alias=log", events.join_log->log_channel);
}

TEST(ServiceAdapterTest, dispatchDecodesTerminationAndAckRequests)
{
    std::vector<std::uint8_t> buffer(128, 0);
    ServiceEvents events;

    {
        ::io::aeron::cluster::codecs::ServiceTerminationPosition encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.logPosition(42).ackRequired(::io::aeron::cluster::codecs::BooleanType::TRUE);
        ASSERT_TRUE(ServiceAdapter::dispatch_message(events, buffer));
    }
    ASSERT_TRUE(events.termination_position.has_value());
    EXPECT_EQ(42, events.termination_position->first);
    EXPECT_TRUE(events.termination_position->second);

    {
        ::io::aeron::cluster::codecs::RequestServiceAck encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.logPosition(77);
        ASSERT_TRUE(ServiceAdapter::dispatch_message(events, buffer));
    }
    ASSERT_TRUE(events.requested_ack_position.has_value());
    EXPECT_EQ(77, *events.requested_ack_position);
}

TEST(LogAdapterTest, decodesSessionAndTimerMessages)
{
    std::vector<std::uint8_t> buffer(128, 0);

    {
        ::io::aeron::cluster::codecs::SessionMessageHeader encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.leadershipTermId(3).clusterSessionId(9).timestamp(1234);
        auto decoded = BoundedLogAdapter::decode(buffer);
        ASSERT_TRUE(decoded) << decoded.error().message.view();
        EXPECT_EQ(LogMessage::Kind::session_message, decoded->kind);
        EXPECT_EQ(9, decoded->cluster_session_id);
        EXPECT_EQ(1234, decoded->timestamp);
    }

    {
        ::io::aeron::cluster::codecs::TimerEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.correlationId(5).timestamp(999);
        auto decoded = BoundedLogAdapter::decode(buffer);
        ASSERT_TRUE(decoded) << decoded.error().message.view();
        EXPECT_EQ(LogMessage::Kind::timer_event, decoded->kind);
        EXPECT_EQ(5, decoded->correlation_id);
        EXPECT_EQ(999, decoded->timestamp);
    }
}

TEST(LogAdapterTest, decodesSessionOpenWithChannelAndPrincipal)
{
    std::vector<std::uint8_t> buffer(512, 0);
    ::io::aeron::cluster::codecs::SessionOpenEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.leadershipTermId(2)
        .clusterSessionId(17)
        .timestamp(555)
        .responseStreamId(102)
        .putResponseChannel(std::string_view{"aeron:udp?endpoint=localhost:12345"})
        .putEncodedPrincipal(std::string_view{"principal"});

    auto decoded = BoundedLogAdapter::decode(buffer);
    ASSERT_TRUE(decoded) << decoded.error().message.view();
    EXPECT_EQ(LogMessage::Kind::session_open, decoded->kind);
    EXPECT_EQ(2, decoded->leadership_term_id);
    EXPECT_EQ(17, decoded->cluster_session_id);
    EXPECT_EQ(555, decoded->timestamp);
    EXPECT_EQ(102, decoded->response_stream_id);
    EXPECT_EQ("aeron:udp?endpoint=localhost:12345", decoded->response_channel);
    ASSERT_EQ(9U, decoded->encoded_principal.size());
    EXPECT_EQ(0, std::memcmp(decoded->encoded_principal.data(), "principal", 9));
}

TEST(LogAdapterTest, decodesCloseActionAndNewLeadershipTerm)
{
    std::vector<std::uint8_t> buffer(256, 0);

    {
        ::io::aeron::cluster::codecs::SessionCloseEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.leadershipTermId(4).clusterSessionId(8).timestamp(66).closeReason(
            ::io::aeron::cluster::codecs::CloseReason::CLIENT_ACTION);
        auto decoded = BoundedLogAdapter::decode(buffer);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(LogMessage::Kind::session_close, decoded->kind);
        EXPECT_EQ(8, decoded->cluster_session_id);
        EXPECT_EQ(66, decoded->timestamp);
        EXPECT_EQ(::io::aeron::cluster::codecs::CloseReason::CLIENT_ACTION, decoded->close_reason);
    }

    {
        ::io::aeron::cluster::codecs::NewLeadershipTermEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.leadershipTermId(6)
            .logPosition(100)
            .timestamp(200)
            .termBaseLogPosition(50)
            .leaderMemberId(1)
            .logSessionId(12)
            .timeUnit(::io::aeron::cluster::codecs::ClusterTimeUnit::MICROS)
            .appVersion(0x00010300);
        auto decoded = BoundedLogAdapter::decode(buffer);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(LogMessage::Kind::new_leadership_term, decoded->kind);
        EXPECT_EQ(6, decoded->leadership_term_id);
        EXPECT_EQ(100, decoded->log_position);
        EXPECT_EQ(200, decoded->timestamp);
        EXPECT_EQ(50, decoded->term_base_log_position);
        EXPECT_EQ(1, decoded->leader_member_id);
        EXPECT_EQ(12, decoded->log_session_id);
        EXPECT_EQ(1, decoded->time_unit_code);
        EXPECT_EQ(0x00010300, decoded->app_version);
    }
}

TEST(ContainerConfigTest, defaultsAndThreadNames)
{
    axc::core::PropertySource properties;
    auto config = container_config_from_properties(properties);
    ASSERT_TRUE(config) << config.error().message.view();
    EXPECT_EQ(0, config->cluster_id);
    EXPECT_EQ(0, config->service_id);
    EXPECT_EQ(104, config->service_stream_id);
    EXPECT_EQ(105, config->consensus_module_stream_id);
    EXPECT_EQ("aeron:ipc?term-length=128k", config->control_channel);
    EXPECT_EQ(50U, config->log_fragment_limit);
    EXPECT_EQ("clustered-service-0-0", config->service_name);
    EXPECT_EQ(121, config->archive_control_response_stream_id);
    EXPECT_EQ("aeron-cluster/cluster-mark-service-0.dat", config->mark_file_path());

    EXPECT_EQ("aeron-cl-cs-3", container_thread_name(7, 3, ThreadNaming::new_naming));
}

TEST(ContainerConfigTest, readsValuesAndRefusesAJavaClass)
{
    axc::core::PropertySource properties;
    properties.set("aeron.cluster.id", "7");
    properties.set("aeron.cluster.service.id", "2");
    properties.set("aeron.cluster.service.name", "service-two");
    properties.set("aeron.thread.naming", "new");
    properties.set("aeron.cluster.control.channel", "aeron:ipc?term-length=64k");
    properties.set("aeron.cluster.log.fragment.limit", "10");
    properties.set("aeron.cluster.service.responder", "false");
    properties.set("aeron.cluster.service.cycle.threshold", "10ms");
    properties.set("aeron.cluster.service.snapshot.threshold", "2s");

    auto config = container_config_from_properties(properties);
    ASSERT_TRUE(config) << config.error().message.view();
    EXPECT_EQ(7, config->cluster_id);
    EXPECT_EQ(2, config->service_id);
    EXPECT_EQ("service-two", config->service_name);
    EXPECT_EQ("aeron:ipc?term-length=64k", config->control_channel);
    EXPECT_EQ(10U, config->log_fragment_limit);
    EXPECT_FALSE(config->responding_service);
    EXPECT_EQ(10'000'000, config->cycle_threshold_ns);
    EXPECT_EQ(2'000'000'000, config->snapshot_duration_threshold_ns);
    EXPECT_EQ(823, config->archive_control_response_stream_id);

    axc::core::PropertySource refused;
    refused.set("aeron.cluster.service.class.name", "com.example.Service");
    auto refused_config = container_config_from_properties(refused);
    ASSERT_FALSE(refused_config);
    EXPECT_NE(std::string::npos, std::string{refused_config.error().message.view()}.find("names a Java class"));

    axc::core::PropertySource bad_id;
    bad_id.set("aeron.cluster.service.id", "10");
    EXPECT_FALSE(container_config_from_properties(bad_id));
}

TEST(RoleTest, mapsTheCounterCodes)
{
    EXPECT_EQ(Role::follower, *role_from_code(0));
    EXPECT_EQ(Role::candidate, *role_from_code(1));
    EXPECT_EQ(Role::leader, *role_from_code(2));
    EXPECT_FALSE(role_from_code(3));
    EXPECT_EQ(2, role_code(Role::leader));
    EXPECT_EQ(TimeUnit::microseconds, time_unit_from_code(1));
    EXPECT_EQ(TimeUnit::milliseconds, time_unit_from_code(-2147483647));
    EXPECT_EQ(1'000'000, time_to_nanos(TimeUnit::milliseconds, 1));
}

}  // namespace
