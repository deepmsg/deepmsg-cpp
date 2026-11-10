// C4a: `ConsensusModule.Context` — the property layer, the defaults and the
// `conclude()` rules that this card honours (`ConsensusModule.java:1752-2185`).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "cluster/consensus/consensus_module_context.h"
#include "core/config/property_source.h"

namespace
{

using axc::core::PropertySource;
using deepmsg::cluster::consensus::consensus_module_config_from_properties;
using deepmsg::cluster::consensus::consensus_module_thread_name;
using deepmsg::cluster::consensus::ConsensusModuleConfig;
using deepmsg::cluster::consensus::StandbySnapshotStartupMode;

constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";

class ConsensusModuleContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-cm-context-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] ConsensusModuleConfig valid_config() const
    {
        ConsensusModuleConfig config;
        config.cluster_dir = directory_ + "/cluster";
        config.aeron_dir = directory_ + "/aeron";
        config.members = MEMBERS;
        config.ingress_channel = "aeron:ipc";
        config.replication_channel = "aeron:udp";
        return config;
    }

    std::string directory_;
};

TEST_F(ConsensusModuleContextTest, concludeRequiresClusterMembers)
{
    auto config = valid_config();
    config.members.clear();

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"ConsensusModule.Context.clusterMembers must be set"}, concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeRejectsLogChannelTermParameters)
{
    for (const char* parameter : {"initial-term-id=1", "term-id=1", "term-offset=0"})
    {
        auto config = valid_config();
        config.log_channel = std::string{"aeron:udp?term-length=64m|"} + parameter;

        auto concluded = config.conclude();
        ASSERT_FALSE(concluded) << parameter;
    }
}

TEST_F(ConsensusModuleContextTest, concludeRejectsServiceCountOutOfRange)
{
    auto config = valid_config();
    config.service_count = ConsensusModuleConfig::MAX_SERVICE_COUNT + 1;

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"service count of range [0, 10]: 11"}, concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeRejectsZeroServices)
{
    auto config = valid_config();
    config.service_count = 0;

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"zero services are only supported when ConsensusModuleExtension is enabled"},
              concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeRequiresIngressAndReplicationChannels)
{
    {
        auto config = valid_config();
        config.ingress_channel.clear();
        auto concluded = config.conclude();
        ASSERT_FALSE(concluded);
        EXPECT_EQ(std::string{"ingressChannel must be specified"}, concluded.error().message.view());
    }
    {
        auto config = valid_config();
        config.replication_channel.clear();
        auto concluded = config.conclude();
        ASSERT_FALSE(concluded);
        EXPECT_EQ(std::string{"replicationChannel must be set"}, concluded.error().message.view());
    }
}

TEST_F(ConsensusModuleContextTest, concludeRequiresIpcArchiveControl)
{
    auto config = valid_config();
    config.archive_local_control_channel = "aeron:udp?endpoint=localhost:0";

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"local archive control must be IPC"}, concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeRejectsStartupCanvassShorterThanTwoHeartbeats)
{
    auto config = valid_config();
    config.startup_canvass_timeout_ns = config.leader_heartbeat_timeout_ns;

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"startupCanvassTimeoutNs=10000000000 must be a multiple of leaderHeartbeatTimeoutNs="
                          "10000000000"},
              concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeRejectsAnUnknownMemberId)
{
    auto config = valid_config();
    config.member_id = 3;

    auto concluded = config.conclude();
    ASSERT_FALSE(concluded);
    EXPECT_EQ(std::string{"could not find memberId=3 in clusterMembers="} + MEMBERS, concluded.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeDerivesTheArchiveResponseStreamIdAndTheClassicThreadName)
{
    auto config = valid_config();
    config.cluster_id = 7;

    ASSERT_TRUE(config.conclude());
    EXPECT_EQ(7 * 100 + 100 + 20, config.archive_control_response_stream_id);
    EXPECT_EQ(std::string{"consensus-module-7-0"}, config.agent_role_name);
    EXPECT_EQ(std::string{"consensus-module-7-0"}, consensus_module_thread_name(config));
}

TEST_F(ConsensusModuleContextTest, concludeRunsOnce)
{
    auto config = valid_config();
    ASSERT_TRUE(config.conclude());

    auto second = config.conclude();
    ASSERT_FALSE(second);
    EXPECT_EQ(std::string{"ConsensusModule.Context already concluded (ConcurrentConclude)"},
              second.error().message.view());
}

TEST_F(ConsensusModuleContextTest, configFromPropertiesReadsTheReferenceDefaults)
{
    PropertySource properties;
    properties.set("aeron.cluster.dir", directory_ + "/cluster");
    properties.set("aeron.cluster.members", MEMBERS);
    properties.set("aeron.cluster.ingress.channel", "aeron:ipc");
    properties.set("aeron.cluster.replication.channel", "aeron:udp");

    auto config = consensus_module_config_from_properties(properties);
    ASSERT_TRUE(config) << config.error().message.view();

    EXPECT_EQ(0, config->cluster_id);
    EXPECT_EQ(0, config->member_id);
    EXPECT_EQ(-1, config->appointed_leader_id);
    EXPECT_EQ(50, config->ingress_fragment_limit);
    EXPECT_EQ(101, config->ingress_stream_id);
    EXPECT_EQ(102, config->egress_stream_id);
    EXPECT_EQ(std::string{"aeron:udp?term-length=64m"}, config->log_channel);
    EXPECT_EQ(100, config->log_stream_id);
    EXPECT_EQ(50, config->log_fragment_limit);
    EXPECT_EQ(std::string{"aeron:udp"}, config->consensus_channel);
    EXPECT_EQ(108, config->consensus_stream_id);
    EXPECT_EQ(std::string{"aeron:udp"}, config->follower_catchup_channel);
    EXPECT_EQ(std::string{"aeron:udp?term-length=64k"}, config->leader_archive_control_channel);
    EXPECT_EQ(1, config->service_count);
    EXPECT_EQ(10, config->max_concurrent_sessions);
    EXPECT_EQ(10'000'000'000, config->session_timeout_ns);
    EXPECT_EQ(10'000'000'000, config->leader_heartbeat_timeout_ns);
    EXPECT_EQ(200'000'000, config->leader_heartbeat_interval_ns);
    EXPECT_EQ(60'000'000'000, config->startup_canvass_timeout_ns);
    EXPECT_EQ(1'000'000'000, config->election_timeout_ns);
    EXPECT_EQ(100'000'000, config->election_status_interval_ns);
    EXPECT_EQ(10'000'000'000, config->termination_timeout_ns);
    EXPECT_EQ(100'000'000, config->cycle_threshold_ns);
    EXPECT_EQ(1'000'000'000, config->total_snapshot_duration_threshold_ns);
    EXPECT_EQ(8'000'000, config->wheel_tick_resolution_ns);
    EXPECT_EQ(128, config->ticks_per_wheel);
    EXPECT_EQ(1'024 * 1'024, config->error_buffer_length);
    EXPECT_EQ(std::string{"aeron:ipc?term-length=64k"}, config->archive_local_control_channel);
    EXPECT_EQ(10, config->archive_local_control_stream_id);
    EXPECT_EQ(120, config->archive_control_response_stream_id);
    EXPECT_EQ(StandbySnapshotStartupMode::warn, config->standby_snapshot_on_startup);
}

TEST_F(ConsensusModuleContextTest, configFromPropertiesReadsTheStandbyStartupMode)
{
    PropertySource properties;
    properties.set("aeron.cluster.dir", directory_ + "/cluster");
    properties.set("aeron.cluster.members", MEMBERS);
    properties.set("aeron.cluster.ingress.channel", "aeron:ipc");
    properties.set("aeron.cluster.replication.channel", "aeron:udp");
    properties.set("aeron.cluster.cpp.standby.snapshot.on.startup", "refuse");

    auto config = consensus_module_config_from_properties(properties);
    ASSERT_TRUE(config) << config.error().message.view();
    EXPECT_EQ(StandbySnapshotStartupMode::refuse, config->standby_snapshot_on_startup);

    properties.set("aeron.cluster.cpp.standby.snapshot.on.startup", "ignore");

    auto invalid = consensus_module_config_from_properties(properties);
    ASSERT_FALSE(invalid);
}

TEST_F(ConsensusModuleContextTest, configFromPropertiesUsesTheNewThreadNamingScheme)
{
    PropertySource properties;
    properties.set("aeron.cluster.dir", directory_ + "/cluster");
    properties.set("aeron.cluster.members", MEMBERS);
    properties.set("aeron.cluster.ingress.channel", "aeron:ipc");
    properties.set("aeron.cluster.replication.channel", "aeron:udp");
    properties.set("aeron.thread.naming", "new");

    auto config = consensus_module_config_from_properties(properties);
    ASSERT_TRUE(config) << config.error().message.view();
    EXPECT_EQ(std::string{"aeron-cl-cm"}, config->agent_role_name);

    properties.set("aeron.thread.naming", "mystery");

    auto invalid = consensus_module_config_from_properties(properties);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(std::string{"Unknown thread naming mode: mystery"}, invalid.error().message.view());
}

TEST_F(ConsensusModuleContextTest, concludeCreatesTheClusterAndMarkFileDirectories)
{
    auto config = valid_config();
    config.mark_file_dir = directory_ + "/mark";

    ASSERT_TRUE(config.conclude());
    EXPECT_TRUE(std::filesystem::is_directory(directory_ + "/cluster"));
    EXPECT_TRUE(std::filesystem::is_directory(directory_ + "/mark"));
    EXPECT_EQ(directory_ + "/mark/cluster-mark.dat", config.mark_file_path());
}

}  // namespace
