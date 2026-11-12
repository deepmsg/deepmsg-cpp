// C3a's criterion: our `AsyncConnect` reaches all seven steps against the
// reference's own `ClusteredMediaDriver` (Java ConsensusModule + archive) and a
// service container. The container is our binary running `noop`; the client
// under test is `deepmsg::cluster::client`.
//
// The reference counterpart is `AeronClusterAsyncConnectTest`, but that one
// mocks `Aeron`; this one is the end-to-end proof that the request bytes,
// the egress templates and the session tuple are the real protocol.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cluster/client/async_connect.h"
#include "cluster/client/client_config.h"
#include "cluster/common/cluster_counters.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif

namespace
{

constexpr std::int32_t CLUSTER_ID = 11;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

class ClientConnectInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        if (!std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            GTEST_SKIP() << "container binary not found: " << AXC_CONTAINER_BINARY;
        }

        directory_ = std::filesystem::temp_directory_path() / ("axc-c3a-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);
        aeron_dir_ = (directory_ / "aeron").string();
        cluster_dir_ = (directory_ / "cluster").string();
        archive_dir_ = (directory_ / "archive").string();
    }

    void TearDown() override
    {
        stop_container();
        if (java_running_)
        {
            java_.terminate(std::chrono::seconds(5));
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::vector<std::string> java_node_arguments() const
    {
        return {"--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeron_dir_,
                "-Daeron.cluster.dir=" + cluster_dir_,
                "-Daeron.archive.dir=" + archive_dir_,
                "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
                "-Daeron.cluster.member.id=0",
                "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0",
                "-Daeron.cluster.ingress.channel=aeron:ipc",
                "-Daeron.cluster.consensus.channel=aeron:ipc",
                "-Daeron.cluster.log.channel=aeron:ipc",
                "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
                "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.service.count=1",
                "-Daeron.threading.mode=SHARED",
                "-Daeron.print.configuration=false",
                "-cp",
                jar_,
                "io.aeron.cluster.ClusteredMediaDriver"};
    }

    void start_java_node()
    {
        auto process = axc::interop::ChildProcess::spawn("java", java_node_arguments());
        ASSERT_TRUE(process) << process.error().message.view();
        java_ = std::move(*process);
        java_running_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(java_.running()) << "the Java cluster node exited while starting";
            if (cm_state() >= 0)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java consensus module never published its state counter";
    }

    [[nodiscard]] std::int64_t counter_value(std::int32_t type_id)
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return -1;
        }
        (void)client->do_work();
        const std::int32_t counter_id =
            deepmsg::cluster::common::find_cluster_counter(client->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == counter_id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), counter_id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] std::int64_t cm_state()
    {
        return counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
    }

    void start_container()
    {
        const std::vector<std::string> arguments = {
            "-Daeron.dir=" + aeron_dir_,
            "-Daeron.cluster.dir=" + cluster_dir_,
            "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
            "-Daeron.cluster.service.id=0",
            "-Daeron.cluster.service.name=clustered-service-" + std::to_string(CLUSTER_ID) + "-0",
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
            "-Daeron.cluster.service.stream.id=104",
            "-Daeron.cluster.consensus.module.stream.id=105",
            "-Daeron.cluster.log.fragment.limit=50",
            "-Daeron.cluster.service.responder=true",
            "-Daeron.cluster.idle.strategy=org.agrona.concurrent.BackoffIdleStrategy",
            "-Daeron.cluster.replay.channel=aeron:ipc",
            "-Daeron.cluster.replay.stream.id=103",
            "-Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot",
            "-Daeron.cluster.snapshot.stream.id=106",
            "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k",
            "-Daeron.archive.local.control.stream.id=10",
            "-Daeron.archive.control.response.stream.id=20",
            "-Ddeepmsg.cluster.service=noop"};

        auto process = axc::interop::ChildProcess::spawn(AXC_CONTAINER_BINARY, arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        container_ = std::move(*process);
        container_running_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(container_.running()) << "the container binary exited while starting";
            if (cm_state() == 1)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the consensus module never became active";
    }

    void stop_container()
    {
        if (container_running_)
        {
            container_.terminate(std::chrono::seconds(10));
            container_running_ = false;
        }
    }

    [[nodiscard]] const std::string& aeron_dir() const noexcept
    {
        return aeron_dir_;
    }

    std::string jar_{};
    std::filesystem::path directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    axc::interop::ChildProcess java_{};
    axc::interop::ChildProcess container_{};
    bool java_running_{false};
    bool container_running_{false};
};

TEST_F(ClientConnectInteropTest, ourAsyncConnectReachesAllSevenStepsAgainstTheReferenceNode)
{
    start_java_node();
    start_container();

    auto connected = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir()});
    ASSERT_TRUE(connected) << connected.error().message.view();
    axc::core::DeepmsgClient client = std::move(*connected);

    deepmsg::cluster::client::ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:ipc";
    config.client_name = "c3a-probe";
    config.message_timeout_ns = 5'000'000'000;
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = deepmsg::cluster::client::AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    std::optional<deepmsg::cluster::client::ClusterClient> cluster;
    std::optional<axc::core::Error> error;
    std::int32_t last_step = connect->step();
    std::int32_t max_step = last_step;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline && !cluster.has_value())
    {
        auto polled = connect->poll(client);
        last_step = connect->step();
        max_step = std::max(max_step, last_step);

        if (!polled)
        {
            error = polled.error();
            break;
        }
        if (polled->has_value())
        {
            cluster = std::move(**polled);
            break;
        }

        (void)client.do_work();
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    if (error.has_value())
    {
        FAIL() << "connect failed at step " << last_step << " ("
               << deepmsg::cluster::client::AsyncConnect::step_name(last_step) << "): " << error->message.view();
    }
    ASSERT_TRUE(cluster.has_value()) << "connect never concluded; last step " << last_step << " ("
                                     << deepmsg::cluster::client::AsyncConnect::step_name(last_step) << ")";

    // The state machine went through every step and ended at DONE.
    EXPECT_EQ(5, max_step);
    EXPECT_EQ("DONE", deepmsg::cluster::client::AsyncConnect::step_name(max_step));
    EXPECT_EQ(0, cluster->leader_member_id());
    EXPECT_GE(cluster->cluster_session_id(), 0);
    EXPECT_GE(cluster->leadership_term_id(), 0);
    EXPECT_NE(nullptr, cluster->egress_image());
    EXPECT_TRUE(cluster->ingress_publication().is_connected());
    EXPECT_EQ(20'000'000'000, cluster->new_leader_timeout_ns());

    cluster->close();
    EXPECT_TRUE(cluster->is_closed());
}

}  // namespace
