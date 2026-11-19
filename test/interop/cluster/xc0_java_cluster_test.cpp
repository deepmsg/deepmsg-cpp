// XC-0: the reference cluster (its `ConsensusModule` and its service
// container) on our archive. The shape the acceptance line names is
// "C driver + C++ archive + Java cluster": one media driver (`aeronmd`, the C
// driver) hosts the archive and the cluster, and only the archive changes —
// our standalone `deepmsg-archive-server` (`io.aeron.archive.Archive` shape)
// against the reference `io.aeron.archive.Archive` in the same place.
//
// The representative subset: the cluster forms, a client echoes, a snapshot
// completes and the cluster restarts and replays. Every step runs on both
// archives; a difference between the two runs is the new-red report.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_counters.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/cluster_client_session.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_ARCHIVE_SERVER_BIN
#define AXC_ARCHIVE_SERVER_BIN ""
#endif
#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ClusterClientSession;

constexpr std::int32_t CLUSTER_ID = 9;

enum class Archive
{
    ours,
    reference
};

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

[[nodiscard]] std::vector<std::string> java_opens()
{
    return {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED"};
}

class Xc0JavaClusterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const std::string jar = aeron_jar();
        if (jar.empty() || !std::filesystem::exists(jar))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar;
        }
        jar_ = jar;
        if (!std::filesystem::exists(AXC_AERONMD_DEFAULT) || !std::filesystem::exists(AXC_ARCHIVE_SERVER_BIN))
        {
            GTEST_SKIP() << "aeronmd or the archive server not found";
        }
        const char* tests = std::getenv("AXC_AERON_TESTS");
        if (nullptr == tests || '\0' == *tests ||
            !std::filesystem::exists(std::string{tests} + "/deepmsg-tests/java/ClusterServiceContainerProbe.java"))
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's Java probe is required";
        }
        tests_root_ = tests;

        std::string pattern = "/tmp/axc-xc0-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        base_port_ = 8000 + ((static_cast<int>(::getpid()) % 32) * 100);
    }

    void TearDown() override
    {
        stop_all();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "XC0_ROUND_DIR=%s\n", root_dir_.c_str());
        }
        else
        {
            std::filesystem::remove_all(root_dir_, error);
        }
    }

    [[nodiscard]] std::string aeron_dir() const
    {
        return root_dir_ + "/aeron";
    }
    [[nodiscard]] std::string cluster_dir() const
    {
        return root_dir_ + "/cluster";
    }
    [[nodiscard]] std::string archive_dir() const
    {
        return root_dir_ + "/archive";
    }

    void compile_java_probes()
    {
        if (java_probes_compiled_)
        {
            return;
        }
        classes_dir_ = root_dir_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        std::vector<std::string> sources;
        for (const char* const name : {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java"})
        {
            for (const std::string& relative :
                 {"/deepmsg-tests/java/" + std::string{name}, "/java/" + std::string{name}})
            {
                const std::string candidate = tests_root_ + relative;
                if (std::filesystem::exists(candidate))
                {
                    sources.push_back(candidate);
                    break;
                }
            }
            ASSERT_FALSE(sources.empty()) << "probe source not found: " << name;
        }
        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        compile.arguments.insert(compile.arguments.end(), sources.begin(), sources.end());
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
        java_probes_compiled_ = true;
    }

    void start_driver(bool fresh = true)
    {
        std::filesystem::create_directories(aeron_dir());
        auto spawned = ChildProcess::spawn(
            AXC_AERONMD_DEFAULT,
            {"-Daeron.dir=" + aeron_dir(), std::string{"-Daeron.dir.delete.on.start="} + (fresh ? "true" : "false")});
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        driver_ = std::make_unique<ChildProcess>(std::move(*spawned));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir()});
            if (client)
            {
                client->close();
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the C driver never published its CnC file";
    }

    void start_archive(Archive archive, bool fresh = true)
    {
        std::filesystem::create_directories(archive_dir());
        const std::string wipe = fresh ? "true" : "false";
        // The consensus module's local archive: IPC control, request stream 10,
        // response stream `clusterId * 100 + 100 + 10` (`ConsensusModule`).
        const std::vector<std::string> common = {
            "-Daeron.dir=" + aeron_dir(),
            "-Daeron.archive.dir=" + archive_dir(),
            "-Daeron.archive.dir.delete.on.start=" + wipe,
            "-Daeron.archive.control.channel=aeron:ipc",
            "-Daeron.archive.control.stream.id=10",
            "-Daeron.archive.control.response.channel=aeron:ipc",
            "-Daeron.archive.control.response.stream.id=" + std::to_string(CLUSTER_ID * 100 + 100 + 10),
            "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 1),
            "-Daeron.archive.threading.mode=SHARED",
            "-Daeron.archive.idle.strategy=yield"};
        if (Archive::ours == archive)
        {
            std::vector<std::string> arguments = common;
            arguments.emplace_back("io.aeron.archive.Archive");
            auto spawned = ChildProcess::spawn(AXC_ARCHIVE_SERVER_BIN, arguments);
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            archive_ = std::make_unique<ChildProcess>(std::move(*spawned));
        }
        else
        {
            std::vector<std::string> arguments = java_opens();
            arguments.insert(arguments.end(), common.begin(), common.end());
            arguments.insert(arguments.end(), {"-cp", jar_, "io.aeron.archive.Archive"});
            auto spawned = ChildProcess::spawn("/usr/bin/java", arguments);
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            archive_ = std::make_unique<ChildProcess>(std::move(*spawned));
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ASSERT_TRUE(archive_->running()) << "the archive exited while starting";
    }

    void start_consensus_module(bool fresh = true)
    {
        std::filesystem::create_directories(cluster_dir());
        const std::string wipe = fresh ? "true" : "false";
        std::vector<std::string> arguments = java_opens();
        const std::vector<std::string> properties = {
            "-Daeron.dir=" + aeron_dir(),
            "-Daeron.cluster.dir=" + cluster_dir(),
            "-Daeron.cluster.dir.delete.on.start=" + wipe,
            "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
            "-Daeron.cluster.member.id=0",
            "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0",
            "-Daeron.cluster.ingress.channel=aeron:ipc",
            "-Daeron.cluster.consensus.channel=aeron:ipc",
            "-Daeron.cluster.log.channel=aeron:udp?endpoint=localhost:" + std::to_string(base_port_),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
            "-Daeron.cluster.service.count=1",
            "-Daeron.cluster.startup.canvass.timeout=10s",
            "-Daeron.cluster.leader.heartbeat.timeout=2s"};
        arguments.insert(arguments.end(), properties.begin(), properties.end());
        arguments.insert(arguments.end(), {"-cp", jar_, "io.aeron.cluster.ConsensusModule"});
        auto spawned = ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        cm_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    /// The reference's own shape: Java media driver + Java archive inside
    /// `ClusteredMediaDriver`, its consensus module in the same process. A
    /// standalone reference archive cannot serve the CM (its `Archive.Context`
    /// requires a UDP control channel while the CM requires an IPC local
    /// archive), so this is the arm the criterion contrasts ours with.
    void start_reference_cluster(bool fresh = true)
    {
        std::filesystem::create_directories(cluster_dir());
        std::filesystem::create_directories(archive_dir());
        const std::string wipe = fresh ? "true" : "false";
        std::vector<std::string> arguments = java_opens();
        const std::vector<std::string> properties = {
            "-Daeron.dir=" + aeron_dir(),
            "-Daeron.dir.delete.on.start=true",
            "-Daeron.cluster.dir=" + cluster_dir(),
            "-Daeron.cluster.dir.delete.on.start=" + wipe,
            "-Daeron.archive.dir=" + archive_dir(),
            "-Daeron.archive.dir.delete.on.start=" + wipe,
            "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
            "-Daeron.cluster.member.id=0",
            "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0",
            "-Daeron.cluster.ingress.channel=aeron:ipc",
            "-Daeron.cluster.consensus.channel=aeron:ipc",
            "-Daeron.cluster.log.channel=aeron:udp?endpoint=localhost:" + std::to_string(base_port_),
            "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
            "-Daeron.cluster.service.count=1",
            "-Daeron.cluster.startup.canvass.timeout=10s",
            "-Daeron.cluster.leader.heartbeat.timeout=2s",
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 2),
            "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 1),
            "-Daeron.threading.mode=SHARED"};
        arguments.insert(arguments.end(), properties.begin(), properties.end());
        arguments.insert(arguments.end(), {"-cp", jar_, "io.aeron.cluster.ClusteredMediaDriver"});
        auto spawned = ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        cm_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void start_container()
    {
        compile_java_probes();
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(), {"-cp", jar_ + ":" + classes_dir_, "ClusterServiceContainerProbe",
                                           aeron_dir(), cluster_dir(), std::to_string(CLUSTER_ID), "0",
                                           "ClusterEchoTimerService", "true", "aeron:ipc?term-length=128k"});
        auto spawned = ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        container_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void stop_all()
    {
        session_.close();
        if (container_)
        {
            container_->terminate(std::chrono::seconds(5));
            container_.reset();
        }
        if (cm_)
        {
            cm_->terminate(std::chrono::seconds(5));
            cm_.reset();
        }
        if (archive_)
        {
            archive_->terminate(std::chrono::seconds(5));
            archive_.reset();
        }
        if (driver_)
        {
            driver_->terminate(std::chrono::seconds(5));
            driver_.reset();
        }
        client_.reset();
    }

    [[nodiscard]] std::int64_t counter(std::int32_t type_id)
    {
        if (!client_)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir()});
            if (!client)
            {
                return -1;
            }
            client_ = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        (void)client_->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(client_->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            client_.reset();
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] bool wait_for_active(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (1 == counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) &&
                2 == counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] bool connect_and_echo(int count)
    {
        if (!session_.connect_ipc(aeron_dir(), std::chrono::seconds(30)))
        {
            std::fprintf(stderr, "XC0 connect failed: %s\n", session_.last_error().c_str());
            return false;
        }
        for (int i = 0; i < count; ++i)
        {
            std::string payload = "xc0-" + std::to_string(i);
            if (!session_.offer_and_await(payload, static_cast<std::size_t>(i + 1), std::chrono::seconds(20)))
            {
                std::fprintf(
                    stderr, "XC0 echo failed at %d: state=%lld\n", i,
                    static_cast<long long>(counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)));
                return false;
            }
        }
        return true;
    }

    /// `ClusterControl.ToggleState.SNAPSHOT` through the CM's counter.
    [[nodiscard]] bool take_snapshot()
    {
        if (!client_)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir()});
            if (!client)
            {
                return false;
            }
            client_ = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        (void)client_->do_work();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            client_->counters_reader(), deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            client_.reset();
            return false;
        }
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        if (nullptr == address)
        {
            return false;
        }
        const auto before = counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
        __atomic_store_n(address, static_cast<std::int64_t>(4), __ATOMIC_RELEASE);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > before)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    std::string jar_{};
    std::string root_dir_{};
    std::string tests_root_{};
    std::string classes_dir_{};
    int base_port_{0};
    bool java_probes_compiled_{false};
    std::unique_ptr<ChildProcess> driver_{};
    std::unique_ptr<ChildProcess> archive_{};
    std::unique_ptr<ChildProcess> cm_{};
    std::unique_ptr<ChildProcess> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
    ClusterClientSession session_{};
};

TEST_F(Xc0JavaClusterTest, theJavaClusterRunsOnOurArchive)
{
    start_driver();
    start_archive(Archive::ours);
    start_consensus_module();
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60)))
        << "the cluster never became active on our archive (state="
        << counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) << ")";
    ASSERT_TRUE(connect_and_echo(10)) << "the cluster on our archive did not echo";
    stop_all();
}

TEST_F(Xc0JavaClusterTest, theJavaClusterRunsOnTheReferenceArchive)
{
    // The control arm for the subset above: the same Java cluster, service and
    // client, in the reference's own driver+archive+CM process.
    start_reference_cluster();
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60)))
        << "the cluster never became active on the reference stack (state="
        << counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) << ")";
    ASSERT_TRUE(connect_and_echo(10)) << "the cluster on the reference stack did not echo";
    stop_all();
}

TEST_F(Xc0JavaClusterTest, theJavaClusterOnOurArchiveSnapshotsAndRestarts)
{
    start_driver();
    start_archive(Archive::ours);
    start_consensus_module();
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60))) << "not active on our archive";
    ASSERT_TRUE(connect_and_echo(10)) << "no echo on our archive";
    ASSERT_TRUE(take_snapshot()) << "no snapshot on our archive";
    stop_all();

    start_driver(false);
    start_archive(Archive::ours, false);
    start_consensus_module(false);
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60))) << "not active after the restart on our archive";
    ASSERT_TRUE(connect_and_echo(5)) << "no echo after the restart on our archive";
    stop_all();
}

TEST_F(Xc0JavaClusterTest, theJavaClusterOnTheReferenceStackSnapshotsAndRestarts)
{
    start_reference_cluster();
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60))) << "not active on the reference stack";
    ASSERT_TRUE(connect_and_echo(10)) << "no echo on the reference stack";
    ASSERT_TRUE(take_snapshot()) << "no snapshot on the reference stack";
    stop_all();

    start_reference_cluster(false);
    start_container();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(60))) << "not active after the restart on the reference stack";
    ASSERT_TRUE(connect_and_echo(5)) << "no echo after the restart on the reference stack";
    stop_all();
}

}  // namespace
