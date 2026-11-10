// C2c's criteria: the container as its own binary (`XC-4a` scenes) and a
// business loaded from a `.so`.
//
// The same Java cluster the C2a/C2b tests use; the difference is the far side
// of the S2 seam is now `deepmsg-clustered-service-container`, not an agent on
// a thread of this process. Scenes: echo 1 and 10 000, three timer rounds, a
// snapshot that survives a restart, and the dynamic echo business.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_versions.h"
#include "cluster/service/container_config.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif
#ifndef AXC_ECHO_SHARED_LIBRARY
#define AXC_ECHO_SHARED_LIBRARY ""
#endif

namespace
{

constexpr std::int32_t CLUSTER_ID = 9;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

[[nodiscard]] std::string tests_root()
{
    const char* from_env = std::getenv("AXC_AERON_TESTS");
    return nullptr == from_env ? std::string{} : std::string{from_env};
}

[[nodiscard]] std::string find_probe(const std::string& root, const std::string& name)
{
    for (const std::string& relative : {"/deepmsg-tests/java/" + name, "/java/" + name, "/" + name})
    {
        const std::string candidate = root + relative;
        if (std::filesystem::exists(candidate))
        {
            return candidate;
        }
    }
    return {};
}

class ContainerBinaryInteropTest : public ::testing::Test
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
        if (!std::filesystem::exists("/usr/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
        }

        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }

        std::vector<std::string> sources;
        for (const char* const name :
             {"ClusterEchoClientProbe.java", "ClusterTimerClientProbe.java", "ClusterAuctionBidClientProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << name << " not found under " << root;
            }
            sources.push_back(path);
        }

        directory_ = std::filesystem::temp_directory_path() / ("axc-c2c-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);
        aeron_dir_ = (directory_ / "aeron").string();
        cluster_dir_ = (directory_ / "cluster").string();
        archive_dir_ = (directory_ / "archive").string();
        classes_dir_ = (directory_ / "classes").string();
        std::filesystem::create_directories(classes_dir_);

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        compile.arguments.insert(compile.arguments.end(), sources.begin(), sources.end());
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
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

    [[nodiscard]] std::vector<std::string> java_node_arguments(bool delete_on_start) const
    {
        const std::string flag = delete_on_start ? "true" : "false";
        return {"--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeron_dir_,
                "-Daeron.dir.delete.on.start=" + flag,
                "-Daeron.cluster.dir=" + cluster_dir_,
                "-Daeron.cluster.dir.delete.on.start=" + flag,
                "-Daeron.archive.dir=" + archive_dir_,
                "-Daeron.archive.dir.delete.on.start=" + flag,
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

    void start_java_node(bool delete_on_start)
    {
        auto process = axc::interop::ChildProcess::spawn("java", java_node_arguments(delete_on_start));
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

    [[nodiscard]] std::vector<std::string> container_arguments(const std::string& service_name,
                                                               const std::string& library) const
    {
        std::vector<std::string> arguments = {
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
            "-Daeron.archive.control.response.stream.id=20"};
        if (!service_name.empty())
        {
            arguments.push_back("-Ddeepmsg.cluster.service=" + service_name);
        }
        if (!library.empty())
        {
            arguments.push_back("-Ddeepmsg.cluster.service.library=" + library);
        }
        return arguments;
    }

    void start_container(const std::string& service_name, const std::string& library = {})
    {
        auto process =
            axc::interop::ChildProcess::spawn(AXC_CONTAINER_BINARY, container_arguments(service_name, library));
        ASSERT_TRUE(process) << process.error().message.view();
        container_ = std::move(*process);
        container_running_ = true;

        // The module turns ACTIVE once every service has acked recovery.
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

    [[nodiscard]] std::string run_probe(const std::string& main_class, const std::vector<std::string>& arguments)
    {
        std::vector<std::string> full = {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                                         "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                                         "-cp",         jar_ + ":" + classes_dir_,
                                         main_class};
        full.insert(full.end(), arguments.begin(), arguments.end());

        axc::interop::ProcessOptions options;
        options.arguments = std::move(full);
        options.timeout = std::chrono::seconds(180);
        auto result = axc::interop::run_process("java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        EXPECT_EQ(0, result->exit_code) << result->output;
        return result->output;
    }

    std::string jar_{};
    std::filesystem::path directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    axc::interop::ChildProcess java_{};
    bool java_running_{false};
    axc::interop::ChildProcess container_{};
    bool container_running_{false};
};

TEST_F(ContainerBinaryInteropTest, theBinaryEchoesAndRunsThreeTimerRounds)
{
    start_java_node(true);
    start_container("echo");

    const std::string one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_NE(std::string::npos, one.find("RECEIVED 1")) << one;

    const std::string many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"});
    EXPECT_NE(std::string::npos, many.find("RECEIVED 10000")) << many;

    for (int round = 0; round < 3; ++round)
    {
        const std::string timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"});
        EXPECT_NE(std::string::npos, timers.find("TIMER 1")) << "round " << round << ": " << timers;
        EXPECT_NE(std::string::npos, timers.find("CANCELLED 2")) << "round " << round << ": " << timers;
        EXPECT_NE(std::string::npos, timers.find("TIMER 3")) << "round " << round << ": " << timers;
    }
}

TEST_F(ContainerBinaryInteropTest, aSnapshotSurvivesABinaryRestart)
{
    start_java_node(true);
    start_container("basic-auction");

    const auto bid = [this](std::uint64_t bids, std::uint64_t customer, std::uint64_t price, bool snapshot)
    {
        return run_probe("ClusterAuctionBidClientProbe",
                         {aeron_dir_, std::to_string(CLUSTER_ID), std::to_string(bids), std::to_string(customer),
                          std::to_string(price), snapshot ? "true" : "false"});
    };

    const std::string bids = bid(10, 1, 100, true);
    EXPECT_NE(std::string::npos, bids.find("LAST 1 109 true")) << bids;
    EXPECT_NE(std::string::npos, bids.find("SNAPSHOT_REQUESTED")) << bids;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (std::chrono::steady_clock::now() < deadline &&
           counter_value(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) < 1)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(counter_value(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), 1);

    stop_container();
    if (java_running_)
    {
        java_.terminate(std::chrono::seconds(10));
        java_running_ = false;
    }
    start_java_node(false);
    start_container("basic-auction");

    const std::string recovered = bid(1, 99, 0, false);
    EXPECT_NE(std::string::npos, recovered.find("LAST 1 109 false")) << recovered;
}

TEST_F(ContainerBinaryInteropTest, aBusinessLoadedFromASharedLibraryEchoes)
{
    if (!std::filesystem::exists(AXC_ECHO_SHARED_LIBRARY))
    {
        GTEST_SKIP() << "the dynamic echo library is missing: " << AXC_ECHO_SHARED_LIBRARY;
    }

    start_java_node(true);
    start_container("", AXC_ECHO_SHARED_LIBRARY);

    const std::string one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_NE(std::string::npos, one.find("RECEIVED 1")) << one;

    const std::string many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1000", "60"});
    EXPECT_NE(std::string::npos, many.find("RECEIVED 1000")) << many;
}

}  // namespace
