// XC-5's first half: the reference `ClusterTool`'s read-only subcommands must
// read a C++ cluster directory. `ClusterTool` takes `<cluster dir> <command>`;
// the mark-file commands work on a stopped directory, the archive-backed ones
// (`recovery-plan`, `backup-query`, `is-leader`, the snapshot describe and the
// recording-log validation) need the node's archive answering.

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

#ifndef AXC_CLUSTER_NODE_BIN
#define AXC_CLUSTER_NODE_BIN ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif
#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ClusterClientSession;

constexpr std::int32_t CLUSTER_ID = 7;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

class Xc5ClusterToolTest : public ::testing::Test
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
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            GTEST_SKIP() << "cluster binaries not found";
        }
        std::string pattern = "/tmp/axc-xc5-tool-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        base_port_ = 9000 + ((static_cast<int>(::getpid()) % 32) * 100);
    }

    void TearDown() override
    {
        stop();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "XC5_TOOL_DIR=%s\n", directory_.c_str());
        }
        else
        {
            std::filesystem::remove_all(directory_, error);
        }
    }

    void write_properties()
    {
        const std::string node = directory_ + "/node.properties";
        std::ofstream out{node};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir_ << "\n"
            << "aeron.cluster.dir=" << cluster_dir_ << "\n"
            << "aeron.archive.dir=" << archive_dir_ << "\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=0\n"
            << "aeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0\n"
            << "aeron.cluster.ingress.channel=aeron:ipc\n"
            << "aeron.cluster.consensus.channel=aeron:ipc\n"
            << "aeron.cluster.log.channel=aeron:udp?endpoint=localhost:" << base_port_ << "\n"
            << "aeron.cluster.replication.channel=aeron:udp\n"
            << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << (base_port_ + 100) << "\n"
            << "aeron.cluster.service.count=1\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        out.close();

        const std::string container = directory_ + "/container.properties";
        std::ofstream cout{container};
        ASSERT_TRUE(cout.good());
        cout << "aeron.dir=" << aeron_dir_ << "\n"
             << "aeron.cluster.dir=" << cluster_dir_ << "\n"
             << "aeron.cluster.id=" << CLUSTER_ID << "\n"
             << "aeron.cluster.service.id=0\n"
             << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
             << "deepmsg.cluster.service=echo\n";
    }

    void start()
    {
        write_properties();
        auto node = ChildProcess::spawn(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", directory_ + "/node.properties"});
        ASSERT_TRUE(node) << node.error().message.view();
        node_ = std::make_unique<ChildProcess>(std::move(*node));

        const auto driver_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < driver_deadline)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                client_ = std::make_unique<DeepmsgClient>(std::move(*client));
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(client_) << "the node's driver never published its CnC file";
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        for (int attempt = 0; attempt < 30; ++attempt)
        {
            auto container = ChildProcess::spawn(AXC_CONTAINER_BINARY, {directory_ + "/container.properties"});
            ASSERT_TRUE(container) << container.error().message.view();
            container_ = std::make_unique<ChildProcess>(std::move(*container));
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            if (container_->running())
            {
                break;
            }
            container_.reset();
        }
        ASSERT_TRUE(container_ && container_->running()) << "the container never stayed up";

        const auto active_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < active_deadline)
        {
            if (counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1 &&
                2 == counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the node never became active";
    }

    void stop()
    {
        if (container_)
        {
            container_->terminate(std::chrono::seconds(10));
            container_.reset();
        }
        if (node_)
        {
            node_->terminate(std::chrono::seconds(10));
            node_.reset();
        }
        client_.reset();
    }

    [[nodiscard]] std::int64_t counter(std::int32_t type_id)
    {
        if (!client_)
        {
            return -1;
        }
        (void)client_->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(client_->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    void set_control_toggle(std::int32_t value)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            client_->counters_reader(), deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    struct ToolResult
    {
        int exit_code;
        std::string output;
    };

    [[nodiscard]] ToolResult run_tool(const std::string& command)
    {
        axc::interop::ProcessOptions options;
        options.arguments = {"--add-opens",
                             "java.base/jdk.internal.misc=ALL-UNNAMED",
                             "--add-opens",
                             "java.base/sun.nio.ch=ALL-UNNAMED",
                             "-cp",
                             jar_,
                             "io.aeron.cluster.ClusterTool",
                             cluster_dir_,
                             command};
        options.timeout = std::chrono::seconds(60);
        auto ran = axc::interop::run_process("java", options);
        EXPECT_TRUE(ran) << ran.error().message.view();
        return ran ? ToolResult{ran->exit_code, ran->output} : ToolResult{-1, {}};
    }

    /// One read-only command against the directory: exit 0, and output unless
    /// the command is one that answers with its exit code (`is-leader`).
    void expect_reads(const std::string& command, bool expect_output = true)
    {
        const ToolResult result = run_tool(command);
        EXPECT_EQ(0, result.exit_code) << command << " failed:\n" << result.output;
        if (expect_output)
        {
            EXPECT_FALSE(result.output.empty()) << command << " printed nothing";
        }
        std::fprintf(stderr, "XC5 tool %-28s exit=%d lines=%zu\n", command.c_str(), result.exit_code,
                     static_cast<std::size_t>(std::count(result.output.begin(), result.output.end(), '\n')));
    }

    std::string jar_{};
    std::string directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    int base_port_{0};
    std::unique_ptr<ChildProcess> node_{};
    std::unique_ptr<ChildProcess> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
};

TEST_F(Xc5ClusterToolTest, theReadOnlyCommandsReadARunningDirectory)
{
    // The archive-backed commands need the node's archive answering.
    start();
    expect_reads("describe");
    expect_reads("pid");
    expect_reads("errors");
    expect_reads("list-members");
    expect_reads("recording-log");
    expect_reads("is-leader", false);
    expect_reads("validate-recording-log");
    stop();
}

TEST_F(Xc5ClusterToolTest, theMarkFileCommandsReadAStoppedDirectory)
{
    // After a clean stop the mark-file commands still read the directory.
    start();
    const auto snapshots_before = counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(4);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline &&
           counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) <= snapshots_before)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    stop();

    // `list-members` is a live query over the consensus channel (the reference
    // opens a subscription), so it belongs to the running case above.
    expect_reads("describe");
    expect_reads("pid");
    expect_reads("errors");
    expect_reads("recording-log");
}

}  // namespace
