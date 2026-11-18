// XC-9: `ClusterBackup`, starting with the process shape the reference uses
// (`ClusterBackupMediaDriver`): the media driver + archive + backup agent in
// one process, configured by a properties file. S2's criterion is that the
// reference tools read the backup's directory: `ClusterTool describe` sees a
// BACKUP component, `pid` sees the process, the four backup counters are
// live, and SIGTERM leaves the mark file terminated.

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
#include "fixture/process.h"

#ifndef AXC_CLUSTER_BACKUP_BIN
#define AXC_CLUSTER_BACKUP_BIN ""
#endif
#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;

constexpr std::int32_t CLUSTER_ID = 9;

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

class Xc9BackupTest : public ::testing::Test
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
        if (!std::filesystem::exists(AXC_CLUSTER_BACKUP_BIN))
        {
            GTEST_SKIP() << "the cluster backup binary is missing";
        }

        std::string pattern = "/tmp/axc-c9-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        base_port_ = 10'000 + ((static_cast<int>(::getpid()) % 32) * 100);
    }

    void TearDown() override
    {
        stop();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "XC9_ROUND_DIR=%s\n", root_dir_.c_str());
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

    void write_properties()
    {
        const std::string path = root_dir_ + "/backup.properties";
        std::ofstream out{path};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir() << "\n"
            << "aeron.cluster.dir=" << cluster_dir() << "\n"
            << "aeron.archive.dir=" << archive_dir() << "\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << (base_port_ + 100) << "\n"
            << "aeron.archive.threading.mode=DEDICATED\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=0\n"
            << "aeron.cluster.members=0,localhost:" << base_port_ << ",localhost:" << (base_port_ + 1)
            << ",localhost:" << (base_port_ + 2) << ",localhost:" << (base_port_ + 3)
            << ",localhost:" << (base_port_ + 4) << "\n"
            << "aeron.cluster.backup.catchup.channel=aeron:udp?endpoint=localhost:0\n";
        out.close();
    }

    void start_backup()
    {
        auto spawned = ChildProcess::spawn(AXC_CLUSTER_BACKUP_BIN, {root_dir_ + "/backup.properties"}, {},
                                           root_dir_ + "/backup.log");
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        backup_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void stop()
    {
        client_.reset();
        if (backup_)
        {
            backup_->terminate(std::chrono::seconds(10));
            backup_.reset();
        }
    }

    /// `ClusterTool <dir> <command>` with the reference jar.
    [[nodiscard]] axc::interop::ProcessResult run_tool(const std::string& command)
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(),
                                 {"-cp", jar_, "io.aeron.cluster.ClusterTool", cluster_dir(), command});
        options.timeout = std::chrono::seconds(60);
        auto result = axc::interop::run_process("/usr/bin/java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        return result ? *result : axc::interop::ProcessResult{};
    }

    [[nodiscard]] bool wait_for_mark_file(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (std::filesystem::exists(cluster_dir() + "/cluster-mark.dat"))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
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
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    std::string jar_{};
    std::string root_dir_{};
    int base_port_{0};
    std::unique_ptr<ChildProcess> backup_{};
    std::unique_ptr<DeepmsgClient> client_{};
};

TEST_F(Xc9BackupTest, theBackupStartsAsAReferenceReadableBackup)
{
    write_properties();
    start_backup();

    ASSERT_TRUE(wait_for_mark_file(std::chrono::seconds(30))) << "the backup never wrote its mark file";

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool counters_live = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (0 == counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) &&
            counter(deepmsg::cluster::common::CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID) > 0)
        {
            counters_live = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(counters_live) << "the backup counters never appeared";

    // 208/209/210/240 are the four the reference allocates
    // (`ClusterBackup.Context.conclude`).
    EXPECT_GE(counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID), 0);
    EXPECT_GE(counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID), 0);
    EXPECT_GT(counter(deepmsg::cluster::common::CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID), 0);
    EXPECT_GE(counter(deepmsg::cluster::common::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID), 0);

    const auto describe = run_tool("describe");
    ASSERT_EQ(0, describe.exit_code) << describe.output;
    EXPECT_NE(std::string::npos, describe.output.find("componentType=BACKUP")) << describe.output;
    EXPECT_NE(std::string::npos, describe.output.find("clusterId=9")) << describe.output;

    const auto pid = run_tool("pid");
    ASSERT_EQ(0, pid.exit_code) << pid.output;
    EXPECT_NE(std::string::npos, pid.output.find(std::to_string(backup_->pid()))) << pid.output;

    const int exit_code = backup_->exit_code();
    ASSERT_TRUE(backup_->running()) << "the backup exited: " << exit_code;

    backup_->terminate(std::chrono::seconds(10));
    EXPECT_EQ(0, backup_->exit_code()) << "the backup did not shut down cleanly";
    backup_.reset();

    const auto stopped = run_tool("describe");
    ASSERT_EQ(0, stopped.exit_code) << stopped.output;
    EXPECT_NE(std::string::npos, stopped.output.find("activityTimestamp=-1")) << stopped.output;
}

}  // namespace
