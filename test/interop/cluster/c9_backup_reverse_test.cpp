// XC-9 S4: the reference `ClusterBackup` against our cluster. The cluster is
// three `deepmsg-cluster-node --embedded-driver` members with their echo
// containers; the backup is the reference's `ClusterBackupMediaDriver` main
// (driver + archive + agent in one process) driven by a properties file. A
// backup is served by a follower, so the cluster must have one.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
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

#include "cluster/backup/cluster_backup_context.h"
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
using deepmsg::cluster::backup::backup_state_code;
using deepmsg::cluster::backup::ClusterBackupState;

constexpr std::int32_t CLUSTER_ID = 9;

/// The C client's default error handler exits the process; a client whose
/// driver was deliberately stopped must not kill the test binary between
/// cases (`aeron_context.c:43`).
void on_client_error(void* /*clientd*/, int /*errcode*/, const char* /*message*/) {}

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

class Xc9JavaBackupAgainstOurClusterTest : public ::testing::Test
{
protected:
    static constexpr int MEMBERS = 3;

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
        std::string pattern = "/tmp/axc-c9-rev-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        base_port_ = 15'500 + ((static_cast<int>(::getpid()) % 8) * 200);
    }

    void TearDown() override
    {
        stop_all();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "XC9_REV_ROUND_DIR=%s\n", root_dir_.c_str());
        }
        else
        {
            std::filesystem::remove_all(root_dir_, error);
        }
    }

    [[nodiscard]] std::string node_dir(int index) const
    {
        return root_dir_ + "/node" + std::to_string(index);
    }
    [[nodiscard]] int member_port(int index, int offset) const
    {
        return base_port_ + (index * 10) + offset;
    }
    [[nodiscard]] int backup_consensus_port() const
    {
        return base_port_ + 50;
    }
    [[nodiscard]] int backup_catchup_port() const
    {
        return base_port_ + 55;
    }
    [[nodiscard]] int backup_archive_port() const
    {
        return base_port_ + 70;
    }
    [[nodiscard]] int egress_port() const
    {
        return base_port_ + 60;
    }

    [[nodiscard]] std::string members() const
    {
        std::string value;
        for (int i = 0; i < MEMBERS; ++i)
        {
            if (0 != i)
            {
                value.push_back('|');
            }
            value.append(std::to_string(i))
                .append(",localhost:")
                .append(std::to_string(member_port(i, 0)))
                .append(",localhost:")
                .append(std::to_string(member_port(i, 1)))
                .append(",localhost:")
                .append(std::to_string(member_port(i, 2)))
                .append(",localhost:")
                .append(std::to_string(member_port(i, 3)))
                .append(",localhost:")
                .append(std::to_string(member_port(i, 4)));
        }
        return value;
    }

    [[nodiscard]] std::string consensus_endpoints() const
    {
        std::string value;
        for (int i = 0; i < MEMBERS; ++i)
        {
            if (0 != i)
            {
                value.push_back(',');
            }
            value.append("localhost:").append(std::to_string(member_port(i, 1)));
        }
        return value;
    }

    void start_cluster()
    {
        for (int i = 0; i < MEMBERS; ++i)
        {
            const std::string dir = node_dir(i);
            std::filesystem::create_directories(dir + "/aeron");
            std::filesystem::create_directories(dir + "/cluster");
            std::filesystem::create_directories(dir + "/archive");
            const std::string path = dir + "/node.properties";
            std::ofstream out{path};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << dir << "/aeron\n"
                << "aeron.dir.delete.on.start=true\n"
                << "aeron.cluster.dir=" << dir << "/cluster\n"
                << "aeron.cluster.dir.delete.on.start=true\n"
                << "aeron.archive.dir=" << dir << "/archive\n"
                << "aeron.archive.dir.delete.on.start=true\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.member.id=" << i << "\n"
                << "aeron.cluster.members=" << members() << "\n"
                << "aeron.cluster.ingress.channel=aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:"
                << member_port(i, 0) << "\n"
                << "aeron.cluster.consensus.channel=aeron:udp?alias=consensus\n"
                << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
                << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
                << "aeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0\n"
                << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << member_port(i, 4) << "\n"
                << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n"
                << "aeron.cluster.leader.heartbeat.timeout=2s\n"
                << "aeron.cluster.startup.canvass.timeout=10s\n"
                << "aeron.cluster.service.count=1\n";
            out.close();

            auto spawned =
                ChildProcess::spawn(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", path}, {}, dir + "/node.log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            nodes_[i] = std::make_unique<ChildProcess>(std::move(*spawned));
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        for (int i = 0; i < MEMBERS; ++i)
        {
            const std::string path = node_dir(i) + "/container.properties";
            std::ofstream out{path};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << node_dir(i) << "/aeron\n"
                << "aeron.dir.delete.on.start=false\n"
                << "aeron.cluster.dir=" << node_dir(i) << "/cluster\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.service.id=0\n"
                << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
                << "deepmsg.cluster.service=echo\n";
            out.close();
            auto spawned = ChildProcess::spawn(AXC_CONTAINER_BINARY, {path}, {}, node_dir(i) + "/container.log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            containers_[i] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
    }

    void start_java_backup()
    {
        const std::string path = root_dir_ + "/backup.properties";
        std::ofstream out{path};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << root_dir_ << "/backup-aeron\n"
            << "aeron.dir.delete.on.start=true\n"
            << "aeron.cluster.dir=" << root_dir_ << "/backup-cluster\n"
            << "aeron.cluster.dir.delete.on.start=true\n"
            << "aeron.archive.dir=" << root_dir_ << "/backup-archive\n"
            << "aeron.archive.dir.delete.on.start=true\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << backup_archive_port() << "\n"
            << "aeron.archive.control.response.channel=aeron:udp?endpoint=localhost:" << (base_port_ + 90) << "\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=0\n"
            << "aeron.cluster.members=" << members() << "\n"
            << "aeron.cluster.consensus.channel=aeron:udp?endpoint=localhost:" << backup_consensus_port() << "\n"
            << "aeron.cluster.consensus.endpoints=" << consensus_endpoints() << "\n"
            << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
            << "aeron.cluster.backup.catchup.endpoint=localhost:" << backup_catchup_port() << "\n"
            << "aeron.cluster.backup.catchup.channel=aeron:udp?endpoint=localhost:0\n"
            << "aeron.cluster.backup.source.type=ANY\n"
            << "aeron.threading.mode=SHARED\n";
        out.close();

        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(), {"-cp", jar_, "io.aeron.cluster.ClusterBackupMediaDriver", path});
        auto spawned = ChildProcess::spawn("/usr/bin/java", arguments, {}, root_dir_ + "/backup.log");
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        backup_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void stop_all()
    {
        session_.close();
        if (backup_)
        {
            backup_->terminate(std::chrono::seconds(10));
            backup_.reset();
        }
        for (auto& container : containers_)
        {
            if (container)
            {
                container->terminate(std::chrono::seconds(10));
                container.reset();
            }
        }
        for (auto& node : nodes_)
        {
            if (node)
            {
                node->terminate(std::chrono::seconds(10));
                node.reset();
            }
        }
        for (auto& client : clients_)
        {
            client.reset();
        }
        backup_client_.reset();
    }

    [[nodiscard]] std::int64_t counter(int index, std::int32_t type_id)
    {
        if (!clients_[index])
        {
            const std::string client_dir = node_dir(index) + "/aeron";
            axc::core::ClientConfig client_config{client_dir};
            client_config.on_error = &on_client_error;
            auto client = DeepmsgClient::connect(client_config);
            if (!client)
            {
                return -1;
            }
            clients_[index] = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        auto& client = clients_[index];
        (void)client->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(client->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] std::int64_t backup_counter(std::int32_t type_id)
    {
        if (!backup_client_)
        {
            const std::string client_dir = root_dir_ + "/backup-aeron";
            axc::core::ClientConfig client_config{client_dir};
            client_config.on_error = &on_client_error;
            auto client = DeepmsgClient::connect(client_config);
            if (!client)
            {
                return -1;
            }
            backup_client_ = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        (void)backup_client_->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(backup_client_->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(backup_client_->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] int leader_index()
    {
        for (int i = 0; i < MEMBERS; ++i)
        {
            if (2 == counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return i;
            }
        }
        return -1;
    }

    [[nodiscard]] bool wait_for_active(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            bool all_active = true;
            for (int i = 0; i < MEMBERS; ++i)
            {
                all_active =
                    all_active && 1 == counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
            }
            if (all_active && leader_index() >= 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void echo(int count)
    {
        const int leader = leader_index();
        ASSERT_GE(leader, 0) << "no leader";
        std::string ingress;
        for (int i = 0; i < MEMBERS; ++i)
        {
            if (0 != i)
            {
                ingress.push_back(',');
            }
            ingress.append(std::to_string(i)).append("=localhost:").append(std::to_string(member_port(i, 0)));
        }
        const std::string egress = "aeron:udp?endpoint=localhost:" + std::to_string(egress_port());
        ASSERT_TRUE(session_.connect_udp(node_dir(leader) + "/aeron", ingress, egress, std::chrono::seconds(30)))
            << session_.last_error();
        for (int i = 0; i < count; ++i)
        {
            std::string payload = "rev-" + std::to_string(i);
            ASSERT_TRUE(session_.offer_and_await(payload, static_cast<std::size_t>(i + 1), std::chrono::seconds(20)))
                << "echo failed at " << i;
        }
    }

    void take_snapshot()
    {
        const int leader = leader_index();
        ASSERT_GE(leader, 0) << "no leader for the snapshot toggle";
        const std::int32_t toggle_id = deepmsg::cluster::common::find_cluster_counter(
            clients_[leader]->counters_reader(), deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, toggle_id) << "no control toggle counter";
        std::int64_t* toggle = aeron_counters_reader_addr(clients_[leader]->counters_reader(), toggle_id);
        ASSERT_NE(nullptr, toggle);

        const std::int64_t before = counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
        __atomic_store_n(toggle, static_cast<std::int64_t>(4), __ATOMIC_RELEASE);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > before)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "our cluster never took the snapshot";
    }

    [[nodiscard]] bool wait_for_backup_state(ClusterBackupState wanted, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (backup_state_code(wanted) == backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] axc::interop::ProcessResult run_tool(const std::string& directory, const std::string& command)
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(),
                                 {"-cp", jar_, "io.aeron.cluster.ClusterTool", directory, command});
        options.timeout = std::chrono::seconds(60);
        auto result = axc::interop::run_process("/usr/bin/java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        return result ? *result : axc::interop::ProcessResult{};
    }

    std::string jar_{};
    std::string root_dir_{};
    int base_port_{0};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> nodes_{};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> containers_{};
    std::array<std::unique_ptr<DeepmsgClient>, MEMBERS> clients_{};
    std::unique_ptr<ChildProcess> backup_{};
    std::unique_ptr<DeepmsgClient> backup_client_{};
    ClusterClientSession session_{};
};

TEST_F(Xc9JavaBackupAgainstOurClusterTest, theJavaBackupBacksUpOurCluster)
{
    start_cluster();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "our cluster never became active";

    echo(10);
    take_snapshot();

    start_java_backup();
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(180)))
        << "the Java backup never reached BACKING_UP (state="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) << ")";

    EXPECT_GE(backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID), 1);

    const auto recording_log = run_tool(root_dir_ + "/backup-cluster", "recording-log");
    ASSERT_EQ(0, recording_log.exit_code) << recording_log.output;
    EXPECT_NE(std::string::npos, recording_log.output.find("type=SNAPSHOT")) << recording_log.output;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    bool live_position = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID) > 0)
        {
            live_position = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(live_position) << "the Java backup never followed the live log";
}

}  // namespace
