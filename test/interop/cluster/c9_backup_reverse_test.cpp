// XC-9 S4: the reference `ClusterBackup` against our cluster. The cluster is
// three `deepmsg-cluster-node --embedded-driver` members with their echo
// containers; the backup is the reference's `ClusterBackupMediaDriver` main
// (driver + archive + agent in one process) driven by a properties file. A
// backup is served by a follower, so the cluster must have one.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
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

/// The tracking service reads a 4-byte little-endian id per client message.
[[nodiscard]] std::string int_payload(std::int32_t value)
{
    std::string bytes(sizeof(value), '\0');
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
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

    void start_cluster(bool clean_start = true)
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
                << "aeron.cluster.dir.delete.on.start=" << (clean_start ? "true" : "false") << "\n"
                << "aeron.archive.dir=" << dir << "/archive\n"
                << "aeron.archive.dir.delete.on.start=" << (clean_start ? "true" : "false") << "\n"
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
                << "aeron.cluster.leader.heartbeat.timeout=" << heartbeat_timeout_ << "\n"
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
            write_container_properties(path, node_dir(i) + "/aeron", node_dir(i) + "/cluster");
            auto spawned = ChildProcess::spawn(AXC_CONTAINER_BINARY, {path}, tracking_environment(tracking_file(i)),
                                               node_dir(i) + "/container.log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            containers_[i] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
    }

    /// The container's properties. The tracking service is the stateful
    /// witness the node-from-backup scenario needs (`C9-2`): it records every
    /// applied message to a file and snapshots its lists, so a recovered node
    /// shows the state it reloaded.
    void write_container_properties(const std::string& path, const std::string& aeron_dir,
                                    const std::string& cluster_dir)
    {
        std::ofstream out{path};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir << "\n"
            << "aeron.dir.delete.on.start=false\n"
            << "aeron.cluster.dir=" << cluster_dir << "\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.service.id=0\n"
            << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n";
        if (tracking_)
        {
            out << "deepmsg.cluster.service.library=" << AXC_TRACKING_SERVICE_LIBRARY << "\n"
                << "deepmsg.cluster.service=tracking\n";
        }
        else
        {
            out << "deepmsg.cluster.service=echo\n";
        }
    }

    [[nodiscard]] std::string tracking_file(int index) const
    {
        return root_dir_ + "/tracking-" + std::to_string(index) + ".txt";
    }

    [[nodiscard]] std::vector<std::string> tracking_environment(const std::string& file) const
    {
        if (!tracking_)
        {
            return {};
        }
        return {"AXC_TEST_TRACKING_FILE=" + file, "AXC_TEST_TRACKING_SERVICE_ID=0", "AXC_TEST_TRACKING_NODE_INDEX=0"};
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
            // The reference's TestCluster also shortens only the cool-down
            // (`TestCluster.java:598`); the leader-change case needs it short.
            << "aeron.cluster.backup.cool.down.interval=1s\n"
            << "aeron.cluster.backup.source.type=ANY\n"
            << "aeron.threading.mode=SHARED\n";
        if (!backup_progress_timeout_.empty())
        {
            out << "aeron.cluster.backup.progress.timeout=" << backup_progress_timeout_ << "\n";
        }
        out.close();

        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(), {"-cp", jar_, "io.aeron.cluster.ClusterBackupMediaDriver", path});
        auto spawned = ChildProcess::spawn("/usr/bin/java", arguments, {}, root_dir_ + "/backup.log");
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        backup_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void stop_cluster()
    {
        session_.close();
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
    }

    void stop_all()
    {
        stop_restart_node();
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
        if (!nodes_[index])
        {
            // The member was stopped deliberately (the leader-change case): its
            // driver is gone, so a counter read is not a failure.
            return -1;
        }
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

    /// The tracking service's client messages carry a 4-byte id; the echo it
    /// answers with is the same payload.
    void send_ints(int first, int count)
    {
        const int leader = leader_index();
        ASSERT_GE(leader, 0) << "no leader";
        // A new leader needs a new session (the reference's `reconnectClient`).
        session_.close();
        ASSERT_TRUE(session_.connect_udp(node_dir(leader) + "/aeron", ingress_endpoints(), egress_channel(),
                                         std::chrono::seconds(30)))
            << session_.last_error();
        for (int i = first; i < first + count; ++i)
        {
            ASSERT_TRUE(session_.offer_and_await(int_payload(i), static_cast<std::size_t>(i - first + 1),
                                                 std::chrono::seconds(20)))
                << "echo failed at " << i;
        }
    }

    [[nodiscard]] std::int64_t leader_commit()
    {
        const int leader = leader_index();
        return leader < 0 ? -1 : counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    }

    /// The backup's live log position reaches the leader's commit position (the
    /// reference's `awaitBackupLiveLogPosition`, `ClusterBackupTest.java:71`).
    [[nodiscard]] bool wait_for_backup_live(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            const std::int64_t commit = leader_commit();
            if (commit >= 0 &&
                backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID) >= commit)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void send_ints(int count)
    {
        send_ints(0, count);
    }

    void stop_cluster_member(int index)
    {
        session_.close();
        if (nodes_[index])
        {
            nodes_[index]->terminate(std::chrono::seconds(10));
            nodes_[index].reset();
        }
        clients_[index].reset();
    }

    [[nodiscard]] bool wait_for_new_leader(int old_leader, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            const int leader = leader_index();
            if (leader >= 0 && leader != old_leader &&
                1 == counter(leader, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] std::string ingress_endpoints() const
    {
        std::string ingress;
        for (int i = 0; i < MEMBERS; ++i)
        {
            if (0 != i)
            {
                ingress.push_back(',');
            }
            ingress.append(std::to_string(i)).append("=localhost:").append(std::to_string(member_port(i, 0)));
        }
        return ingress;
    }

    [[nodiscard]] std::string egress_channel() const
    {
        return "aeron:udp?endpoint=localhost:" + std::to_string(egress_port());
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

    void stop_restart_node()
    {
        restart_session_.close();
        if (restart_container_)
        {
            restart_container_->terminate(std::chrono::seconds(10));
            restart_container_.reset();
        }
        if (restart_node_)
        {
            restart_node_->terminate(std::chrono::seconds(10));
            restart_node_.reset();
        }
        restart_client_.reset();
    }

    [[nodiscard]] int restart_port(int offset) const
    {
        return base_port_ + 200 + offset;
    }

    [[nodiscard]] std::string restart_tracking_file() const
    {
        return root_dir_ + "/tracking-restart.txt";
    }

    /// Reference `TestCluster.startStaticNodeFromBackup` (`:623-687`): one
    /// member whose cluster directory and archive are the backup's, started
    /// with `deleteDirOnStart(false)` / `deleteArchiveOnStart(false)` and
    /// appointed as its own leader.
    void start_node_from_backup()
    {
        const std::string dir = root_dir_ + "/restart";
        std::filesystem::create_directories(dir + "/aeron");
        const int archive_port = restart_port(4);
        const std::string member =
            "0,localhost:" + std::to_string(restart_port(0)) + ",localhost:" + std::to_string(restart_port(1)) +
            ",localhost:" + std::to_string(restart_port(2)) + ",localhost:" + std::to_string(restart_port(3)) +
            ",localhost:" + std::to_string(archive_port);

        const std::string path = dir + "/node.properties";
        std::ofstream out{path};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << dir << "/aeron\n"
            << "aeron.dir.delete.on.start=true\n"
            << "aeron.cluster.dir=" << root_dir_ << "/backup-cluster\n"
            << "aeron.cluster.dir.delete.on.start=false\n"
            << "aeron.archive.dir=" << root_dir_ << "/backup-archive\n"
            << "aeron.archive.dir.delete.on.start=false\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=0\n"
            << "aeron.cluster.appointed.leader.id=0\n"
            << "aeron.cluster.members=" << member << "\n"
            << "aeron.cluster.ingress.channel=aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:"
            << restart_port(0) << "\n"
            << "aeron.cluster.consensus.channel=aeron:udp?alias=consensus\n"
            << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
            << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
            << "aeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0\n"
            << "aeron.cluster.leader.heartbeat.timeout=2s\n"
            << "aeron.cluster.startup.canvass.timeout=10s\n"
            << "aeron.cluster.service.count=1\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << archive_port << "\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        out.close();

        auto spawned = ChildProcess::spawn(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", path}, {}, dir + "/node.log");
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        restart_node_ = std::make_unique<ChildProcess>(std::move(*spawned));

        await_restart_container();
    }

    /// The container's start-up ack is offered exactly once and a not-yet
    /// connected control publication is fatal (`three_node_rig.h`'s
    /// `await_container_started`); the CM's recovery widens that window, so the
    /// container is respawned until it prints `STARTED serviceId=`.
    void await_restart_container()
    {
        constexpr int MAX_ATTEMPTS = 20;
        constexpr auto START_TIMEOUT = std::chrono::seconds(15);
        const std::string dir = root_dir_ + "/restart";
        const std::string container_path = dir + "/container.properties";
        const std::string log_path = dir + "/container.log";
        write_container_properties(container_path, dir + "/aeron", root_dir_ + "/backup-cluster");

        for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt)
        {
            if (!restart_node_ || !restart_node_->running())
            {
                return;
            }
            std::error_code error;
            std::filesystem::remove(log_path, error);
            auto container = ChildProcess::spawn(AXC_CONTAINER_BINARY, {container_path},
                                                 tracking_environment(restart_tracking_file()), log_path);
            ASSERT_TRUE(container) << container.error().message.view();
            restart_container_ = std::make_unique<ChildProcess>(std::move(*container));

            const auto deadline = std::chrono::steady_clock::now() + START_TIMEOUT;
            bool started = false;
            while (std::chrono::steady_clock::now() < deadline)
            {
                if (tracking_dump(log_path).find("STARTED serviceId=") != std::string::npos)
                {
                    started = true;
                    break;
                }
                if (!restart_container_->running())
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (started)
            {
                return;
            }
            if (restart_container_->running())
            {
                restart_container_->terminate(std::chrono::seconds(5));
            }
            restart_container_.reset();
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    }

    [[nodiscard]] std::int64_t restart_counter(std::int32_t type_id)
    {
        if (!restart_client_)
        {
            const std::string client_dir = root_dir_ + "/restart/aeron";
            axc::core::ClientConfig client_config{client_dir};
            client_config.on_error = &on_client_error;
            auto client = DeepmsgClient::connect(client_config);
            if (!client)
            {
                return -1;
            }
            restart_client_ = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        (void)restart_client_->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(restart_client_->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(restart_client_->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] bool wait_for_restart_leader(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (1 == restart_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) &&
                2 == restart_counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    struct TrackingFile
    {
        int snapshot_loaded{-1};
        std::vector<std::int32_t> messages;
    };

    [[nodiscard]] TrackingFile read_tracking(const std::string& path) const
    {
        TrackingFile tracking;
        std::ifstream in{path};
        std::string line;
        while (std::getline(in, line))
        {
            if (line.size() < 3 || ' ' != line[1])
            {
                continue;
            }
            const std::int64_t value = std::strtoll(line.c_str() + 2, nullptr, 10);
            if ('M' == line[0])
            {
                tracking.messages.push_back(static_cast<std::int32_t>(value));
            }
            else if ('L' == line[0])
            {
                tracking.snapshot_loaded = static_cast<int>(value);
            }
        }
        return tracking;
    }

    [[nodiscard]] std::string tracking_dump(const std::string& path) const
    {
        std::ifstream in{path};
        return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    }

    [[nodiscard]] bool wait_for_tracking(const std::string& path, const std::vector<std::int32_t>& expected,
                                         std::chrono::milliseconds timeout) const
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (read_tracking(path).messages == expected)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    /// The backup has the complete state: the snapshot retrieved and the live
    /// log replayed to the leader's commit position (the reference's
    /// `awaitBackupLiveLogPosition`, `ClusterBackupTest.java:71`).
    [[nodiscard]] bool wait_for_backup_caught_up(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            const int leader = leader_index();
            if (leader >= 0 &&
                1 <= backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID))
            {
                const std::int64_t commit = counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
                const std::int64_t live =
                    backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID);
                if (commit >= 0 && live >= commit)
                {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
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
    bool tracking_{false};
    std::string heartbeat_timeout_{"2s"};
    std::string backup_progress_timeout_{};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> nodes_{};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> containers_{};
    std::array<std::unique_ptr<DeepmsgClient>, MEMBERS> clients_{};
    std::unique_ptr<ChildProcess> backup_{};
    std::unique_ptr<DeepmsgClient> backup_client_{};
    std::unique_ptr<ChildProcess> restart_node_{};
    std::unique_ptr<ChildProcess> restart_container_{};
    std::unique_ptr<DeepmsgClient> restart_client_{};
    ClusterClientSession restart_session_{};
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

/// C9-2 (`shouldBackupClusterWithSnapshot`, `ClusterBackupTest.java:138-168`):
/// the backup directory is a complete cluster directory. The cluster is
/// stopped and one member starts on the backup's `cluster-backup` and
/// `backup-archive`; it reloads the snapshot and replays the log, so the
/// tracking service's state is the ten messages and the cluster echoes again.
TEST_F(Xc9JavaBackupAgainstOurClusterTest, theNodeStartsFromTheBackupDirectory)
{
    tracking_ = true;
    start_cluster();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "our cluster never became active";

    constexpr int message_count = 10;
    send_ints(message_count);
    take_snapshot();

    start_java_backup();
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(180)))
        << "the Java backup never reached BACKING_UP (state="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) << ")";
    ASSERT_TRUE(wait_for_backup_caught_up(std::chrono::seconds(120)))
        << "the backup never caught up: live="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID)
        << " snapshots=" << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID);

    stop_all();

    start_node_from_backup();
    ASSERT_TRUE(wait_for_restart_leader(std::chrono::seconds(120)))
        << "the node started from the backup never became the active leader (state="
        << restart_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) << ")";

    std::vector<std::int32_t> expected;
    for (int i = 0; i < message_count; ++i)
    {
        expected.push_back(i);
    }
    ASSERT_TRUE(wait_for_tracking(restart_tracking_file(), expected, std::chrono::seconds(60)))
        << "the recovered service state:\n"
        << tracking_dump(restart_tracking_file());
    EXPECT_EQ(1, read_tracking(restart_tracking_file()).snapshot_loaded)
        << "the recovery plan did not load the snapshot:\n"
        << tracking_dump(restart_tracking_file());

    // The recovered cluster serves: a new client message is applied once and echoed.
    const std::string egress = "aeron:udp?endpoint=localhost:" + std::to_string(restart_port(60));
    ASSERT_TRUE(restart_session_.connect_udp(root_dir_ + "/restart/aeron",
                                             "0=localhost:" + std::to_string(restart_port(0)), egress,
                                             std::chrono::seconds(30)))
        << restart_session_.last_error();
    ASSERT_TRUE(restart_session_.offer_and_await(int_payload(message_count), 1, std::chrono::seconds(20)))
        << "the recovered cluster did not echo";
    expected.push_back(message_count);
    EXPECT_TRUE(wait_for_tracking(restart_tracking_file(), expected, std::chrono::seconds(30)))
        << "the recovered service did not apply the new message:\n"
        << tracking_dump(restart_tracking_file());
}

/// C9-2 (`shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure`,
/// `ClusterBackupTest.java:539-580`, and the live follow of
/// `shouldBackupClusterAndJoinLive`, `:612-634`): no snapshot, a non-empty log,
/// messages that arrive after BACKING_UP, and the member the backup was
/// following dies. The backup must reset, re-query, follow the new leader, and
/// the node started from its directory must replay every message.
TEST_F(Xc9JavaBackupAgainstOurClusterTest, theBackupFollowsTheClusterThroughALeaderChange)
{
    tracking_ = true;
    start_cluster();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "our cluster never became active";

    send_ints(10);

    start_java_backup();
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(180)))
        << "state=" << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID);
    ASSERT_TRUE(wait_for_backup_live(std::chrono::seconds(120)))
        << "live=" << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID)
        << " commit=" << leader_commit();

    // Messages after BACKING_UP are recorded by the live log subscription.
    send_ints(10, 5);
    ASSERT_TRUE(wait_for_backup_live(std::chrono::seconds(120)))
        << "the backup did not follow the five messages sent after BACKING_UP";

    // The member the backup was following dies: progress stalls, the backup goes
    // through RESET_BACKUP, cools down and re-queries; a follower answers for the
    // new leader (`ConsensusModuleAgent.java:2454-2458`).
    const int old_leader = leader_index();
    ASSERT_GE(old_leader, 0) << "no leader";
    stop_cluster_member(old_leader);
    ASSERT_TRUE(wait_for_new_leader(old_leader, std::chrono::seconds(60))) << "our cluster never elected a new leader";
    send_ints(15, 5);

    ASSERT_TRUE(wait_for_backup_live(std::chrono::seconds(120)))
        << "the backup did not follow the new leader: live="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID)
        << " commit=" << leader_commit();

    stop_all();

    start_node_from_backup();
    ASSERT_TRUE(wait_for_restart_leader(std::chrono::seconds(120)))
        << "the node started from the backup never became the active leader (state="
        << restart_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) << ")";

    std::vector<std::int32_t> expected;
    for (int i = 0; i < 20; ++i)
    {
        expected.push_back(i);
    }
    ASSERT_TRUE(wait_for_tracking(restart_tracking_file(), expected, std::chrono::seconds(60)))
        << "the recovered service state:\n"
        << tracking_dump(restart_tracking_file());
    EXPECT_EQ(0, read_tracking(restart_tracking_file()).snapshot_loaded)
        << "the snapshotless backup must not load a snapshot:\n"
        << tracking_dump(restart_tracking_file());
}

/// C9-2 (the reset path of `ClusterBackupAgent.hasProgressStalled`): the
/// cluster stops answering while the backup is BACKING_UP (the members are
/// stopped, their directories kept). After the progress timeout the backup must
/// go through `RESET_BACKUP`, cool down and re-query; when the cluster comes
/// back on the same directories it must reach `BACKING_UP` again and follow the
/// log.
TEST_F(Xc9JavaBackupAgainstOurClusterTest, theBackupResetsWhenTheClusterStopsResponding)
{
    tracking_ = true;
    // Short enough that the reset happens inside the test, long enough that the
    // healthy cluster does not trip it.
    backup_progress_timeout_ = "2s";
    start_cluster();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "our cluster never became active";

    send_ints(10);

    start_java_backup();
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(180)))
        << "state=" << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID);
    ASSERT_TRUE(wait_for_backup_live(std::chrono::seconds(120)))
        << "the backup never reached the leader's commit position";

    stop_cluster();

    bool saw_reset = false;
    const auto reset_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (std::chrono::steady_clock::now() < reset_deadline && !saw_reset)
    {
        saw_reset = backup_state_code(ClusterBackupState::reset_backup) ==
                    backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID);
        if (!saw_reset)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    EXPECT_TRUE(saw_reset) << "the backup never went through RESET_BACKUP while the cluster was stopped";

    start_cluster(false);
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(120))) << "the restarted cluster never became active";
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(180)))
        << "the backup never returned to BACKING_UP (state="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) << ")";
    ASSERT_TRUE(wait_for_backup_live(std::chrono::seconds(120))) << "the backup did not follow the restarted cluster";
}

}  // namespace
