#pragma once

// The three-node cluster rig (three `deepmsg-cluster-node` processes with
// embedded driver + archive + CM, three C++ containers, one Java client
// driver) shared by the C6a nine-round test and the C6b follower-path tests.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <signal.h>
#include <unistd.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/recording_log.h"
#include "cluster/consensus/cluster_control.h"
#include "core/deepmsg/deepmsg_client.h"
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
#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT ""
#endif

namespace axc::interop
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ProcessResult;

inline constexpr std::int32_t CLUSTER_ID = 7;
constexpr int NODES = 3;
constexpr int ROUNDS = 9;

[[nodiscard]] inline int rounds_to_run()
{
    if (const char* from_env = std::getenv("AXC_THREE_NODE_ROUNDS"); nullptr != from_env && '\0' != *from_env)
    {
        return std::max(1, std::atoi(from_env));
    }
    return ROUNDS;
}
inline constexpr std::int64_t ECHO_MESSAGES = 100;

[[nodiscard]] inline std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

[[nodiscard]] inline std::string tests_root()
{
    const char* from_env = std::getenv("AXC_AERON_TESTS");
    return nullptr == from_env ? std::string{} : std::string{from_env};
}

[[nodiscard]] inline std::string find_probe(const std::string& root, const std::string& name)
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

[[nodiscard]] inline std::vector<std::string> java_opens()
{
    return {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED"};
}

[[nodiscard]] inline std::expected<ChildProcess, axc::core::Error> spawn_logged(
    const std::string& executable, const std::vector<std::string>& arguments, const std::string& log)
{
    std::string command = "exec " + executable;
    for (const std::string& argument : arguments)
    {
        command.push_back(' ');
        command.append(argument);
    }
    command.append(" > ");
    command.append(log);
    command.append(" 2>&1");
    return ChildProcess::spawn("/bin/sh", {"-c", command});
}

class ThreeNodeRig : public ::testing::Test
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
        if (!std::filesystem::exists("/usr/bin/javac") && !std::filesystem::exists("/usr/local/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
        }
        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }
        const std::string probe = find_probe(root, "ClusterEchoClientProbe.java");
        if (probe.empty())
        {
            GTEST_SKIP() << "ClusterEchoClientProbe.java not found under " << root;
        }
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY) ||
            !std::filesystem::exists(AXC_AERONMD_DEFAULT))
        {
            GTEST_SKIP() << "cluster binaries or aeronmd not found";
        }

        std::string pattern = "/tmp/axc-c6a-three-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        classes_dir_ = root_dir_ + "/classes";
        std::filesystem::create_directories(classes_dir_);

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_, probe};
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void TearDown() override
    {
        stop_round();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            // Logs only: a full copy of the round is ~0.6 GB of recordings.
            const std::string kept = "/tmp/c6a-failure-" + std::to_string(::getpid());
            std::filesystem::remove_all(kept, error);
            std::filesystem::create_directories(kept, error);
            for (int i = 0; i < NODES; ++i)
            {
                const std::string node_dir = round_dir(round_) + "/node" + std::to_string(i);
                for (const char* const name : {"/node.log", "/container.log"})
                {
                    std::filesystem::copy_file(node_dir + name, kept + "/node" + std::to_string(i) + name,
                                               std::filesystem::copy_options::overwrite_existing, error);
                }
            }
            std::filesystem::copy_file(round_dir(round_) + "/client-driver.log", kept + "/client-driver.log",
                                       std::filesystem::copy_options::overwrite_existing, error);
            std::fprintf(stderr, "C6A_LOGS=%s\n", kept.c_str());
        }
        if (!::testing::Test::HasFailure())
        {
            std::filesystem::remove_all(root_dir_, error);
        }
        else
        {
            std::fprintf(stderr, "C6A_ROUND_DIR=%s\n", root_dir_.c_str());
        }
    }

    struct Ports
    {
        int ingress[NODES]{};
        int consensus[NODES]{};
        int log[NODES]{};
        int archive_control[NODES]{};
        int client_egress{0};
    };

    [[nodiscard]] Ports ports_for(int round) const
    {
        const int base = 22000 + ((static_cast<int>(::getpid()) % 200) * 100) + (round * 50);
        Ports ports;
        for (int i = 0; i < NODES; ++i)
        {
            ports.ingress[i] = base + i;
            ports.consensus[i] = base + 10 + i;
            ports.log[i] = base + 20 + i;
            ports.archive_control[i] = base + 30 + i;
        }
        ports.client_egress = base + 40;
        return ports;
    }

    [[nodiscard]] std::string round_dir(int round) const
    {
        return root_dir_ + "/round-" + std::to_string(round);
    }

    [[nodiscard]] std::string members_string(int round) const
    {
        const Ports ports = ports_for(round);
        std::string members;
        for (int i = 0; i < NODES; ++i)
        {
            if (0 != i)
            {
                members.push_back('|');
            }
            members.append(std::to_string(i)).append(",localhost:").append(std::to_string(ports.ingress[i]));
            members.append(",localhost:").append(std::to_string(ports.consensus[i]));
            members.append(",localhost:").append(std::to_string(ports.log[i]));
            // catchup endpoint is resolved from the log subscription; the
            // archive endpoint is what a follower replicates from.
            members.append(",localhost:0,localhost:").append(std::to_string(ports.archive_control[i]));
        }
        return members;
    }

    void write_node_properties(int round)
    {
        const Ports ports = ports_for(round);
        const std::string dir = round_dir(round);
        for (int i = 0; i < NODES; ++i)
        {
            const std::string node_dir = dir + "/node" + std::to_string(i);
            std::filesystem::create_directories(node_dir);
            std::ofstream out{node_dir + "/node.properties"};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << node_dir << "/aeron\n"
                << "aeron.dir.delete.on.start=true\n"
                << "aeron.cluster.dir=" << node_dir << "/cluster\n"
                << "aeron.archive.dir=" << node_dir << "/archive\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.member.id=" << i << "\n"
                << "aeron.cluster.members=" << members_string(round) << "\n"
                << "aeron.cluster.ingress.channel=aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:"
                << ports.ingress[i] << "\n"
                << "aeron.cluster.consensus.channel=aeron:udp?alias=consensus\n"
                << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
                << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
                << "aeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0\n"
                << "aeron.cluster.leader.heartbeat.timeout=2s\n"
                << "aeron.cluster.startup.canvass.timeout=10s\n"
                << "aeron.cluster.service.count=1\n"
                << "aeron.cluster.log.control.enable=true\n"
                << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << ports.archive_control[i] << "\n"
                << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        }
    }

    void write_container_properties(int round)
    {
        const std::string dir = round_dir(round);
        for (int i = 0; i < NODES; ++i)
        {
            const std::string node_dir = dir + "/node" + std::to_string(i);
            std::ofstream out{node_dir + "/container.properties"};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << node_dir << "/aeron\n"
                << "aeron.dir.delete.on.start=true\n"
                << "aeron.cluster.dir=" << node_dir << "/cluster\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.service.id=0\n"
                << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
                << "deepmsg.cluster.service=echo\n";
        }
    }

    void start_client_driver(int round)
    {
        const std::string dir = round_dir(round) + "/client-aeron";
        auto driver = spawn_logged(AXC_AERONMD_DEFAULT, {"-Daeron.dir=" + dir, "-Daeron.dir.delete.on.start=true"},
                                   round_dir(round) + "/client-driver.log");
        ASSERT_TRUE(driver) << driver.error().message.view();
        client_driver_ = std::make_unique<ChildProcess>(std::move(*driver));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{dir});
            if (client)
            {
                client->close();
                client_aeron_dir_ = dir;
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the client media driver never published its CnC file";
    }

    void start_node(int round, int index)
    {
        const std::string node_dir = round_dir(round) + "/node" + std::to_string(index);
        auto node = spawn_logged(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", node_dir + "/node.properties"},
                                 node_dir + "/node.log");
        ASSERT_TRUE(node) << node.error().message.view();
        nodes_[index] = std::make_unique<ChildProcess>(std::move(*node));
        node_clients_[index].reset();
    }

    void start_container(int round, int index)
    {
        const std::string node_dir = round_dir(round) + "/node" + std::to_string(index);
        auto container =
            spawn_logged(AXC_CONTAINER_BINARY, {node_dir + "/container.properties"}, node_dir + "/container.log");
        ASSERT_TRUE(container) << container.error().message.view();
        containers_[index] = std::make_unique<ChildProcess>(std::move(*container));
    }

    void stop_round()
    {
        // Release the counter clients first: terminating a node can take a few
        // seconds and an undriven client trips the driver's service interval.
        for (auto& client : node_clients_)
        {
            client.reset();
        }
        for (auto& container : containers_)
        {
            if (container)
            {
                container->terminate(std::chrono::seconds(5));
                container.reset();
            }
        }
        for (auto& node : nodes_)
        {
            if (node)
            {
                node->terminate(std::chrono::seconds(5));
                node.reset();
            }
        }
        if (client_driver_)
        {
            client_driver_->terminate(std::chrono::seconds(5));
            client_driver_.reset();
        }
    }

    /// SIGTERM the container and the node. A clean stop marks the cluster mark
    /// file terminated, so the node can be started again immediately.
    void stop_node_gracefully(int index)
    {
        if (containers_[index])
        {
            containers_[index]->terminate(std::chrono::seconds(5));
            containers_[index].reset();
        }
        if (nodes_[index])
        {
            nodes_[index]->terminate(std::chrono::seconds(5));
            nodes_[index].reset();
        }
        node_clients_[index].reset();
    }

    /// Rewrites one node's `recording.log` the way the reference's
    /// `StartFromTruncatedRecordingLogTest` does: the last TERM entry, its
    /// commit position, and the latest snapshot of the service and of the
    /// cluster configuration. The mark files go, and so do the recording
    /// segments the new log no longer references.
    void truncate_recording_log(int round, int index)
    {
        namespace common = deepmsg::cluster::common;
        const std::string node_dir = round_dir(round) + "/node" + std::to_string(index);
        const std::string cluster_dir = node_dir + "/cluster";
        const std::string archive_dir = node_dir + "/archive";

        auto old = common::RecordingLog::open(cluster_dir, false);
        ASSERT_TRUE(old) << "node " << index << ": " << old.error().message.view();
        const common::RecordingLog::Entry* last_term = old->find_last_term();
        ASSERT_NE(nullptr, last_term) << "node " << index << " has no TERM entry";
        const common::RecordingLog::Entry term = *last_term;
        const common::RecordingLog::Entry* service_snapshot = old->get_latest_snapshot(0);
        const common::RecordingLog::Entry* config_snapshot = old->get_latest_snapshot(common::RecordingLog::SERVICE_ID);
        ASSERT_NE(nullptr, service_snapshot) << "node " << index << " has no service snapshot";
        ASSERT_NE(nullptr, config_snapshot) << "node " << index << " has no configuration snapshot";
        const common::RecordingLog::Entry service = *service_snapshot;
        const common::RecordingLog::Entry configuration = *config_snapshot;

        const std::string temp_dir = node_dir + "/truncated";
        std::error_code error;
        std::filesystem::remove_all(temp_dir, error);
        std::filesystem::create_directories(temp_dir, error);
        {
            auto fresh = common::RecordingLog::open(temp_dir, true);
            ASSERT_TRUE(fresh) << fresh.error().message.view();
            ASSERT_TRUE(fresh->append_term(term.recording_id, term.leadership_term_id, term.term_base_log_position,
                                           term.timestamp));
            ASSERT_TRUE(fresh->commit_log_position(term.leadership_term_id, term.log_position));
            ASSERT_TRUE(fresh->append_snapshot(service.recording_id, service.leadership_term_id,
                                               service.term_base_log_position, service.log_position, service.timestamp,
                                               0));
            ASSERT_TRUE(fresh->append_snapshot(configuration.recording_id, configuration.leadership_term_id,
                                               configuration.term_base_log_position, configuration.log_position,
                                               configuration.timestamp, common::RecordingLog::SERVICE_ID));
            ASSERT_TRUE(fresh->force(1));
        }
        std::filesystem::copy_file(temp_dir + "/recording.log", cluster_dir + "/recording.log",
                                   std::filesystem::copy_options::overwrite_existing, error);
        ASSERT_FALSE(error) << "node " << index << ": " << error.message();
        std::filesystem::remove(temp_dir, error);

        std::filesystem::remove(archive_dir + "/archive-mark.dat", error);
        std::filesystem::remove(cluster_dir + "/cluster-mark.dat", error);

        auto copied = common::RecordingLog::open(cluster_dir, false);
        ASSERT_TRUE(copied) << copied.error().message.view();
        ASSERT_LE(copied->entries().size(), 3U) << "node " << index << " kept too many entries";
        std::set<std::int64_t> recorded;
        for (const common::RecordingLog::Entry& entry : copied->entries())
        {
            recorded.insert(entry.recording_id);
        }
        for (const auto& item : std::filesystem::directory_iterator(archive_dir))
        {
            const std::string name = item.path().filename().string();
            if (name.size() < 5 || 0 != name.compare(name.size() - 4, 4, ".rec"))
            {
                continue;
            }
            const std::int64_t recording_id = std::stoll(name.substr(0, name.find('-')));
            if (recorded.end() == recorded.find(recording_id))
            {
                std::filesystem::remove(item.path(), error);
            }
        }
    }

    /// SIGKILL a process without the graceful path: the follower must look
    /// crashed, not stopped.
    void kill_hard(std::unique_ptr<ChildProcess>& process)
    {
        if (!process)
        {
            return;
        }
        (void)::kill(process->pid(), SIGKILL);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (process->running() && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        process.reset();
    }

    [[nodiscard]] DeepmsgClient* node_client(int index)
    {
        if (!node_clients_[index])
        {
            const std::string dir = round_dir(round_) + "/node" + std::to_string(index) + "/aeron";
            axc::core::ClientConfig config{dir};
            config.on_error = [](void* clientd, int errcode, const char* message)
            {
                std::fprintf(stderr, "NODECLIENTERR node=%lld code=%d %s\n",
                             static_cast<long long>(reinterpret_cast<std::intptr_t>(clientd)), errcode, message);
            };
            config.on_error_clientd = reinterpret_cast<void*>(static_cast<std::intptr_t>(index));
            auto client = DeepmsgClient::connect(config);
            if (!client)
            {
                return nullptr;
            }
            node_clients_[index] = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        return node_clients_[index].get();
    }

    void pause_for(std::chrono::milliseconds duration)
    {
        const auto deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void drive_node_clients()
    {
        for (auto& client : node_clients_)
        {
            if (client)
            {
                (void)client->do_work();
            }
        }
    }

    [[nodiscard]] std::int64_t node_counter(int index, std::int32_t type_id)
    {
        auto* client = node_client(index);
        if (nullptr == client)
        {
            return -1;
        }
        (void)client->do_work();
        auto* reader = client->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(reader, type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            // The client may have connected to the previous driver's stale CnC
            // (the restarted driver deletes and recreates the directory). Drop
            // it so the next read reconnects.
            node_clients_[index].reset();
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    void set_control_toggle(int index, std::int32_t value)
    {
        auto* client = node_client(index);
        ASSERT_NE(nullptr, client);
        (void)client->do_work();
        auto* reader = client->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            reader, deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    [[nodiscard]] bool wait_for_cluster()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline)
        {
            std::int64_t term = -2;
            int leaders = 0;
            bool all_active = true;
            for (int i = 0; i < NODES; ++i)
            {
                const std::int64_t state =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
                const std::int64_t leadership_term =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
                const std::int64_t role = node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID);
                all_active = all_active && (1 == state);
                if (-2 == term)
                {
                    term = leadership_term;
                }
                if (leadership_term != term)
                {
                    all_active = false;
                }
                leaders += 2 == role ? 1 : 0;
            }
            if (all_active && 1 == leaders && term >= 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] std::string node_dump() const
    {
        std::string dump;
        for (int i = 0; i < NODES; ++i)
        {
            auto* self = const_cast<ThreeNodeRig*>(this);
            dump.append(" node")
                .append(std::to_string(i))
                .append(" alive=")
                .append(nodes_[i] && nodes_[i]->running() ? "1" : "0")
                .append(" state=")
                .append(std::to_string(
                    self->node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)))
                .append(" role=")
                .append(std::to_string(self->node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID)))
                .append(" term=")
                .append(
                    std::to_string(self->node_counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID)))
                .append(" election=")
                .append(std::to_string(self->node_counter(i, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID)))
                .append(" commit=")
                .append(
                    std::to_string(self->node_counter(i, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID)));
        }
        return dump;
    }

    [[nodiscard]] int leader_index()
    {
        for (int i = 0; i < NODES; ++i)
        {
            if (2 == node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return i;
            }
        }
        return -1;
    }

    /// Samples until exactly one of `live` is an ACTIVE leader at a term above
    /// `above_term`; returns its index, or -1 on timeout.
    [[nodiscard]] int await_new_leader(std::int64_t above_term, const std::vector<int>& live,
                                       std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            std::vector<int> leaders;
            for (const int i : live)
            {
                if (2 == node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) &&
                    1 == node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
                {
                    leaders.push_back(i);
                }
            }
            if (1 == leaders.size() &&
                node_counter(leaders[0], deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID) > above_term)
            {
                return leaders[0];
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return -1;
    }

    [[nodiscard]] ProcessResult run_echo(int round, std::int64_t count, std::chrono::milliseconds timeout)
    {
        const Ports ports = ports_for(round);
        std::string endpoints;
        for (int i = 0; i < NODES; ++i)
        {
            if (0 != i)
            {
                endpoints.push_back(',');
            }
            endpoints.append(std::to_string(i)).append("=localhost:").append(std::to_string(ports.ingress[i]));
        }
        const std::string egress = "aeron:udp?endpoint=localhost:" + std::to_string(ports.client_egress);

        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(
            options.arguments.end(),
            {"-cp", jar_ + ":" + classes_dir_, "ClusterEchoClientProbe", client_aeron_dir_, std::to_string(count), "30",
             std::to_string(count), "aeron:udp?term-length=128k", endpoints, egress});
        options.timeout = timeout;

        // The counter clients live in this process and must keep their
        // conductors serviced while the Java probe blocks.
        ProcessResult result;
        std::atomic<bool> done{false};
        std::thread runner(
            [&result, &done, &options]
            {
                auto ran = axc::interop::run_process("java", options);
                if (ran)
                {
                    result = std::move(*ran);
                }
                done.store(true, std::memory_order_release);
            });

        const auto deadline = std::chrono::steady_clock::now() + timeout + std::chrono::seconds(10);
        while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        {
            for (auto& client : node_clients_)
            {
                if (client)
                {
                    (void)client->do_work();
                }
            }
            (void)node_client(0);
            (void)node_client(1);
            (void)node_client(2);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        runner.join();
        return result;
    }

    /// The probe gives a connect a fixed 5 s budget; right after a catchup the
    /// cluster can spend that on a short election, so a bounded retry keeps the
    /// judgement on "the cluster serves again" instead of a connect race.
    [[nodiscard]] ProcessResult run_echo_until_served(int round, std::int64_t count, std::chrono::milliseconds timeout,
                                                      int attempts = 2)
    {
        ProcessResult result = run_echo(round, count, timeout);
        for (int attempt = 1; attempt < attempts; ++attempt)
        {
            if (0 == result.exit_code && std::string::npos != result.output.find("RECEIVED " + std::to_string(count)))
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
            result = run_echo(round, count, timeout);
        }
        return result;
    }

    [[nodiscard]] std::string node_log(int round, int index) const
    {
        std::ifstream in{round_dir(round) + "/node" + std::to_string(index) + "/node.log"};
        if (!in)
        {
            return "";
        }
        return std::string{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    void start_cluster(int round)
    {
        round_ = round;
        stop_round();
        std::error_code error;
        std::filesystem::remove_all(round_dir(round), error);
        start_round(round);
    }

    /// Start (or restart) the processes of a round whose directory exists.
    void start_round(int round)
    {
        write_node_properties(round);
        write_container_properties(round);
        start_client_driver(round);

        // Start each node with its container, staggered: the consensus and log
        // subscriptions must exist before the election starts sending
        // NewLeadershipTerm, or a late follower can miss the only position where
        // the leader publishes it before closing on quorum.
        for (int i = 0; i < NODES; ++i)
        {
            start_node(round, i);
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            start_container(round, i);
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        ASSERT_TRUE(wait_for_cluster()) << "round " << round << " never elected" << node_dump() << node_log(round, 0)
                                        << node_log(round, 1) << node_log(round, 2);
    }

    void run_round(int round)
    {
        start_cluster(round);

        const auto before = run_echo(round, ECHO_MESSAGES, std::chrono::seconds(60));
        ASSERT_EQ(0, before.exit_code) << before.output << node_dump();
        ASSERT_NE(std::string::npos, before.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES)))
            << before.output << node_dump();

        const int leader = leader_index();
        ASSERT_GE(leader, 0);
        const int follower = (leader + 1) % NODES;
        const std::int64_t leader_commit_before =
            node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);

        // Kill the follower's node and container (SIGKILL), keep the leader
        // and the other follower: quorum 2/3 still commits.
        kill_hard(containers_[follower]);
        kill_hard(nodes_[follower]);
        node_clients_[follower].reset();

        const auto during = run_echo(round, ECHO_MESSAGES, std::chrono::seconds(60));
        ASSERT_EQ(0, during.exit_code) << during.output;
        ASSERT_NE(std::string::npos, during.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << during.output;

        const auto commit_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        bool advanced = false;
        while (std::chrono::steady_clock::now() < commit_deadline)
        {
            if (node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID) > leader_commit_before)
            {
                advanced = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(advanced) << "round " << round << ": the leader stopped committing after the follower died";

        EXPECT_EQ(0, node_counter(0, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID))
            << "round " << round << ": node 0 reported an error" << node_log(round, 0);

        // C6a stops here: a restarted follower must rejoin through a leader
        // change (C6b); this card judges that a follower's loss does not stop
        // the leader's commits or the client's echoes.
        stop_round();
    }

    std::string jar_{};
    std::string root_dir_{};
    std::string classes_dir_{};
    std::string client_aeron_dir_{};
    int round_{0};
    std::unique_ptr<ChildProcess> nodes_[NODES]{};
    std::unique_ptr<ChildProcess> containers_[NODES]{};
    std::unique_ptr<DeepmsgClient> node_clients_[NODES]{};
    std::unique_ptr<ChildProcess> client_driver_{};
};

}  // namespace axc::interop
