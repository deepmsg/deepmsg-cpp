// C8: mixed clusters. The reference's consensus module (`ClusteredMediaDriver`)
// and ours have to form one cluster: canvass, ballots, heartbeats, catch-up
// and replication are all CM-to-CM, which C6 never exercised (it had a Java
// client, not a Java member).
//
// S1 is the feasibility probe: two members, one Java and one ours, no services
// (`aeron.cluster.service.count=0`), and the question is whether they elect a
// leader, agree on the term, and commit a client message.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <algorithm>
#include <atomic>
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
#include "cluster/common/recording_log.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/cluster_client_session.h"
#include "fixture/java_process.h"
#include "fixture/process.h"

#ifndef AXC_CLUSTER_NODE_BIN
#define AXC_CLUSTER_NODE_BIN ""
#endif
#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ClusterClientSession;

constexpr std::int32_t CLUSTER_ID = 8;

/// The C client's default error handler prints and calls `exit(EXIT_FAILURE)`
/// (`aeron_context.c`), so one counter client left unserviced for ten seconds
/// during a restart would kill the test binary without a gtest summary. The
/// rig's counters are read lazily and reconnect after a force-close, so a
/// starved conductor must not be fatal.
void on_client_error(void* /*clientd*/, int /*errcode*/, const char* /*message*/) {}

enum class Stack
{
    java,
    cpp
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

class MixedRig : public ::testing::Test
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
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN))
        {
            GTEST_SKIP() << "cluster node not found";
        }
        // The Java container is the fork's probe; like every Java-side test in
        // this suite, a missing checkout is a SKIP, not a half-run cluster.
        const std::string tests = tests_root();
        if (tests.empty() || find_probe(tests, "ClusterServiceContainerProbe.java").empty() ||
            find_probe(tests, "ClusterEchoTimerService.java").empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's Java container probe is required";
        }

        std::string pattern = "/tmp/axc-c8-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        // The port block stays below the ephemeral range (main's #65 rule) and
        // below the three-node rig's 12000 block: 4000..7100 cannot collide
        // with it whatever the pids are. Tests in one process share the pid,
        // so each takes the next 50-port slot.
        static std::atomic<int> port_round{0};
        base_port_ = 4000 + ((static_cast<int>(::getpid()) % 32) * 100) + (port_round.fetch_add(1) * 50);
    }

    void TearDown() override
    {
        stop_members();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "C8_ROUND_DIR=%s\n", root_dir_.c_str());
        }
        else
        {
            std::filesystem::remove_all(root_dir_, error);
        }
    }

    [[nodiscard]] std::string members_string(int members) const
    {
        std::string members_list;
        for (int i = 0; i < members; ++i)
        {
            if (0 != i)
            {
                members_list.push_back('|');
            }
            members_list.append(std::to_string(i))
                .append(",localhost:")
                .append(std::to_string(base_port_ + i))
                .append(",localhost:")
                .append(std::to_string(base_port_ + 10 + i))
                .append(",localhost:")
                .append(std::to_string(base_port_ + 20 + i))
                .append(",localhost:0,localhost:")
                .append(std::to_string(base_port_ + 30 + i));
        }
        return members_list;
    }

    [[nodiscard]] std::vector<std::pair<std::string, std::string>> member_properties(int index, int members,
                                                                                     bool fresh) const
    {
        const std::string node_dir = root_dir_ + "/node" + std::to_string(index);
        const std::string wipe = fresh ? "true" : "false";
        std::vector<std::pair<std::string, std::string>> properties{
            {"aeron.dir", node_dir + "/aeron"},
            {"aeron.dir.delete.on.start", wipe},
            {"aeron.cluster.dir", node_dir + "/cluster"},
            {"aeron.cluster.dir.delete.on.start", wipe},
            {"aeron.archive.dir", node_dir + "/archive"},
            {"aeron.archive.dir.delete.on.start", wipe},
            {"aeron.cluster.id", std::to_string(CLUSTER_ID)},
            {"aeron.cluster.member.id", std::to_string(index)},
            {"aeron.cluster.members", members_string(members)},
            {"aeron.cluster.ingress.channel",
             "aeron:udp?term-length=128k|alias=ingress|endpoint=localhost:" + std::to_string(base_port_ + index)},
            {"aeron.cluster.consensus.channel", "aeron:udp?alias=consensus"},
            {"aeron.cluster.log.channel", "aeron:udp?term-length=512k|alias=raft"},
            {"aeron.cluster.control.channel", "aeron:ipc?term-length=128k"},
            {"aeron.cluster.replication.channel", "aeron:udp?endpoint=localhost:0"},
            {"aeron.archive.control.channel",
             "aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 30 + index)},
            {"aeron.archive.replication.channel", "aeron:udp?endpoint=localhost:0"},
            {"aeron.cluster.leader.heartbeat.timeout", "2s"},
            {"aeron.cluster.startup.canvass.timeout", "10s"},
            {"aeron.cluster.service.count", "1"}};
        if (appointed_leader_ >= 0)
        {
            properties.emplace_back("aeron.cluster.appointed.leader.id", std::to_string(appointed_leader_));
        }
        return properties;
    }

    void start_member(int index, int members, Stack stack, bool fresh = true)
    {
        const std::string node_dir = root_dir_ + "/node" + std::to_string(index);
        std::filesystem::create_directories(node_dir);
        const auto properties = member_properties(index, members, fresh);

        if (Stack::java == stack)
        {
            std::vector<std::string> arguments = java_opens();
            for (const auto& [name, value] : properties)
            {
                arguments.push_back("-D" + name + "=" + value);
            }
            arguments.insert(arguments.end(), {"-Daeron.threading.mode=SHARED", "-Daeron.print.configuration=true",
                                               "-cp", jar_, "io.aeron.cluster.ClusteredMediaDriver"});
            auto spawned = ChildProcess::spawn("/usr/bin/java", arguments, {}, node_dir + "/node.log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            members_[index] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
        else
        {
            std::ofstream out{node_dir + "/node.properties"};
            ASSERT_TRUE(out.good());
            for (const auto& [name, value] : properties)
            {
                out << name << "=" << value << "\n";
            }
            out.close();
            auto spawned = ChildProcess::spawn(
                AXC_CLUSTER_NODE_BIN, {"--embedded-driver", node_dir + "/node.properties"}, {}, node_dir + "/node.log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            members_[index] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
    }

    [[nodiscard]] std::string tests_root() const
    {
        const char* from_env = std::getenv("AXC_AERON_TESTS");
        return nullptr == from_env ? std::string{} : std::string{from_env};
    }

    [[nodiscard]] std::string find_probe(const std::string& root, const std::string& name) const
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

    /// The Java container is the fork's `ClusterServiceContainerProbe` around
    /// the reference `ClusteredServiceContainer`; it needs the two probe
    /// sources compiled once.
    void compile_java_probes()
    {
        if (java_probes_compiled_)
        {
            return;
        }
        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }
        classes_dir_ = root_dir_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        std::vector<std::string> sources;
        for (const char* const name : {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java"})
        {
            const std::string path = find_probe(root, name);
            ASSERT_FALSE(path.empty()) << "probe source not found under " << root << ": " << name;
            sources.push_back(path);
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

    /// Containers can exit early while the consensus module is still opening
    /// its control publication; retry like the single-node fixture.
    void start_container(int index, Stack stack)
    {
        for (int attempt = 0; attempt < 20; ++attempt)
        {
            start_container_once(index, stack);
            // The member leaves INIT only after its container's first ack. A
            // container that dies at that ack (or whose ack is lost) is
            // respawned; otherwise the CM's recovery waits for ever. The
            // counter clients are driven because the window can outlast their
            // ten-second service interval.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            bool acked = false;
            while (std::chrono::steady_clock::now() < deadline)
            {
                drive_counter_clients(static_cast<int>(stacks_.size()));
                if (0 < counter(index, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
                {
                    acked = true;
                    break;
                }
                if (!containers_[index] || !containers_[index]->running())
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (acked)
            {
                for (auto& client : clients_)
                {
                    client.reset();
                }
                return;
            }
            containers_[index].reset();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        FAIL() << "container " << index << " never got its first ack: " << member_dump(4);
    }

    void start_container_once(int index, Stack stack)
    {
        const std::string node_dir = root_dir_ + "/node" + std::to_string(index);
        const std::string aeron_dir = node_dir + "/aeron";
        const std::string cluster_dir = node_dir + "/cluster";

        if (Stack::java == stack)
        {
            compile_java_probes();
            std::vector<std::string> arguments = java_opens();
            arguments.insert(arguments.end(), {"-cp", jar_ + ":" + classes_dir_, "ClusterServiceContainerProbe",
                                               aeron_dir, cluster_dir, std::to_string(CLUSTER_ID), "0",
                                               "ClusterEchoTimerService", "true", "aeron:ipc?term-length=128k"});
            auto spawned = ChildProcess::spawn("/usr/bin/java", arguments);
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            containers_[index] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
        else
        {
            std::ofstream out{node_dir + "/container.properties"};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << aeron_dir << "\n"
                << "aeron.dir.delete.on.start=false\n"
                << "aeron.cluster.dir=" << cluster_dir << "\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.service.id=0\n"
                << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
                << "deepmsg.cluster.service=echo\n";
            out.close();
            auto spawned = ChildProcess::spawn(AXC_CONTAINER_BINARY, {node_dir + "/container.properties"});
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            containers_[index] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
    }

    void stop_members()
    {
        for (auto& client : clients_)
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
        for (auto& member : members_)
        {
            if (member)
            {
                member->terminate(std::chrono::seconds(5));
                member.reset();
            }
        }
    }

    [[nodiscard]] DeepmsgClient* client_for(int index)
    {
        if (!clients_[index])
        {
            const std::string dir = root_dir_ + "/node" + std::to_string(index) + "/aeron";
            axc::core::ClientConfig config{dir};
            config.on_error = &on_client_error;
            auto client = DeepmsgClient::connect(config);
            if (!client)
            {
                return nullptr;
            }
            clients_[index] = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        return clients_[index].get();
    }

    [[nodiscard]] std::int64_t counter(int index, std::int32_t type_id)
    {
        auto* client = client_for(index);
        if (nullptr == client)
        {
            return -1;
        }
        (void)client->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(client->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            // A force-closed conductor (its ten-second window) leaves a dead
            // client behind; drop it so the next read reconnects.
            clients_[index].reset();
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] std::string member_dump(int members)
    {
        std::string dump;
        for (int i = 0; i < members; ++i)
        {
            dump.append(" m")
                .append(std::to_string(i))
                .append(" alive=")
                .append(members_[i] && members_[i]->running() ? "1" : "0")
                .append(" state=")
                .append(std::to_string(counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)))
                .append(" role=")
                .append(std::to_string(counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID)))
                .append(" term=")
                .append(std::to_string(counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID)));
        }
        return dump;
    }

    [[nodiscard]] std::string node_log(int index) const
    {
        std::ifstream in{root_dir_ + "/node" + std::to_string(index) + "/node.log"};
        if (!in)
        {
            return "";
        }
        return std::string{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    [[nodiscard]] bool wait_for_leader(int members, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            std::int64_t term = -2;
            int leaders = 0;
            bool all_active = true;
            for (int i = 0; i < members; ++i)
            {
                const std::int64_t state = counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
                const std::int64_t leadership_term =
                    counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
                const std::int64_t role = counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID);
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

    [[nodiscard]] std::string ingress_endpoints(int members) const
    {
        std::string endpoints;
        for (int i = 0; i < members; ++i)
        {
            if (0 != i)
            {
                endpoints.push_back(',');
            }
            endpoints.append(std::to_string(i)).append("=localhost:").append(std::to_string(base_port_ + i));
        }
        return endpoints;
    }

    std::string jar_{};
    std::string root_dir_{};
    int base_port_{0};
    std::unique_ptr<ChildProcess> members_[5]{};
    std::unique_ptr<ChildProcess> containers_[5]{};
    std::string classes_dir_{};
    bool java_probes_compiled_{false};
    std::vector<Stack> stacks_{};
    int appointed_leader_{-1};
    std::unique_ptr<DeepmsgClient> clients_[5]{};
    /// Starts every member (node + container), waits for a leader, then
    /// takes the session through `rounds` batches: send, await the echoes.
    void start_cluster(const std::vector<Stack>& stacks)
    {
        stacks_ = stacks;
        const auto members = static_cast<int>(stacks.size());
        for (int i = 0; i < members; ++i)
        {
            start_member(i, members, stacks[static_cast<std::size_t>(i)]);
            // The consensus and log subscriptions must exist before the
            // election sends NewLeadershipTerm (`ThreeNodeRig` staggers too).
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        for (int i = 0; i < members; ++i)
        {
            start_container(i, stacks[static_cast<std::size_t>(i)]);
        }
        ASSERT_TRUE(wait_for_leader(members, std::chrono::seconds(90)))
            << member_dump(members) << node_log(0) << node_log(1) << node_log(2);
    }

    [[nodiscard]] int leader_of(int members)
    {
        for (int i = 0; i < members; ++i)
        {
            if (2 == counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
            {
                return i;
            }
        }
        return -1;
    }

    [[nodiscard]] std::int64_t term_of(int index)
    {
        return counter(index, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    }

    [[nodiscard]] bool await_leader_above(int members, std::int64_t term, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            int leaders = 0;
            bool agreed = true;
            std::int64_t seen = -2;
            for (int i = 0; i < members; ++i)
            {
                if (nullptr == members_[i])
                {
                    continue;
                }
                const std::int64_t leadership_term = term_of(i);
                if (-2 == seen)
                {
                    seen = leadership_term;
                }
                agreed = agreed && leadership_term == seen;
                leaders += 2 == counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) ? 1 : 0;
            }
            if (1 == leaders && agreed && seen > term)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] Stack stack_of(int index) const
    {
        return stacks_[static_cast<std::size_t>(index)];
    }

    void kill_member(int index)
    {
        if (containers_[index])
        {
            containers_[index]->terminate(std::chrono::seconds(5));
            containers_[index].reset();
        }
        if (members_[index])
        {
            members_[index]->terminate(std::chrono::seconds(5));
            members_[index].reset();
        }
        // The termination waits block this thread, so the counter clients
        // starve and their conductors force-close; every client is reconnected
        // lazily on the next read.
        for (auto& client : clients_)
        {
            client.reset();
        }
    }

    /// Stops `index` (if alive) and brings it back with `stack`, then waits for
    /// it to be active again.
    void restart_with(int index, Stack stack, int members)
    {
        kill_member(index);
        // The restart keeps the member's directory: a wiped one would make it
        // a brand new member instead of the one that comes back.
        start_member(index, members, stack, false);
        start_container(index, stack);
        stacks_[static_cast<std::size_t>(index)] = stack;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (1 == counter(index, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) &&
                counter(index, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) >= 0)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "member " << index << " never came back: " << member_dump(members) << node_log(index);
    }

    /// Keeps the counter clients alive: an aeron client whose conductor is
    /// not serviced for ten seconds is force-closed, and the waits below are
    /// longer than that.
    void drive_counter_clients(int members)
    {
        for (int i = 0; i < members; ++i)
        {
            if (nullptr == members_[i])
            {
                continue;
            }
            if (auto* client = client_for(i); nullptr != client)
            {
                (void)client->do_work();
            }
        }
    }

    /// A client against the current membership: `count` messages in order,
    /// each waiting for its echo. The counter clients are driven while the
    /// echo is outstanding.
    [[nodiscard]] bool echo_round(int members, int first_id, int count, int echo_base)
    {
        ClusterClientSession session;
        const int leader = leader_of(members);
        const std::string driver_dir = root_dir_ + "/node" + std::to_string(leader < 0 ? 0 : leader) + "/aeron";
        if (!session.connect_udp(driver_dir, ingress_endpoints(members),
                                 "aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 40),
                                 std::chrono::seconds(30)))
        {
            std::fprintf(stderr, "C8 connect failed: %s %s\n", session.last_error().c_str(),
                         member_dump(members).c_str());
            return false;
        }
        for (int i = 0; i < count; ++i)
        {
            std::string payload(32, 'e');
            const std::int32_t id = first_id + i;
            std::memcpy(payload.data(), &id, sizeof(id));
            if (!session.offer_only(payload, std::chrono::seconds(10)))
            {
                std::fprintf(stderr, "C8 offer failed at id=%d: %s\n", id, member_dump(members).c_str());
                return false;
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
            while (session.messages().size() < static_cast<std::size_t>(echo_base + i + 1) &&
                   std::chrono::steady_clock::now() < deadline)
            {
                (void)session.pump_until([] { return false; }, std::chrono::milliseconds(1));
                drive_counter_clients(members);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (session.messages().size() < static_cast<std::size_t>(echo_base + i + 1))
            {
                std::fprintf(stderr, "C8 echo failed at id=%d: %s\n", id, member_dump(members).c_str());
                return false;
            }
        }
        session.close();
        return true;
    }
    void set_control_toggle(int index, std::int32_t value)
    {
        auto* client = client_for(index);
        ASSERT_NE(nullptr, client);
        (void)client->do_work();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            client->counters_reader(), deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    /// Reads a recording's first segment file (`<id>-0.rec`) as bytes, at most
    /// `window` of them (the segment is a large zero-filled file; the entries
    /// live at its front).
    [[nodiscard]] std::string read_recording_file(int index, std::int64_t recording_id,
                                                  std::size_t window = 1U << 20U) const
    {
        const std::string path =
            root_dir_ + "/node" + std::to_string(index) + "/archive/" + std::to_string(recording_id) + "-0.rec";
        std::ifstream in{path, std::ios::binary};
        if (!in)
        {
            return {};
        }
        std::string bytes(window, '\0');
        in.read(bytes.data(), static_cast<std::streamsize>(window));
        bytes.resize(static_cast<std::size_t>(in.gcount()));
        return bytes;
    }

    [[nodiscard]] std::int64_t recording_file_size(int index, std::int64_t recording_id) const
    {
        const std::string path =
            root_dir_ + "/node" + std::to_string(index) + "/archive/" + std::to_string(recording_id) + "-0.rec";
        std::error_code error;
        const auto size = std::filesystem::file_size(path, error);
        return error ? -1 : static_cast<std::int64_t>(size);
    }

    /// The payloads of the frames in a recording (`32`-byte data header each;
    /// a PAD frame, header type 0x00, marks the end of the written region).
    [[nodiscard]] static std::vector<std::string> recording_payloads(const std::string& bytes)
    {
        std::vector<std::string> payloads;
        std::size_t offset = 0;
        constexpr std::size_t HEADER_LENGTH = 32;
        while (offset + HEADER_LENGTH <= bytes.size())
        {
            std::int32_t frame_length = 0;
            std::memcpy(&frame_length, bytes.data() + offset, sizeof(frame_length));
            if (frame_length <= 0 || offset + static_cast<std::size_t>(frame_length) > bytes.size())
            {
                break;
            }
            const auto type = static_cast<std::uint8_t>(bytes[offset + 6]);
            if (0x00 != type)
            {
                payloads.emplace_back(bytes, offset + HEADER_LENGTH,
                                      static_cast<std::size_t>(frame_length) - HEADER_LENGTH);
            }
            offset += static_cast<std::size_t>(frame_length);
        }
        return payloads;
    }

    /// `-1` when equal, else the offset of the first differing byte.
    [[nodiscard]] static std::int64_t first_difference(const std::string& a, const std::string& b)
    {
        const std::size_t common = std::min(a.size(), b.size());
        for (std::size_t i = 0; i < common; ++i)
        {
            if (a[i] != b[i])
            {
                return static_cast<std::int64_t>(i);
            }
        }
        return a.size() == b.size() ? -1 : static_cast<std::int64_t>(common);
    }
};

TEST_F(MixedRig, aJavaMemberAndOurMemberServeAClient)
{
    // S3 probe, composition {java, cpp}: whoever leads, a client must finish
    // `AsyncConnect` and ten messages must echo.
    start_cluster({Stack::java, Stack::cpp});
    const int leader = leader_of(2);
    ASSERT_GE(leader, 0) << member_dump(2);
    std::fprintf(stderr, "C8 leader=%d stack=%s %s\n", leader, Stack::java == stack_of(leader) ? "java" : "cpp",
                 member_dump(2).c_str());
    ASSERT_TRUE(echo_round(2, 1, 10, 0)) << member_dump(2);
    stop_members();
}

TEST_F(MixedRig, ourMemberAndAJavaMemberServeAClient)
{
    // S3 probe, composition {cpp, java}.
    start_cluster({Stack::cpp, Stack::java});
    const int leader = leader_of(2);
    ASSERT_GE(leader, 0) << member_dump(2);
    std::fprintf(stderr, "C8 leader=%d stack=%s %s\n", leader, Stack::java == stack_of(leader) ? "java" : "cpp",
                 member_dump(2).c_str());
    ASSERT_TRUE(echo_round(2, 1, 10, 0)) << member_dump(2);
    stop_members();
}

TEST_F(MixedRig, ourAppointedLeaderServesAClientInAMixedCluster)
{
    // S3 attribution: appoint our member as the leader of a mixed cluster and
    // run the same client. If this hangs where the Java leader served, the
    // break is the client path against our leader when a Java member is in
    // the cluster.
    appointed_leader_ = 0;
    start_cluster({Stack::cpp, Stack::java});
    ASSERT_EQ(0, leader_of(2)) << member_dump(2);
    std::fprintf(stderr, "C8 appointed leader=0 %s\n", member_dump(2).c_str());
    ASSERT_TRUE(echo_round(2, 1, 10, 0)) << member_dump(2);
    stop_members();
}

TEST_F(MixedRig, ourAppointedLeaderServesAClientWithThreeMembers)
{
    // S3 attribution, three members: our member appointed as leader.
    appointed_leader_ = 0;
    start_cluster({Stack::cpp, Stack::java, Stack::cpp});
    ASSERT_EQ(0, leader_of(3)) << member_dump(3);
    std::fprintf(stderr, "C8 appointed leader=0 %s\n", member_dump(3).c_str());
    ASSERT_TRUE(echo_round(3, 1, 10, 0)) << member_dump(3);
    stop_members();
}

TEST_F(MixedRig, aJavaDirectoryRestartsUnderOurNode)
{
    // XC-5's core: the same member directory, first a Java member, then ours.
    // The archive it opens knows the Java-written recording (the 2026-11-18
    // "unknown recording id: 0" note is stale); what made this case flaky was
    // the container's first ack racing its publication, which left the CM in
    // its recovery wait (`start_container` now waits for the member to leave
    // INIT).
    start_cluster({Stack::java, Stack::cpp});
    ASSERT_GE(leader_of(2), 0) << member_dump(2);
    kill_member(0);
    start_member(0, 2, Stack::cpp, false);
    start_container(0, Stack::cpp);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (1 == counter(0, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(1, counter(0, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
        << "our node could not take over the Java member's directory: " << member_dump(2);
    stop_members();
}

TEST_F(MixedRig, theMembersLogRecordingsAreByteIdentical)
{
    // XC-3's byte criterion, log side: in one mixed cluster every member's log
    // recording is the same bytes (the follower's recording is the leader's
    // frames), so a format difference between the stacks would show here.
    start_cluster({Stack::java, Stack::cpp, Stack::java});
    ASSERT_TRUE(echo_round(3, 1, 10, 0)) << member_dump(3);
    stop_members();

    const std::string reference = read_recording_file(0, 0);
    ASSERT_FALSE(reference.empty()) << "node 0 has no log recording";
    for (int i = 1; i < 3; ++i)
    {
        EXPECT_EQ(recording_file_size(0, 0), recording_file_size(i, 0)) << "node " << i << " segment size";
        const std::int64_t offset = first_difference(reference, read_recording_file(i, 0));
        EXPECT_EQ(-1, offset) << "node " << i << " log recording differs from node 0's at offset " << offset;
    }
}

// XC-3's byte criterion, snapshot side, as far as it can hold: the frame
// payloads match between members except for fields that are *member-local* in
// the CM snapshot (the log/service session ids), which the reference differs
// on too — the reference CM does not replicate snapshots either. So cross-stack
// CM-snapshot byte equality is not a criterion; the log recording equality
// above is. Kept as the witness of the measurement, disabled.
TEST_F(MixedRig, DISABLED_theMembersConsensusModuleSnapshotsCarryTheSameBytes)
{
    start_cluster({Stack::java, Stack::cpp, Stack::java});
    ASSERT_TRUE(echo_round(3, 1, 10, 0)) << member_dump(3);

    const int leader = leader_of(3);
    ASSERT_GE(leader, 0) << member_dump(3);
    const auto snapshots_before = counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(leader, 4);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline &&
           counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) <= snapshots_before)
    {
        drive_counter_clients(3);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GT(counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), snapshots_before)
        << "the snapshot never completed: " << member_dump(3);
    stop_members();

    // The CM snapshot's recording id comes from each member's own
    // `recording.log`; the ids are per-archive, so they are read, not assumed.
    std::vector<std::int64_t> ids;
    for (int i = 0; i < 3; ++i)
    {
        auto log =
            deepmsg::cluster::common::RecordingLog::open(root_dir_ + "/node" + std::to_string(i) + "/cluster", false);
        ASSERT_TRUE(log) << log.error().message.view();
        const auto* snapshot = log->get_latest_snapshot(deepmsg::cluster::common::RecordingLog::SERVICE_ID);
        ASSERT_NE(nullptr, snapshot) << "node " << i << " has no CM snapshot entry";
        ids.push_back(snapshot->recording_id);
    }

    const std::vector<std::string> reference = recording_payloads(read_recording_file(0, ids[0]));
    ASSERT_FALSE(reference.empty()) << "node 0's CM snapshot has no frames";
    for (int i = 1; i < 3; ++i)
    {
        const std::vector<std::string> payloads =
            recording_payloads(read_recording_file(i, ids[static_cast<std::size_t>(i)]));
        ASSERT_EQ(reference.size(), payloads.size()) << "node " << i << " CM snapshot frame count";
        for (std::size_t frame = 0; frame < reference.size(); ++frame)
        {
            EXPECT_EQ(reference[frame], payloads[frame]) << "node " << i << " CM snapshot frame " << frame;
        }
    }
}

// S9 kept this direction as a witness: the Java member never reached ACTIVE
// on our leader's directory. The XC-0 reference restart showed the same
// family's real cause — a restart that wipes the directory it should recover
// from — so this arm now restarts on the kept directory (`fresh=false`). The
// leader is not appointed: `Election.canvass` refuses to nominate anyone but
// an appointed leader (`Election.java:715`), so a pinned member that dies
// leaves the survivors unable to elect at all. The natural election may pick
// the Java member first; the test turns the leadership over to one of ours
// before the measured step instead of asserting the luck of the election.
TEST_F(MixedRig, ourLeadersDirectoryRestartsUnderAJavaMember)
{
    // The other direction: our member leads, goes, and the Java member takes
    // its directory over.
    start_cluster({Stack::cpp, Stack::java, Stack::cpp});
    ASSERT_TRUE(echo_round(3, 1, 10, 0)) << member_dump(3);
    int leader = leader_of(3);
    ASSERT_GE(leader, 0) << member_dump(3);
    if (Stack::java == stack_of(leader))
    {
        // The natural election may pick the Java member first. Turn the
        // leadership over before the measured step: killing it leaves two of
        // ours, which elect one of themselves; the Java member then rejoins
        // on its own directory.
        const std::int64_t java_term = term_of(leader);
        kill_member(leader);
        ASSERT_TRUE(await_leader_above(3, java_term, std::chrono::seconds(90))) << member_dump(3);
        restart_with(leader, Stack::java, 3);
        leader = leader_of(3);
        ASSERT_GE(leader, 0) << member_dump(3);
    }
    ASSERT_EQ(Stack::cpp, stack_of(leader)) << "expected one of our members to lead: " << member_dump(3);
    const std::int64_t term = term_of(leader);
    kill_member(leader);
    ASSERT_TRUE(await_leader_above(3, term, std::chrono::seconds(90))) << member_dump(3);

    start_member(leader, 3, Stack::java, false);
    start_container(leader, Stack::java);
    stacks_[static_cast<std::size_t>(leader)] = Stack::java;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (1 == counter(leader, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(1, counter(leader, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID))
        << "the Java member could not take over our leader's directory: " << member_dump(3) << node_log(leader);
    ASSERT_TRUE(echo_round(3, 21, 10, 0)) << member_dump(3);
    stop_members();
}

TEST_F(MixedRig, threeMembersFormACluster)
{
    // Probe only: the three-member mixed cluster elects.
    start_cluster({Stack::cpp, Stack::java, Stack::cpp});
    EXPECT_GE(leader_of(3), 0) << member_dump(3);
    stop_members();
}

// XC-3's turn-taking arm: whoever leads is killed, the other stack must take
// over, the killed member rejoins, and the cluster echoes through every step.
// The restart keeps the member's directory (`fresh=false`). The counter
// clients are non-fatal and reconnect lazily (see `on_client_error`), so the
// ten-second gaps of a restart no longer kill the test binary.
TEST_F(MixedRig, membersOfBothStacksLeadATurn)
{
    // XC-3's first arm: a three-member cluster of both stacks; whoever leads
    // is killed, the other stack must take over, the killed member rejoins
    // with the other stack, and the cluster echoes through every step.
    start_cluster({Stack::cpp, Stack::java, Stack::cpp});
    ASSERT_TRUE(echo_round(3, 1, 10, 0)) << member_dump(3);

    std::set<int> led_stacks;  // java = 1, cpp = 0
    led_stacks.insert(stack_of(leader_of(3)) == Stack::java ? 1 : 0);
    std::int64_t term = term_of(leader_of(3));
    int echo_base = 10;
    for (int round = 0; round < 4 && led_stacks.size() < 2; ++round)
    {
        const int leader = leader_of(3);
        ASSERT_GE(leader, 0) << member_dump(3);
        std::fprintf(stderr, "C8 turn round %d: killing %d (%s) ", round, leader,
                     Stack::java == stack_of(leader) ? "java" : "cpp");
        kill_member(leader);
        std::fprintf(stderr, "killed\n");
        ASSERT_TRUE(await_leader_above(3, term, std::chrono::seconds(90))) << member_dump(3);
        term = term_of(leader_of(3));
        std::fprintf(stderr, "C8 turn round %d: new leader %d term=%lld\n", round, leader_of(3),
                     static_cast<long long>(term));
        led_stacks.insert(stack_of(leader_of(3)) == Stack::java ? 1 : 0);
        ASSERT_TRUE(echo_round(3, 1 + echo_base, 10, 0)) << member_dump(3);
        std::fprintf(stderr, "C8 turn round %d: echoed\n", round);
        echo_base += 10;
        // Bring the killed member back with its own stack so the quorum holds
        // for the next round; the *other* stack on the same directory is
        // XC-5's case (`aJavaDirectoryRestartsUnderOurNode`).
        restart_with(leader, stack_of(leader), 3);
        std::fprintf(stderr, "C8 turn round %d: %d back\n", round, leader);
    }
    EXPECT_EQ(2U, led_stacks.size()) << "only one stack led: " << member_dump(3);

    stop_members();
}

TEST_F(MixedRig, aJavaMemberAndOurMemberFormACluster)
{
    // The probe: two members, one per stack, no services. Pass when they agree
    // on one leader and one term, and a client's message reaches the commit
    // position — the canvass, ballot and heartbeat paths are CM-to-CM.
    start_member(0, 2, Stack::java);
    start_member(1, 2, Stack::cpp);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    start_container(0, Stack::java);
    start_container(1, Stack::cpp);

    ASSERT_TRUE(wait_for_leader(2, std::chrono::seconds(60))) << member_dump(2) << node_log(0) << node_log(1);

    // A client on either driver connects and its message commits.
    const int leader = 2 == counter(0, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) ? 0 : 1;
    const std::int64_t commit_before = counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    ASSERT_GE(commit_before, 0) << member_dump(2);

    const std::string egress = "aeron:udp?endpoint=localhost:" + std::to_string(base_port_ + 40);
    const std::string driver_dir = root_dir_ + "/node" + std::to_string(leader) + "/aeron";
    ClusterClientSession session;
    ASSERT_TRUE(session.connect_udp(driver_dir, ingress_endpoints(2), egress, std::chrono::seconds(30)))
        << session.last_error() << member_dump(2);
    std::string payload(64, 'm');
    ASSERT_TRUE(session.offer_only(payload, std::chrono::seconds(20))) << member_dump(2);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    bool advanced = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID) > commit_before)
        {
            advanced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(advanced) << "the mixed cluster never committed a client message: " << member_dump(2) << node_log(0)
                          << node_log(1);

    session.close();
    stop_members();
}

}  // namespace
