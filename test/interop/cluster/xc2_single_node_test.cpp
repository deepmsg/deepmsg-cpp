// C4f: XC-2 on a single node -- a Java client against the all-C++ cluster
// (`deepmsg-cluster-node --embedded-driver` + the C++ service container).
//
// The reference's 179-case system table is C6c; this is the single-node smoke
// table agreed in `plan-c4f.md` §3.2: echo, timers, admin snapshot,
// snapshot/restart, suspend/resume and orderly shutdown.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/recording_log.h"
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

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ProcessResult;

constexpr std::int32_t CLUSTER_ID = 7;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t SERVICE_COUNT = 1;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";

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

[[nodiscard]] std::vector<std::string> java_opens()
{
    return {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED"};
}

/// Run a child with its stdout/stderr in `log`; the child is exec'd by a shell
/// so `ChildProcess::terminate` still signals the real process.
[[nodiscard]] std::expected<ChildProcess, axc::core::Error> spawn_logged(const std::string& executable,
                                                                         const std::vector<std::string>& arguments,
                                                                         const std::string& log)
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

class Xc2SingleNodeTest : public ::testing::Test
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
        for (const char* const name : {"ClusterEchoClientProbe.java", "ClusterTimerClientProbe.java",
                                       "ClusterAdminRequestProbe.java", "ClusterSessionEventProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << "probe source not found under " << root << ": " << name;
            }
            probe_sources_.push_back(path);
        }
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            GTEST_SKIP() << "cluster binaries not found";
        }

        std::string pattern = "/tmp/axc-c4f-xc2-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        classes_dir_ = directory_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        log_port_ = 24500 + (static_cast<int>(::getpid()) % 500);

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        for (const std::string& source : probe_sources_)
        {
            compile.arguments.push_back(source);
        }
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void TearDown() override
    {
        stop();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void write_properties(const std::string& supplier = {})
    {
        const std::string node = directory_ + "/node.properties";
        std::ofstream out{node};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir_ << "\n"
            << "aeron.cluster.dir=" << cluster_dir_ << "\n"
            << "aeron.archive.dir=" << archive_dir_ << "\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=" << MEMBER_ID << "\n"
            << "aeron.cluster.members=" << MEMBERS << "\n"
            << "aeron.cluster.ingress.channel=aeron:ipc\n"
            << "aeron.cluster.consensus.channel=aeron:ipc\n"
            << "aeron.cluster.log.channel=aeron:udp?endpoint=localhost:" << log_port_ << "\n"
            << "aeron.cluster.replication.channel=aeron:udp\n"
            << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
            << "aeron.cluster.service.count=" << SERVICE_COUNT << "\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        if (!supplier.empty())
        {
            out << "aeron.cluster.authorisation.service.supplier=" << supplier << "\n";
        }
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

    void start(const std::string& supplier = {})
    {
        write_properties(supplier);
        auto node = spawn_logged(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", directory_ + "/node.properties"},
                                 directory_ + "/node.log");
        ASSERT_TRUE(node) << node.error().message.view();
        node_ = std::make_unique<ChildProcess>(std::move(*node));

        // The container connects to the CnC file, so wait for the embedded
        // driver before spawning it. It also needs the CM's control publication
        // (105) to exist: the reference container does not retry that add, so
        // give the CM a moment and respawn a container that exits early.
        ASSERT_TRUE(wait_for_driver()) << log_text("node");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        for (int attempt = 0; attempt < 20; ++attempt)
        {
            auto container = spawn_logged(AXC_CONTAINER_BINARY, {directory_ + "/container.properties"},
                                          directory_ + "/container.log");
            ASSERT_TRUE(container) << container.error().message.view();
            container_ = std::make_unique<ChildProcess>(std::move(*container));
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            if (container_->running())
            {
                break;
            }
            container_.reset();
        }
        ASSERT_TRUE(container_ && container_->running()) << log_text("container");

        ASSERT_TRUE(wait_for_active()) << log_text("node") << log_text("container");
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

    [[nodiscard]] bool wait_for_driver()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                client_ = std::make_unique<DeepmsgClient>(std::move(*client));
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] bool wait_for_active()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (nullptr == node_ || !node_->running())
            {
                return false;
            }
            if (cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1 &&
                cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID) >= 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    [[nodiscard]] std::int64_t cluster_counter(std::int32_t type_id)
    {
        if (!client_)
        {
            return -1;
        }
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(reader, type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    void set_control_toggle(std::int32_t value)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            reader, deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    [[nodiscard]] std::vector<deepmsg::cluster::common::RecordingLog::Entry> recording_log_entries(std::int32_t type)
    {
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        EXPECT_TRUE(log) << log.error().message.view();
        std::vector<deepmsg::cluster::common::RecordingLog::Entry> entries;
        if (!log)
        {
            return entries;
        }
        for (const auto& entry : log->entries())
        {
            if (entry.type == type && entry.is_valid)
            {
                entries.push_back(entry);
            }
        }
        return entries;
    }

    [[nodiscard]] ProcessResult run_probe(const std::string& main_class, const std::vector<std::string>& arguments,
                                          std::chrono::milliseconds timeout = std::chrono::seconds(90))
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(), {"-cp", jar_ + ":" + classes_dir_, main_class});
        options.arguments.insert(options.arguments.end(), arguments.begin(), arguments.end());
        options.timeout = timeout;
        auto result = axc::interop::run_process("java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        return result ? *result : ProcessResult{};
    }

    /// Runs a probe while `tick` runs every millisecond (a control toggle must
    /// land while the client is connected).
    [[nodiscard]] ProcessResult run_probe_with(const std::string& main_class, const std::vector<std::string>& arguments,
                                               std::chrono::milliseconds timeout, const std::function<void()>& tick)
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(), {"-cp", jar_ + ":" + classes_dir_, main_class});
        options.arguments.insert(options.arguments.end(), arguments.begin(), arguments.end());
        options.timeout = timeout;

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
            tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        runner.join();
        return result;
    }

    [[nodiscard]] std::string log_text(const std::string& name) const
    {
        std::ifstream in{directory_ + "/" + name + ".log"};
        if (!in)
        {
            return "";
        }
        return "\n--- " + name + ".log ---\n" +
               std::string{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    std::string jar_{};
    std::string directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    std::vector<std::string> probe_sources_{};
    std::int32_t log_port_{0};
    std::unique_ptr<ChildProcess> node_{};
    std::unique_ptr<ChildProcess> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
};

TEST_F(Xc2SingleNodeTest, theNodeStartsAndEchoes)
{
    start();

    const auto one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, one.exit_code) << one.output;
    EXPECT_NE(std::string::npos, one.output.find("RECEIVED 1")) << one.output;

    const auto many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"});
    EXPECT_EQ(0, many.exit_code) << many.output;
    EXPECT_NE(std::string::npos, many.output.find("RECEIVED 10000")) << many.output;
}

TEST_F(Xc2SingleNodeTest, theTimersFireAndCancel)
{
    start();

    for (int round = 0; round < 2; ++round)
    {
        const auto timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"});
        EXPECT_EQ(0, timers.exit_code) << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 1")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("CANCELLED 2")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 3")) << "round " << round << ": " << timers.output;
    }
}

TEST_F(Xc2SingleNodeTest, anAdminSnapshotRequestTakesASnapshot)
{
    start("ALLOW_ALL");

    const auto snapshots_before = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    const auto probe =
        run_probe("ClusterAdminRequestProbe", {aeron_dir_, "5000", "SNAPSHOT"}, std::chrono::seconds(30));
    EXPECT_EQ(0, probe.exit_code) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("ADMIN_RESPONSE OK")) << probe.output;

    bool taken = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > snapshots_before &&
            cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1)
        {
            taken = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(taken) << "the admin snapshot was not taken";
}

TEST_F(Xc2SingleNodeTest, aSnapshotSurvivesANodeRestart)
{
    start();
    const auto snapshots_before = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(4);

    bool taken = false;
    const auto snapshot_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < snapshot_deadline)
    {
        if (cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > snapshots_before &&
            cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1)
        {
            taken = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(taken) << "the snapshot was not taken";
    const auto snapshot_entries = recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT);
    ASSERT_FALSE(snapshot_entries.empty());

    const std::size_t terms_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    stop();
    start();

    const auto echo = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo.exit_code) << echo.output;
    EXPECT_NE(std::string::npos, echo.output.find("RECEIVED 1")) << echo.output;

    EXPECT_EQ(snapshot_entries.size(),
              recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    EXPECT_EQ(terms_before + 1, recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size());
}

TEST_F(Xc2SingleNodeTest, suspendStopsAndResumeRestarts)
{
    start();

    const auto start_time = std::chrono::steady_clock::now();
    bool suspended = false;
    bool resumed = false;
    std::int64_t resumed_at_ms = 0;

    const auto probe = run_probe_with(
        "ClusterSessionEventProbe", {aeron_dir_, "12000", "3", "1500"}, std::chrono::seconds(45),
        [&]()
        {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
            if (!suspended && elapsed > std::chrono::milliseconds(300))
            {
                suspended = true;
                set_control_toggle(2);
            }
            else if (suspended && !resumed && elapsed > std::chrono::milliseconds(2800))
            {
                resumed = true;
                resumed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
                set_control_toggle(3);
            }
        });

    EXPECT_EQ(0, probe.exit_code) << probe.output;
    ASSERT_TRUE(resumed) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("RECEIVED 3")) << probe.output;

    // Every echo belongs after the RESUME (the probe delays its offers so they
    // wait in the ingress publication while the module is suspended).
    std::size_t echoes_after_resume = 0;
    std::istringstream lines{probe.output};
    std::string line;
    while (std::getline(lines, line))
    {
        if (0 == line.rfind("ECHO ", 0))
        {
            std::istringstream fields{line};
            std::string tag;
            std::int64_t at = 0;
            fields >> tag >> at;
            if (at >= resumed_at_ms - 100)
            {
                ++echoes_after_resume;
            }
        }
    }
    EXPECT_EQ(3U, echoes_after_resume) << probe.output;
}

// Reference `ClusterTest.shouldSetClientName`: the CM and the container set
// `aeron.client.name`, which the heartbeat counter's label carries.
TEST_F(Xc2SingleNodeTest, theClientNamesAppearOnTheHeartbeatCounters)
{
    start();

    const std::string cm_name = "consensus-module-" + std::to_string(CLUSTER_ID) + "-0";
    const std::string container_name = "clustered-service-" + std::to_string(CLUSTER_ID) + "-0";
    bool cm_seen = false;
    bool container_seen = false;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while ((!cm_seen || !container_seen) && std::chrono::steady_clock::now() < deadline)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t max_id = aeron_counters_reader_max_counter_id(reader);
        for (std::int32_t id = 0; id < max_id; ++id)
        {
            std::int32_t state = 0;
            if (0 != aeron_counters_reader_counter_state(reader, id, &state) || AERON_COUNTER_RECORD_ALLOCATED != state)
            {
                continue;
            }
            char label[1024] = {};
            const int length = aeron_counters_reader_counter_label(reader, id, label, sizeof(label));
            if (length <= 0)
            {
                continue;
            }
            const std::string text{label, static_cast<std::size_t>(length)};
            cm_seen = cm_seen || std::string::npos != text.find("name=" + cm_name);
            container_seen = container_seen || std::string::npos != text.find("name=" + container_name);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_TRUE(cm_seen) << "no heartbeat counter names " << cm_name;
    EXPECT_TRUE(container_seen) << "no heartbeat counter names " << container_name;
}

// Reference `ClusterTest.shouldHandleReplayAfterShutdown`: an orderly shutdown
// snapshots and terminates; the restarted node replays the log and serves the
// client again.
TEST_F(Xc2SingleNodeTest, aShutdownRestartsAndReplays)
{
    start();

    const auto echo_before = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo_before.exit_code) << echo_before.output;
    EXPECT_NE(std::string::npos, echo_before.output.find("RECEIVED 1")) << echo_before.output;

    const std::size_t terms_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    const std::size_t snapshots_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size();

    set_control_toggle(5);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (node_ && !node_->running() && container_ && !container_->running())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(node_ && !node_->running()) << "the node did not stop" << log_text("node");
    ASSERT_TRUE(container_ && !container_->running()) << "the container did not stop" << log_text("container");

    stop();
    start();

    const auto echo_after = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo_after.exit_code) << echo_after.output;
    EXPECT_NE(std::string::npos, echo_after.output.find("RECEIVED 1")) << echo_after.output;

    EXPECT_EQ(terms_before + 1, recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size());
    EXPECT_GE(recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size(),
              snapshots_before);
    EXPECT_EQ(1, cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID));
}

TEST_F(Xc2SingleNodeTest, shutdownTerminatesBothProcesses)
{
    start();

    set_control_toggle(5);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (node_ && !node_->running() && container_ && !container_->running())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(node_ && !node_->running()) << "the node did not stop" << log_text("node");
    ASSERT_TRUE(container_ && !container_->running()) << "the container did not stop" << log_text("container");
}

}  // namespace
