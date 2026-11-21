// T2: `deepmsg-cluster-tool` against the reference `io.aeron.cluster.ClusterTool`.
//
// The parity shape is T1's: run both tools on the same directory and compare
// stdout after normalising only the wall clock, the pid and the live counter
// time; the exit code too. The switch commands are run against a live
// single-member cluster and their effect is read from the counters the
// reference reads. `describe-latest-cm-snapshot` runs on a cluster whose
// archive is on `aeron:ipc`, which is the channel the reference hardcodes.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
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
#ifndef AXC_CLUSTER_TOOL_BIN
#define AXC_CLUSTER_TOOL_BIN ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ProcessOptions;
using axc::interop::run_process;
using deepmsg::cluster::common::ClusterMarkFile;
using deepmsg::cluster::common::RecordingLog;

constexpr std::int32_t CLUSTER_ID = 7;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

void split_words(const std::string& text, std::vector<std::string>& words)
{
    std::string word;
    for (const char character : text)
    {
        if (' ' == character)
        {
            if (!word.empty())
            {
                words.push_back(word);
                word.clear();
            }
        }
        else
        {
            word += character;
        }
    }
    if (!word.empty())
    {
        words.push_back(word);
    }
}

struct ToolResult
{
    int exit_code{-1};
    std::string output{};
};

/// Normalises what both tools read live: the wall clock in `Type:` lines, the
/// mark header's changing timestamps and pid, and the membership query's
/// current time. Everything else is the byte contract.
[[nodiscard]] std::string normalise(const std::string& text, bool pid_only = false)
{
    if (pid_only)
    {
        return "<PID>";
    }
    std::string normalised = std::regex_replace(
        text, std::regex(R"((Type: [A-Z_]+ )\d{2}:\d{2}:\d{2} \(start: [^,]+, activity: [^)]+\))"), "$1<TIME>");
    normalised = std::regex_replace(normalised, std::regex(R"((currentTimeNs=)-?\d+)"), "$1<TIME>");
    normalised = std::regex_replace(normalised, std::regex(R"((timeOfLastAppendPositionNs=)-?\d+)"), "$1<TIME>");
    // The replay image's position in the term buffer, not the recording's
    // bytes: the reference itself prints a different value on every run.
    normalised =
        std::regex_replace(normalised, std::regex(R"(End Snapshot, offset=\d+)"), "End Snapshot, offset=<OFFSET>");
    normalised = std::regex_replace(normalised, std::regex(R"((pid=)-?\d+)"), "$1<PID>");
    normalised = std::regex_replace(normalised, std::regex(R"((activityTimestamp=)-?\d+)"), "$1<TIME>");
    normalised = std::regex_replace(normalised, std::regex(R"((startTimestamp=)-?\d+)"), "$1<TIME>");
    normalised = std::regex_replace(normalised, std::regex(R"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"), "<DATE>");
    return normalised;
}

class T2ClusterToolTest : public ::testing::Test
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
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY) ||
            !std::filesystem::exists(AXC_CLUSTER_TOOL_BIN))
        {
            GTEST_SKIP() << "cluster binaries not found";
        }

        std::string pattern = "/tmp/axc-t2-tool-XXXXXX";
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
        if (::testing::Test::HasFailure() || nullptr != std::getenv("T2_KEEP"))
        {
            std::fprintf(stderr, "T2_TOOL_DIR=%s\n", directory_.c_str());
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
            // The reference's snapshot describe hardcodes `aeron:ipc`.
            << "aeron.archive.control.channel=aeron:ipc\n"
            << "aeron.cluster.service.count=1\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        out.close();

        const std::string container = directory_ + "/container.properties";
        std::ofstream container_out{container};
        ASSERT_TRUE(container_out.good());
        container_out << "aeron.dir=" << aeron_dir_ << "\n"
                      << "aeron.cluster.dir=" << cluster_dir_ << "\n"
                      << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                      << "aeron.cluster.service.id=0\n"
                      << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
                      << "deepmsg.cluster.service=echo\n";
    }

    void start()
    {
        write_properties();
        auto node = ChildProcess::spawn(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", directory_ + "/node.properties"},
                                        {}, directory_ + "/node.log");
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

        // C9's lesson: the container's ServiceAck is sent once, so spawn until
        // one stays up.
        for (int attempt = 0; attempt < 30; ++attempt)
        {
            auto container = ChildProcess::spawn(AXC_CONTAINER_BINARY, {directory_ + "/container.properties"}, {},
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
        if (java_node_)
        {
            java_node_->terminate(std::chrono::seconds(10));
            java_node_.reset();
        }
        if (java_driver_)
        {
            java_driver_->terminate(std::chrono::seconds(10));
            java_driver_.reset();
        }
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

    void set_counter(std::int32_t type_id, std::int64_t value)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        const std::int32_t id =
            deepmsg::cluster::common::find_cluster_counter(client_->counters_reader(), type_id, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, value, __ATOMIC_RELEASE);
    }

    [[nodiscard]] bool await_counter(std::int32_t type_id, std::int64_t expected,
                                     std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (counter(type_id) == expected)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] ToolResult run_reference(const std::string& command,
                                           const std::vector<std::string>& jvm_options = {}) const
    {
        ProcessOptions options;
        options.arguments = {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
                             "java.base/sun.nio.ch=ALL-UNNAMED"};
        options.arguments.insert(options.arguments.end(), jvm_options.begin(), jvm_options.end());
        options.arguments.insert(options.arguments.end(), {"-cp", jar_, "io.aeron.cluster.ClusterTool", cluster_dir_});
        split_words(command, options.arguments);
        options.timeout = std::chrono::seconds(60);
        auto ran = run_process("java", options);
        EXPECT_TRUE(ran) << ran.error().message.view();
        return ran ? ToolResult{ran->exit_code, ran->output} : ToolResult{-1, {}};
    }

    [[nodiscard]] ToolResult run_ours(const std::string& command, const std::vector<std::string>& extra = {}) const
    {
        ProcessOptions options;
        options.arguments = {cluster_dir_};
        split_words(command, options.arguments);
        options.arguments.insert(options.arguments.end(), extra.begin(), extra.end());
        options.timeout = std::chrono::seconds(60);
        auto ran = run_process(AXC_CLUSTER_TOOL_BIN, options);
        EXPECT_TRUE(ran) << ran.error().message.view();
        return ran ? ToolResult{ran->exit_code, ran->output} : ToolResult{-1, {}};
    }

    void expect_same(const std::string& command, bool pid_only = false,
                     const std::vector<std::string>& reference_jvm = {})
    {
        const ToolResult reference = run_reference(command, reference_jvm);
        const ToolResult ours = run_ours(command);
        EXPECT_EQ(reference.exit_code, ours.exit_code) << command << "\nreference:\n"
                                                       << reference.output << "\nours:\n"
                                                       << ours.output;
        EXPECT_EQ(normalise(reference.output, pid_only), normalise(ours.output, pid_only))
            << command << "\nreference:\n"
            << reference.output << "\nours:\n"
            << ours.output;
        std::fprintf(stderr, "T2 tool %-28s exit=%d/%d lines=%zu\n", command.c_str(), reference.exit_code,
                     ours.exit_code,
                     static_cast<std::size_t>(std::count(ours.output.begin(), ours.output.end(), '\n')));
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
    std::unique_ptr<ChildProcess> java_node_{};
    std::unique_ptr<ChildProcess> java_driver_{};
};

TEST_F(T2ClusterToolTest, t2TheReadOnlyCommandsMatchTheReference)
{
    start();
    expect_same("describe");
    expect_same("pid", true);
    expect_same("errors");
    expect_same("recording-log");
    expect_same("is-leader");
    expect_same("list-members");
}

TEST_F(T2ClusterToolTest, t2TheFileWritesMatchTheReference)
{
    const std::string source = directory_ + "/source";
    std::filesystem::create_directories(source);
    {
        auto log = RecordingLog::open(source, true);
        ASSERT_TRUE(log) << log.error().message.view();
        ASSERT_TRUE(log->append_term(21, 2, 100, 100));
        ASSERT_TRUE(log->append_snapshot(1, 2, 50, 60, 42, 89));
        ASSERT_TRUE(log->append_term(21, 1, 1024, 200));
        ASSERT_TRUE(log->append_snapshot(0, 0, 0, 0, 200, 0));
    }

    for (const char* command : {"sort-recording-log", "seed-recording-log-from-snapshot", "invalidate-latest-snapshot"})
    {
        const std::string reference_dir = directory_ + "/reference-" + command;
        const std::string ours_dir = directory_ + "/ours-" + command;
        std::error_code error;
        std::filesystem::copy(source, reference_dir, std::filesystem::copy_options::recursive, error);
        ASSERT_FALSE(error) << error.message();
        std::filesystem::copy(source, ours_dir, std::filesystem::copy_options::recursive, error);
        ASSERT_FALSE(error) << error.message();

        ProcessOptions reference_options;
        reference_options.arguments = {"--add-opens",
                                       "java.base/jdk.internal.misc=ALL-UNNAMED",
                                       "--add-opens",
                                       "java.base/sun.nio.ch=ALL-UNNAMED",
                                       "-cp",
                                       jar_,
                                       "io.aeron.cluster.ClusterTool",
                                       reference_dir,
                                       command};
        reference_options.timeout = std::chrono::seconds(60);
        auto reference = run_process("java", reference_options);
        ASSERT_TRUE(reference) << reference.error().message.view();

        ProcessOptions ours_options;
        ours_options.arguments = {ours_dir, command};
        ours_options.timeout = std::chrono::seconds(60);
        auto ours = run_process(AXC_CLUSTER_TOOL_BIN, ours_options);
        ASSERT_TRUE(ours) << ours.error().message.view();

        EXPECT_EQ(reference->exit_code, ours->exit_code) << command << "\nreference:\n"
                                                         << reference->output << "\nours:\n"
                                                         << ours->output;
        EXPECT_EQ(normalise(reference->output), normalise(ours->output)) << command;

        for (const char* file : {"recording.log", "recording.log.bak"})
        {
            const std::filesystem::path reference_file = std::filesystem::path{reference_dir} / file;
            const std::filesystem::path ours_file = std::filesystem::path{ours_dir} / file;
            EXPECT_EQ(std::filesystem::exists(reference_file), std::filesystem::exists(ours_file))
                << command << " " << file;
            if (std::filesystem::exists(reference_file) && std::filesystem::exists(ours_file))
            {
                std::ifstream reference_input{reference_file, std::ios::binary};
                std::ifstream ours_input{ours_file, std::ios::binary};
                const std::string reference_bytes{std::istreambuf_iterator<char>{reference_input},
                                                  std::istreambuf_iterator<char>{}};
                const std::string ours_bytes{std::istreambuf_iterator<char>{ours_input},
                                             std::istreambuf_iterator<char>{}};
                EXPECT_EQ(reference_bytes, ours_bytes) << command << " " << file;
            }
        }
    }
}

TEST_F(T2ClusterToolTest, t2DescribeLatestSnapshotMatchesTheReference)
{
    start();
    const std::int64_t snapshots = counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_counter(deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, 4);
    ASSERT_TRUE(await_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, snapshots + 1,
                              std::chrono::seconds(30)))
        << "no snapshot was taken";

    expect_same("describe-latest-cm-snapshot");
}

TEST_F(T2ClusterToolTest, t2RecoveryPlanMatchesTheReference)
{
    start();
    expect_same("recovery-plan 1", false,
                {"-Daeron.dir=" + aeron_dir_, "-Daeron.archive.control.channel=aeron:ipc",
                 "-Daeron.archive.control.response.channel=aeron:ipc"});
}

TEST_F(T2ClusterToolTest, t2TheTogglesTakeEffectOnARunningCluster)
{
    start();

    // snapshot: the reference's counter goes up and the message is the same.
    const std::int64_t snapshots = counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    const ToolResult reference_snapshot = run_reference("snapshot");
    EXPECT_EQ(0, reference_snapshot.exit_code) << reference_snapshot.output;
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, snapshots + 1,
                              std::chrono::seconds(30)));
    const ToolResult ours_snapshot = run_ours("snapshot");
    EXPECT_EQ(0, ours_snapshot.exit_code) << ours_snapshot.output;
    EXPECT_EQ(reference_snapshot.output, ours_snapshot.output);
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, snapshots + 2,
                              std::chrono::seconds(30)));

    // suspend then resume: the state counter is the observable.
    const ToolResult reference_suspend = run_reference("suspend");
    EXPECT_EQ(0, reference_suspend.exit_code) << reference_suspend.output;
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, 2));
    EXPECT_NE(std::string::npos, reference_suspend.output.find("SUSPEND applied successfully"));

    // `is-leader` still answers 0 while suspended (role and election only), and
    // `snapshot` is refused because the state is not ACTIVE.
    const ToolResult suspended_is_leader = run_ours("is-leader");
    EXPECT_EQ(0, suspended_is_leader.exit_code) << suspended_is_leader.output;
    const ToolResult suspended_snapshot = run_ours("snapshot");
    EXPECT_EQ(255, suspended_snapshot.exit_code) << suspended_snapshot.output;
    EXPECT_NE(std::string::npos, suspended_snapshot.output.find("but needs to be ACTIVE")) << suspended_snapshot.output;

    const ToolResult ours_resume = run_ours("resume");
    EXPECT_EQ(0, ours_resume.exit_code) << ours_resume.output;
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, 1));
    EXPECT_NE(std::string::npos, ours_resume.output.find("RESUME applied successfully"));

    // validate-recording-log: the node's control toggle comes back to NEUTRAL.
    const ToolResult reference_validate = run_reference("validate-recording-log");
    EXPECT_EQ(0, reference_validate.exit_code) << reference_validate.output;
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::NODE_CONTROL_TOGGLE_TYPE_ID, 1, std::chrono::seconds(30)));
    const ToolResult ours_validate = run_ours("validate-recording-log");
    EXPECT_EQ(0, ours_validate.exit_code) << ours_validate.output;
    EXPECT_EQ(reference_validate.output, ours_validate.output);
    EXPECT_TRUE(await_counter(deepmsg::cluster::common::NODE_CONTROL_TOGGLE_TYPE_ID, 1, std::chrono::seconds(30)));
}

TEST_F(T2ClusterToolTest, t2TheToolReadsAJavaWrittenDirectory)
{
    // A Java single-node cluster writes `aeron-cluster-0/` in its working
    // directory; the sample then fails on its client's missing egress channel,
    // which is irrelevant here: the directory is the fixture.
    const std::string java_root = directory_ + "/java";
    std::filesystem::create_directories(java_root);

    auto driver =
        ChildProcess::spawn(AXC_AERONMD_DEFAULT, {}, {"AERON_DIR=" + aeron_dir_}, directory_ + "/java-driver.log");
    ASSERT_TRUE(driver) << driver.error().message.view();
    java_driver_ = std::make_unique<ChildProcess>(std::move(*driver));

    const std::string script = "cd '" + java_root +
                               "' && exec java --add-opens "
                               "java.base/jdk.internal.misc=ALL-UNNAMED --add-opens "
                               "java.base/sun.nio.ch=ALL-UNNAMED -Daeron.dir=" +
                               aeron_dir_ + " -cp '" + jar_ + "' io.aeron.samples.cluster.tutorial.SingleNodeCluster";
    auto sample = ChildProcess::spawn("/bin/sh", {"-c", script}, {}, directory_ + "/java-node.log");
    ASSERT_TRUE(sample) << sample.error().message.view();
    java_node_ = std::make_unique<ChildProcess>(std::move(*sample));

    const std::string java_cluster_dir = java_root + "/aeron-cluster-0/cluster";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline && !std::filesystem::exists(java_cluster_dir + "/recording.log"))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(std::filesystem::exists(java_cluster_dir + "/recording.log"))
        << "the Java cluster never wrote its directory";

    // The container sets its mark file's `memberId` when it joins the log;
    // before that the file still holds the initial -1, and the two tools would
    // read different snapshots of the same file.
    const std::string service_mark = java_cluster_dir + "/cluster-mark-service-0.dat";
    const auto service_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < service_deadline)
    {
        if (std::filesystem::exists(service_mark))
        {
            auto mark =
                ClusterMarkFile::open_existing(service_mark, std::chrono::duration_cast<std::chrono::milliseconds>(
                                                                 std::chrono::system_clock::now().time_since_epoch())
                                                                 .count());
            if (mark && mark->member_id() >= 0)
            {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    for (const char* command : {"describe", "pid", "recording-log"})
    {
        ProcessOptions reference_options;
        reference_options.arguments = {"--add-opens",
                                       "java.base/jdk.internal.misc=ALL-UNNAMED",
                                       "--add-opens",
                                       "java.base/sun.nio.ch=ALL-UNNAMED",
                                       "-cp",
                                       jar_,
                                       "io.aeron.cluster.ClusterTool",
                                       java_cluster_dir,
                                       command};
        reference_options.timeout = std::chrono::seconds(60);
        auto reference = run_process("java", reference_options);
        ASSERT_TRUE(reference) << reference.error().message.view();

        ProcessOptions ours_options;
        ours_options.arguments = {java_cluster_dir, command};
        ours_options.timeout = std::chrono::seconds(60);
        auto ours = run_process(AXC_CLUSTER_TOOL_BIN, ours_options);
        ASSERT_TRUE(ours) << ours.error().message.view();

        EXPECT_EQ(reference->exit_code, ours->exit_code) << command;
        EXPECT_EQ(normalise(reference->output, std::string{"pid"} == command),
                  normalise(ours->output, std::string{"pid"} == command))
            << command << "\nreference:\n"
            << reference->output << "\nours:\n"
            << ours->output;
    }
}

TEST_F(T2ClusterToolTest, t2BackupQueryMatchesTheReference)
{
    start();

    auto add = deepmsg::cluster::common::allocate(
        *client_, "backup query deadline", deepmsg::cluster::common::CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID, CLUSTER_ID);
    axc::core::CounterPtr deadline_counter;
    const auto counter_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < counter_deadline)
    {
        (void)client_->do_work();
        auto polled = add.poll();
        ASSERT_TRUE(polled) << polled.error().message.view();
        if (polled->has_value())
        {
            deadline_counter = std::move(**polled);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(deadline_counter);

    // A BACKUP mark file beside the node's directory: `backup-query` reads the
    // counter 210 the node's CnC holds.
    const std::string backup_dir = directory_ + "/backup";
    std::filesystem::create_directories(backup_dir);
    deepmsg::cluster::common::ClusterMarkFile::Options options;
    options.path = backup_dir + "/" + std::string{deepmsg::cluster::common::ClusterMarkFile::FILE_NAME};
    options.component_type = deepmsg::cluster::common::ClusterComponentType::backup;
    options.error_buffer_length = deepmsg::cluster::common::ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH;
    options.timeout_ms = 10'000;
    options.file_page_size = 4 * 1024;
    options.now_ms = 1'700'000'000'000;
    auto backup_mark = deepmsg::cluster::common::ClusterMarkFile::create_or_open(options);
    ASSERT_TRUE(backup_mark) << backup_mark.error().message.view();
    backup_mark->set_cluster_id(CLUSTER_ID);
    backup_mark->set_member_id(0);
    backup_mark->set_channels(aeron_dir_, "aeron:ipc", "aeron:ipc", "", "", "");
    ASSERT_TRUE(backup_mark->signal_ready(1'700'000'000'000));

    const std::string saved_cluster_dir = cluster_dir_;
    auto run_on = [&](const std::string& command) -> ToolResult
    {
        ProcessOptions reference_options;
        reference_options.arguments = {"--add-opens",
                                       "java.base/jdk.internal.misc=ALL-UNNAMED",
                                       "--add-opens",
                                       "java.base/sun.nio.ch=ALL-UNNAMED",
                                       "-cp",
                                       jar_,
                                       "io.aeron.cluster.ClusterTool",
                                       backup_dir};
        split_words(command, reference_options.arguments);
        reference_options.timeout = std::chrono::seconds(60);

        ProcessOptions ours_options;
        ours_options.arguments = {backup_dir};
        split_words(command, ours_options.arguments);
        ours_options.timeout = std::chrono::seconds(60);

        auto reference = run_process("java", reference_options);
        auto ours = run_process(AXC_CLUSTER_TOOL_BIN, ours_options);
        EXPECT_TRUE(reference) << reference.error().message.view();
        EXPECT_TRUE(ours) << ours.error().message.view();
        EXPECT_EQ(reference->exit_code, ours->exit_code) << command;
        EXPECT_EQ(normalise(reference->output), normalise(ours->output)) << command << "\nreference:\n"
                                                                         << reference->output << "\nours:\n"
                                                                         << ours->output;
        return ToolResult{ours->exit_code, ours->output};
    };

    (void)run_on("backup-query");

    // The setter: the reference's `nextBackupQuery` reads the mark file's
    // decoder twice, and the second `aeronDirectory()` has advanced to the
    // control channel, so the reference itself throws `CnC file not created:
    // <controlChannel>/cnc.dat` (1.54.0, `ClusterToolOperator.java:405-408`).
    // Ours keeps the accessor's position independent, as the Rust tool does;
    // the effect on counter 210 is the criterion here.
    const std::int64_t before = __atomic_load_n(aeron_counter_addr(deadline_counter.get()), __ATOMIC_ACQUIRE);
    ProcessOptions set_options;
    set_options.arguments = {backup_dir, "backup-query", "1s"};
    set_options.timeout = std::chrono::seconds(60);
    auto set_run = run_process(AXC_CLUSTER_TOOL_BIN, set_options);
    ASSERT_TRUE(set_run) << set_run.error().message.view();
    const ToolResult set{set_run->exit_code, set_run->output};
    EXPECT_EQ(0, set.exit_code) << set.output;
    EXPECT_NE(std::string::npos, set.output.find("setting next:")) << set.output;
    const std::int64_t after = __atomic_load_n(aeron_counter_addr(deadline_counter.get()), __ATOMIC_ACQUIRE);
    EXPECT_GT(after, before);
    (void)saved_cluster_dir;
}

TEST_F(T2ClusterToolTest, t2ShutdownStopsTheCluster)
{
    start();
    const ToolResult result = run_ours("shutdown");
    EXPECT_EQ(0, result.exit_code) << result.output;
    EXPECT_NE(std::string::npos, result.output.find("SHUTDOWN applied successfully")) << result.output;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline && node_->running())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(node_->running()) << "the node did not shut down";
}

TEST_F(T2ClusterToolTest, t2AbortStopsTheCluster)
{
    start();
    const ToolResult result = run_ours("abort");
    EXPECT_EQ(0, result.exit_code) << result.output;
    EXPECT_NE(std::string::npos, result.output.find("ABORT applied successfully")) << result.output;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline && node_->running())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(node_->running()) << "the node did not abort";
}

}  // namespace
