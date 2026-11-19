// XC-9 S6: the backup's credentials and challenge paths against a Java cluster
// whose authenticator is the fork's `ChallengeAuthenticatorSupplier` (the
// reference's sample authenticator shape: a challenge, then an answer).
//
// The cluster is started with `-Daeron.cluster.authenticator.supplier`; the
// client sessions would need the same answer, so this fixture drives the
// cluster through counters only (no client session): a snapshot toggle and the
// backup. A backup that answers correctly reaches BACKING_UP; one that answers
// wrongly is rejected by the cluster (`CHALLENGE_REJECTED` in the node log) and
// never does.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

/// XC-9 S3-S5: our backup against a Java cluster. The cluster is the
/// reference's own shape — three `ClusteredMediaDriver` members, each with its
/// service container, because a backup is served by a FOLLOWER
/// (`ConsensusModuleAgent.java:2454-2458`). The criterion is the reference's:
/// the backup reaches BACKING_UP, its directory holds the retrieved snapshot
/// and the replayed log, and `ClusterTool recording-log` lists them.
class Xc9BackupAuthTest : public ::testing::Test
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
        const char* tests = std::getenv("AXC_AERON_TESTS");
        if (nullptr == tests || '\0' == *tests ||
            !std::filesystem::exists(std::string{tests} + "/deepmsg-tests/java/ClusterServiceContainerProbe.java"))
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's Java probe is required";
        }
        tests_root_ = tests;
        if (!std::filesystem::exists(AXC_CLUSTER_BACKUP_BIN))
        {
            GTEST_SKIP() << "the cluster backup binary is missing";
        }

        std::string pattern = "/tmp/axc-c9-java-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_dir_ = created;
        base_port_ = 13'500 + ((static_cast<int>(::getpid()) % 8) * 200);
    }

    void TearDown() override
    {
        stop_all();
        std::error_code error;
        if (::testing::Test::HasFailure())
        {
            std::fprintf(stderr, "XC9_JAVA_ROUND_DIR=%s\n", root_dir_.c_str());
        }
        else
        {
            std::filesystem::remove_all(root_dir_, error);
        }
    }

    [[nodiscard]] std::string member_dir(int index, std::string_view leaf) const
    {
        return root_dir_ + "/m" + std::to_string(index) + "-" + std::string{leaf};
    }
    [[nodiscard]] std::string backup_aeron() const
    {
        return root_dir_ + "/backup-aeron";
    }
    [[nodiscard]] std::string backup_cluster() const
    {
        return root_dir_ + "/backup-cluster";
    }
    [[nodiscard]] std::string backup_archive() const
    {
        return root_dir_ + "/backup-archive";
    }
    [[nodiscard]] int member_port(int index, int offset) const
    {
        return base_port_ + (index * 10) + offset;
    }
    [[nodiscard]] int backup_consensus_port() const
    {
        return base_port_ + 50;
    }
    [[nodiscard]] int egress_port() const
    {
        return base_port_ + 60;
    }
    [[nodiscard]] int backup_archive_port() const
    {
        return base_port_ + 70;
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

    void compile_java_probes()
    {
        if (classes_dir_compiled_)
        {
            return;
        }
        classes_dir_compiled_ = true;
        simple_probe_available_ = true;
        classes_dir_ = root_dir_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        std::vector<std::string> sources;
        for (const char* const name : {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java",
                                       "ChallengeAuthenticatorSupplier.java", "SimpleAuthenticatorSupplier.java"})
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
            // `SimpleAuthenticatorSupplier` is a C9-2 probe: an older fork
            // checkout does not have it, and only the simple-credentials case
            // needs it.
            if (sources.empty() && 0 == std::string{name}.compare("SimpleAuthenticatorSupplier.java"))
            {
                simple_probe_available_ = false;
                continue;
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
    }

    void start_java_cluster(const std::string& authenticator = "ChallengeAuthenticatorSupplier")
    {
        compile_java_probes();
        for (int i = 0; i < MEMBERS; ++i)
        {
            std::filesystem::create_directories(member_dir(i, "aeron"));
            std::filesystem::create_directories(member_dir(i, "cluster"));
            std::filesystem::create_directories(member_dir(i, "archive"));
            const std::string path = root_dir_ + "/member" + std::to_string(i) + ".properties";
            std::ofstream out{path};
            ASSERT_TRUE(out.good());
            out << "aeron.dir=" << member_dir(i, "aeron") << "\n"
                << "aeron.dir.delete.on.start=true\n"
                << "aeron.cluster.dir=" << member_dir(i, "cluster") << "\n"
                << "aeron.cluster.dir.delete.on.start=true\n"
                << "aeron.archive.dir=" << member_dir(i, "archive") << "\n"
                << "aeron.archive.dir.delete.on.start=true\n"
                << "aeron.cluster.id=" << CLUSTER_ID << "\n"
                << "aeron.cluster.member.id=" << i << "\n"
                << "aeron.cluster.members=" << members() << "\n"
                << "aeron.cluster.ingress.channel=aeron:udp?endpoint=localhost:" << member_port(i, 0) << "\n"
                << "aeron.cluster.consensus.channel=aeron:udp?alias=consensus\n"
                << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
                << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
                << "aeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0\n"
                << "aeron.cluster.service.count=1\n"
                << "aeron.cluster.startup.canvass.timeout=10s\n"
                << "aeron.cluster.leader.heartbeat.timeout=2s\n"
                << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << member_port(i, 4) << "\n"
                << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n"
                << "aeron.threading.mode=SHARED\n";
            out.close();

            std::vector<std::string> arguments = java_opens();
            arguments.insert(arguments.end(), {"-cp", jar_ + ":" + root_dir_ + "/classes",
                                               "-Daeron.cluster.authenticator.supplier=" + authenticator,
                                               "io.aeron.cluster.ClusteredMediaDriver", path});
            auto spawned =
                ChildProcess::spawn("/usr/bin/java", arguments, {}, root_dir_ + "/member" + std::to_string(i) + ".log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            members_process_[i] = std::make_unique<ChildProcess>(std::move(*spawned));
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            bool all_up = true;
            for (int i = 0; i < MEMBERS; ++i)
            {
                const std::string client_dir = member_dir(i, "aeron");
                axc::core::ClientConfig client_config{client_dir};
                client_config.on_error = &on_client_error;
                auto client = DeepmsgClient::connect(client_config);
                if (client)
                {
                    client->close();
                }
                else
                {
                    all_up = false;
                    break;
                }
            }
            if (all_up)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java members never published their CnC files";
    }

    void start_containers()
    {
        compile_java_probes();
        for (int i = 0; i < MEMBERS; ++i)
        {
            std::vector<std::string> arguments = java_opens();
            arguments.insert(arguments.end(),
                             {"-cp", jar_ + ":" + classes_dir_, "ClusterServiceContainerProbe", member_dir(i, "aeron"),
                              member_dir(i, "cluster"), std::to_string(CLUSTER_ID), "0", "ClusterEchoTimerService",
                              "true", "aeron:ipc?term-length=128k"});
            auto spawned = ChildProcess::spawn("/usr/bin/java", arguments, {},
                                               root_dir_ + "/container" + std::to_string(i) + ".log");
            ASSERT_TRUE(spawned) << spawned.error().message.view();
            containers_[i] = std::make_unique<ChildProcess>(std::move(*spawned));
        }
    }

    void start_backup(const std::string& challenge_answer, const std::string& suffix,
                      const std::string& credentials = "")
    {
        const std::string path = root_dir_ + "/backup-" + suffix + ".properties";
        std::ofstream out{path};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << backup_aeron() << "\n"
            << "aeron.cluster.dir=" << backup_cluster() << "\n"
            << "aeron.archive.dir=" << backup_archive() << "\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << backup_archive_port() << "\n"
            << "aeron.archive.control.response.channel=aeron:udp?endpoint=localhost:" << (base_port_ + 90) << "\n"
            << "deepmsg.cluster.backup.challenge.response=" << challenge_answer << "\n"
            << "deepmsg.cluster.backup.credentials=" << credentials << "\n"
            << "aeron.archive.threading.mode=DEDICATED\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=0\n"
            << "aeron.cluster.members=" << members() << "\n"
            << "aeron.cluster.consensus.channel=aeron:udp?endpoint=localhost:" << backup_consensus_port() << "\n"
            << "aeron.cluster.consensus.endpoints=" << consensus_endpoints() << "\n"
            << "aeron.cluster.log.channel=aeron:udp?term-length=512k|alias=raft\n"
            << "aeron.cluster.backup.catchup.endpoint=localhost:" << (base_port_ + 80) << "\n"
            << "aeron.cluster.backup.catchup.channel=aeron:udp?endpoint=localhost:0\n"
            << "aeron.cluster.backup.source.type=ANY\n";
        out.close();

        auto spawned = ChildProcess::spawn(AXC_CLUSTER_BACKUP_BIN, {path}, {}, root_dir_ + "/backup.log");
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        backup_ = std::make_unique<ChildProcess>(std::move(*spawned));
    }

    void stop_all()
    {
        for (auto& process : members_process_)
        {
            if (process)
            {
                process->terminate(std::chrono::seconds(10));
                process.reset();
            }
        }
        for (auto& process : containers_)
        {
            if (process)
            {
                process->terminate(std::chrono::seconds(10));
                process.reset();
            }
        }
        if (backup_)
        {
            backup_->terminate(std::chrono::seconds(10));
            backup_.reset();
        }
        for (auto& client : java_clients_)
        {
            client.reset();
        }
        backup_client_.reset();
    }

    [[nodiscard]] std::int64_t java_counter(int index, std::int32_t type_id)
    {
        if (!java_clients_[index])
        {
            const std::string client_dir = member_dir(index, "aeron");
            axc::core::ClientConfig client_config{client_dir};
            client_config.on_error = &on_client_error;
            auto client = DeepmsgClient::connect(client_config);
            if (!client)
            {
                return -1;
            }
            java_clients_[index] = std::make_unique<DeepmsgClient>(std::move(*client));
        }
        auto& client = java_clients_[index];
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
            const std::string client_dir = backup_aeron();
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
            if (2 == java_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
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
                all_active = all_active &&
                             1 == java_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
            }
            if (all_active && leader_index() >= 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void take_snapshot()
    {
        const int leader = leader_index();
        ASSERT_GE(leader, 0) << "no leader for the snapshot toggle";
        const std::int32_t toggle_id = deepmsg::cluster::common::find_cluster_counter(
            java_clients_[leader]->counters_reader(), deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID,
            CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, toggle_id) << "no control toggle counter";
        std::int64_t* toggle = aeron_counters_reader_addr(java_clients_[leader]->counters_reader(), toggle_id);
        ASSERT_NE(nullptr, toggle);

        const std::int64_t before = java_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
        __atomic_store_n(toggle, static_cast<std::int64_t>(4), __ATOMIC_RELEASE);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (java_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > before)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java cluster never took the snapshot";
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

    [[nodiscard]] bool member_log_contains(const std::string& text) const
    {
        for (int i = 0; i < MEMBERS; ++i)
        {
            std::ifstream in{root_dir_ + "/member" + std::to_string(i) + ".log"};
            if (!in)
            {
                continue;
            }
            const std::string member_log{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
            if (std::string::npos != member_log.find(text))
            {
                return true;
            }
        }
        return false;
    }

    std::string jar_{};
    std::string tests_root_{};
    std::string classes_dir_{};
    bool classes_dir_compiled_{false};
    bool simple_probe_available_{false};
    std::string root_dir_{};
    int base_port_{0};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> members_process_{};
    std::array<std::unique_ptr<ChildProcess>, MEMBERS> containers_{};
    std::array<std::unique_ptr<DeepmsgClient>, MEMBERS> java_clients_{};
    std::unique_ptr<ChildProcess> backup_{};
    std::unique_ptr<DeepmsgClient> backup_client_{};
};

TEST_F(Xc9BackupAuthTest, theBackupAuthenticatesWithSimpleCredentials)
{
    start_java_cluster("SimpleAuthenticatorSupplier");
    if (!simple_probe_available_)
    {
        GTEST_SKIP() << "the fork's SimpleAuthenticatorSupplier probe is missing";
    }
    start_containers();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "the Java cluster never became active";

    start_backup("", "simple", "c9-credentials");
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(120)))
        << "the backup never reached BACKING_UP (state="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) << ")";

    EXPECT_TRUE(member_log_contains("SIMPLE_OK")) << "the probe authenticator never accepted the credentials";
}

TEST_F(Xc9BackupAuthTest, theBackupWithWrongSimpleCredentialsIsRejected)
{
    start_java_cluster("SimpleAuthenticatorSupplier");
    if (!simple_probe_available_)
    {
        GTEST_SKIP() << "the fork's SimpleAuthenticatorSupplier probe is missing";
    }
    start_containers();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "the Java cluster never became active";

    start_backup("", "simple-bad", "not-the-credentials");

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool backing_up = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (backup_state_code(ClusterBackupState::backing_up) ==
            backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID))
        {
            backing_up = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(backing_up) << "the backup reached BACKING_UP despite the wrong credentials";
    EXPECT_TRUE(member_log_contains("SIMPLE_REJECTED")) << "no member logged SIMPLE_REJECTED";
}

TEST_F(Xc9BackupAuthTest, theBackupAnswersTheChallengeAndReachesBackingUp)
{
    start_java_cluster();
    start_containers();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "the Java cluster never became active";

    start_backup("lc1-answer", "good");
    ASSERT_TRUE(wait_for_backup_state(ClusterBackupState::backing_up, std::chrono::seconds(120)))
        << "the backup never reached BACKING_UP (state="
        << backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID) << ")";
}

TEST_F(Xc9BackupAuthTest, theBackupWithAWrongAnswerIsRejected)
{
    start_java_cluster();
    start_containers();
    ASSERT_TRUE(wait_for_active(std::chrono::seconds(90))) << "the Java cluster never became active";

    start_backup("not-the-answer", "bad");

    // The cluster rejects the session (`CHALLENGE_REJECTED` in the member's
    // log, printed by the probe) and the backup's session-event path reports
    // it: the state must never become BACKING_UP.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool backing_up = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (backup_state_code(ClusterBackupState::backing_up) ==
            backup_counter(deepmsg::cluster::common::CLUSTER_BACKUP_STATE_TYPE_ID))
        {
            backing_up = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(backing_up) << "the backup reached BACKING_UP despite the wrong answer";

    bool rejected = false;
    for (int i = 0; i < MEMBERS && !rejected; ++i)
    {
        std::ifstream in{root_dir_ + "/member" + std::to_string(i) + ".log"};
        if (!in)
        {
            continue;
        }
        const std::string text{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        rejected = std::string::npos != text.find("CHALLENGE_REJECTED");
    }
    EXPECT_TRUE(rejected) << "no member logged CHALLENGE_REJECTED";
}

}  // namespace
