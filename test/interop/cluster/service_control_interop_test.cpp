// C4c criterion: the S2 service-control seam end to end -- our consensus
// module side (`ServiceProxy`, `ServiceAck`, `PendingServiceMessageTracker`,
// `ConsensusModuleAdapter`, the timer service and the commit advance) against
// the reference's own `ClusteredServiceContainer` with the fork's
// `ClusterEchoTimerService`, and the reference's own `AeronCluster` client
// probes.
//
// The CM is driven on the test thread (no AgentRunner) so the interop harness
// can promote it to leader and call `joinLogAsLeader`, the same way the
// reference's `ConsensusModuleAgentTest` drives `role(LEADER)`. The Election
// itself is C4d.
//
// Criteria: JoinLog -> ServiceAck; echo 1 and 10,000; three timer rounds; the
// service-message round trip; and 1.54.0's unknown-service-id guards (report
// and drop) for both a service message and an ack.
//
// Missing jar/java/javac/probes = SKIP with the reason printed, never a
// silent pass.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_codecs.h"
#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_versions.h"
#include "cluster/consensus/consensus_module_agent.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/consensus/pending_service_message_tracker.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using deepmsg::cluster::common::ClusterMarkFile;
using deepmsg::cluster::consensus::ConsensusModuleAgent;
using deepmsg::cluster::consensus::ConsensusModuleConfig;
using deepmsg::cluster::consensus::PendingServiceMessageTracker;

constexpr std::int32_t CLUSTER_ID = 11;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t SERVICE_COUNT = 1;
constexpr std::int64_t TERM = 1;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";
constexpr const char* CONTROL_CHANNEL = "aeron:ipc?term-length=128k";

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

class ServiceControlInteropTest : public ::testing::Test
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
        for (const char* const name :
             {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java", "ClusterEchoClientProbe.java",
              "ClusterTimerClientProbe.java", "ClusterServiceMessageClientProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << "probe source not found under " << root << ": " << name;
            }
            probe_sources_.push_back(path);
        }

        std::string pattern = "/tmp/axc-c4c-interop-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        classes_dir_ = directory_ + "/classes";
        std::filesystem::create_directories(archive_dir_);
        std::filesystem::create_directories(classes_dir_);

        compile_probes();
        start_java_driver();
        start_java_archive();
        start_agent();
    }

    void TearDown() override
    {
        if (agent_)
        {
            agent_->on_close();
        }
        agent_.reset();
        if (container_)
        {
            container_->terminate(std::chrono::seconds(5));
        }
        container_.reset();
        archive_.reset();
        driver_.reset();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::vector<std::string> java_opens() const
    {
        return {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED"};
    }

    void compile_probes()
    {
        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        for (const std::string& source : probe_sources_)
        {
            compile.arguments.push_back(source);
        }
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void start_java_driver()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-Daeron.dir=" + aeron_dir_, "-Daeron.print.configuration=false",
                          "-Daeron.threading.mode=SHARED", "-cp", jar_, "io.aeron.driver.MediaDriver"});

        auto process = axc::interop::ChildProcess::spawn("java", arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        driver_ = std::make_unique<axc::interop::ChildProcess>(std::move(*process));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(driver_->running()) << "the Java media driver exited while starting";
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                client->close();
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "the Java media driver never published its CnC file";
    }

    void start_java_archive()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-Daeron.dir=" + aeron_dir_, "-Daeron.archive.dir=" + archive_dir_,
                          "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
                          "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
                          "-Daeron.print.configuration=false", "-cp", jar_, "io.aeron.archive.Archive"});

        auto process = axc::interop::ChildProcess::spawn("java", arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        archive_ = std::make_unique<axc::interop::ChildProcess>(std::move(*process));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(archive_->running()) << "the Java archive exited while starting";
            if (archive_is_reachable())
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "the Java archive never answered on its local control channel";
    }

    [[nodiscard]] bool archive_is_reachable()
    {
        auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return false;
        }

        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return false;
        }
        (void)aeron_archive_context_set_aeron(context, client->handle());
        (void)aeron_archive_context_set_owns_aeron_client(context, false);
        (void)aeron_archive_context_set_control_request_channel(context, "aeron:ipc?term-length=64k");
        (void)aeron_archive_context_set_control_request_stream_id(context, 10);
        (void)aeron_archive_context_set_control_response_channel(context, "aeron:ipc?term-length=64k");
        (void)aeron_archive_context_set_control_response_stream_id(context, CLUSTER_ID * 100 + 100 + 20);
        (void)aeron_archive_context_set_message_timeout_ns(context, 100'000'000ULL);

        aeron_archive_t* archive = nullptr;
        const int result = aeron_archive_connect(&archive, context);
        if (nullptr != archive)
        {
            (void)aeron_archive_close(archive);
        }
        (void)aeron_archive_context_close(context);
        return 0 == result;
    }

    [[nodiscard]] ConsensusModuleConfig make_config() const
    {
        ConsensusModuleConfig config;
        config.cluster_id = CLUSTER_ID;
        config.member_id = MEMBER_ID;
        config.members = MEMBERS;
        config.aeron_dir = aeron_dir_;
        config.cluster_dir = cluster_dir_;
        config.ingress_channel = "aeron:ipc";
        config.log_channel = "aeron:ipc";
        config.consensus_channel = "aeron:ipc";
        config.replication_channel = "aeron:udp";
        config.control_channel = CONTROL_CHANNEL;
        config.service_count = SERVICE_COUNT;
        config.archive_control_response_stream_id = 20;
        return config;
    }

    void start_agent()
    {
        // `ConsensusModule.launch` concludes the configuration; constructing
        // the agent directly means the test does it (C4a's pitfall: without
        // `conclude()` the directory does not exist and the mark file fails).
        ConsensusModuleConfig config = make_config();
        auto concluded = config.conclude();
        ASSERT_TRUE(concluded) << concluded.error().message.view();

        agent_ = std::make_unique<ConsensusModuleAgent>(std::move(config));
        auto started = agent_->on_start();
        ASSERT_TRUE(started) << started.error().message.view();
    }

    void start_container()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-cp", jar_ + ":" + classes_dir_, "ClusterServiceContainerProbe", aeron_dir_, cluster_dir_,
                          std::to_string(CLUSTER_ID), "0", "ClusterEchoTimerService", "true", CONTROL_CHANNEL});

        auto process = axc::interop::ChildProcess::spawn("java", arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        container_ = std::make_unique<axc::interop::ChildProcess>(std::move(*process));

        const std::string mark_path = cluster_dir_ + "/cluster-mark-service-0.dat";
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(container_->running()) << "the Java container exited while starting";
            auto mark = ClusterMarkFile::open_existing(mark_path, 0);
            if (mark && mark->semantic_version() == deepmsg::cluster::common::CLUSTER_MARK_SEMANTIC_VERSION)
            {
                // The agent creates its subscriptions after the mark file, so
                // give it a moment before the JoinLog offer can connect.
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java container never signalled ready";
    }

    void drive(std::chrono::milliseconds duration)
    {
        const auto deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline)
        {
            (void)agent_->do_work();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void join_and_wait_for_the_acks()
    {
        // The Election promotes the node in C4d; the test is the caller here.
        agent_->role(deepmsg::cluster::consensus::ClusterRole::leader);

        // The reference blocks in `awaitServicesReady`; here the same wait is
        // a loop over `doWork`, which polls the control adapter.
        bool joined = false;
        const auto join_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < join_deadline && !joined)
        {
            auto result = agent_->join_log_as_leader(TERM, 0, agent_->log_publisher()->session_id(), true);
            if (result)
            {
                joined = true;
            }
            else
            {
                (void)agent_->do_work();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(joined);

        bool acked = false;
        const auto ack_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < ack_deadline && !acked)
        {
            (void)agent_->do_work();
            auto reached = agent_->service_acks_reached();
            ASSERT_TRUE(reached) << reached.error().message.view();
            acked = *reached;
            if (!acked)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        ASSERT_TRUE(acked) << "the container never acked the JoinLog";
        EXPECT_EQ(1, agent_->service_ack_id());
    }

    [[nodiscard]] axc::interop::ProcessResult run_probe(const std::string& main_class,
                                                        const std::vector<std::string>& arguments,
                                                        std::chrono::milliseconds timeout)
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(), {"-cp", jar_ + ":" + classes_dir_, main_class});
        options.arguments.insert(options.arguments.end(), arguments.begin(), arguments.end());
        options.timeout = timeout;

        axc::interop::ProcessResult result;
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
            (void)agent_->do_work();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        runner.join();
        return result;
    }

    [[nodiscard]] std::int64_t cluster_counter(std::int32_t type_id)
    {
        (void)agent_->client().do_work();
        auto* reader = agent_->client().counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(reader, type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    std::string jar_{};
    std::string directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    std::vector<std::string> probe_sources_{};
    std::unique_ptr<axc::interop::ChildProcess> driver_{};
    std::unique_ptr<axc::interop::ChildProcess> archive_{};
    std::unique_ptr<axc::interop::ChildProcess> container_{};
    std::unique_ptr<ConsensusModuleAgent> agent_{};
};

TEST_F(ServiceControlInteropTest, theJoinLogIsAckedAndTheEchoTimersAndServiceMessagesRun)
{
    start_container();
    join_and_wait_for_the_acks();

    const auto echo_one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo_one.exit_code) << echo_one.output;
    EXPECT_NE(std::string::npos, echo_one.output.find("RECEIVED 1")) << echo_one.output;

    const auto echo_many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"}, std::chrono::seconds(110));
    EXPECT_EQ(0, echo_many.exit_code) << echo_many.output;
    EXPECT_NE(std::string::npos, echo_many.output.find("RECEIVED 10000")) << echo_many.output;

    // The card asks for three consecutive timer rounds; each probe run is a
    // new session that schedules, cancels and fires.
    for (int round = 0; round < 3; round++)
    {
        const auto timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"}, std::chrono::seconds(45));
        EXPECT_EQ(0, timers.exit_code) << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 1")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("CANCELLED 2")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 3")) << "round " << round << ": " << timers.output;
    }

    const auto service_message =
        run_probe("ClusterServiceMessageClientProbe", {aeron_dir_, "10"}, std::chrono::seconds(45));
    EXPECT_EQ(0, service_message.exit_code) << service_message.output;
    EXPECT_NE(std::string::npos, service_message.output.find("RECEIVED 10")) << service_message.output;

    // The commit position moved past zero, which is what lets the container
    // process any of the log records above.
    EXPECT_GT(cluster_counter(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), 0);
}

TEST_F(ServiceControlInteropTest, aServiceMessageAndAnAckWithAnUnknownServiceIdAreReportedAndDropped)
{
    start_container();
    join_and_wait_for_the_acks();

    const std::int64_t errors_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);
    ASSERT_GE(errors_before, 0);

    // 1.54.0 `8554648566`: the service id comes from the low 32 bits of the
    // cluster session id for a message from a service, and serviceCount is 1.
    const std::span<const std::uint8_t> empty{};
    auto dropped_message = agent_->on_service_message(3, empty);
    EXPECT_TRUE(dropped_message) << dropped_message.error().message.view();
    EXPECT_EQ(errors_before + 1,
              cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
    ASSERT_EQ(1u, agent_->pending_service_message_trackers().size());
    EXPECT_EQ(0u, agent_->pending_service_message_trackers()[0].size());

    auto dropped_ack = agent_->on_service_ack(0, 0, 0, -1, 7);
    EXPECT_TRUE(dropped_ack) << dropped_ack.error().message.view();
    EXPECT_EQ(errors_before + 2,
              cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
}

}  // namespace
