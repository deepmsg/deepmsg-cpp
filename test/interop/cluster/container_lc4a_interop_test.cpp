// C2a's criterion: the reference's own driver, archive and ConsensusModule
// (inside `ClusteredMediaDriver`), our C++ service container, and the
// reference's own `AeronCluster` client.
//
// Echo: one message, then ten thousand, through one session. Timers: one
// scheduled timer fires, a scheduled-and-cancelled one does not, and a third
// fires. The business is the test's `EchoTimerService`, the same protocol the
// Rust track's Java twin implements.
//
// Everything the test needs beyond the jar is a probe source from the fork's
// `deepmsg-tests` branch, pointed at by AXC_AERON_TESTS. Missing jar/java/javac
// or probes = SKIP with the reason printed, never a silent pass.

#include <gtest/gtest.h>

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "cluster/common/cluster_counters.h"
#include "cluster/service/clustered_service_agent.h"
#include "cluster/service/container_config.h"
#include "core/agent/agent_runner.h"
#include "core/agent/idle_strategy.h"
#include "core/error/error_handler.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT ""
#endif

namespace
{

using namespace deepmsg::cluster::service;

constexpr std::int32_t CLUSTER_ID = 7;

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

/// The business: echoes everything the client sends, and implements the timer
/// probe's 16-byte command protocol.
struct EchoTimerService
{
    struct Pending
    {
        std::int64_t correlation_id;
        std::int64_t session_id;
    };

    std::vector<Pending> pending;

    std::expected<void, Error> on_start(Cluster&, SnapshotImage*)
    {
        return {};
    }

    std::expected<void, Error> on_session_open(ClientSession&, std::int64_t)
    {
        return {};
    }

    std::expected<void, Error> on_session_close(ClientSession&, std::int64_t, CloseReason)
    {
        return {};
    }

    std::expected<void, Error> on_session_message(Cluster& cluster, ClientSession* session, std::int64_t timestamp,
                                                  std::span<const std::uint8_t> payload, const LogHeader&)
    {
        if (16 == payload.size())
        {
            std::int32_t command = 0;
            std::int64_t correlation_id = 0;
            std::int32_t delay_millis = 0;
            std::memcpy(&command, payload.data(), sizeof(command));
            std::memcpy(&correlation_id, payload.data() + 4, sizeof(correlation_id));
            std::memcpy(&delay_millis, payload.data() + 12, sizeof(delay_millis));

            if (1 == command)
            {
                auto scheduled = cluster.schedule_timer(correlation_id, timestamp + delay_millis);
                if (!scheduled)
                {
                    return std::unexpected(scheduled.error());
                }
                if (*scheduled && nullptr != session)
                {
                    pending.push_back(Pending{correlation_id, session->id().value});
                }
            }
            else if (2 == command)
            {
                auto cancelled = cluster.cancel_timer(correlation_id);
                if (!cancelled)
                {
                    return std::unexpected(cancelled.error());
                }
            }
            return {};
        }

        if (nullptr != session)
        {
            return echo(cluster, *session, payload);
        }
        return {};
    }

    std::expected<void, Error> on_timer_event(Cluster& cluster, std::int64_t correlation_id, std::int64_t)
    {
        for (auto it = pending.begin(); it != pending.end(); ++it)
        {
            if (it->correlation_id == correlation_id)
            {
                const std::int64_t session_id = it->session_id;
                pending.erase(it);

                auto session = cluster.session(SessionId{session_id});
                if (!session.has_value())
                {
                    return {};
                }

                std::uint8_t bytes[8]{};
                std::memcpy(bytes, &correlation_id, sizeof(correlation_id));
                return echo(cluster, *session, std::span<const std::uint8_t>(bytes, sizeof(bytes)));
            }
        }
        return {};
    }

    std::expected<void, Error> on_take_snapshot(SnapshotPublication&)
    {
        return {};
    }

    std::expected<void, Error> on_role_change(Role)
    {
        return {};
    }

    std::expected<void, Error> on_terminate(Cluster&)
    {
        return {};
    }

    static std::expected<void, Error> echo(Cluster& cluster, ClientSession& session,
                                           std::span<const std::uint8_t> payload)
    {
        while (true)
        {
            auto result = session.offer(payload);
            if (!result)
            {
                return std::unexpected(result.error());
            }
            if (*result > 0)
            {
                return {};
            }
            if (OFFER_BACK_PRESSURED == *result || OFFER_ADMIN_ACTION == *result)
            {
                auto idled = cluster.idle(0);
                if (!idled)
                {
                    return std::unexpected(idled.error());
                }
                continue;
            }
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                         "egress publication error: " + std::to_string(*result)));
        }
    }
};

class ContainerInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        if (!std::filesystem::exists("/usr/bin/javac") && !std::filesystem::exists("/usr/local/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
        }

        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }
        echo_probe_ = find_probe(root, "ClusterEchoClientProbe.java");
        timer_probe_ = find_probe(root, "ClusterTimerClientProbe.java");
        if (echo_probe_.empty() || timer_probe_.empty())
        {
            GTEST_SKIP() << "probe sources not found under " << root;
        }

        directory_ = std::filesystem::temp_directory_path() / ("axc-lc4a-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);
        aeron_dir_ = (directory_ / "aeron").string();
        cluster_dir_ = (directory_ / "cluster").string();
        archive_dir_ = (directory_ / "archive").string();
        classes_dir_ = (directory_ / "classes").string();
        std::filesystem::create_directories(classes_dir_);

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_, echo_probe_, timer_probe_};
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void TearDown() override
    {
        if (runner_active_)
        {
            stop_container();
        }
        if (java_running_)
        {
            java_.terminate(std::chrono::seconds(5));
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::vector<std::string> java_node_arguments() const
    {
        return {"--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeron_dir_,
                "-Daeron.cluster.dir=" + cluster_dir_,
                "-Daeron.archive.dir=" + archive_dir_,
                "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
                "-Daeron.cluster.member.id=0",
                "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0",
                "-Daeron.cluster.ingress.channel=aeron:ipc",
                "-Daeron.cluster.consensus.channel=aeron:ipc",
                "-Daeron.cluster.log.channel=aeron:ipc",
                "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
                "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.service.count=1",
                "-Daeron.threading.mode=SHARED",
                "-Daeron.print.configuration=false",
                "-cp",
                jar_,
                "io.aeron.cluster.ClusteredMediaDriver"};
    }

    void start_java_node()
    {
        auto process = axc::interop::ChildProcess::spawn("java", java_node_arguments());
        ASSERT_TRUE(process) << process.error().message.view();
        java_ = std::move(*process);
        java_running_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(java_.running()) << "the Java cluster node exited while starting";

            auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                for (int i = 0; i < 100; ++i)
                {
                    (void)client->do_work();
                    if (AERON_NULL_COUNTER_ID != deepmsg::cluster::common::find_cluster_counter(
                                                     client->counters_reader(),
                                                     deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID,
                                                     CLUSTER_ID))
                    {
                        return;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        FAIL() << "the Java consensus module never published its commit counter";
    }

    void start_container()
    {
        ServiceContainerConfig config;
        config.cluster_id = CLUSTER_ID;
        config.service_id = 0;
        config.service_name = "clustered-service-7-0";
        config.aeron_dir = aeron_dir_;
        config.cluster_dir = cluster_dir_;
        config.control_channel = "aeron:ipc?term-length=128k";
        config.service_stream_id = 104;
        config.consensus_module_stream_id = 105;
        config.log_fragment_limit = 50;
        config.responding_service = true;
        config.error_buffer_length = 1024 * 1024;
        config.idle_strategy = "org.agrona.concurrent.BackoffIdleStrategy";
        config.app_version = 1;
        // C4e: `runTerminationHooks` (`CSA:1302-1315`) on the shutdown path.
        config.extended_termination_hook = [this](const deepmsg::cluster::common::TerminationCause&)
        { extended_hook_calls_.fetch_add(1); };
        config.termination_hook = [this]()
        {
            if (1 == extended_hook_calls_.load(std::memory_order_acquire))
            {
                extended_then_termination_.store(1, std::memory_order_release);
            }
            termination_hook_calls_.fetch_add(1);
        };

        agent_ = std::make_unique<ClusteredServiceAgent<EchoTimerService>>(config, EchoTimerService{});
        auto idle = axc::core::IdleStrategy::by_agrona_name("org.agrona.concurrent.BackoffIdleStrategy");
        ASSERT_TRUE(idle) << idle.error().message.view();

        runner_ =
            std::make_unique<axc::core::AgentRunner<ClusteredServiceAgent<EchoTimerService>>>(*agent_, *idle, errors_);
        auto started = runner_->start();
        ASSERT_TRUE(started) << started.error().message.view();
        runner_active_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (agent_->is_terminated())
            {
                FAIL() << "the container terminated: "
                       << (agent_->termination_error().has_value()
                               ? std::string{agent_->termination_error()->message.view()}
                               : std::string{"unknown"});
            }
            if (agent_->is_service_active())
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        FAIL() << "the container never became active";
    }

    void stop_container()
    {
        if (runner_active_ && runner_)
        {
            agent_->request_shutdown();
            runner_->close();
            runner_active_ = false;
        }
        runner_.reset();
        agent_.reset();
    }

    [[nodiscard]] std::string run_probe(const std::string& main_class, const std::vector<std::string>& arguments)
    {
        std::vector<std::string> full = {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                                         "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                                         "-cp",         jar_ + ":" + classes_dir_,
                                         main_class};
        full.insert(full.end(), arguments.begin(), arguments.end());

        axc::interop::ProcessOptions options;
        options.arguments = std::move(full);
        options.timeout = std::chrono::seconds(120);
        auto result = axc::interop::run_process("java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        EXPECT_EQ(0, result->exit_code) << result->output;
        return result->output;
    }

    std::string jar_{};
    std::string echo_probe_{};
    std::string timer_probe_{};
    std::filesystem::path directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    axc::interop::ChildProcess java_{};
    bool java_running_{false};
    axc::core::NoOpErrorHandler errors_{};
    std::atomic<int> extended_hook_calls_{0};
    std::atomic<int> termination_hook_calls_{0};
    std::atomic<int> extended_then_termination_{0};
    std::unique_ptr<ClusteredServiceAgent<EchoTimerService>> agent_{};
    std::unique_ptr<axc::core::AgentRunner<ClusteredServiceAgent<EchoTimerService>>> runner_{};
    bool runner_active_{false};
};

TEST_F(ContainerInteropTest, aJavaClusterAndClientEchoThroughOurContainer)
{
    start_java_node();
    start_container();

    const std::string one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_NE(std::string::npos, one.find("RECEIVED 1")) << one;

    const std::string many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"});
    EXPECT_NE(std::string::npos, many.find("RECEIVED 10000")) << many;

    // The card asks for three consecutive timer rounds; each probe run is a new
    // session and the service's pending list is keyed by correlation id, so a
    // leaked entry from the previous round would show up here.
    for (int round = 0; round < 3; ++round)
    {
        const std::string timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"});
        EXPECT_NE(std::string::npos, timers.find("TIMER 1")) << "round " << round << ": " << timers;
        EXPECT_NE(std::string::npos, timers.find("CANCELLED 2")) << "round " << round << ": " << timers;
        EXPECT_NE(std::string::npos, timers.find("TIMER 3")) << "round " << round << ": " << timers;
    }
}

// C4e: `ClusteredServiceAgentTest.shouldRunTerminationHooksWhenTerminating`
// (`1.54.0`), on the shutdown path the C++ container has.
TEST_F(ContainerInteropTest, terminationHooksRunExtendedThenTermination)
{
    start_java_node();
    start_container();

    agent_->request_shutdown();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (termination_hook_calls_.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_EQ(1, extended_hook_calls_.load(std::memory_order_acquire));
    EXPECT_EQ(1, termination_hook_calls_.load(std::memory_order_acquire));
    EXPECT_EQ(1, extended_then_termination_.load(std::memory_order_acquire))
        << "the extended hook must run before the termination hook";
}

}  // namespace
