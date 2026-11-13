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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_codecs.h"
#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_versions.h"
#include "cluster/common/recording_log.h"
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

constexpr std::int32_t CLUSTER_ID = 12;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t SERVICE_COUNT = 1;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";
constexpr const char* CONTROL_CHANNEL = "aeron:ipc?term-length=128k";
constexpr std::int32_t LOG_PORT = 24000 + 137;

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

class SnapshotRecoveryInteropTest : public ::testing::Test
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
              "ClusterTimerClientProbe.java", "ClusterServiceMessageClientProbe.java",
              "ClusterSessionEventProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << "probe source not found under " << root << ": " << name;
            }
            probe_sources_.push_back(path);
        }

        std::string pattern = "/tmp/axc-c4d-interop-XXXXXX";
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

        // C4d: `on_start` now recovers and blocks for the container's startup
        // ack, while the container waits for our mark file. They must start
        // concurrently, as the reference's agent thread does.
        start_agent_async();
        start_container();
        finish_start_agent();
        wait_for_leader_and_services();
    }

    void TearDown() override
    {
        if (join_abandoned_)
        {
            // A thread is still inside `do_work`; destroying the agent (or the
            // driver under it) is undefined. The test has already failed, so
            // the agent is leaked until the process exits.
            (void)agent_.release();
            return;
        }
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
        // The recovered log publication must continue at its append position,
        // which only network publications support (the C driver has no
        // `initial-position` for IPC). Production runs the log over UDP, too.
        config.log_channel = "aeron:udp?endpoint=localhost:" + std::to_string(LOG_PORT);
        config.consensus_channel = "aeron:ipc";
        config.replication_channel = "aeron:udp";
        config.control_channel = CONTROL_CHANNEL;
        config.service_count = SERVICE_COUNT;
        config.archive_control_response_stream_id = 20;
        return config;
    }

    void start_agent_async()
    {
        // `ConsensusModule.launch` concludes the configuration; constructing
        // the agent directly means the test does it (C4a's pitfall: without
        // `conclude()` the directory does not exist and the mark file fails).
        ConsensusModuleConfig config = make_config();
        auto concluded = config.conclude();
        ASSERT_TRUE(concluded) << concluded.error().message.view();

        agent_ = std::make_unique<ConsensusModuleAgent>(std::move(config));
        starter_ = std::thread(
            [this]()
            {
                auto started = agent_->on_start();
                start_ok_ = static_cast<bool>(started);
                if (!started)
                {
                    start_error_ = started.error();
                }
            });
    }

    void finish_start_agent()
    {
        starter_.join();
        ASSERT_TRUE(start_ok_) << start_error_.message.view();
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

    /// `ConsensusModuleAgent::do_work` blocks inside the Election's
    /// `await_services_ready`, so only one thread may drive the agent at a
    /// time. The worker that drives it during the join can be left blocked
    /// there when a wait gives up; from then on this thread must not call
    /// `do_work` again, or two threads run the election state machine
    /// concurrently and a second `join_log_as_leader` re-records the log
    /// without stopping the first one (the -202 that used to show up here).
    void drive_agent_once()
    {
        if (!join_abandoned_ && !agent_driven_by_worker_.load(std::memory_order_acquire))
        {
            (void)agent_->do_work();
        }
    }

    void drive(std::chrono::milliseconds duration)
    {
        const auto deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_agent_once();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    /// The greatest valid TERM entry currently in the recording log, or -1
    /// when the cluster has never recorded a term.
    [[nodiscard]] std::int64_t last_leadership_term_id()
    {
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        if (!log)
        {
            return -1;
        }
        std::int64_t last = -1;
        for (const auto& entry : log->entries())
        {
            if (deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM == entry.type && entry.is_valid)
            {
                last = std::max(last, entry.leadership_term_id);
            }
        }
        return last;
    }

    void wait_for_leader_and_services()
    {
        // What "ready" means here: the module has completed an election, not
        // merely been started. `on_start` already sets the state ACTIVE, the
        // election sets the leader role when it enters LEADER_LOG_REPLICATION,
        // and the container's *startup* ack already makes `service_ack_id` 2 --
        // all before the log replay and the join. The leadership term is set
        // by `election_complete`, which runs after `joinLogAsLeader` and
        // `updateRecordingLog`, so it is the one signal that is not early.
        const std::int64_t required_term = last_leadership_term_id() + 1;

        // The agent's `do_work` blocks inside the Election's
        // `await_services_ready`, so it runs on a worker while this thread
        // waits for the join to finish.
        std::atomic<bool> ready{false};
        std::atomic<bool> timed_out{false};

        std::thread worker(
            [this, &ready, &timed_out, required_term]()
            {
                agent_driven_by_worker_.store(true, std::memory_order_release);
                struct Release
                {
                    std::atomic<bool>& flag;
                    ~Release()
                    {
                        flag.store(false, std::memory_order_release);
                    }
                } release{agent_driven_by_worker_};

                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (std::chrono::steady_clock::now() < deadline)
                {
                    (void)agent_->do_work();
                    if (deepmsg::cluster::consensus::ClusterRole::leader == agent_->cluster_role() &&
                        agent_->leadership_term_id() >= required_term && agent_->service_ack_id() >= 2)
                    {
                        ready.store(true);
                        return;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                timed_out.store(true);
            });

        const auto probe_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(35);
        while (!ready.load() && !timed_out.load() && std::chrono::steady_clock::now() < probe_deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!ready.load())
        {
            // The worker is still inside `do_work` and cannot be stopped from
            // here: it keeps the agent to itself, this thread stops driving it
            // (`drive_agent_once`) and `TearDown` leaks the agent.
            join_abandoned_ = true;
            worker.detach();
            FAIL() << "the election never made this node leader and joined the log: role="
                   << static_cast<int>(agent_->cluster_role()) << " ackId=" << agent_->service_ack_id()
                   << " state=" << static_cast<int>(agent_->module_state());
        }
        worker.join();
    }

    [[nodiscard]] axc::interop::ProcessResult run_probe(const std::string& main_class,
                                                        const std::vector<std::string>& arguments,
                                                        std::chrono::milliseconds timeout,
                                                        const std::function<void()>& tick = {})
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
            drive_agent_once();
            if (tick)
            {
                tick();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        runner.join();
        return result;
    }

    [[nodiscard]] std::int64_t cluster_counter(std::int32_t type_id)
    {
        if (join_abandoned_)
        {
            return -1;
        }
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
    std::thread starter_{};
    bool start_ok_{false};
    axc::core::Error start_error_{};
    /// Set while the join worker drives the agent; a worker left blocked
    /// inside `do_work` keeps it set for the rest of the test.
    std::atomic<bool> agent_driven_by_worker_{false};
    /// A wait gave up while the join worker was still inside `do_work`: the
    /// agent cannot be touched (or destroyed) for the rest of the test.
    bool join_abandoned_{false};

    std::vector<deepmsg::cluster::common::RecordingLog::Entry> entries_of_type(std::int32_t type)
    {
        std::vector<deepmsg::cluster::common::RecordingLog::Entry> selected{};
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        EXPECT_TRUE(log) << log.error().message.view();
        if (!log)
        {
            return selected;
        }
        for (const auto& entry : log->entries())
        {
            if (entry.type == type && entry.is_valid)
            {
                selected.push_back(entry);
            }
        }
        return selected;
    }

    [[nodiscard]] static deepmsg::cluster::common::RecordingLog::Entry find_snapshot(
        const std::vector<deepmsg::cluster::common::RecordingLog::Entry>& snapshots, std::int32_t service_id)
    {
        for (const auto& entry : snapshots)
        {
            if (entry.service_id == service_id)
            {
                return entry;
            }
        }
        return deepmsg::cluster::common::RecordingLog::Entry{};
    }

    void set_control_toggle(std::int32_t value)    {
        (void)agent_->client().do_work();
        auto* reader = agent_->client().counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            reader, deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    void wait_for_snapshot()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_agent_once();
            if (cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) >= 1 &&
                deepmsg::cluster::consensus::ConsensusModuleState::active == agent_->module_state())
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        FAIL() << "the snapshot never completed: snapshots="
               << cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
               << " state=" << static_cast<int>(agent_->module_state());
    }

    [[nodiscard]] static std::int64_t epoch_millis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    [[nodiscard]] std::int32_t module_state()
    {
        return static_cast<std::int32_t>(
            cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID));
    }

    void wait_for_module_state(std::int32_t wanted, std::chrono::seconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::int32_t current = -1;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_agent_once();
            current = module_state();
            if (wanted == current)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        FAIL() << "the module state never became " << wanted << ": " << current;
    }

    /// The epoch milliseconds of every `ECHO <epoch> <index>` line, in order.
    [[nodiscard]] static std::vector<std::int64_t> echo_epochs(const std::string& output)
    {
        std::vector<std::int64_t> epochs{};
        std::istringstream stream(output);
        std::string line;
        while (std::getline(stream, line))
        {
            if (0 != line.rfind("ECHO ", 0))
            {
                continue;
            }
            std::istringstream fields(line);
            std::string tag;
            std::int64_t epoch = 0;
            int index = 0;
            fields >> tag >> epoch >> index;
            epochs.push_back(epoch);
        }
        return epochs;
    }

    void stop_container_and_agent()
    {
        if (container_)
        {
            container_->terminate(std::chrono::seconds(10));
        }
        container_.reset();
        if (agent_)
        {
            agent_->on_close();
        }
        agent_.reset();
        await_control_publication_gone();
    }

    /// C4d: `ConsensusModuleAgent.onClose` tells the services their final
    /// position (`ConsensusModuleAgent.java:284-292`), and the driver keeps
    /// that IPC publication reachable for a short while after the client is
    /// gone (measured 0.1-0.9 s here; `IpcPublication` goes
    /// ACTIVE -> LINGER -> DONE and the driver only removes a publication on
    /// an explicit command). A service container that subscribes inside that
    /// window reads the previous incarnation's `ServiceTerminationPosition`,
    /// recovers to the same log position and terminates at once -- an
    /// "expected" `ClusterTerminationException`, which agrona's `AgentRunner`
    /// swallows without a single line of output, so the log is never joined
    /// and the `JoinLog` is never acked.
    /// `notes-2026-11-13-the-container-reads-a-stale-termination.md`.
    void await_control_publication_gone()
    {
        // The agent is gone and the new one has not started: whichever
        // publication this subscription matches is the previous incarnation's.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        int quiet_probes = 0;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (control_publication_visible())
            {
                quiet_probes = 0;
            }
            else if (++quiet_probes >= 2)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        FAIL() << "the previous consensus module's control publication never went away";
    }

    /// A probe that keeps its subscription holds the publication in the
    /// driver's linger state, so each probe is a fresh client that is closed
    /// again; two quiet probes in a row are taken as "gone".
    [[nodiscard]] bool control_publication_visible()
    {
        constexpr auto PROBE_WINDOW = std::chrono::milliseconds(200);

        auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return true;
        }
        const ConsensusModuleConfig config = make_config();
        auto add = client->add_subscription(CONTROL_CHANNEL, config.service_stream_id);
        axc::core::SubPtr subscription{};
        const auto subscribe_deadline = std::chrono::steady_clock::now() + PROBE_WINDOW;
        while (!subscription && std::chrono::steady_clock::now() < subscribe_deadline)
        {
            (void)client->do_work();
            auto polled = add.poll();
            if (!polled)
            {
                // Cannot tell; keep waiting rather than declare the channel clean.
                return true;
            }
            if (polled->has_value())
            {
                subscription = std::move(**polled);
            }
        }
        if (!subscription)
        {
            return true;
        }

        bool visible = false;
        const auto image_deadline = std::chrono::steady_clock::now() + PROBE_WINDOW;
        while (std::chrono::steady_clock::now() < image_deadline)
        {
            (void)client->do_work();
            if (0 < aeron_subscription_image_count(subscription.get()))
            {
                visible = true;
                break;
            }
        }
        // Handles close through the client's conductor: release the
        // subscription first, or its closer dereferences a freed conductor.
        subscription.reset();
        client->close();
        return visible;
    }

    void restart()
    {
        start_agent_async();
        start_container();
        finish_start_agent();
        wait_for_leader_and_services();
    }
};

TEST_F(SnapshotRecoveryInteropTest, theLogReplayRebuildsTheClusterAfterARestart)
{
    const auto echo_before = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo_before.exit_code) << echo_before.output;
    EXPECT_NE(std::string::npos, echo_before.output.find("RECEIVED 1")) << echo_before.output;

    const std::size_t terms_before = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    ASSERT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());

    stop_container_and_agent();
    restart();

    // The recovery plan had a log and no snapshot: the election replayed it
    // (`LogReplay`) before joining, so the same client work still succeeds.
    const auto echo_after = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo_after.exit_code) << echo_after.output;
    EXPECT_NE(std::string::npos, echo_after.output.find("RECEIVED 1")) << echo_after.output;

    EXPECT_EQ(terms_before + 1, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size())
        << "the restart must append exactly one new term entry";
    EXPECT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    EXPECT_GE(cluster_counter(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), 0);
}

TEST_F(SnapshotRecoveryInteropTest, aSnapshotSurvivesARestartAndTheLogContinues)
{
    const auto echo_before = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo_before.exit_code) << echo_before.output;
    EXPECT_NE(std::string::npos, echo_before.output.find("RECEIVED 1")) << echo_before.output;

    // `ClusterControl.ToggleState.SNAPSHOT` is code 4 in counter 202.
    set_control_toggle(4);
    wait_for_snapshot();

    const auto snapshots = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT);
    ASSERT_EQ(2u, snapshots.size()) << "the consensus module and the service both snapshot";
    const auto find_service = [&snapshots](std::int32_t service_id)
    {
        for (const auto& entry : snapshots)
        {
            if (entry.service_id == service_id)
            {
                return entry;
            }
        }
        return deepmsg::cluster::common::RecordingLog::Entry{};
    };
    const auto cm_snapshot = find_service(deepmsg::cluster::common::RecordingLog::SERVICE_ID);
    const auto service_snapshot = find_service(0);
    EXPECT_EQ(cm_snapshot.log_position, service_snapshot.log_position);
    EXPECT_GT(cm_snapshot.recording_id, 0);
    EXPECT_GT(service_snapshot.recording_id, 0);

    const std::size_t terms_before = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    stop_container_and_agent();
    restart();

    const auto echo_after = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo_after.exit_code) << echo_after.output;
    EXPECT_NE(std::string::npos, echo_after.output.find("RECEIVED 1")) << echo_after.output;

    // The restart replays the log tail after the snapshot and appends exactly
    // one term; it must not take another snapshot by itself.
    EXPECT_EQ(terms_before + 1, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size());
    EXPECT_EQ(2u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
}

// C: SUSPEND/RESUME (`ConsensusModuleAgent.java:2578-2587`, `:2663-2677`).
// The session is opened before the module stops polling ingress (a connect
// request travels on that stream), and the first offer is delayed past the
// suspension, so the three messages wait in the ingress publication and must
// all be answered after the RESUME -- none before it.
TEST_F(SnapshotRecoveryInteropTest, aSuspendStopsProcessingAndAResumeRestartsIt)
{
    const auto start = std::chrono::steady_clock::now();
    bool suspended = false;
    std::int64_t suspended_at = 0;
    std::int64_t resumed_at = 0;

    const auto probe = run_probe(
        "ClusterSessionEventProbe", {aeron_dir_, "12000", "3", "4000"}, std::chrono::seconds(45),
        [&]()
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            if (!suspended && elapsed > std::chrono::milliseconds(1500))
            {
                set_control_toggle(2);
                suspended = true;
                suspended_at = epoch_millis();
            }
            else if (suspended && 0 == resumed_at && elapsed > std::chrono::milliseconds(7000))
            {
                EXPECT_EQ(2, module_state()) << "the module never suspended";
                resumed_at = epoch_millis();
                set_control_toggle(3);
            }
        });

    EXPECT_EQ(0, probe.exit_code) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("RECEIVED 3")) << probe.output;

    const auto epochs = echo_epochs(probe.output);
    ASSERT_EQ(3u, epochs.size()) << probe.output;
    for (const std::int64_t echo_epoch : epochs)
    {
        EXPECT_GE(echo_epoch, resumed_at) << "an echo arrived while the module was suspended: " << probe.output;
    }
    EXPECT_GT(resumed_at, suspended_at);
    EXPECT_EQ(1, module_state());
}

// F: STANDBY_SNAPSHOT (`Agent:2601-2609`). It appends the action with flags=1
// and only resets the toggle: the module stays ACTIVE, no snapshot is taken,
// and a replay of the same action does not change the state either.
TEST_F(SnapshotRecoveryInteropTest, aStandbySnapshotRequestAppendsAndResetsButTakesNoSnapshot)
{
    ASSERT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    // One committed record, so the restart replays a real log (an
    // uncommitted term entry makes the recovery plan keep the same term and
    // the flags=1 action would never be replayed).
    const auto echo = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"}, std::chrono::seconds(40));
    EXPECT_EQ(0, echo.exit_code) << echo.output;
    EXPECT_NE(std::string::npos, echo.output.find("RECEIVED 1")) << echo.output;

    const std::int64_t snapshots_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);

    set_control_toggle(7);
    drive(std::chrono::seconds(2));

    EXPECT_EQ(1, module_state()) << "the module must stay ACTIVE";
    EXPECT_EQ(1, cluster_counter(deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID))
        << "the toggle must be reset to NEUTRAL";
    EXPECT_EQ(snapshots_before, cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID));
    EXPECT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());

    const std::size_t terms_before = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    std::string before_dump{};
    for (const auto& entry : entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM))
    {
        before_dump.append(entry.to_string()).append("\n");
    }
    stop_container_and_agent();
    restart();

    // `onReplayClusterAction` acts only on flags == DEFAULT.
    EXPECT_EQ(1, module_state());

    EXPECT_EQ(snapshots_before, cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID));
    EXPECT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    std::string after_dump{};
    for (const auto& entry : entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM))
    {
        after_dump.append(entry.to_string()).append("\n");
    }
    EXPECT_EQ(terms_before + 1, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size())
        << "before:\n" << before_dump << "after:\n" << after_dump;
}

// D: SHUTDOWN (`Agent:2611-2634`, `ClusterTermination.java:22-85`,
// `Agent:3308-3362`). The SNAPSHOT action is appended, both snapshots are
// written, the services are told the termination position, and the sessions
// are closed -- the client sees the CLOSED event.
TEST_F(SnapshotRecoveryInteropTest, aShutdownSnapshotsThenTerminatesTheSessions)
{
    const std::int64_t snapshots_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    const std::int64_t errors_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);

    const auto start = std::chrono::steady_clock::now();
    bool shutdown_sent = false;
    bool closed_seen = false;
    std::int64_t snapshots_after = -1;
    std::int64_t commit_after = -1;
    std::int64_t errors_after = -1;

    const auto probe = run_probe(
        "ClusterSessionEventProbe", {aeron_dir_, "12000", "1"}, std::chrono::seconds(45),
        [&]()
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            if (!shutdown_sent && elapsed > std::chrono::milliseconds(1500))
            {
                shutdown_sent = true;
                set_control_toggle(5);
            }
            else if (shutdown_sent && !closed_seen && 6 == module_state())
            {
                // Read the counters before `on_close`, which closes the client.
                snapshots_after = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
                commit_after = cluster_counter(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
                errors_after = cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);
                closed_seen = true;
                // What the reference's runner does after the termination
                // exception: `onClose` closes every session (`SessionManager.java:1063-1068`).
                agent_->on_close();
            }
        });

    ASSERT_TRUE(closed_seen) << "the shutdown never closed the module: " << probe.output;
    EXPECT_EQ(0, probe.exit_code) << probe.output;
    // The reference's `closeSessions` closes every session without sending a
    // CLOSED event (`SessionManager.java:1049-1054`; `ClusterSession.close`
    // only disconnects), so the client learns the cluster is gone by the
    // egress image disappearing (`AeronCluster.pollEgress:603-607`).
    EXPECT_NE(std::string::npos, probe.output.find("FINAL_IMAGES 0")) << probe.output;
    EXPECT_EQ(snapshots_before + 1, snapshots_after);
    EXPECT_EQ(errors_before, errors_after) << "a clean shutdown must not report an error";

    const auto snapshots = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT);
    ASSERT_EQ(2u, snapshots.size()) << "the consensus module and the service both snapshot";
    const auto cm_snapshot = find_snapshot(snapshots, deepmsg::cluster::common::RecordingLog::SERVICE_ID);
    const auto service_snapshot = find_snapshot(snapshots, 0);
    EXPECT_EQ(cm_snapshot.log_position, service_snapshot.log_position);
    EXPECT_GT(cm_snapshot.recording_id, 0);
    EXPECT_GT(service_snapshot.recording_id, 0);

    // The termination position is the snapshot's log position: the term
    // entry's committed position is what `commitLogPosition` wrote.
    EXPECT_EQ(cm_snapshot.log_position, commit_after);
    const auto terms = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM);
    ASSERT_FALSE(terms.empty());
    EXPECT_EQ(commit_after, terms.back().log_position);
}

// E: ABORT (`Agent:2636-2655`). No action is appended and no snapshot is
// taken; the sessions still close, and the committed position is the
// termination position.
TEST_F(SnapshotRecoveryInteropTest, anAbortTerminatesWithoutASnapshot)
{
    const std::int64_t snapshots_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    const std::int64_t errors_before =
        cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);

    const auto start = std::chrono::steady_clock::now();
    bool abort_sent = false;
    bool closed_seen = false;
    std::int64_t snapshots_after = -1;
    std::int64_t commit_after = -1;
    std::int64_t errors_after = -1;

    const auto probe = run_probe(
        "ClusterSessionEventProbe", {aeron_dir_, "12000", "1"}, std::chrono::seconds(45),
        [&]()
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            if (!abort_sent && elapsed > std::chrono::milliseconds(1500))
            {
                abort_sent = true;
                set_control_toggle(6);
            }
            else if (abort_sent && !closed_seen && 6 == module_state())
            {
                snapshots_after = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
                commit_after = cluster_counter(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
                errors_after = cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);
                closed_seen = true;
                agent_->on_close();
            }
        });

    ASSERT_TRUE(closed_seen) << "the abort never closed the module: " << probe.output;
    EXPECT_EQ(0, probe.exit_code) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("FINAL_IMAGES 0")) << probe.output;
    EXPECT_EQ(snapshots_before, snapshots_after) << "ABORT must not take a snapshot";
    EXPECT_EQ(0u, entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    EXPECT_EQ(errors_before, errors_after) << "the unclean path must not report a spurious error";

    const auto terms = entries_of_type(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM);
    ASSERT_FALSE(terms.empty());
    EXPECT_EQ(commit_after, terms.back().log_position)
        << "the committed position must be the termination position";
}

}  // namespace
