// C4a criterion: the consensus-module skeleton starts against the reference
// stack — the reference's own media driver and archive (`MediaDriver` and
// `Archive` mains) — connects to the local archive over its IPC control
// channel, publishes counter 200 = ACTIVE (1), and writes a CM mark file.
//
// Q6 criterion: a cluster directory whose recording.log holds a standby
// snapshot newer than every local snapshot warns through the counted error
// handler (counter 212 + 1) and still starts; with
// `aeron.cluster.cpp.standby.snapshot.on.startup=refuse` the start fails.
//
// Missing jar/java = SKIP with the reason printed, never a silent pass.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_versions.h"
#include "cluster/common/recording_log.h"
#include "cluster/consensus/consensus_module.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/time/clock.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::cluster::common::ClusterComponentType;
using deepmsg::cluster::common::ClusterMarkFile;
using deepmsg::cluster::common::RecordingLog;
using deepmsg::cluster::consensus::ConsensusModule;
using deepmsg::cluster::consensus::ConsensusModuleConfig;
using deepmsg::cluster::consensus::ConsensusModuleState;
using deepmsg::cluster::consensus::StandbySnapshotStartupMode;

constexpr std::int32_t CLUSTER_ID = 9;
constexpr std::int32_t MEMBER_ID = 0;
// `onStart` waits for every service's startup ack before it returns
// (`CMA:1888-1897`), so this fixture runs the reference's own service
// container. The standby entries are written one per id in
// `[-1, SERVICE_COUNT)` and `latestStandbySnapshots` wants exactly
// `serviceCount + 1` at a position (`RecordingLog:911-941`).
constexpr std::int32_t SERVICE_COUNT = 1;
// `ConsensusModuleConfig::control_channel`'s default; the container must use
// the same one.
constexpr const char* CONTROL_CHANNEL = "aeron:ipc?term-length=128k";
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

class ConsensusModuleInteropTest : public ::testing::Test
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

        std::string pattern = "/tmp/axc-cm-interop-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;

        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        classes_dir_ = directory_ + "/classes";
        std::filesystem::create_directories(archive_dir_);
        std::filesystem::create_directories(classes_dir_);

        start_java_driver();
        start_java_archive();
        compile_probes_if_available();
    }

    void TearDown() override
    {
        finish_launch();
        module_.reset();
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
            auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
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
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
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
        if (0 == result)
        {
            (void)aeron_archive_close(archive);
        }
        (void)aeron_archive_context_close(context);
        client->close();
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

    /// The reference container is what acks `onStart`, so the two tests that
    /// expect a running module need it; the `refuse` test fails before the
    /// startup ack and runs without one.
    void compile_probes_if_available()
    {
        if (!std::filesystem::exists("/usr/bin/javac") && !std::filesystem::exists("/usr/local/bin/javac"))
        {
            std::fprintf(stderr, "C4a: javac not found, the service container probe is skipped\n");
            return;
        }
        const std::string root = tests_root();
        if (root.empty())
        {
            std::fprintf(stderr, "C4a: AXC_AERON_TESTS is not set, the service container probe is skipped\n");
            return;
        }
        for (const char* const name : {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                std::fprintf(stderr, "C4a: probe source not found under %s: %s\n", root.c_str(), name);
                return;
            }
            probe_sources_.push_back(path);
        }

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        for (const std::string& source : probe_sources_)
        {
            compile.arguments.push_back(source);
        }
        auto compiled = axc::interop::run_process("javac", compile);
        if (!compiled || 0 != compiled->exit_code)
        {
            std::fprintf(stderr, "C4a: probe compilation failed: %s\n",
                         compiled ? compiled->output.c_str() : compiled.error().message.view().data());
            return;
        }
        probes_ready_ = true;
    }

    /// Started after `launch_async`: the container's `ack` in `recoverState`
    /// throws if its publication is not connected yet (`ConsensusModuleProxy
    /// .checkResult:218-228`), and it only connects once the consensus
    /// module's control subscription exists.
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
                // The agent creates its subscriptions after the mark file.
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java container never signalled ready";
    }

    void append_newer_standby_snapshot()
    {
        std::filesystem::create_directories(cluster_dir_);
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log) << log.error().message.view();

        for (std::int32_t service_id = -1; service_id < SERVICE_COUNT; ++service_id)
        {
            auto appended =
                log->append_standby_snapshot(100 + service_id + 1, 1, 0, 500, 1'000, service_id, "localhost:8080");
            ASSERT_TRUE(appended) << appended.error().message.view();
        }
    }

    [[nodiscard]] std::int64_t counter_value(std::int32_t type_id)
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return -1;
        }
        (void)client->do_work();
        const std::int32_t counter_id =
            deepmsg::cluster::common::find_cluster_counter(client->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == counter_id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), counter_id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    std::string jar_;
    std::string directory_;
    std::string aeron_dir_;
    std::string cluster_dir_;
    std::string archive_dir_;
    void launch_async(ConsensusModuleConfig config)
    {
        launcher_ = std::thread([this, config = std::move(config)]() mutable
                                { launched_.emplace(ConsensusModule::launch(std::move(config))); });
    }

    void finish_launch()
    {
        if (launcher_.joinable())
        {
            launcher_.join();
        }
    }

    std::thread launcher_{};
    std::optional<std::expected<ConsensusModule, axc::core::Error>> launched_{};
    std::unique_ptr<axc::interop::ChildProcess> driver_;
    std::unique_ptr<axc::interop::ChildProcess> archive_;
    std::unique_ptr<axc::interop::ChildProcess> container_;
    std::unique_ptr<ConsensusModule> module_;
    std::string classes_dir_{};
    std::vector<std::string> probe_sources_{};
    /// False when javac or the fork's probe sources are missing: the tests
    /// that need a running service skip, `refuseModeStopsTheStart` does not.
    bool probes_ready_{false};
};

TEST_F(ConsensusModuleInteropTest, theSkeletonStartsAgainstTheReferenceArchiveAndPublishesItsState)
{
    if (!probes_ready_)
    {
        GTEST_SKIP() << "the service container probe is required (set AXC_AERON_TESTS)";
    }

    launch_async(make_config());
    start_container();
    finish_launch();

    ASSERT_TRUE(launched_.has_value() && launched_->has_value())
        << (launched_.has_value() ? launched_->error().message.view().data() : "no launch");
    module_ = std::make_unique<ConsensusModule>(std::move(**launched_));

    EXPECT_EQ(ConsensusModuleState::active, module_->agent().module_state());
    EXPECT_GE(module_->agent().archive_id(), 0);
    EXPECT_EQ(static_cast<std::int64_t>(ConsensusModuleState::active),
              counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID));
    EXPECT_EQ(0, counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
    // With a service in place the module is a live single-member cluster: its
    // conductor appends and commits its own leadership term, so the commit
    // position moves off zero (C4a asserted zero, when nothing ran).
    const auto commit_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < commit_deadline &&
           0 == counter_value(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GT(counter_value(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), 0);

    auto mark_file =
        ClusterMarkFile::open_existing(cluster_dir_ + "/cluster-mark.dat", axc::core::EpochClock::now_ms());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    EXPECT_EQ(ClusterComponentType::consensus_module, mark_file->component_type());
    EXPECT_EQ(CLUSTER_ID, mark_file->cluster_id());
    EXPECT_EQ(MEMBER_ID, mark_file->member_id());
    EXPECT_EQ(10, mark_file->archive_stream_id());
    EXPECT_TRUE(std::filesystem::exists(cluster_dir_ + "/node-state.dat"));
    EXPECT_TRUE(std::filesystem::exists(cluster_dir_ + "/recording.log"));

    // The conductor thread is running the loop: its slow tick refreshes the
    // mark file's activity timestamp once per `MARK_FILE_UPDATE_INTERVAL_NS`.
    const std::int64_t activity_before = mark_file->activity_timestamp_volatile();
    std::this_thread::sleep_for(std::chrono::milliseconds(1'200));
    const std::int64_t activity_after = mark_file->activity_timestamp_volatile();
    EXPECT_GT(activity_after, activity_before);

    module_->close();
}

TEST_F(ConsensusModuleInteropTest, aNewerStandbySnapshotWarnsAndCountsAnError)
{
    if (!probes_ready_)
    {
        GTEST_SKIP() << "the service container probe is required (set AXC_AERON_TESTS)";
    }

    append_newer_standby_snapshot();

    auto config = make_config();
    config.standby_snapshot_on_startup = StandbySnapshotStartupMode::warn;

    launch_async(std::move(config));
    start_container();
    finish_launch();

    ASSERT_TRUE(launched_.has_value() && launched_->has_value())
        << (launched_.has_value() ? launched_->error().message.view().data() : "no launch");
    module_ = std::make_unique<ConsensusModule>(std::move(**launched_));

    EXPECT_EQ(ConsensusModuleState::active, module_->agent().module_state());
    EXPECT_EQ(1, counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
}

TEST_F(ConsensusModuleInteropTest, refuseModeStopsTheStart)
{
    append_newer_standby_snapshot();

    auto config = make_config();
    config.standby_snapshot_on_startup = StandbySnapshotStartupMode::refuse;

    auto launched = ConsensusModule::launch(std::move(config));
    ASSERT_FALSE(launched);
    EXPECT_NE(std::string::npos,
              launched.error().message.view().find("aeron.cluster.cpp.standby.snapshot.on.startup=refuse"));

    // The failed start leaves the mark file and its distinct error log behind.
    auto mark_file =
        ClusterMarkFile::open_existing(cluster_dir_ + "/cluster-mark.dat", axc::core::EpochClock::now_ms());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    const std::string errors = ClusterMarkFile::render_error_log(mark_file->error_buffer());
    EXPECT_NE(std::string::npos, errors.find("standby snapshot is newer than the local snapshot"));
}

}  // namespace
