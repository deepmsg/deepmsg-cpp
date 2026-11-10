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
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
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
        std::filesystem::create_directories(archive_dir_);

        start_java_driver();
        start_java_archive();
    }

    void TearDown() override
    {
        module_.reset();
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
        config.service_count = SERVICE_COUNT;
        config.archive_control_response_stream_id = 20;
        return config;
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
    std::unique_ptr<axc::interop::ChildProcess> driver_;
    std::unique_ptr<axc::interop::ChildProcess> archive_;
    std::unique_ptr<ConsensusModule> module_;
};

TEST_F(ConsensusModuleInteropTest, theSkeletonStartsAgainstTheReferenceArchiveAndPublishesItsState)
{
    auto launched = ConsensusModule::launch(make_config());
    ASSERT_TRUE(launched) << launched.error().message.view();
    module_ = std::make_unique<ConsensusModule>(std::move(*launched));

    EXPECT_EQ(ConsensusModuleState::active, module_->agent().state());
    EXPECT_GE(module_->agent().archive_id(), 0);
    EXPECT_EQ(static_cast<std::int64_t>(ConsensusModuleState::active),
              counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID));
    EXPECT_EQ(0, counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
    EXPECT_EQ(0, counter_value(deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID));

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
    append_newer_standby_snapshot();

    auto config = make_config();
    config.standby_snapshot_on_startup = StandbySnapshotStartupMode::warn;

    auto launched = ConsensusModule::launch(std::move(config));
    ASSERT_TRUE(launched) << launched.error().message.view();
    module_ = std::make_unique<ConsensusModule>(std::move(*launched));

    EXPECT_EQ(ConsensusModuleState::active, module_->agent().state());
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
