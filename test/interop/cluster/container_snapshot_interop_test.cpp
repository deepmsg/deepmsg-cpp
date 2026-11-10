// C2b's criteria, both against the reference's consensus module:
//
//  1. a snapshot survives a container restart (BasicAuction's state is read
//     back from the archive recording on recovery);
//  2. the twin's snapshot is the Java BasicAuction's bytes, for the same log —
//     one consensus module, a Java container as service 0 and ours as service
//     1, one SNAPSHOT, two recordings compared byte for byte.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "apps/services/basic-auction-service/basic_auction_service.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_versions.h"
#include "cluster/common/recording_log.h"
#include "cluster/service/client_util.h"
#include "cluster/service/clustered_service_agent.h"
#include "cluster/service/container_config.h"
#include "cluster/service/snapshot.h"
#include "core/agent/agent_runner.h"
#include "core/agent/idle_strategy.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error_handler.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using namespace deepmsg::cluster::service;

constexpr std::int32_t CLUSTER_ID = 8;
constexpr std::int32_t REPLAY_STREAM_ID = 103;
constexpr std::int32_t READER_RESPONSE_STREAM_ID = 9'000;
constexpr std::string_view JAVA_AUCTION = "ClusterAuctionServiceProbe$BasicAuctionClusteredService";

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

class SnapshotInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        if (!std::filesystem::exists("/usr/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
        }

        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }
        std::vector<std::string> sources;
        for (const char* const probe_name : {"ClusterAuctionBidClientProbe.java", "ClusterServiceContainerProbe.java",
                                             "ClusterAuctionServiceProbe.java"})
        {
            const std::string path = find_probe(root, probe_name);
            if (path.empty())
            {
                GTEST_SKIP() << probe_name << " not found under " << root;
            }
            sources.push_back(path);
        }

        directory_ = std::filesystem::temp_directory_path() / ("axc-lc4a-snapshot-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);
        aeron_dir_ = (directory_ / "aeron").string();
        cluster_dir_ = (directory_ / "cluster").string();
        archive_dir_ = (directory_ / "archive").string();
        classes_dir_ = (directory_ / "classes").string();
        std::filesystem::create_directories(classes_dir_);

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        compile.arguments.insert(compile.arguments.end(), sources.begin(), sources.end());
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void TearDown() override
    {
        stop_container_if_running();
        if (java_running_)
        {
            java_.terminate(std::chrono::seconds(5));
        }
        if (java_container_running_)
        {
            java_container_.terminate(std::chrono::seconds(5));
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::vector<std::string> java_node_arguments(bool delete_on_start, std::int32_t service_count) const
    {
        const std::string flag = delete_on_start ? "true" : "false";
        return {"--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeron_dir_,
                "-Daeron.dir.delete.on.start=" + flag,
                "-Daeron.cluster.dir=" + cluster_dir_,
                "-Daeron.cluster.dir.delete.on.start=" + flag,
                "-Daeron.archive.dir=" + archive_dir_,
                "-Daeron.archive.dir.delete.on.start=" + flag,
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
                "-Daeron.cluster.service.count=" + std::to_string(service_count),
                "-Daeron.threading.mode=SHARED",
                "-Daeron.print.configuration=false",
                "-cp",
                jar_,
                "io.aeron.cluster.ClusteredMediaDriver"};
    }

    void start_java_node(bool delete_on_start, std::int32_t service_count)
    {
        auto process = axc::interop::ChildProcess::spawn("java", java_node_arguments(delete_on_start, service_count));
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

    void start_java_container(std::int32_t service_id, const std::string& class_name, bool responder)
    {
        const std::vector<std::string> arguments = {"--add-opens",
                                                    "java.base/jdk.internal.misc=ALL-UNNAMED",
                                                    "--add-opens",
                                                    "java.base/sun.nio.ch=ALL-UNNAMED",
                                                    "-cp",
                                                    jar_ + ":" + classes_dir_,
                                                    "ClusterServiceContainerProbe",
                                                    aeron_dir_,
                                                    cluster_dir_,
                                                    std::to_string(CLUSTER_ID),
                                                    std::to_string(service_id),
                                                    class_name,
                                                    responder ? "true" : "false",
                                                    "aeron:ipc?term-length=128k"};

        auto process = axc::interop::ChildProcess::spawn("java", arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        java_container_ = std::move(*process);
        java_container_running_ = true;

        const std::string mark_path = cluster_dir_ + "/cluster-mark-service-" + std::to_string(service_id) + ".dat";
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(java_container_.running()) << "the Java container exited while starting";
            auto mark = deepmsg::cluster::common::ClusterMarkFile::open_existing(mark_path, 0);
            if (mark && mark->semantic_version() == deepmsg::cluster::common::CLUSTER_MARK_SEMANTIC_VERSION)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java container never signalled ready";
    }

    [[nodiscard]] ServiceContainerConfig container_config(std::int32_t service_id, bool responder) const
    {
        ServiceContainerConfig config;
        config.cluster_id = CLUSTER_ID;
        config.service_id = service_id;
        config.service_name = "clustered-service-" + std::to_string(CLUSTER_ID) + "-" + std::to_string(service_id);
        config.aeron_dir = aeron_dir_;
        config.cluster_dir = cluster_dir_;
        config.control_channel = "aeron:ipc?term-length=128k";
        config.service_stream_id = 104;
        config.consensus_module_stream_id = 105;
        config.log_fragment_limit = 50;
        config.responding_service = responder;
        config.error_buffer_length = 1024 * 1024;
        config.idle_strategy = "org.agrona.concurrent.BackoffIdleStrategy";
        config.app_version = 1;
        config.replay_channel = "aeron:ipc";
        config.replay_stream_id = REPLAY_STREAM_ID;
        config.snapshot_channel = "aeron:ipc?alias=snapshot";
        config.snapshot_stream_id = 106;
        config.archive_local_control_channel = "aeron:ipc?term-length=64k";
        config.archive_local_control_stream_id = 10;
        config.archive_control_response_stream_id = CLUSTER_ID * 100 + 100 + 20 + (service_id + 1);
        return config;
    }

    void start_container(std::int32_t service_id, bool responder)
    {
        agent_ = std::make_unique<ClusteredServiceAgent<deepmsg::apps::BasicAuctionService>>(
            container_config(service_id, responder), deepmsg::apps::BasicAuctionService{});
        auto idle = axc::core::IdleStrategy::by_agrona_name("org.agrona.concurrent.BackoffIdleStrategy");
        ASSERT_TRUE(idle) << idle.error().message.view();
        runner_ = std::make_unique<axc::core::AgentRunner<ClusteredServiceAgent<deepmsg::apps::BasicAuctionService>>>(
            *agent_, *idle, errors_);
        auto started = runner_->start();
        ASSERT_TRUE(started) << started.error().message.view();
        runner_active_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
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

    void stop_container_if_running()
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

    void stop_java_node()
    {
        if (java_running_)
        {
            java_.terminate(std::chrono::seconds(10));
            java_running_ = false;
        }
    }

    void stop_java_container()
    {
        if (java_container_running_)
        {
            java_container_.terminate(std::chrono::seconds(10));
            java_container_running_ = false;
        }
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
        options.timeout = std::chrono::seconds(180);
        auto result = axc::interop::run_process("java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        EXPECT_EQ(0, result->exit_code) << result->output;
        return result->output;
    }

    [[nodiscard]] std::string bid(std::uint64_t bids, std::uint64_t customer, std::uint64_t first_price, bool snapshot,
                                  std::uint64_t hold_millis = 0)
    {
        return run_probe("ClusterAuctionBidClientProbe",
                         {aeron_dir_, std::to_string(CLUSTER_ID), std::to_string(bids), std::to_string(customer),
                          std::to_string(first_price), snapshot ? "true" : "false", std::to_string(hold_millis)});
    }

    [[nodiscard]] std::int64_t snapshot_count()
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return -1;
        }
        (void)client->do_work();
        const std::int32_t counter_id = deepmsg::cluster::common::find_cluster_counter(
            client->counters_reader(), deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == counter_id)
        {
            return 0;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), counter_id);
        return nullptr == address ? 0 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    void await_snapshot_count(std::int64_t expected)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (snapshot_count() >= expected)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the consensus module never counted " << expected << " snapshot(s)";
    }

    [[nodiscard]] std::optional<std::int64_t> snapshot_recording_id(std::int32_t service_id)
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return std::nullopt;
        }
        (void)client->do_work();
        auto* reader = client->counters_reader();

        struct Scan
        {
            std::int32_t cluster_id;
            std::int32_t found;
        } scan{CLUSTER_ID, AERON_NULL_COUNTER_ID};

        aeron_counters_reader_foreach_counter(
            reader,
            [](int64_t, int32_t id, int32_t type_id, const uint8_t* key, size_t key_length, const char*, size_t,
               void* clientd)
            {
                auto* s = static_cast<Scan*>(clientd);
                if (AERON_NULL_COUNTER_ID != s->found ||
                    type_id != deepmsg::cluster::common::CLUSTER_RECOVERY_STATE_TYPE_ID || key_length < 32)
                {
                    return;
                }
                std::int32_t cluster_id = 0;
                std::memcpy(&cluster_id, key + 24, sizeof(cluster_id));
                if (cluster_id == s->cluster_id)
                {
                    s->found = id;
                }
            },
            &scan);

        if (AERON_NULL_COUNTER_ID == scan.found)
        {
            return std::nullopt;
        }

        std::uint8_t* key = nullptr;
        if (aeron_counters_reader_metadata_key(reader, scan.found, &key) < 0 || nullptr == key)
        {
            return std::nullopt;
        }

        std::int32_t service_count = 0;
        std::memcpy(&service_count, key + 28, sizeof(service_count));
        if (service_id < 0 || service_id >= service_count)
        {
            return std::nullopt;
        }

        std::int64_t recording_id = -1;
        std::memcpy(&recording_id, key + 32 + 8 * static_cast<std::size_t>(service_id), sizeof(recording_id));
        return recording_id;
    }

    [[nodiscard]] std::vector<std::uint8_t> read_recording(std::int64_t recording_id)
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        EXPECT_TRUE(client) << client.error().message.view();

        ServiceContainerConfig config = container_config(0, true);
        config.archive_control_response_stream_id = READER_RESPONSE_STREAM_ID;

        auto archive = ArchiveClient::connect(*client, config);
        EXPECT_TRUE(archive) << archive.error().message.view();

        auto subscription_add = client->add_subscription("aeron:ipc", REPLAY_STREAM_ID);
        auto subscription = await_async(*client, subscription_add, 5'000'000'000);
        EXPECT_TRUE(subscription) << subscription.error().message.view();

        aeron_subscription_constants_t constants{};
        EXPECT_EQ(0, aeron_subscription_constants(subscription->get(), &constants));

        auto replay_session =
            archive->start_replay(recording_id, "aeron:ipc", REPLAY_STREAM_ID, constants.registration_id);
        EXPECT_TRUE(replay_session) << replay_session.error().message.view();

        aeron_image_t* image = nullptr;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (nullptr == image && std::chrono::steady_clock::now() < deadline)
        {
            (void)client->do_work();
            image =
                aeron_subscription_image_by_session_id(subscription->get(), static_cast<std::int32_t>(*replay_session));
            if (nullptr == image)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        EXPECT_NE(nullptr, image);

        std::vector<std::uint8_t> bytes;
        auto handler = [](void* clientd, const std::uint8_t* buffer, size_t length, aeron_header_t*)
        {
            static_cast<std::vector<std::uint8_t>*>(clientd)->insert(
                static_cast<std::vector<std::uint8_t>*>(clientd)->end(), buffer, buffer + length);
        };

        while (std::chrono::steady_clock::now() < deadline)
        {
            const bool end = aeron_image_is_end_of_stream(image);
            const int work = aeron_image_poll(image, handler, &bytes, 100);
            if (end && 0 == work)
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        (void)aeron_subscription_image_release(subscription->get(), image);
        return bytes;
    }

    std::string jar_{};
    std::filesystem::path directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    axc::interop::ChildProcess java_{};
    bool java_running_{false};
    axc::interop::ChildProcess java_container_{};
    bool java_container_running_{false};
    axc::core::NoOpErrorHandler errors_{};
    std::unique_ptr<ClusteredServiceAgent<deepmsg::apps::BasicAuctionService>> agent_{};
    std::unique_ptr<axc::core::AgentRunner<ClusteredServiceAgent<deepmsg::apps::BasicAuctionService>>> runner_{};
    bool runner_active_{false};
};

TEST_F(SnapshotInteropTest, aSnapshotSurvivesAContainerRestart)
{
    start_java_node(true, 1);
    start_container(0, true);

    const std::string bids = bid(10, 1, 100, true);
    EXPECT_NE(std::string::npos, bids.find("RECEIVED 10")) << bids;
    EXPECT_NE(std::string::npos, bids.find("LAST 1 109 true")) << bids;
    EXPECT_NE(std::string::npos, bids.find("SNAPSHOT_REQUESTED")) << bids;

    await_snapshot_count(1);

    stop_container_if_running();
    stop_java_node();

    // The module comes back without a container: its recovery state names the
    // snapshot recording, and it waits for the service's ack.
    start_java_node(false, 1);

    std::optional<std::int64_t> recording_id;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        recording_id = snapshot_recording_id(0);
        if (recording_id.has_value() && *recording_id != -1)
        {
            break;
        }
        recording_id.reset();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(recording_id.has_value()) << "the recovery state never named the service snapshot";

    start_container(0, true);

    const std::string recovered = bid(1, 99, 0, false);
    EXPECT_NE(std::string::npos, recovered.find("LAST 1 109 false"))
        << "the recovered auction answered with the snapshot's state: " << recovered;
}

TEST_F(SnapshotInteropTest, theTwinsSnapshotIsTheJavaOnesByteForByte)
{
    start_java_node(true, 2);
    start_java_container(0, std::string{JAVA_AUCTION}, true);
    start_container(1, false);

    std::string first_reading;
    std::string second_reading;
    std::thread first{[this, &first_reading] { first_reading = bid(1, 1, 100, false, 8'000); }};
    std::thread second{[this, &second_reading] { second_reading = bid(1, 2, 200, false, 8'000); }};

    std::this_thread::sleep_for(std::chrono::seconds(3));

    const std::string toggle = bid(0, 0, 0, true);
    EXPECT_NE(std::string::npos, toggle.find("SNAPSHOT_REQUESTED")) << toggle;
    await_snapshot_count(1);

    first.join();
    second.join();
    EXPECT_NE(std::string::npos, first_reading.find("LAST ")) << first_reading;
    EXPECT_NE(std::string::npos, second_reading.find("LAST ")) << second_reading;

    // The recording log carries the snapshot entries and their recording ids.
    std::optional<std::int64_t> java_recording;
    std::optional<std::int64_t> our_recording;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        if (log)
        {
            const auto* java_snapshot = log->get_latest_snapshot(0);
            const auto* our_snapshot = log->get_latest_snapshot(1);
            if (nullptr != java_snapshot && nullptr != our_snapshot &&
                java_snapshot->log_position == our_snapshot->log_position)
            {
                java_recording = java_snapshot->recording_id;
                our_recording = our_snapshot->recording_id;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(java_recording.has_value()) << "the recording log never carried both snapshot entries";
    ASSERT_TRUE(our_recording.has_value());

    const auto java_bytes = read_recording(*java_recording);
    const auto our_bytes = read_recording(*our_recording);

    ASSERT_EQ(java_bytes.size(), our_bytes.size()) << "the two snapshots are the same number of bytes";
    EXPECT_TRUE(java_bytes == our_bytes) << "our twin's snapshot is the Java one's bytes";
}

}  // namespace
