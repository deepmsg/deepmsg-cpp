// A5a criteria: the three threading modes of the real binary
// (`ArchiveThreadingMode`, `Archive.java:148-170`), the reference thread names
// (`Archive.java:126-134`, `CommonContext.java:582-591`) and the duty-cycle
// counters 103/104 (`Archive.java:1470-1545`).
//
// The archive runs as the real child process: the three caller-driven cases use
// the standalone shape (`io.aeron.archive.Archive`) against `aeronmd`, the
// embedded case uses `ArchivingMediaDriver` with its own driver. Every mode has
// to complete one record/list/replay round through the reference C client.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "archive/server/recording_pos.h"
#include "fixture/c_driver.h"
#include "fixture/media_driver.h"
#include "fixture/process.h"

// The internal archive client headers carry no `extern "C"` guard.
extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
}

#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT "aeronmd"
#endif

#ifndef AXC_ARCHIVE_SERVER_BIN
#define AXC_ARCHIVE_SERVER_BIN "deepmsg-archive-server"
#endif

namespace
{

using axc::interop::CDriver;
using axc::interop::ChildProcess;

constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t LOCAL_CONTROL_STREAM_ID = 12;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr std::int32_t RECORDING_STREAM_ID = 33;
constexpr std::int32_t REPLAY_STREAM_ID = 34;
constexpr std::int64_t ARCHIVE_ID = 42;
constexpr std::int32_t MESSAGE_COUNT = 10;
constexpr std::int32_t MESSAGE_LENGTH = 64;

struct CounterLabel
{
    std::int32_t type_id{0};
    std::string label;
};

void collect_counter(std::int64_t value, std::int32_t id, std::int32_t type_id, const std::uint8_t* key,
                     std::size_t key_length, const char* label, std::size_t label_length, void* clientd)
{
    (void)value;
    (void)id;
    (void)key;
    (void)key_length;
    auto* labels = static_cast<std::vector<CounterLabel>*>(clientd);
    labels->push_back({type_id, std::string{label, label_length}});
}

[[nodiscard]] std::vector<std::string> task_names(int pid)
{
    std::vector<std::string> names;
    const std::filesystem::path tasks = std::filesystem::path{"/proc"} / std::to_string(pid) / "task";
    std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator{tasks, error})
    {
        std::ifstream comm{entry.path() / "comm"};
        std::string name;
        if (std::getline(comm, name))
        {
            names.push_back(name);
        }
    }
    return names;
}

[[nodiscard]] bool contains(const std::vector<std::string>& values, std::string_view value)
{
    for (const std::string& candidate : values)
    {
        if (candidate == value)
        {
            return true;
        }
    }
    return false;
}

class ThreadingInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-threading-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        archive_dir_ = root_ + "/archive";
        std::filesystem::create_directories(archive_dir_);

        if (!std::filesystem::exists(axc::interop::media_driver_path()))
        {
            GTEST_SKIP() << "aeronmd not built at " << axc::interop::media_driver_path();
        }
        if (!std::filesystem::exists(AXC_ARCHIVE_SERVER_BIN))
        {
            GTEST_SKIP() << "archive server not built at " << AXC_ARCHIVE_SERVER_BIN;
        }
    }

    void TearDown() override
    {
        close_client();
        archive_.terminate();
        driver_.stop();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    /// Starts the driver (standalone cases) and the archive child in `mode`.
    /// `recorder_threads` is the A6f extension (default 1 = the reference);
    /// `events_enabled` turns on the recording-events publication path.
    void start_archive(std::string_view mode, bool standalone, std::int32_t recorder_threads = 1,
                       bool events_enabled = false)
    {
        if (standalone)
        {
            auto driver =
                CDriver::start(axc::interop::media_driver_path(), aeron_dir_, {"AERON_SPIES_SIMULATE_CONNECTION=true"});
            ASSERT_TRUE(driver) << driver.error().message.view();
            driver_ = std::move(*driver);
        }

        std::vector<std::string> arguments = {
            "-Daeron.dir=" + aeron_dir_, "-Daeron.archive.dir=" + archive_dir_,
            "-Daeron.archive.control.channel=aeron:ipc",
            "-Daeron.archive.control.stream.id=" + std::to_string(CONTROL_STREAM_ID),
            "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k",
            "-Daeron.archive.local.control.stream.id=" + std::to_string(LOCAL_CONTROL_STREAM_ID),
            "-Daeron.archive.id=" + std::to_string(ARCHIVE_ID), "-Daeron.archive.threading.mode=" + std::string{mode},
            "-Daeron.thread.naming=new", "-Daeron.archive.idle.strategy=yield",
            "-Daeron.archive.segment.file.length=131072", "-Daeron.archive.dir.delete.on.start=true",
            events_enabled ? "-Daeron.archive.recording.events.enabled=true"
                           : "-Daeron.archive.recording.events.enabled=false",
            "-Daeron.archive.recording.events.channel=aeron:ipc",
            // LOCAL + UDP records through an `aeron-spy:` subscription; the
            // driver only counts a spy as a subscriber with this on
            // (`TestArchive.h` passes it too).
            "-Daeron.spies.simulate.connection=true"};
        if (1 != recorder_threads)
        {
            arguments.push_back("-Ddeepmsg.archive.recorder.threads=" + std::to_string(recorder_threads));
        }
        arguments.push_back(standalone ? "io.aeron.archive.Archive" : "io.aeron.archive.ArchivingMediaDriver");

        auto spawned = ChildProcess::spawn(AXC_ARCHIVE_SERVER_BIN, arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        archive_ = std::move(*spawned);

        connect_client();
    }

    void connect_client()
    {
        ASSERT_EQ(0, aeron_archive_context_init(&archive_context_));
        aeron_archive_context_set_aeron_directory_name(archive_context_, aeron_dir_.c_str());
        aeron_archive_context_set_control_request_channel(archive_context_, "aeron:ipc");
        aeron_archive_context_set_control_request_stream_id(archive_context_, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(archive_context_, "aeron:ipc");
        aeron_archive_context_set_control_response_stream_id(archive_context_, RESPONSE_STREAM_ID);
        aeron_archive_context_set_message_timeout_ns(archive_context_, 2'000'000'000LL);

        aeron_archive_async_connect_t* async = nullptr;
        ASSERT_EQ(0, aeron_archive_async_connect(&async, archive_context_));
        aeron_archive_t* archive = nullptr;
        for (int iteration = 0; iteration < 500 && nullptr == archive; iteration++)
        {
            const int poll = aeron_archive_async_connect_poll(&archive, async);
            ASSERT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                async = nullptr;
            }
            if (nullptr == archive)
            {
                ::usleep(10000);
            }
        }
        client_archive_ = archive;
        connected_ = nullptr != archive;
        if (connected_)
        {
            aeron_ = aeron_archive_context_get_aeron(archive_context_);
        }
    }

    void close_client()
    {
        if (nullptr != client_archive_)
        {
            EXPECT_EQ(0, aeron_archive_close(client_archive_));
            client_archive_ = nullptr;
        }
        if (nullptr != archive_context_)
        {
            EXPECT_EQ(0, aeron_archive_context_close(archive_context_));
            archive_context_ = nullptr;
            aeron_ = nullptr;
        }
    }

    /// The C harness's `waitUntilCaughtUp`: the recording position counter
    /// reaches the publication position before the stop is requested.
    void wait_caught_up(std::int32_t session_id, std::int64_t position)
    {
        auto* reader = aeron_counters_reader(aeron_);
        std::int32_t counter_id = AERON_NULL_COUNTER_ID;
        for (int iteration = 0; iteration < 5000 && AERON_NULL_COUNTER_ID == counter_id; iteration++)
        {
            counter_id = deepmsg::archive::server::RecordingPos::find_by_session(reader, ARCHIVE_ID, session_id);
            if (AERON_NULL_COUNTER_ID == counter_id)
            {
                ::usleep(200);
            }
        }
        ASSERT_NE(AERON_NULL_COUNTER_ID, counter_id) << "recording position counter not found";

        std::int64_t* address = aeron_counters_reader_addr(reader, counter_id);
        for (int iteration = 0; iteration < 25'000; iteration++)
        {
            if (std::atomic_ref<std::int64_t>(*address).load(std::memory_order_acquire) >= position)
            {
                return;
            }
            ::usleep(200);
        }
        ADD_FAILURE() << "recording position never reached " << position;
    }

    /// One record → stop → list → replay round through the reference client.
    void run_round()
    {
        const std::string recording_channel = "aeron:udp?endpoint=localhost:33433";

        std::int64_t subscription_id = -1;
        ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, client_archive_, recording_channel.c_str(),
                                                   RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false))
            << aeron_errmsg();

        aeron_publication_t* publication = nullptr;
        aeron_async_add_publication_t* publication_add = nullptr;
        ASSERT_EQ(
            0, aeron_async_add_publication(&publication_add, aeron_, recording_channel.c_str(), RECORDING_STREAM_ID));
        for (int iteration = 0; iteration < 5000 && nullptr == publication; iteration++)
        {
            const int poll = aeron_async_add_publication_poll(&publication, publication_add);
            ASSERT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                publication_add = nullptr;
            }
            if (nullptr == publication)
            {
                ::usleep(200);
            }
        }
        ASSERT_NE(nullptr, publication);

        std::array<std::uint8_t, MESSAGE_LENGTH> message{};
        for (std::int32_t index = 0; index < MESSAGE_COUNT; index++)
        {
            while (aeron_publication_offer(publication, message.data(), message.size(), nullptr, nullptr) < 0)
            {
                ::usleep(100);
            }
        }
        const std::int64_t position = aeron_publication_position(publication);
        wait_caught_up(aeron_publication_session_id(publication), position);

        ASSERT_EQ(0, aeron_archive_stop_recording_subscription(client_archive_, subscription_id)) << aeron_errmsg();

        std::int64_t stop_position = -1;
        for (int iteration = 0; iteration < 5000; iteration++)
        {
            ASSERT_EQ(0, aeron_archive_get_stop_position(&stop_position, client_archive_, 0));
            if (stop_position == position)
            {
                break;
            }
            ::usleep(200);
        }
        EXPECT_EQ(position, stop_position);

        std::int32_t count = 0;
        aeron_archive_recording_descriptor_t descriptor{};
        const auto consume = [](aeron_archive_recording_descriptor_t* listed, void* clientd)
        { *static_cast<aeron_archive_recording_descriptor_t*>(clientd) = *listed; };
        ASSERT_EQ(0, aeron_archive_list_recording(&count, client_archive_, 0, consume, &descriptor)) << aeron_errmsg();
        EXPECT_EQ(1, count);

        std::int64_t found = -1;
        ASSERT_EQ(0, aeron_archive_get_stop_position(&found, client_archive_, descriptor.recording_id));

        // Replay the recording on the destination archive side.
        aeron_subscription_t* replay_subscription = nullptr;
        aeron_async_add_subscription_t* subscription_add = nullptr;
        const std::string replay_channel = "aeron:udp?endpoint=localhost:33434";
        ASSERT_EQ(0, aeron_async_add_subscription(&subscription_add, aeron_, replay_channel.c_str(), REPLAY_STREAM_ID,
                                                  nullptr, nullptr, nullptr, nullptr));
        for (int iteration = 0; iteration < 5000 && nullptr == replay_subscription; iteration++)
        {
            const int poll = aeron_async_add_subscription_poll(&replay_subscription, subscription_add);
            ASSERT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                subscription_add = nullptr;
            }
            if (nullptr == replay_subscription)
            {
                ::usleep(200);
            }
        }
        ASSERT_NE(nullptr, replay_subscription);

        aeron_archive_replay_params_t replay_params;
        ASSERT_EQ(0, aeron_archive_replay_params_init(&replay_params));
        replay_params.position = 0;
        replay_params.length = position;

        std::int64_t replay_session_id = -1;
        ASSERT_EQ(0, aeron_archive_start_replay(&replay_session_id, client_archive_, descriptor.recording_id,
                                                replay_channel.c_str(), REPLAY_STREAM_ID, &replay_params))
            << aeron_errmsg();

        std::int32_t received = 0;
        for (int iteration = 0; iteration < 10'000 && received < MESSAGE_COUNT; iteration++)
        {
            const int fragments = aeron_subscription_poll(
                replay_subscription,
                [](void* clientd, const std::uint8_t* /*buffer*/, std::size_t /*length*/, aeron_header_t* /*header*/)
                { ++*static_cast<std::int32_t*>(clientd); }, &received, 10);
            ASSERT_GE(fragments, 0) << aeron_errmsg();
            if (0 == fragments)
            {
                ::usleep(200);
            }
        }
        EXPECT_EQ(MESSAGE_COUNT, received);

        EXPECT_EQ(0, aeron_subscription_close(replay_subscription, nullptr, nullptr));
        EXPECT_EQ(0, aeron_publication_close(publication, nullptr, nullptr));
    }

    [[nodiscard]] std::vector<std::string> wait_for_task_names(std::string_view first, std::string_view second,
                                                               std::string_view third)
    {
        std::vector<std::string> names;
        for (int iteration = 0; iteration < 200; iteration++)
        {
            names = task_names(archive_.pid());
            if (contains(names, first) && contains(names, second) && contains(names, third))
            {
                return names;
            }
            ::usleep(10000);
        }
        return names;
    }

    [[nodiscard]] std::vector<CounterLabel> counter_labels()
    {
        std::vector<CounterLabel> labels;
        aeron_counters_reader_foreach_counter(aeron_counters_reader(aeron_), &collect_counter, &labels);
        return labels;
    }

    [[nodiscard]] static bool has_label(const std::vector<CounterLabel>& labels, std::string_view fragment)
    {
        for (const CounterLabel& label : labels)
        {
            if (label.label.find(fragment) != std::string::npos)
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] static std::size_t count_type(const std::vector<CounterLabel>& labels, std::int32_t type_id)
    {
        std::size_t count = 0;
        for (const CounterLabel& label : labels)
        {
            if (type_id == label.type_id)
            {
                ++count;
            }
        }
        return count;
    }

    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    CDriver driver_;
    ChildProcess archive_;
    bool connected_{false};
    aeron_archive_t* client_archive_{nullptr};
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_t* aeron_{nullptr};
};

TEST_F(ThreadingInteropTest, dedicatedModeRunsThreeNamedAgentsAndTheRecorderReplayerCounters)
{
    start_archive("DEDICATED", true);
    ASSERT_TRUE(connected_) << "the archive client did not connect";

    const auto names = wait_for_task_names("aeron-ar-cnd", "aeron-ar-rec", "aeron-ar-rep");
    EXPECT_TRUE(contains(names, "aeron-ar-cnd"));
    EXPECT_TRUE(contains(names, "aeron-ar-rec"));
    EXPECT_TRUE(contains(names, "aeron-ar-rep"));

    run_round();

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-conductor max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-recorder max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-replayer max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-conductor work cycle time exceeded count: threshold="));
    EXPECT_EQ(3u, count_type(labels, 103));
    EXPECT_EQ(3u, count_type(labels, 104));
}

// A6f: N > 1 recorders start one agent each (the classic name is truncated by
// `pthread_setname_np`, hence the short stem) and get one labelled duty-cycle
// pair each, while the three write counters keep a single writer.
TEST_F(ThreadingInteropTest, twoRecordersRunTwoAgentsAndTwoLabelledCounterPairs)
{
    start_archive("DEDICATED", true, 2);
    ASSERT_TRUE(connected_) << "the archive client did not connect";

    const auto names = wait_for_task_names("aeron-ar-cnd", "aeron-ar-rec-0", "aeron-ar-rec-1");
    EXPECT_TRUE(contains(names, "aeron-ar-rec-0"));
    EXPECT_TRUE(contains(names, "aeron-ar-rec-1"));

    run_round();

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-recorder-0 max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-recorder-1 max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-recorder-0 work cycle time exceeded count: threshold="));
    EXPECT_TRUE(has_label(labels, "archive-recorder-1 work cycle time exceeded count: threshold="));
    // conductor + replayer + two recorders.
    EXPECT_EQ(4u, count_type(labels, 103));
    EXPECT_EQ(4u, count_type(labels, 104));
}

// A6f-3: with events enabled, N recorders own one publication each (a single
// publication has one writer); the recording path still runs end to end.
TEST_F(ThreadingInteropTest, twoRecordersWithRecordingEventsCompleteARound)
{
    start_archive("DEDICATED", true, 2, true);
    ASSERT_TRUE(connected_) << "the archive client did not connect";

    const auto names = wait_for_task_names("aeron-ar-cnd", "aeron-ar-rec-0", "aeron-ar-rec-1");
    EXPECT_TRUE(contains(names, "aeron-ar-rec-0"));
    EXPECT_TRUE(contains(names, "aeron-ar-rec-1"));

    run_round();

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-recorder-0 max cycle time in ns: DEDICATED"));
    EXPECT_TRUE(has_label(labels, "archive-recorder-1 max cycle time in ns: DEDICATED"));
}

TEST_F(ThreadingInteropTest, sharedModeRunsCallerDrivenAndStartsNoWorkerThread)
{
    start_archive("SHARED", true);
    ASSERT_TRUE(connected_) << "the archive client did not connect";
    run_round();

    const auto names = task_names(archive_.pid());
    EXPECT_FALSE(contains(names, "aeron-ar-cnd"));
    EXPECT_FALSE(contains(names, "aeron-ar-rec"));
    EXPECT_FALSE(contains(names, "aeron-ar-rep"));

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-conductor max cycle time in ns: SHARED"));
    EXPECT_FALSE(has_label(labels, "archive-recorder max cycle time in ns:"));
    EXPECT_FALSE(has_label(labels, "archive-replayer max cycle time in ns:"));
    EXPECT_EQ(1u, count_type(labels, 103));
    EXPECT_EQ(1u, count_type(labels, 104));
}

TEST_F(ThreadingInteropTest, invokerModeRunsCallerDrivenAndStartNoAgentThread)
{
    start_archive("INVOKER", true);
    ASSERT_TRUE(connected_) << "the archive client did not connect";
    run_round();

    const auto names = task_names(archive_.pid());
    EXPECT_FALSE(contains(names, "aeron-ar-cnd"));
    EXPECT_FALSE(contains(names, "aeron-ar-shd"));

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-conductor max cycle time in ns: INVOKER"));
    EXPECT_EQ(1u, count_type(labels, 103));
}

TEST_F(ThreadingInteropTest, theEmbeddedShapeRunsDedicatedToo)
{
    // The archive child owns the driver in this shape (no `aeronmd`).
    start_archive("DEDICATED", false);
    ASSERT_TRUE(connected_) << "the archive client did not connect";

    const auto names = wait_for_task_names("aeron-ar-cnd", "aeron-ar-rec", "aeron-ar-rep");
    EXPECT_TRUE(contains(names, "aeron-ar-cnd"));
    EXPECT_TRUE(contains(names, "aeron-ar-rec"));
    EXPECT_TRUE(contains(names, "aeron-ar-rep"));

    const auto labels = counter_labels();
    EXPECT_TRUE(has_label(labels, "archive-conductor max cycle time in ns: DEDICATED"));
    EXPECT_EQ(3u, count_type(labels, 103));

    run_round();
}

}  // namespace
