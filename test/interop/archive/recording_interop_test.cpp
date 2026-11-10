// A2b-4 criterion: the stop family (`StopRecordingSubscriptionRequest` 14 and
// `StopRecordingByIdentityRequest` 65) ends a live recording, writes the stop
// position into the catalog and sends the STOP signal.
//
// Both peers run in this process: `aeronmd` is the interop fixture's C driver,
// the archive is `ArchiveConductor` (driven on a test thread), the control
// client is `aeron_archive_*`, and the recording publication is a plain C
// client. The archive subscribes with `aeron-spy:` (LOCAL + UDP), so the driver
// is started with `AERON_SPIES_SIMULATE_CONNECTION=true`.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "archive/server/archive_conductor.h"
#include "archive/server/catalog.h"
#include "archive/server/recording_pos.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
#include "fixture/c_driver.h"
#include "fixture/java_process.h"

#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT "aeronmd"
#endif

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::archive::server::ArchiveConductor;
using deepmsg::archive::server::ArchiveConductorContext;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOpenOptions;
using deepmsg::archive::server::RecordingPos;

constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr std::int32_t RECORDING_STREAM_ID = 33;
constexpr std::int64_t ARCHIVE_ID = 42;
constexpr std::int32_t MESSAGE_COUNT = 10;
constexpr std::int32_t MESSAGE_LENGTH = 64;

/// The reference `aeron-all` jar, or "" when it was not built.
[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

/// `ArchiveTool`'s arguments (`CatalogInteropTest.archive_tool_arguments`).
[[nodiscard]] std::vector<std::string> archive_tool_arguments(const std::string& jar, const std::string& archive_dir)
{
    return {"--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-cp",
            jar,
            "io.aeron.archive.ArchiveTool",
            archive_dir};
}

class RecordingInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-recording-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        archive_dir_ = root_ + "/archive";
        std::filesystem::create_directories(archive_dir_);

        if (!std::filesystem::exists(AXC_AERONMD_DEFAULT))
        {
            GTEST_SKIP() << "aeronmd not built at " << AXC_AERONMD_DEFAULT;
        }

        auto driver =
            axc::interop::CDriver::start(AXC_AERONMD_DEFAULT, aeron_dir_, {"AERON_SPIES_SIMULATE_CONNECTION=true"});
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::move(*driver);
    }

    void TearDown() override
    {
        stop_conductor();
        if (nullptr != client_archive_)
        {
            EXPECT_EQ(0, aeron_archive_close(client_archive_));
            client_archive_ = nullptr;
        }
        if (nullptr != archive_context_)
        {
            EXPECT_EQ(0, aeron_archive_context_close(archive_context_));
            archive_context_ = nullptr;
        }
        if (nullptr != aeron_)
        {
            EXPECT_EQ(0, aeron_close(aeron_));
            aeron_ = nullptr;
        }
        if (nullptr != aeron_context_)
        {
            EXPECT_EQ(0, aeron_context_close(aeron_context_));
            aeron_context_ = nullptr;
        }
        driver_.stop();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    void start_conductor()
    {
        ArchiveConductorContext context;
        context.archive_dir = archive_dir_;
        context.aeron_dir = aeron_dir_;
        context.control_channel = "aeron:ipc";
        context.control_stream_id = CONTROL_STREAM_ID;
        // Same test-side split as `ConductorInteropTest`: the local control
        // channel defaults to IPC and the same stream id, which would deliver
        // every request twice.
        context.local_control_channel = "aeron:ipc?term-length=64k";
        context.local_control_stream_id = CONTROL_STREAM_ID + 2;
        context.archive_id = ARCHIVE_ID;
        context.recording.segment_file_length = 128 * 1024;
        context.recording.file_io_max_length = 64 * 1024;

        auto created = ArchiveConductor::create(context);
        ASSERT_TRUE(created) << created.error().message.view();
        conductor_ = std::move(*created);

        running_ = true;
        worker_ = std::thread(
            [this]()
            {
                while (running_)
                {
                    conductor_->do_work();
                    ::usleep(100);
                }
            });
    }

    void stop_conductor()
    {
        if (worker_.joinable())
        {
            running_ = false;
            worker_.join();
        }
    }

    void connect_client()
    {
        ASSERT_EQ(0, aeron_archive_context_init(&archive_context_));
        aeron_archive_context_set_aeron_directory_name(archive_context_, aeron_dir_.c_str());
        aeron_archive_context_set_control_request_channel(archive_context_, "aeron:ipc");
        aeron_archive_context_set_control_request_stream_id(archive_context_, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(archive_context_, "aeron:ipc");
        aeron_archive_context_set_control_response_stream_id(archive_context_, RESPONSE_STREAM_ID);

        aeron_archive_async_connect_t* async = nullptr;
        ASSERT_EQ(0, aeron_archive_async_connect(&async, archive_context_));
        async_ = async;

        for (int iteration = 0; iteration < 20'000; iteration++)
        {
            aeron_archive_t* archive = nullptr;
            const int poll = aeron_archive_async_connect_poll(&archive, async);
            ASSERT_GE(poll, 0) << aeron_errmsg();
            if (nullptr != archive)
            {
                client_archive_ = archive;
                return;
            }
            ::usleep(200);
        }

        FAIL() << "the async connect did not complete";
    }

    void start_local_aeron()
    {
        ASSERT_EQ(0, aeron_context_init(&aeron_context_));
        aeron_context_set_dir(aeron_context_, aeron_dir_.c_str());
        ASSERT_EQ(0, aeron_init(&aeron_, aeron_context_));
        ASSERT_EQ(0, aeron_start(aeron_));
    }

    [[nodiscard]] std::string recording_channel() const
    {
        return "aeron:udp?endpoint=localhost:34433|term-length=64k";
    }

    [[nodiscard]] std::int64_t wait_for_recording_counter(std::int32_t session_id)
    {
        aeron_counters_reader_t* reader = aeron_counters_reader(aeron_);
        for (int iteration = 0; iteration < 20'000; iteration++)
        {
            const std::int32_t counter_id = RecordingPos::find_by_session(reader, ARCHIVE_ID, session_id);
            if (AERON_NULL_COUNTER_ID != counter_id)
            {
                return counter_id;
            }
            ::usleep(200);
        }
        return AERON_NULL_COUNTER_ID;
    }

    [[nodiscard]] std::int64_t recording_id_of(std::int32_t counter_id)
    {
        std::uint8_t* key = nullptr;
        if (0 != aeron_counters_reader_metadata_key(aeron_counters_reader(aeron_), counter_id, &key) || nullptr == key)
        {
            return -1;
        }

        std::int32_t identity_length = 0;
        std::memcpy(&identity_length, key + RecordingPos::SOURCE_IDENTITY_LENGTH_OFFSET, sizeof(identity_length));
        const auto view = RecordingPos::parse_key(
            {key, static_cast<std::size_t>(RecordingPos::SOURCE_IDENTITY_OFFSET + identity_length + 8)});
        return view.has_value() ? view->recording_id : -1;
    }

    [[nodiscard]] std::int64_t wait_for_position(std::int32_t counter_id, std::int64_t position)
    {
        std::int64_t* address = aeron_counters_reader_addr(aeron_counters_reader(aeron_), counter_id);
        std::int64_t value = 0;
        for (int iteration = 0; iteration < 50'000; iteration++)
        {
            value = std::atomic_ref<std::int64_t>(*address).load(std::memory_order_acquire);
            if (value >= position)
            {
                return value;
            }
            ::usleep(200);
        }
        return value;
    }

    struct Published
    {
        aeron_publication_t* publication{nullptr};
        std::int64_t position{0};
        std::int32_t session_id{0};
    };

    // Publishes `MESSAGE_COUNT` messages on the recording channel and returns
    // the publication position (the recording has to reach it).
    Published publish_messages()
    {
        Published published;
        aeron_async_add_publication_t* async = nullptr;
        EXPECT_EQ(0, aeron_async_add_publication(&async, aeron_, recording_channel().c_str(), RECORDING_STREAM_ID));
        for (int iteration = 0; iteration < 20'000 && nullptr == published.publication; iteration++)
        {
            const int result = aeron_async_add_publication_poll(&published.publication, async);
            EXPECT_GE(result, 0) << aeron_errmsg();
            if (result < 0)
            {
                break;
            }
            if (nullptr == published.publication)
            {
                ::usleep(200);
            }
        }
        EXPECT_NE(nullptr, published.publication);
        published.session_id = aeron_publication_session_id(published.publication);

        std::array<std::uint8_t, MESSAGE_LENGTH> message{};
        for (std::int32_t index = 0; index < MESSAGE_COUNT; index++)
        {
            std::int64_t result = 0;
            while ((result = aeron_publication_offer(published.publication, message.data(), message.size(), nullptr,
                                                     nullptr)) < 0)
            {
                ::usleep(100);
            }
        }

        published.position = aeron_publication_position(published.publication);
        // Each message carries the 32-byte data header (the frame is already
        // 32-byte aligned at this length).
        EXPECT_EQ(static_cast<std::int64_t>(MESSAGE_COUNT) * (MESSAGE_LENGTH + 32), published.position);
        return published;
    }

    [[nodiscard]] std::expected<axc::interop::ProcessResult, axc::core::Error> run_archive_tool(
        const std::string& subcommand)
    {
        const std::string jar = aeron_jar();
        if (jar.empty() || !std::filesystem::exists(jar))
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::not_found,
                                                         "reference jar not found (set AXC_AERON_JAR): " + jar));
        }

        std::vector<std::string> arguments = archive_tool_arguments(jar, archive_dir_);
        arguments.push_back(subcommand);
        return java_.run(arguments, std::chrono::seconds(60));
    }

    [[nodiscard]] std::int64_t catalog_stop_position(std::int64_t recording_id)
    {
        CatalogOpenOptions options;
        options.archive_dir = archive_dir_;
        options.catalog_capacity = 1024 * 1024;
        auto catalog = Catalog::open(options);
        EXPECT_TRUE(catalog) << (catalog ? std::string{} : std::string{catalog.error().message.view()});
        if (!catalog)
        {
            return -1;
        }
        const std::int64_t position = catalog->stop_position(recording_id);
        (void)catalog->close();
        return position;
    }

    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    axc::interop::CDriver driver_;
    std::unique_ptr<ArchiveConductor> conductor_;
    std::thread worker_;
    std::atomic<bool> running_{false};
    aeron_archive_t* client_archive_{nullptr};
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_async_connect_t* async_{nullptr};
    aeron_t* aeron_{nullptr};
    aeron_context_t* aeron_context_{nullptr};
    axc::interop::JavaProcess java_{};
};

TEST_F(RecordingInteropTest, stopBySubscriptionEndsTheRecordingAndWritesTheStopPosition)
{
    start_conductor();
    connect_client();
    start_local_aeron();

    std::int64_t subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));

    const Published published = publish_messages();
    const std::int64_t counter_id = wait_for_recording_counter(published.session_id);
    ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), counter_id);
    const std::int64_t recording_id = recording_id_of(static_cast<std::int32_t>(counter_id));
    ASSERT_GE(recording_id, 0);
    EXPECT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(counter_id), published.position));

    // The four position queries while the recording is live.
    std::int64_t start_position = -1;
    ASSERT_EQ(0, aeron_archive_get_start_position(&start_position, client_archive_, recording_id));
    EXPECT_EQ(0, start_position);

    std::int64_t recording_position = -1;
    ASSERT_EQ(0, aeron_archive_get_recording_position(&recording_position, client_archive_, recording_id));
    EXPECT_EQ(published.position, recording_position);

    std::int64_t stop_position = 0;
    ASSERT_EQ(0, aeron_archive_get_stop_position(&stop_position, client_archive_, recording_id));
    EXPECT_EQ(AERON_NULL_VALUE, stop_position);

    std::int64_t max_recorded_position = -1;
    ASSERT_EQ(0, aeron_archive_get_max_recorded_position(&max_recorded_position, client_archive_, recording_id));
    EXPECT_EQ(published.position, max_recorded_position);

    ASSERT_EQ(0, aeron_archive_stop_recording_subscription(client_archive_, subscription_id));
    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    EXPECT_EQ(0u, conductor_->recording_session_count());

    // After the stop: the counter is gone, the catalog carries the stop
    // position, and the max recorded position falls back to it.
    std::int64_t recording_position_after = 0;
    ASSERT_EQ(0, aeron_archive_get_recording_position(&recording_position_after, client_archive_, recording_id));
    EXPECT_EQ(AERON_NULL_VALUE, recording_position_after);

    std::int64_t stop_position_after = 0;
    ASSERT_EQ(0, aeron_archive_get_stop_position(&stop_position_after, client_archive_, recording_id));
    EXPECT_EQ(published.position, stop_position_after);

    std::int64_t max_recorded_after = 0;
    ASSERT_EQ(0, aeron_archive_get_max_recorded_position(&max_recorded_after, client_archive_, recording_id));
    EXPECT_EQ(published.position, max_recorded_after);

    stop_conductor();
    EXPECT_EQ(published.position, catalog_stop_position(recording_id));

    (void)aeron_publication_close(published.publication, nullptr, nullptr);
}

TEST_F(RecordingInteropTest, stopByChannelAndStreamEndsTheRecording)
{
    start_conductor();
    connect_client();
    start_local_aeron();

    std::int64_t subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));

    const Published published = publish_messages();
    const std::int64_t counter_id = wait_for_recording_counter(published.session_id);
    ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), counter_id);
    const std::int64_t recording_id = recording_id_of(static_cast<std::int32_t>(counter_id));
    ASSERT_GE(recording_id, 0);
    ASSERT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(counter_id), published.position));

    ASSERT_EQ(0, aeron_archive_stop_recording_channel_and_stream(client_archive_, recording_channel().c_str(),
                                                                 RECORDING_STREAM_ID));
    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    EXPECT_EQ(0u, conductor_->recording_session_count());

    stop_conductor();
    EXPECT_EQ(published.position, catalog_stop_position(recording_id));

    (void)aeron_publication_close(published.publication, nullptr, nullptr);
}

TEST_F(RecordingInteropTest, stopByIdentityEndsTheRecordingAndReportsFound)
{
    start_conductor();
    connect_client();
    start_local_aeron();

    std::int64_t subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));

    const Published published = publish_messages();
    const std::int64_t counter_id = wait_for_recording_counter(published.session_id);
    ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), counter_id);
    const std::int64_t recording_id = recording_id_of(static_cast<std::int32_t>(counter_id));
    ASSERT_GE(recording_id, 0);
    ASSERT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(counter_id), published.position));

    bool stopped = false;
    ASSERT_EQ(0, aeron_archive_try_stop_recording_by_identity(&stopped, client_archive_, recording_id));
    EXPECT_TRUE(stopped);

    bool stopped_again = true;
    ASSERT_EQ(0, aeron_archive_try_stop_recording_by_identity(&stopped_again, client_archive_, recording_id));
    EXPECT_FALSE(stopped_again);

    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    EXPECT_EQ(0u, conductor_->recording_session_count());

    stop_conductor();
    EXPECT_EQ(published.position, catalog_stop_position(recording_id));

    (void)aeron_publication_close(published.publication, nullptr, nullptr);
}

TEST_F(RecordingInteropTest, truncateAndPurgeLeaveADirectoryTheReferenceToolVerifies)
{
    if (aeron_jar().empty() || !std::filesystem::exists(aeron_jar()))
    {
        GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << aeron_jar();
    }

    start_conductor();
    connect_client();
    start_local_aeron();

    // Recording A: stop it, then truncate it to position 0 (all files gone).
    std::int64_t subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));
    std::int64_t recording_a = -1;
    {
        const Published published = publish_messages();
        const std::int64_t counter_id = wait_for_recording_counter(published.session_id);
        ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), counter_id);
        recording_a = recording_id_of(static_cast<std::int32_t>(counter_id));
        ASSERT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(counter_id), published.position));
        (void)aeron_publication_close(published.publication, nullptr, nullptr);
    }
    ASSERT_GE(recording_a, 0);
    ASSERT_EQ(0, aeron_archive_stop_recording_subscription(client_archive_, subscription_id));
    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    ASSERT_EQ(0u, conductor_->recording_session_count());

    std::int64_t truncated = -1;
    ASSERT_EQ(0, aeron_archive_truncate_recording(&truncated, client_archive_, recording_a, 0));
    EXPECT_EQ(1, truncated) << "the ten messages live in one segment";

    // Recording B: stop it, then purge it (the catalog entry disappears).
    std::int64_t second_subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&second_subscription_id, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));
    std::int64_t recording_b = -1;
    {
        const Published published = publish_messages();
        const std::int64_t counter_id = wait_for_recording_counter(published.session_id);
        ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), counter_id);
        recording_b = recording_id_of(static_cast<std::int32_t>(counter_id));
        ASSERT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(counter_id), published.position));
        (void)aeron_publication_close(published.publication, nullptr, nullptr);
    }
    ASSERT_GT(recording_b, recording_a);
    ASSERT_EQ(0, aeron_archive_stop_recording_subscription(client_archive_, second_subscription_id));
    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    ASSERT_EQ(0u, conductor_->recording_session_count());

    std::int64_t purged = -1;
    ASSERT_EQ(0, aeron_archive_purge_recording(&purged, client_archive_, recording_b));
    EXPECT_EQ(1, purged);

    stop_conductor();

    // The reference tool's own consistency pass: catalog fields against the
    // segment files that are actually on disk.
    auto verified = run_archive_tool("verify");
    ASSERT_TRUE(verified) << verified.error().message.view();
    ASSERT_EQ(0, verified->exit_code) << verified->output;
    EXPECT_NE(std::string::npos, verified->output.find("recordingId=" + std::to_string(recording_a)))
        << verified->output;
}

TEST_F(RecordingInteropTest, aLivePublicationIsRecordedAgainAfterAStop)
{
    start_conductor();
    connect_client();
    start_local_aeron();

    std::int64_t first_subscription = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&first_subscription, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));
    Published published = publish_messages();
    const std::int64_t first_counter = wait_for_recording_counter(published.session_id);
    ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), first_counter);
    const std::int64_t first_recording = recording_id_of(static_cast<std::int32_t>(first_counter));
    ASSERT_GE(first_recording, 0);
    ASSERT_EQ(published.position, wait_for_position(static_cast<std::int32_t>(first_counter), published.position));

    // Stop while the publication is still live.
    ASSERT_EQ(0, aeron_archive_stop_recording_subscription(client_archive_, first_subscription));
    for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
    {
        ::usleep(200);
    }
    ASSERT_EQ(0u, conductor_->recording_session_count());

    // The live image must be recordable again at once — it has not reached end
    // of stream, so it is not the closed publication's lingering image.
    std::int64_t second_subscription = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&second_subscription, client_archive_, recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false));

    const std::int64_t second_counter = wait_for_recording_counter(published.session_id);
    ASSERT_NE(static_cast<std::int64_t>(AERON_NULL_COUNTER_ID), second_counter)
        << "a recording stopped while its publication stays live must be startable again";
    const std::int64_t second_recording = recording_id_of(static_cast<std::int32_t>(second_counter));
    ASSERT_GE(second_recording, 0);

    // More data on the same publication reaches the second recording.
    std::array<std::uint8_t, MESSAGE_LENGTH> message{};
    for (std::int32_t index = 0; index < MESSAGE_COUNT; index++)
    {
        std::int64_t result = 0;
        while ((result = aeron_publication_offer(published.publication, message.data(), message.size(), nullptr,
                                                 nullptr)) < 0)
        {
            ::usleep(100);
        }
    }
    const std::int64_t end_position = aeron_publication_position(published.publication);
    EXPECT_EQ(end_position, wait_for_position(static_cast<std::int32_t>(second_counter), end_position));

    (void)aeron_publication_close(published.publication, nullptr, nullptr);
}

}  // namespace
