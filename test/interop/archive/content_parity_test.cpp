// A5b / XA-3: content parity — the same input recorded by our archive and by the
// reference `io.aeron.archive.Archive`, compared at three layers.
//
// Topology (the point of the test): one reference Java media driver, one
// publication, two archives that are told to record the same channel *before*
// the publication exists, so both images join at the same position. The input
// is identical by construction, not by running twice.
//
//   L0  the two catalog descriptors describe the same stream (our Catalog reads
//       both files; the reference's ArchiveTool describe-all is compared too);
//   L1  the segment files' logical range [start, stop) is byte-identical
//       (session/term ids masked only on the cross-driver case);
//   L2  the reference C archive client replays both recordings and the payload
//       stream equals the generator's, fragment by fragment.
//
// Six configurations, the Rust track's table: single, multi-segment, empty,
// two concurrent streams, UDP with MTU 512, paced. Two more run the same
// comparison across drivers: the reference's Java driver + its archive versus
// the reference C driver + ours.
//
// Skips when the reference jar, aeronmd or the archive binary is missing.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "archive/server/catalog.h"
#include "archive/server/frame_layout.h"
#include "archive/server/recording_pos.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/c_driver.h"
#include "fixture/process.h"

extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
}

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOpenOptions;
using deepmsg::archive::server::RecordingPos;

/// The two archives cannot share an id on one driver (the reference refuses a
/// leftover counter for the same id, `Archive.java:1661`), so each side has its
/// own; the descriptor does not carry the id.
constexpr std::int64_t OUR_ARCHIVE_ID = 0x5150'0000'0000'0001;
constexpr std::int64_t REFERENCE_ARCHIVE_ID = 0x5150'0000'0000'0002;
constexpr std::int32_t RECORD_STREAM_ID = 1001;
constexpr std::int32_t REPLAY_STREAM_ID = 1002;
constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr std::int32_t MESSAGE_LENGTHS[] = {32, 64, 100, 1344, 5000};
constexpr std::size_t MESSAGE_LENGTH_COUNT = sizeof(MESSAGE_LENGTHS) / sizeof(MESSAGE_LENGTHS[0]);
constexpr auto DEADLINE = std::chrono::seconds(120);

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

// ── the generator: byte-deterministic, no random state ──────────────────────

[[nodiscard]] std::size_t length_for(std::uint64_t sequence)
{
    return MESSAGE_LENGTHS[sequence % MESSAGE_LENGTH_COUNT];
}

[[nodiscard]] std::vector<std::uint8_t> payload(std::uint64_t sequence, std::int32_t stream_id, std::size_t length)
{
    std::vector<std::uint8_t> buffer(length, 0);
    std::memcpy(buffer.data(), &sequence, sizeof(sequence));
    std::memcpy(buffer.data() + sizeof(sequence), &stream_id, sizeof(stream_id));

    std::uint64_t x = (sequence * 0x9E37'79B9'7F4A'7C15ULL) ^ static_cast<std::uint64_t>(stream_id);
    for (std::size_t index = 12; index < length; ++index)
    {
        x ^= x >> 30;
        x *= 0xBF58'476D'1CE4'E5B9ULL;
        x ^= x >> 27;
        x *= 0x94D0'49BB'1331'11EBULL;
        x ^= x >> 31;
        buffer[index] = static_cast<std::uint8_t>(x);
    }
    return buffer;
}

struct ContentCase
{
    const char* name;
    std::int32_t message_count;
    std::int32_t stream_count;
    bool udp;
    std::int32_t mtu;
    std::int32_t segment_length;
    std::int32_t messages_per_second;
};

constexpr ContentCase CONTENT_CASES[] = {
    {"single", 500, 1, false, 0, 128 * 1024, 0},      {"multi", 20000, 1, false, 0, 128 * 1024, 0},
    {"empty", 0, 1, false, 0, 128 * 1024, 0},         {"two-streams", 500, 2, false, 0, 128 * 1024, 0},
    {"udp-mtu512", 500, 1, true, 512, 128 * 1024, 0}, {"paced", 2000, 1, false, 0, 128 * 1024, 1000},
};

class ContentParityTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (aeron_jar().empty() || !std::filesystem::exists(aeron_jar()))
        {
            GTEST_SKIP() << "reference aeron-all jar not built (set AXC_AERON_JAR)";
        }
        if (!std::filesystem::exists(AXC_AERONMD_DEFAULT))
        {
            GTEST_SKIP() << "aeronmd not built at " << AXC_AERONMD_DEFAULT;
        }
        if (!std::filesystem::exists(AXC_ARCHIVE_SERVER_BIN))
        {
            GTEST_SKIP() << "archive server not built at " << AXC_ARCHIVE_SERVER_BIN;
        }
    }

    void TearDown() override
    {
        close_client();
        our_archive_.terminate();
        reference_archive_.terminate();
        driver_.stop();
        java_driver_.terminate();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
        // Let the terminated driver processes release their ports and CnC files
        // before the next fixture reuses this process.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    /// One case's whole fixture, with fresh directories and ports.
    void prepare(const ContentCase& content_case, bool cross_driver)
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-parity-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        aeron_dir_b_ = root_ + "/aeron-b";
        archive_dir_a_ = root_ + "/archive-a";
        archive_dir_b_ = root_ + "/archive-b";
        std::filesystem::create_directories(archive_dir_a_);
        std::filesystem::create_directories(archive_dir_b_);
        case_name_ = content_case.name;
        segment_length_ = content_case.segment_length;
        instance_ = instance_counter_.fetch_add(1);

        if (cross_driver)
        {
            // Reference Java driver under the reference archive; reference C driver
            // under ours.
            start_java_driver(aeron_dir_);
            start_c_driver(aeron_dir_b_);
        }
        else
        {
            start_java_driver(aeron_dir_);
            aeron_dir_b_ = aeron_dir_;
        }
        start_our_archive(aeron_dir_b_);
        start_reference_archive(aeron_dir_);
    }

    void start_java_driver(const std::string& aeron_dir)
    {
        std::vector<std::string> arguments = {
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED", "-Daeron.dir=" + aeron_dir, "-Daeron.term.buffer.length=64k",
            "-Daeron.ipc.term.buffer.length=64k", "-Daeron.threading.mode=SHARED", "-Daeron.idle.strategy=yield",
            // LOCAL + UDP records through an `aeron-spy:` subscription; the driver
            // only counts a spy as a subscriber with this on.
            "-Daeron.spies.simulate.connection=true", "-cp", aeron_jar(), "io.aeron.driver.MediaDriver"};
        auto spawned = axc::interop::ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        java_driver_ = std::move(*spawned);
        wait_for_driver(aeron_dir);
    }

    void start_c_driver(const std::string& aeron_dir)
    {
        auto driver =
            axc::interop::CDriver::start(AXC_AERONMD_DEFAULT, aeron_dir,
                                         {"AERON_SPIES_SIMULATE_CONNECTION=true", "AERON_TERM_BUFFER_LENGTH=65536",
                                          "AERON_IPC_TERM_BUFFER_LENGTH=65536"});
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::move(*driver);
        wait_for_driver(aeron_dir);
    }

    void wait_for_driver(const std::string& aeron_dir)
    {
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        bool ready = false;
        while (!ready && std::chrono::steady_clock::now() < deadline)
        {
            axc::core::ClientConfig config;
            config.dir = aeron_dir;
            ready = axc::core::DeepmsgClient::connect(config).has_value();
            if (!ready)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(ready) << "driver never came up in " << aeron_dir;
    }

    void start_our_archive(const std::string& aeron_dir)
    {
        our_control_ = udp_endpoint(control_port_base());
        our_response_ = udp_endpoint(control_port_base() + 1);

        std::vector<std::string> arguments = {
            "-Daeron.dir=" + aeron_dir,
            "-Daeron.archive.dir=" + archive_dir_a_,
            "-Daeron.archive.control.channel=" + our_control_,
            "-Daeron.archive.replication.channel=" + udp_endpoint(control_port_base() + 5),
            "-Daeron.archive.control.stream.id=" + std::to_string(CONTROL_STREAM_ID),
            "-Daeron.archive.local.control.stream.id=12",
            "-Daeron.archive.control.response.channel=" + our_response_,
            "-Daeron.archive.id=" + std::to_string(OUR_ARCHIVE_ID),
            "-Daeron.archive.segment.file.length=" + std::to_string(segment_length_),
            "-Daeron.archive.threading.mode=SHARED",
            "-Daeron.archive.idle.strategy=yield",
            "-Daeron.archive.dir.delete.on.start=true",
            "-Daeron.spies.simulate.connection=true"};
        // A6c: the parity suite under preallocation (C++ only, docs/compat.md).
        if (const char* fallocate = std::getenv("AXC_IO_FALLOCATE"); nullptr != fallocate)
        {
            arguments.emplace_back(std::string{"-Ddeepmsg.archive.file.io.fallocate="} + fallocate);
        }
        arguments.emplace_back("io.aeron.archive.Archive");
        auto spawned = axc::interop::ChildProcess::spawn(AXC_ARCHIVE_SERVER_BIN, arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        our_archive_ = std::move(*spawned);
        wait_for_control(our_control_, our_response_, aeron_dir);
    }

    void start_reference_archive(const std::string& aeron_dir)
    {
        reference_control_ = udp_endpoint(control_port_base() + 2);
        reference_response_ = udp_endpoint(control_port_base() + 3);

        std::vector<std::string> arguments = {
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-Daeron.dir=" + aeron_dir,
            "-Daeron.term.buffer.length=64k",
            "-Daeron.ipc.term.buffer.length=64k",
            "-Daeron.archive.dir=" + archive_dir_b_,
            "-Daeron.archive.control.channel=" + reference_control_,
            "-Daeron.archive.replication.channel=" + udp_endpoint(control_port_base() + 4),
            "-Daeron.archive.control.stream.id=" + std::to_string(CONTROL_STREAM_ID),
            "-Daeron.archive.control.response.channel=" + reference_response_,
            "-Daeron.archive.id=" + std::to_string(REFERENCE_ARCHIVE_ID),
            "-Daeron.archive.segment.file.length=" + std::to_string(segment_length_),
            "-Daeron.archive.threading.mode=SHARED",
            "-Daeron.archive.idle.strategy=yield",
            "-Daeron.spies.simulate.connection=true",
            "-cp",
            aeron_jar(),
            "io.aeron.archive.Archive"};
        auto spawned = axc::interop::ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        reference_archive_ = std::move(*spawned);
        wait_for_control(reference_control_, reference_response_, aeron_dir);
    }

    void wait_for_control(const std::string& control, const std::string& response, const std::string& aeron_dir)
    {
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        aeron_archive_t* archive = nullptr;
        while (std::chrono::steady_clock::now() < deadline && nullptr == archive)
        {
            aeron_archive_context_t* context = nullptr;
            ASSERT_EQ(0, aeron_archive_context_init(&context));
            aeron_archive_context_set_aeron_directory_name(context, aeron_dir.c_str());
            aeron_archive_context_set_control_request_channel(context, control.c_str());
            aeron_archive_context_set_control_request_stream_id(context, CONTROL_STREAM_ID);
            aeron_archive_context_set_control_response_channel(context, response.c_str());
            aeron_archive_context_set_control_response_stream_id(context, RESPONSE_STREAM_ID);

            aeron_archive_async_connect_t* async = nullptr;
            ASSERT_EQ(0, aeron_archive_async_connect(&async, context));
            for (int poll = 0; poll < 50 && nullptr == archive; ++poll)
            {
                const int result = aeron_archive_async_connect_poll(&archive, async);
                if (result < 0 || 0 != result)
                {
                    async = nullptr;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (nullptr == archive)
            {
                if (nullptr != async)
                {
                    aeron_archive_async_connect_delete(async);
                    async = nullptr;
                }
                (void)aeron_archive_context_close(context);
            }
            else
            {
                (void)aeron_archive_close(archive);
                (void)aeron_archive_context_close(context);
            }
        }
        ASSERT_NE(nullptr, archive) << "archive never answered on " << control;
    }

    [[nodiscard]] std::uint16_t control_port_base() const
    {
        return static_cast<std::uint16_t>(14200 + (::getpid() % 100) * 40 + (instance_ % 200) * 8);
    }

    [[nodiscard]] static std::string udp_endpoint(std::uint16_t port)
    {
        return "aeron:udp?endpoint=localhost:" + std::to_string(port);
    }

    /// `side` 0 is the reference stack's driver, 1 ours; the same-driver cases
    /// use side 0 for both.
    [[nodiscard]] std::string record_channel(const ContentCase& content_case, std::int32_t stream_index,
                                             std::int32_t side) const
    {
        if (content_case.udp)
        {
            return "aeron:udp?endpoint=localhost:" +
                   std::to_string(24000 + (::getpid() % 100) * 40 + (instance_ % 200) * 4 + stream_index * 2 + side) +
                   "|mtu=" + std::to_string(content_case.mtu);
        }
        (void)content_case;
        (void)stream_index;
        (void)side;
        return "aeron:ipc";
    }

    [[nodiscard]] std::int32_t record_stream(const ContentCase& content_case, std::int32_t stream_index) const
    {
        return RECORD_STREAM_ID + stream_index;
    }

    /// StartRecording on both archives for one stream; answers (recording id per
    /// archive, recording id).
    struct Recordings
    {
        std::int64_t ours{0};
        std::int64_t reference{0};
        std::int64_t our_subscription{-1};
        std::int64_t reference_subscription{-1};
    };

    Recordings start_recording_both(const ContentCase& content_case, std::int32_t stream_index, bool cross_driver)
    {
        Recordings recordings;
        const std::int32_t stream = record_stream(content_case, stream_index);

        EXPECT_EQ(
            0, aeron_archive_start_recording(&recordings.our_subscription, ours_,
                                             record_channel(content_case, stream_index, cross_driver ? 1 : 0).c_str(),
                                             stream, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false))
            << aeron_errmsg();
        EXPECT_EQ(0, aeron_archive_start_recording(&recordings.reference_subscription, reference_,
                                                   record_channel(content_case, stream_index, 0).c_str(), stream,
                                                   AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false))
            << aeron_errmsg();
        return recordings;
    }

    void close_client()
    {
        if (nullptr != ours_)
        {
            aeron_archive_close(ours_);
            ours_ = nullptr;
        }
        if (nullptr != ours_context_)
        {
            aeron_archive_context_close(ours_context_);
            ours_context_ = nullptr;
        }
        if (nullptr != reference_)
        {
            aeron_archive_close(reference_);
            reference_ = nullptr;
        }
        if (nullptr != reference_context_)
        {
            aeron_archive_context_close(reference_context_);
            reference_context_ = nullptr;
        }
        if (nullptr != aeron_)
        {
            aeron_close(aeron_);
            aeron_ = nullptr;
        }
        if (nullptr != aeron_context_)
        {
            aeron_context_close(aeron_context_);
            aeron_context_ = nullptr;
        }
        if (nullptr != aeron_b_)
        {
            aeron_close(aeron_b_);
            aeron_b_ = nullptr;
        }
        if (nullptr != aeron_context_b_)
        {
            aeron_context_close(aeron_context_b_);
            aeron_context_b_ = nullptr;
        }
    }

    void connect_archives()
    {
        // A connect can land while the driver is moving a receive endpoint to
        // CLOSING (the reference's own hint is "please retry"), so retry briefly
        // instead of failing the fixture on the first transient.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (nullptr == ours_ && std::chrono::steady_clock::now() < deadline)
        {
            if (nullptr == ours_context_)
            {
                ours_context_ = connect_context(our_control_, our_response_, aeron_dir_b_);
            }
            if (nullptr != ours_context_ && 0 == aeron_archive_connect(&ours_, ours_context_))
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_NE(nullptr, ours_) << "our archive never accepted a client: " << aeron_errmsg();

        while (nullptr == reference_ && std::chrono::steady_clock::now() < deadline)
        {
            if (nullptr == reference_context_)
            {
                reference_context_ = connect_context(reference_control_, reference_response_, aeron_dir_);
            }
            if (nullptr != reference_context_ && 0 == aeron_archive_connect(&reference_, reference_context_))
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_NE(nullptr, reference_) << "the reference archive never accepted a client: " << aeron_errmsg();

        ASSERT_EQ(0, aeron_context_init(&aeron_context_));
        aeron_context_set_dir(aeron_context_, aeron_dir_.c_str());
        aeron_context_set_use_conductor_agent_invoker(aeron_context_, true);
        ASSERT_EQ(0, aeron_init(&aeron_, aeron_context_));

        if (aeron_dir_b_ != aeron_dir_)
        {
            ASSERT_EQ(0, aeron_context_init(&aeron_context_b_));
            aeron_context_set_dir(aeron_context_b_, aeron_dir_b_.c_str());
            aeron_context_set_use_conductor_agent_invoker(aeron_context_b_, true);
            ASSERT_EQ(0, aeron_init(&aeron_b_, aeron_context_b_));
        }
    }

    [[nodiscard]] aeron_archive_context_t* connect_context(const std::string& control, const std::string& response,
                                                           const std::string& aeron_dir)
    {
        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return nullptr;
        }
        aeron_archive_context_set_aeron_directory_name(context, aeron_dir.c_str());
        aeron_archive_context_set_control_request_channel(context, control.c_str());
        aeron_archive_context_set_control_request_stream_id(context, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(context, response.c_str());
        aeron_archive_context_set_control_response_stream_id(context, RESPONSE_STREAM_ID);
        return context;
    }

    struct Published
    {
        std::int32_t session_id{0};
        std::int64_t end_position{0};
        std::uint64_t messages{0};
    };

    /// Offers the generated stream, waiting for the publication to link; returns
    /// the publication's session and end position (computed from the generator).
    Published publish(aeron_t* client, const ContentCase& content_case, std::int32_t stream_index, std::int32_t side)
    {
        const std::string channel = record_channel(content_case, stream_index, side);
        const std::int32_t stream = record_stream(content_case, stream_index);

        aeron_publication_t* publication = nullptr;
        aeron_async_add_publication_t* async = nullptr;
        EXPECT_EQ(0, aeron_async_add_publication(&async, client, channel.c_str(), stream));
        for (int iteration = 0; iteration < 20000 && nullptr == publication; ++iteration)
        {
            const int poll = aeron_async_add_publication_poll(&publication, async);
            EXPECT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                async = nullptr;
                break;
            }
            (void)aeron_main_do_work(client);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (nullptr == publication)
        {
            ADD_FAILURE() << "publication not created on " << channel << ": " << aeron_errmsg();
            return {};
        }

        Published published;
        published.session_id = aeron_publication_session_id(publication);
        const auto start = std::chrono::steady_clock::now();
        std::uint64_t sequence = 0;
        for (std::int32_t message = 0; message < content_case.message_count; ++message, ++sequence)
        {
            const auto body = payload(sequence, stream, length_for(sequence));
            std::int64_t offered = 0;
            while ((offered = aeron_publication_offer(publication, body.data(), body.size(), nullptr, nullptr)) < 0)
            {
                if (AERON_PUBLICATION_NOT_CONNECTED == offered || AERON_PUBLICATION_CLOSED == offered)
                {
                    ADD_FAILURE() << "publication not connected";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            if (content_case.messages_per_second > 0)
            {
                const auto target = start + std::chrono::microseconds(static_cast<std::int64_t>(message + 1) * 1000000 /
                                                                      content_case.messages_per_second);
                std::this_thread::sleep_until(target);
            }
        }

        // The publication's own position is the input's end: it includes the
        // driver's term-boundary padding, which a payload-sum cannot know.
        published.end_position = aeron_publication_position(publication);
        published.messages = sequence;
        return published;
    }

    void stop_both(const std::string& channel, std::int32_t stream)
    {
        (void)channel;
        (void)stream;
    }

    /// The recording id each archive gave the same publication session.
    [[nodiscard]] std::int64_t recording_id_from_counter(const std::string& aeron_dir, std::int64_t archive_id,
                                                         std::int32_t session_id)
    {
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (std::chrono::steady_clock::now() < deadline)
        {
            axc::core::ClientConfig config;
            config.dir = aeron_dir;
            auto client = axc::core::DeepmsgClient::connect(config);
            if (client)
            {
                (void)client->do_work();
                auto* counters = aeron_counters_reader(client->handle());
                const std::int32_t counter_id = RecordingPos::find_by_session(counters, archive_id, session_id);
                if (AERON_NULL_COUNTER_ID != counter_id)
                {
                    return RecordingPos::recording_id(counters, counter_id);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return -1;
    }

    void wait_recorded(const std::string& aeron_dir, std::int64_t archive_id, std::int32_t session_id,
                       std::int64_t position)
    {
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (std::chrono::steady_clock::now() < deadline)
        {
            axc::core::ClientConfig config;
            config.dir = aeron_dir;
            auto client = axc::core::DeepmsgClient::connect(config);
            if (client)
            {
                (void)client->do_work();
                auto* counters = aeron_counters_reader(client->handle());
                const std::int32_t counter_id = RecordingPos::find_by_session(counters, archive_id, session_id);
                if (AERON_NULL_COUNTER_ID != counter_id)
                {
                    std::int64_t* address = aeron_counters_reader_addr(counters, counter_id);
                    const auto value = nullptr == address
                                           ? -1
                                           : std::atomic_ref<std::int64_t>(*address).load(std::memory_order_acquire);
                    if (value == position)
                    {
                        return;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ADD_FAILURE() << "the recording never reached " << position << " for session " << session_id;
    }

    void stop_recordings(const Recordings& recordings)
    {
        ASSERT_EQ(0, aeron_archive_stop_recording_subscription(ours_, recordings.our_subscription));
        ASSERT_EQ(0, aeron_archive_stop_recording_subscription(reference_, recordings.reference_subscription));
    }

    void wait_stop_position(aeron_archive_t* archive, std::int64_t recording_id, std::int64_t expected)
    {
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (std::chrono::steady_clock::now() < deadline)
        {
            std::int64_t stop_position = AERON_NULL_VALUE;
            if (0 == aeron_archive_get_stop_position(&stop_position, archive, recording_id) &&
                stop_position == expected)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ADD_FAILURE() << "the catalog's stop position never reached " << expected << " for recording " << recording_id;
    }

    struct Descriptor
    {
        std::int64_t recording_id{0};
        std::int64_t start_position{0};
        std::int64_t stop_position{0};
        std::int32_t initial_term_id{0};
        std::int32_t segment_file_length{0};
        std::int32_t term_buffer_length{0};
        std::int32_t mtu_length{0};
        std::int32_t session_id{0};
        std::int32_t stream_id{0};
        std::string stripped_channel;
        std::string original_channel;
        std::string source_identity;
    };

    [[nodiscard]] Descriptor read_descriptor(const std::string& archive_dir, std::int64_t recording_id)
    {
        Descriptor descriptor;
        CatalogOpenOptions options;
        options.archive_dir = archive_dir;
        options.catalog_capacity = 1024 * 1024;
        auto catalog = Catalog::open(options);
        if (!catalog)
        {
            ADD_FAILURE() << "catalog open failed in " << archive_dir << ": " << catalog.error().message.view();
            return descriptor;
        }

        bool found = false;
        (void)catalog->for_each(
            [&](deepmsg::archive::server::CatalogEntry& entry)
            {
                if (static_cast<std::int64_t>(entry.descriptor.recordingId()) != recording_id)
                {
                    return;
                }
                found = true;
                descriptor.recording_id = recording_id;
                descriptor.start_position = static_cast<std::int64_t>(entry.descriptor.startPosition());
                descriptor.stop_position = static_cast<std::int64_t>(entry.descriptor.stopPosition());
                descriptor.initial_term_id = entry.descriptor.initialTermId();
                descriptor.segment_file_length = entry.descriptor.segmentFileLength();
                descriptor.term_buffer_length = entry.descriptor.termBufferLength();
                descriptor.mtu_length = entry.descriptor.mtuLength();
                descriptor.session_id = entry.descriptor.sessionId();
                descriptor.stream_id = entry.descriptor.streamId();
                descriptor.stripped_channel.assign(entry.descriptor.strippedChannel(),
                                                   entry.descriptor.strippedChannelLength());
                descriptor.original_channel.assign(entry.descriptor.originalChannel(),
                                                   entry.descriptor.originalChannelLength());
                descriptor.source_identity.assign(entry.descriptor.sourceIdentity(),
                                                  entry.descriptor.sourceIdentityLength());
            });
        if (!found)
        {
            ADD_FAILURE() << "recording " << recording_id << " not found in " << archive_dir;
        }
        (void)catalog->close();
        return descriptor;
    }

    /// The two cross-driver stacks must listen on different ports; compare the
    /// channels with the endpoint port normalised (the Rust track's L0 rule).
    [[nodiscard]] static std::string normalized_channel(std::string_view channel, bool cross_driver)
    {
        if (!cross_driver)
        {
            return std::string{channel};
        }

        std::string value{channel};
        const auto endpoint = value.find("endpoint=");
        if (std::string::npos == endpoint)
        {
            return value;
        }
        const auto port_start = value.find(':', endpoint);
        if (std::string::npos == port_start)
        {
            return value;
        }
        auto port_end = port_start + 1;
        while (port_end < value.size() && std::isdigit(static_cast<unsigned char>(value[port_end])))
        {
            ++port_end;
        }
        value.replace(port_start + 1, port_end - port_start - 1, "0");
        return value;
    }

    /// L0: the descriptors describe the same stream. `cross_driver` drops the three
    /// fields the driver itself chooses and normalises the endpoint port.
    void compare_descriptors(const Descriptor& ours, const Descriptor& reference, bool cross_driver)
    {
        EXPECT_EQ(ours.start_position, reference.start_position);
        EXPECT_EQ(ours.stop_position, reference.stop_position);
        EXPECT_EQ(ours.segment_file_length, reference.segment_file_length);
        EXPECT_EQ(ours.term_buffer_length, reference.term_buffer_length);
        EXPECT_EQ(ours.mtu_length, reference.mtu_length);
        EXPECT_EQ(ours.stream_id, reference.stream_id);
        EXPECT_EQ(normalized_channel(ours.stripped_channel, cross_driver),
                  normalized_channel(reference.stripped_channel, cross_driver));
        EXPECT_EQ(normalized_channel(ours.original_channel, cross_driver),
                  normalized_channel(reference.original_channel, cross_driver));
        if (!cross_driver)
        {
            EXPECT_EQ(ours.initial_term_id, reference.initial_term_id);
            EXPECT_EQ(ours.session_id, reference.session_id);
            EXPECT_EQ(ours.source_identity, reference.source_identity);
        }
    }

    /// L1: every segment file's logical range, byte for byte (session/term ids
    /// masked across drivers).
    void compare_segment_files(const std::string& ours_dir, const std::string& reference_dir,
                               std::int64_t our_recording_id, std::int64_t reference_recording_id, std::int64_t start,
                               std::int64_t stop, bool cross_driver)
    {
        const auto ours = segment_files(ours_dir, our_recording_id);
        const auto reference = segment_files(reference_dir, reference_recording_id);
        ASSERT_EQ(ours.size(), reference.size()) << "different segment counts";

        for (std::size_t index = 0; index < ours.size(); ++index)
        {
            ASSERT_EQ(ours[index].base_position, reference[index].base_position);
            compare_logical_range(ours[index], reference[index], start, stop, cross_driver);
        }
    }

    struct SegmentFile
    {
        std::filesystem::path path;
        std::int64_t base_position{0};
    };

    [[nodiscard]] std::vector<SegmentFile> segment_files(const std::string& dir, std::int64_t recording_id)
    {
        std::vector<SegmentFile> files;
        const std::string prefix = std::to_string(recording_id) + "-";
        std::error_code error;
        for (const auto& entry : std::filesystem::directory_iterator(dir, error))
        {
            const std::string name = entry.path().filename().string();
            if (0 != name.compare(0, prefix.size(), prefix) || name.size() < 5 ||
                name.compare(name.size() - 4, 4, ".rec") != 0)
            {
                continue;
            }
            SegmentFile file;
            file.path = entry.path();
            file.base_position = std::strtoll(name.c_str() + prefix.size(), nullptr, 10);
            files.push_back(file);
        }
        std::sort(files.begin(), files.end(), [](const SegmentFile& left, const SegmentFile& right)
                  { return left.base_position < right.base_position; });
        return files;
    }

    [[nodiscard]] static std::vector<std::uint8_t> read_range(const SegmentFile& file, std::int64_t from,
                                                              std::int64_t length)
    {
        std::vector<std::uint8_t> bytes(static_cast<std::size_t>(std::max<std::int64_t>(0, length)), 0);
        if (bytes.empty())
        {
            return bytes;
        }
        std::ifstream input{file.path, std::ios::binary};
        input.seekg(from);
        input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        return bytes;
    }

    static void compare_logical_range(const SegmentFile& ours, const SegmentFile& reference, std::int64_t start,
                                      std::int64_t stop, bool cross_driver)
    {
        const std::int64_t file_end =
            ours.base_position + static_cast<std::int64_t>(std::filesystem::file_size(ours.path));
        const std::int64_t from = std::max(start, ours.base_position);
        const std::int64_t to = std::min(stop, file_end);
        if (to <= from)
        {
            return;
        }

        auto a = read_range(ours, from - ours.base_position, to - from);
        auto b = read_range(reference, from - reference.base_position, to - from);
        ASSERT_EQ(a.size(), b.size());

        if (!cross_driver)
        {
            ASSERT_EQ(a, b) << "segment " << ours.path.filename().string() << " range " << from << ".." << to;
            return;
        }

        // Mask the driver-chosen fields in every frame header: session id
        // (12..16) and term id (20..24).
        for (std::size_t offset = 0; offset + 32 <= a.size();)
        {
            for (std::size_t index = 0; index < 32 && offset + index < a.size(); ++index)
            {
                if ((index >= 12 && index < 16) || (index >= 20 && index < 24))
                {
                    continue;
                }
                ASSERT_EQ(a[offset + index], b[offset + index])
                    << "frame header byte " << index << " at " << from + static_cast<std::int64_t>(offset);
            }

            std::int32_t frame_length = 0;
            std::memcpy(&frame_length, a.data() + offset, sizeof(frame_length));
            if (frame_length <= 0)
            {
                break;
            }
            const std::int64_t aligned =
                deepmsg::archive::server::frame_align(frame_length, deepmsg::archive::server::FRAME_ALIGNMENT);
            for (std::int64_t index = 32; index < aligned && offset + index < a.size(); ++index)
            {
                ASSERT_EQ(a[offset + index], b[offset + index])
                    << "frame payload byte " << index << " at " << from + static_cast<std::int64_t>(offset);
            }
            offset += static_cast<std::size_t>(aligned);
        }
    }

    /// L2: replay both recordings with the reference C archive client and check the
    /// payload stream against the generator.
    void compare_replays(aeron_archive_t* ours, aeron_t* our_client, std::int64_t our_recording_id,
                         aeron_archive_t* reference, aeron_t* reference_client, std::int64_t reference_recording_id,
                         const ContentCase& content_case, std::int32_t stream_index, std::int64_t start_position)
    {
        const std::int32_t stream = record_stream(content_case, stream_index);
        if (0 == content_case.message_count)
        {
            return;
        }
        const auto ours_payloads = replay(ours, our_client, our_recording_id, stream, content_case.message_count);
        const auto reference_payloads =
            replay(reference, reference_client, reference_recording_id, stream, content_case.message_count);
        EXPECT_EQ(ours_payloads.size(), reference_payloads.size());
        EXPECT_EQ(static_cast<std::size_t>(content_case.message_count), ours_payloads.size());

        for (std::size_t sequence = 0; sequence < ours_payloads.size(); ++sequence)
        {
            const auto expected = payload(sequence, stream, length_for(sequence));
            ASSERT_EQ(expected, ours_payloads[sequence]) << "our replay, sequence " << sequence;
            ASSERT_EQ(expected, reference_payloads[sequence]) << "reference replay, sequence " << sequence;
        }
        (void)start_position;
    }

    [[nodiscard]] std::vector<std::vector<std::uint8_t>> replay(aeron_archive_t* archive, aeron_t* client,
                                                                std::int64_t recording_id, std::int32_t stream,
                                                                std::int32_t expected_count)
    {
        // A unique port per replay: the two archives' replay publications must not
        // share one subscription, or each poll would interleave both streams.
        const std::string replay_channel =
            "aeron:udp?endpoint=localhost:" +
            std::to_string(34000 + (::getpid() % 100) * 40 + (instance_ % 200) * 4 + (replay_sequence_++ % 4));

        aeron_subscription_t* subscription = nullptr;
        aeron_async_add_subscription_t* subscription_add = nullptr;
        EXPECT_EQ(0, aeron_async_add_subscription(&subscription_add, client, replay_channel.c_str(), REPLAY_STREAM_ID,
                                                  nullptr, nullptr, nullptr, nullptr));
        for (int iteration = 0; iteration < 20000 && nullptr == subscription; ++iteration)
        {
            const int poll = aeron_async_add_subscription_poll(&subscription, subscription_add);
            EXPECT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                subscription_add = nullptr;
                break;
            }
            (void)aeron_main_do_work(client);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (nullptr == subscription)
        {
            ADD_FAILURE() << "replay subscription failed";
            return {};
        }

        aeron_archive_replay_params_t params;
        EXPECT_EQ(0, aeron_archive_replay_params_init(&params));
        params.position = 0;
        params.length = -1;

        std::int64_t replay_session_id = -1;
        EXPECT_EQ(0, aeron_archive_start_replay(&replay_session_id, archive, recording_id, replay_channel.c_str(),
                                                REPLAY_STREAM_ID, &params))
            << aeron_errmsg();

        std::vector<std::vector<std::uint8_t>> payloads;
        aeron_image_controlled_fragment_assembler_t* assembler = nullptr;
        EXPECT_EQ(0, aeron_image_controlled_fragment_assembler_create(
                         &assembler,
                         [](void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t*)
                         {
                             auto* into = static_cast<std::vector<std::vector<std::uint8_t>>*>(clientd);
                             into->emplace_back(buffer, buffer + length);
                             return AERON_ACTION_CONTINUE;
                         },
                         &payloads));

        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (std::chrono::steady_clock::now() < deadline &&
               payloads.size() < static_cast<std::size_t>(expected_count))
        {
            (void)aeron_main_do_work(client);
            aeron_image_t* image = nullptr;
            if (aeron_subscription_image_count(subscription) > 0)
            {
                image = aeron_subscription_image_at_index(subscription, 0);
            }
            if (nullptr != image)
            {
                const int fragments = aeron_image_controlled_poll(
                    image, &aeron_image_controlled_fragment_assembler_handler, assembler, 10);
                EXPECT_GE(fragments, 0) << aeron_errmsg();
                aeron_image_release(image);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        (void)aeron_archive_stop_replay(archive, replay_session_id);
        if (nullptr != assembler)
        {
            (void)aeron_image_controlled_fragment_assembler_delete(assembler);
        }
        (void)aeron_subscription_close(subscription, nullptr, nullptr);
        return payloads;
    }

    /// The reference's own `ArchiveTool checksum CRC-32 -a` over both directories.
    void checksum_directories()
    {
        for (const std::string& dir : {archive_dir_a_, archive_dir_b_})
        {
            axc::interop::ProcessOptions options;
            options.arguments = {"--add-opens",
                                 "java.base/jdk.internal.misc=ALL-UNNAMED",
                                 "--add-opens",
                                 "java.base/java.util.zip=ALL-UNNAMED",
                                 "-cp",
                                 aeron_jar(),
                                 "io.aeron.archive.ArchiveTool",
                                 dir,
                                 "checksum",
                                 "CRC-32",
                                 "-a"};
            options.timeout = std::chrono::seconds(120);
            auto result = axc::interop::run_process("/usr/bin/java", options);
            ASSERT_TRUE(result) << result.error().message.view();
            EXPECT_EQ(0, result->exit_code) << result->output;
            EXPECT_EQ(std::string::npos, result->output.find("ERR")) << result->output;
        }
    }

    void run_content_case(const ContentCase& content_case, bool cross_driver)
    {
        prepare(content_case, cross_driver);
        connect_archives();

        for (std::int32_t stream_index = 0; stream_index < content_case.stream_count; ++stream_index)
        {
            const Recordings recordings = start_recording_both(content_case, stream_index, cross_driver);

            // The reference stack publishes on its driver; ours publishes the same
            // generated stream on its own driver, so both archives see the same
            // input even though the two drivers are unrelated.
            const Published reference_published = publish(aeron_, content_case, stream_index, 0);
            const Published our_published =
                cross_driver ? publish(aeron_b_, content_case, stream_index, 1) : reference_published;

            const std::int64_t our_recording =
                recording_id_from_counter(aeron_dir_b_, OUR_ARCHIVE_ID, our_published.session_id);
            const std::int64_t reference_recording =
                recording_id_from_counter(aeron_dir_, REFERENCE_ARCHIVE_ID, reference_published.session_id);
            ASSERT_GE(our_recording, 0) << "our archive's RecordingPos counter not found";
            ASSERT_GE(reference_recording, 0) << "the reference archive's counter not found";

            wait_recorded(aeron_dir_b_, OUR_ARCHIVE_ID, our_published.session_id, our_published.end_position);
            wait_recorded(aeron_dir_, REFERENCE_ARCHIVE_ID, reference_published.session_id,
                          reference_published.end_position);

            stop_recordings(recordings);
            wait_stop_position(ours_, our_recording, our_published.end_position);
            wait_stop_position(reference_, reference_recording, reference_published.end_position);

            const Descriptor ours = read_descriptor(archive_dir_a_, our_recording);
            const Descriptor reference = read_descriptor(archive_dir_b_, reference_recording);
            compare_descriptors(ours, reference, cross_driver);

            if (content_case.message_count > 0)
            {
                compare_segment_files(archive_dir_a_, archive_dir_b_, our_recording, reference_recording,
                                      ours.start_position, ours.stop_position, cross_driver);
            }
            compare_replays(ours_, cross_driver ? aeron_b_ : aeron_, our_recording, reference_, aeron_,
                            reference_recording, content_case, stream_index, ours.start_position);
        }

        checksum_directories();
    }

    std::string root_;
    std::string aeron_dir_;
    std::string aeron_dir_b_;
    std::string archive_dir_a_;
    std::string archive_dir_b_;
    std::string our_control_;
    std::string our_response_;
    std::string reference_control_;
    std::string reference_response_;
    std::string case_name_;
    std::int32_t segment_length_{128 * 1024};
    int replay_sequence_{0};
    int instance_{0};
    static inline std::atomic<int> instance_counter_{0};
    axc::interop::ChildProcess java_driver_;
    axc::interop::CDriver driver_;
    axc::interop::ChildProcess our_archive_;
    axc::interop::ChildProcess reference_archive_;
    aeron_archive_t* ours_{nullptr};
    aeron_archive_context_t* ours_context_{nullptr};
    aeron_archive_t* reference_{nullptr};
    aeron_archive_context_t* reference_context_{nullptr};
    aeron_t* aeron_{nullptr};
    aeron_context_t* aeron_context_{nullptr};
    aeron_t* aeron_b_{nullptr};
    aeron_context_t* aeron_context_b_{nullptr};
};

TEST_F(ContentParityTest, singleSegmentIsByteIdentical)
{
    run_content_case(CONTENT_CASES[0], false);
}

TEST_F(ContentParityTest, multiSegmentIsByteIdentical)
{
    run_content_case(CONTENT_CASES[1], false);
}

TEST_F(ContentParityTest, emptyRecordingDescribesTheSameStream)
{
    run_content_case(CONTENT_CASES[2], false);
}

TEST_F(ContentParityTest, twoConcurrentStreamsAreByteIdentical)
{
    run_content_case(CONTENT_CASES[3], false);
}

TEST_F(ContentParityTest, udpWithMtu512IsByteIdentical)
{
    run_content_case(CONTENT_CASES[4], false);
}

TEST_F(ContentParityTest, pacedPublishingIsByteIdentical)
{
    run_content_case(CONTENT_CASES[5], false);
}

TEST_F(ContentParityTest, acrossDriversSingleSegment)
{
    run_content_case(CONTENT_CASES[0], true);
}

TEST_F(ContentParityTest, acrossDriversMultiSegment)
{
    run_content_case(CONTENT_CASES[1], true);
}

TEST_F(ContentParityTest, acrossDriversUdpMtu512)
{
    run_content_case(CONTENT_CASES[4], true);
}

}  // namespace
