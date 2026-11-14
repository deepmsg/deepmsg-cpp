// E2: the archive on the DPDK port, recorded and replayed through the media.
//
// Shape: our `deepmsg-media-driver` runs the driver on 0000:03:00.0 with
// 10.10.0.1; the archive conductor and the C archive client are both clients
// of that one driver (the same shape the other archive interop cases use).
// The client and the archive therefore address each other at the media's own
// address, which the stack loops inside itself — nothing about the recording
// path goes through the kernel.
//
// Skipped unless the bench environment names the port (see dpdk_bench_test.cpp:
// AXC_DPDK_BENCH_PCI / LOCAL / PEER / PEER_MAC).
//
// Compiled only in a build with `-DAXC_WITH_DPDK=ON`.

#if defined(AXC_WITH_DPDK)

#include <gtest/gtest.h>

#include <aeronc.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "archive/server/archive_conductor.h"
#include "archive/server/recording_pos.h"
#include "fixture/c_driver.h"
#include "fixture/media_driver.h"

extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
}

#ifndef AXC_MEDIA_DRIVER_BIN
#define AXC_MEDIA_DRIVER_BIN ""
#endif

namespace
{

using deepmsg::archive::server::ArchiveConductor;
using deepmsg::archive::server::ArchiveConductorContext;
using deepmsg::archive::server::RecordingPos;

constexpr std::int64_t ARCHIVE_ID = 0x0423'6483'BEE1;
constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr std::int32_t RECORDING_STREAM_ID = 1001;
constexpr std::int32_t REPLAY_STREAM_ID = 1002;
constexpr std::int32_t MESSAGE_COUNT = 10;
constexpr std::int32_t MESSAGE_LENGTH = 64;
constexpr auto DEADLINE = std::chrono::seconds(30);

void collect_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t* /*header*/)
{
    auto* into = static_cast<std::vector<std::array<std::uint8_t, MESSAGE_LENGTH>>*>(clientd);
    if (length < MESSAGE_LENGTH || nullptr == into)
    {
        return;
    }
    std::array<std::uint8_t, MESSAGE_LENGTH> got{};
    std::memcpy(got.data(), buffer, got.size());
    into->push_back(got);
}

[[nodiscard]] std::optional<std::string> bench_port()
{
    const char* pci = std::getenv("AXC_DPDK_BENCH_PCI");
    const char* local = std::getenv("AXC_DPDK_BENCH_LOCAL");
    const char* peer = std::getenv("AXC_DPDK_BENCH_PEER");
    if (nullptr == pci || nullptr == local || nullptr == peer)
    {
        return std::nullopt;
    }
    return std::string{pci};
}

[[nodiscard]] std::string local_address()
{
    const char* local = std::getenv("AXC_DPDK_BENCH_LOCAL");
    return nullptr == local ? std::string{"10.10.0.1/24"} : std::string{local};
}

[[nodiscard]] std::string peer_address()
{
    const char* peer = std::getenv("AXC_DPDK_BENCH_PEER");
    return nullptr == peer ? std::string{"10.10.0.2"} : std::string{peer};
}

[[nodiscard]] std::string media_address()
{
    return local_address().substr(0, local_address().find('/'));
}

[[nodiscard]] std::string driver_binary()
{
    if (const char* from_env = std::getenv("AXC_MEDIA_DRIVER"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_MEDIA_DRIVER_BIN;
}

class DpdkArchiveTest
{
public:
    ~DpdkArchiveTest()
    {
        stop_conductor();
        if (nullptr != client_archive_)
        {
            (void)aeron_archive_close(client_archive_);
        }
        if (nullptr != archive_context_)
        {
            (void)aeron_archive_context_close(archive_context_);
        }
        if (nullptr != aeron_)
        {
            (void)aeron_close(aeron_);
        }
        if (nullptr != aeron_context_)
        {
            (void)aeron_context_close(aeron_context_);
        }
        driver_.stop();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    void start()
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-dpdk-archive-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        archive_dir_ = root_ + "/archive";
        std::filesystem::create_directories(archive_dir_);

        const std::string binary = driver_binary();
        ASSERT_FALSE(binary.empty());
        ASSERT_TRUE(std::filesystem::exists(binary)) << binary;

        std::vector<std::string> environment{"AERON_SPIES_SIMULATE_CONNECTION=true", "DEEPMSG_DPDK_ENABLED=true",
                                             std::string{"DEEPMSG_DPDK_EAL_ARGS=-a "} + pci_,
                                             std::string{"DEEPMSG_DPDK_PORT="} + pci_,
                                             std::string{"DEEPMSG_DPDK_IPV4_ADDRESS="} + local_address()};
        if (const char* mac = std::getenv("AXC_DPDK_BENCH_PEER_MAC"); nullptr != mac && '\0' != *mac)
        {
            environment.emplace_back(std::string{"DEEPMSG_DPDK_STATIC_NEIGHBOURS="} + peer_address() + "=" + mac);
        }

        auto driver = axc::interop::CDriver::start(binary, aeron_dir_, environment);
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::move(*driver);

        start_conductor();
    }

    void connect_client()
    {
        ASSERT_EQ(0, aeron_archive_context_init(&archive_context_));
        aeron_archive_context_set_aeron_directory_name(archive_context_, aeron_dir_.c_str());
        aeron_archive_context_set_control_request_channel(archive_context_, control_channel().c_str());
        aeron_archive_context_set_control_request_stream_id(archive_context_, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(archive_context_, "aeron:udp?endpoint=localhost:0");
        aeron_archive_context_set_control_response_stream_id(archive_context_, RESPONSE_STREAM_ID);

        aeron_archive_async_connect_t* async = nullptr;
        ASSERT_EQ(0, aeron_archive_async_connect(&async, archive_context_));
        for (int iteration = 0; iteration < 30'000; iteration++)
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

    [[nodiscard]] aeron_t* aeron() const
    {
        return aeron_;
    }
    [[nodiscard]] aeron_archive_t* archive() const
    {
        return client_archive_;
    }
    [[nodiscard]] std::string control_channel() const
    {
        return "aeron:udp?endpoint=" + media_address() + ":8010";
    }
    [[nodiscard]] std::string recording_channel() const
    {
        return "aeron:udp?endpoint=" + media_address() + ":34433|term-length=64k";
    }

    void stop_conductor()
    {
        if (worker_.joinable())
        {
            running_ = false;
            worker_.join();
        }
    }

    void stop_recording_and_settle()
    {
        // The conductor runs on a thread, so a wait has to give it time.
        for (int iteration = 0; iteration < 20'000 && conductor_->recording_session_count() > 0; iteration++)
        {
            ::usleep(200);
        }
        EXPECT_EQ(0u, conductor_->recording_session_count());
    }

    [[nodiscard]] bool media_counters_moved()
    {
        aeron_counters_reader_t* reader = aeron_counters_reader(aeron_);
        if (nullptr == reader)
        {
            return false;
        }
        bool found = false;
        aeron_counters_reader_foreach_counter(
            reader,
            [](std::int64_t value, std::int32_t, std::int32_t type_id, const std::uint8_t*, std::size_t, const char*,
               std::size_t, void* clientd)
            {
                // 76 is a transport's received count; 82 is what the port saw.
                if ((76 == type_id || 82 == type_id) && value > 0)
                {
                    *static_cast<bool*>(clientd) = true;
                }
            },
            &found);
        return found;
    }

private:
    void start_conductor()
    {
        ArchiveConductorContext context;
        context.archive_dir = archive_dir_;
        context.aeron_dir = aeron_dir_;
        context.control_channel = control_channel();
        context.control_stream_id = CONTROL_STREAM_ID;
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

    std::string pci_{bench_port().value_or("")};
    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    axc::interop::CDriver driver_;
    std::unique_ptr<ArchiveConductor> conductor_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    aeron_context_t* aeron_context_{nullptr};
    aeron_t* aeron_{nullptr};
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_t* client_archive_{nullptr};
};

TEST(DpdkArchive, TheArchiveRecordsAndReplaysOverTheMedia)
{
    if (!bench_port())
    {
        GTEST_SKIP() << "set AXC_DPDK_BENCH_PCI, AXC_DPDK_BENCH_LOCAL and AXC_DPDK_BENCH_PEER";
    }

    DpdkArchiveTest test;
    test.start();
    test.connect_client();
    test.start_local_aeron();

    std::int64_t subscription_id = 0;
    ASSERT_EQ(0, aeron_archive_start_recording(&subscription_id, test.archive(), test.recording_channel().c_str(),
                                               RECORDING_STREAM_ID, AERON_ARCHIVE_SOURCE_LOCATION_REMOTE, false))
        << aeron_errmsg();

    // The recording publication and the archive's subscription are two clients
    // of one driver, so the frames are the stack's own loopback.
    aeron_async_add_publication_t* async = nullptr;
    ASSERT_EQ(0,
              aeron_async_add_publication(&async, test.aeron(), test.recording_channel().c_str(), RECORDING_STREAM_ID));
    aeron_publication_t* publication = nullptr;
    for (int iteration = 0; iteration < 30'000 && nullptr == publication; ++iteration)
    {
        ASSERT_GE(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
        if (nullptr == publication)
        {
            ::usleep(200);
        }
    }
    ASSERT_NE(nullptr, publication);
    const std::int32_t session_id = aeron_publication_session_id(publication);

    std::array<std::uint8_t, MESSAGE_LENGTH> message{};
    std::vector<std::array<std::uint8_t, MESSAGE_LENGTH>> sent;
    for (std::int32_t index = 0; index < MESSAGE_COUNT; ++index)
    {
        std::fill(message.begin(), message.end(), static_cast<std::uint8_t>(index));
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (aeron_publication_offer(publication, message.data(), message.size(), nullptr, nullptr) < 0)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                FAIL() << "the publication never accepted message " << index;
            }
            (void)aeron_main_do_work(test.aeron());
            ::usleep(200);
        }
        sent.push_back(message);
    }
    const std::int64_t position = aeron_publication_position(publication);

    // The recording reaches the published position, read through the driver's
    // own counter.
    std::int32_t counter_id = AERON_NULL_COUNTER_ID;
    for (int iteration = 0; iteration < 30'000 && AERON_NULL_COUNTER_ID == counter_id; ++iteration)
    {
        counter_id = RecordingPos::find_by_session(aeron_counters_reader(test.aeron()), ARCHIVE_ID, session_id);
        ::usleep(200);
    }
    ASSERT_NE(static_cast<std::int32_t>(AERON_NULL_COUNTER_ID), counter_id);
    std::int64_t recorded = 0;
    std::int64_t* counter_address = aeron_counters_reader_addr(aeron_counters_reader(test.aeron()), counter_id);
    for (int iteration = 0; iteration < 30'000 && recorded < position; ++iteration)
    {
        recorded = std::atomic_ref<std::int64_t>(*counter_address).load(std::memory_order_acquire);
        ::usleep(200);
    }
    EXPECT_EQ(position, recorded);

    ASSERT_EQ(0, aeron_archive_stop_recording_subscription(test.archive(), subscription_id)) << aeron_errmsg();
    test.stop_recording_and_settle();

    std::int64_t recording_id = -1;
    std::uint8_t* key = nullptr;
    ASSERT_EQ(0, aeron_counters_reader_metadata_key(aeron_counters_reader(test.aeron()), counter_id, &key));
    ASSERT_NE(nullptr, key);
    std::int32_t identity_length = 0;
    std::memcpy(&identity_length, key + RecordingPos::SOURCE_IDENTITY_LENGTH_OFFSET, sizeof(identity_length));
    const auto view = RecordingPos::parse_key(
        {key, static_cast<std::size_t>(RecordingPos::SOURCE_IDENTITY_OFFSET + identity_length + 8)});
    ASSERT_TRUE(view.has_value());
    recording_id = view->recording_id;
    ASSERT_GE(recording_id, 0);

    // Replay: the archive sends to a subscription of ours; that leg uses the
    // kernel because the endpoint is loopback, which is the mixed-path check.
    const std::string replay_channel = "aeron:udp?endpoint=localhost:33434";
    aeron_async_add_subscription_t* subscription_async = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(&subscription_async, test.aeron(), replay_channel.c_str(),
                                              REPLAY_STREAM_ID, nullptr, nullptr, nullptr, nullptr));
    aeron_subscription_t* subscription = nullptr;
    for (int iteration = 0; iteration < 30'000 && nullptr == subscription; ++iteration)
    {
        ASSERT_GE(aeron_async_add_subscription_poll(&subscription, subscription_async), 0) << aeron_errmsg();
        if (nullptr == subscription)
        {
            ::usleep(200);
        }
    }
    ASSERT_NE(nullptr, subscription);

    aeron_archive_replay_params_t params{};
    ASSERT_EQ(0, aeron_archive_replay_params_init(&params));
    params.position = 0;
    params.length = position;
    std::int64_t replay_session_id = 0;
    ASSERT_EQ(0, aeron_archive_start_replay(&replay_session_id, test.archive(), recording_id, replay_channel.c_str(),
                                            REPLAY_STREAM_ID, &params))
        << aeron_errmsg();

    std::vector<std::array<std::uint8_t, MESSAGE_LENGTH>> replayed;
    const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
    while (replayed.size() < sent.size() && std::chrono::steady_clock::now() < deadline)
    {
        (void)aeron_main_do_work(test.aeron());
        for (int index = 0; index < aeron_subscription_image_count(subscription); ++index)
        {
            aeron_image_t* image = aeron_subscription_image_at_index(subscription, index);
            if (nullptr != image)
            {
                (void)aeron_image_poll(image, &collect_fragment, &replayed, 10);
                aeron_image_release(image);
            }
        }
        ::usleep(200);
    }
    (void)aeron_archive_stop_replay(test.archive(), replay_session_id);

    ASSERT_EQ(sent.size(), replayed.size()) << "the replay returned " << replayed.size() << " messages";
    for (std::size_t index = 0; index < sent.size(); ++index)
    {
        EXPECT_EQ(0, std::memcmp(sent[index].data(), replayed[index].data(), MESSAGE_LENGTH)) << index;
    }

    // "On the media" is a counter, not an absence of errors: 76 grows as the
    // archive's transports take datagrams off the stack.
    EXPECT_TRUE(test.media_counters_moved()) << "no transport (76) or port (82) counter moved";

    (void)aeron_subscription_close(subscription, nullptr, nullptr);
    (void)aeron_publication_close(publication, nullptr, nullptr);
}

}  // namespace

#endif  // AXC_WITH_DPDK
