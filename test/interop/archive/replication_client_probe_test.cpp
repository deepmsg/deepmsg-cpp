// A4b-1 probe: the replication source client is the official C archive
// client's **non-blocking pieces** (async connect + proxy + pollers), sharing
// the conductor's own Aeron client — the C++ counterpart of the reference's
// `ctx.archiveClientContext().aeron(aeron)` (`Archive.java:1620`).
//
// What it measures, before any replication code is written:
//   1. `aeron_archive_async_connect` (which synchronously draws a session id
//      inside `aeron_archive_context_conclude`) does not wedge when the client
//      conductor is driven by us in invoker mode;
//   2. a request offered through `aeron_archive_proxy_*` and read back through
//      `aeron_archive_control_response_poller` completes with the right
//      correlation id.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include <unistd.h>

#include "archive/server/archive_conductor.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/c_driver.h"

// The internal C headers carry no `extern "C"` guard.
extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
#include "client/aeron_archive_client.h"
#include "client/aeron_archive_context.h"
#include "client/aeron_archive_control_response_poller.h"
#include "client/aeron_archive_proxy.h"
}

#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT "aeronmd"
#endif

namespace
{

using axc::core::ClientConfig;
using axc::core::DeepmsgClient;
using deepmsg::archive::server::ArchiveConductor;
using deepmsg::archive::server::ArchiveConductorContext;

constexpr const char* CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:34600";
constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 20;

class ReplicationClientProbeTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-replication-probe-XXXXXX");
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

        auto driver = axc::interop::CDriver::start(AXC_AERONMD_DEFAULT, aeron_dir_, {});
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::move(*driver);

        ArchiveConductorContext context;
        context.archive_dir = archive_dir_;
        context.aeron_dir = aeron_dir_;
        context.control_channel = CONTROL_CHANNEL;
        context.control_stream_id = CONTROL_STREAM_ID;
        context.local_control_channel = "aeron:ipc?term-length=64k";
        context.local_control_stream_id = CONTROL_STREAM_ID + 2;
        context.archive_id = 42;

        auto conductor = ArchiveConductor::create(context);
        ASSERT_TRUE(conductor) << conductor.error().message.view();
        conductor_ = std::move(*conductor);

        ClientConfig client_config;
        client_config.dir = aeron_dir_;
        client_config.use_conductor_agent_invoker = true;
        auto client = DeepmsgClient::connect(client_config);
        ASSERT_TRUE(client) << client.error().message.view();
        client_ = std::move(*client);
    }

    void TearDown() override
    {
        if (nullptr != archive_)
        {
            (void)aeron_archive_close(archive_);
            archive_ = nullptr;
        }
        if (nullptr != async_)
        {
            aeron_archive_async_connect_delete(async_);
            async_ = nullptr;
        }
        if (nullptr != archive_context_)
        {
            (void)aeron_archive_context_close(archive_context_);
            archive_context_ = nullptr;
        }
        client_.close();
        if (conductor_)
        {
            conductor_->close();
            conductor_.reset();
        }
        driver_.stop();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    /// Drives both sides once: the archive (control subscriptions) and the
    /// shared Aeron client (async adds, offers, pollers).
    void drive_once()
    {
        (void)conductor_->do_work();
        (void)client_.do_work();
    }

    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    axc::interop::CDriver driver_;
    std::unique_ptr<ArchiveConductor> conductor_;
    DeepmsgClient client_;
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_async_connect_t* async_{nullptr};
    aeron_archive_t* archive_{nullptr};
};

TEST_F(ReplicationClientProbeTest, theSourceClientConnectsOnTheSharedAeronClientAndGetsAResponse)
{
    // The archive's own control subscription has to be connected before the
    // source client's connect request goes out; there is no retry in the
    // client's connect state machine.
    for (int iteration = 0; iteration < 100; iteration++)
    {
        drive_once();
        ::usleep(1000);
    }

    ASSERT_EQ(0, aeron_archive_context_init(&archive_context_));
    ASSERT_EQ(0, aeron_archive_context_set_aeron_directory_name(archive_context_, aeron_dir_.c_str()));
    ASSERT_EQ(0, aeron_archive_context_set_control_request_channel(archive_context_, CONTROL_CHANNEL));
    ASSERT_EQ(0, aeron_archive_context_set_control_request_stream_id(archive_context_, CONTROL_STREAM_ID));
    ASSERT_EQ(0,
              aeron_archive_context_set_control_response_channel(archive_context_, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ(0, aeron_archive_context_set_control_response_stream_id(archive_context_, RESPONSE_STREAM_ID));

    // Share the conductor's Aeron client (`Archive.java:1620`).
    archive_context_->aeron = client_.handle();
    archive_context_->owns_aeron_client = false;

    const auto connect_start = std::chrono::steady_clock::now();
    ASSERT_EQ(0, aeron_archive_async_connect(&async_, archive_context_)) << aeron_errmsg();

    for (int iteration = 0; iteration < 20'000 && nullptr == archive_; iteration++)
    {
        drive_once();
        const int poll_result = aeron_archive_async_connect_poll(&archive_, async_);
        if (0 != poll_result)
        {
            // `aeron_archive_async_connect_transition_to_done` deletes the
            // async object on both outcomes (`:513-515`, `:422-424`).
            async_ = nullptr;
        }
        if (poll_result < 0)
        {
            FAIL() << "async connect failed: " << aeron_errmsg();
        }
        ::usleep(200);
    }

    const auto connect_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - connect_start).count();
    ASSERT_NE(nullptr, archive_) << "the shared-client connect did not complete";
    EXPECT_LT(connect_ms, 5000) << "the session-id draw inside conclude must not wedge the turn";
    EXPECT_GE(aeron_archive_control_session_id(archive_), 0);

    // One request/response round trip through the non-blocking proxy and the
    // control response poller: an unknown recording is answered with ERROR.
    aeron_archive_proxy_t* proxy = aeron_archive_proxy(archive_);
    ASSERT_NE(nullptr, proxy);

    const std::int64_t correlation_id = aeron_archive_next_correlation_id(archive_);
    const std::int64_t unknown_recording_id = 999'999;
    ASSERT_TRUE(aeron_archive_proxy_list_recording(proxy, correlation_id, unknown_recording_id));

    aeron_archive_control_response_poller_t* poller = aeron_archive_control_response_poller(archive_);
    ASSERT_NE(nullptr, poller);

    bool completed = false;
    for (int iteration = 0; iteration < 20'000 && !completed; iteration++)
    {
        drive_once();
        ASSERT_GE(aeron_archive_control_response_poller_poll(poller), 0) << aeron_errmsg();
        completed = poller->is_poll_complete && poller->correlation_id == correlation_id;
        ::usleep(200);
    }

    ASSERT_TRUE(completed) << "no control response for correlationId=" << correlation_id;
    // `ControlResponseCode.RECORDING_UNKNOWN` (2), relevantId = the recording id
    // (`ControlSession.java:703-706`); it is not the generic ERROR code.
    EXPECT_EQ(2, poller->code_value);
    EXPECT_FALSE(poller->is_code_error);
    EXPECT_EQ(unknown_recording_id, poller->relevant_id);
}

}  // namespace
