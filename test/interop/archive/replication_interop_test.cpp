// A4b-3 criteria: the `ReplicateRequest2` (66) / `StopReplicationRequest` (51)
// arms and the refusals `ArchiveConductor.replicate` makes before a session
// exists. The reference C suite's own replication cases need the full state
// machine and land with A4b-5..A4b-7.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "archive/server/archive_conductor.h"
#include "fixture/c_driver.h"

// The internal async connect header carries no `extern "C"` guard.
extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
}

#ifndef AXC_AERONMD_DEFAULT
#define AXC_AERONMD_DEFAULT "aeronmd"
#endif

namespace
{

using deepmsg::archive::server::ArchiveConductor;
using deepmsg::archive::server::ArchiveConductorContext;

constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr std::int64_t ARCHIVE_ID = 42;
constexpr const char* SRC_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8010";
constexpr std::int32_t SRC_CONTROL_STREAM_ID = 10;
constexpr const char* REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8012";

class ReplicationInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-replication-XXXXXX");
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

        start_conductor();
        connect_client();
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
        if (nullptr != async_)
        {
            aeron_archive_async_connect_delete(async_);
            async_ = nullptr;
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
        context.local_control_channel = "aeron:ipc?term-length=64k";
        context.local_control_stream_id = CONTROL_STREAM_ID + 2;
        context.archive_id = ARCHIVE_ID;

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
            if (0 != poll)
            {
                // The library deletes the async object on completion.
                async_ = nullptr;
            }
            if (nullptr != archive)
            {
                client_archive_ = archive;
                return;
            }
            ::usleep(200);
        }

        FAIL() << "the async connect did not complete";
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
};

TEST_F(ReplicationInteropTest, replicateReturnsAReplicationIdAndTryStopSeesIt)
{
    aeron_archive_replication_params_t params;
    ASSERT_EQ(0, aeron_archive_replication_params_init(&params));
    params.replication_channel = REPLICATION_CHANNEL;
    params.file_io_max_length = 4096;
    // The conductor's own control channel here is IPC, so there is no endpoint
    // to derive a response channel from; the client supplies one, exactly like
    // a real client replicating from a destination with an IPC control
    // channel.
    params.src_response_channel = "aeron:udp?endpoint=localhost:0";

    std::int64_t replication_id = -1;
    ASSERT_EQ(0, aeron_archive_replicate(&replication_id, client_archive_, 1, SRC_CONTROL_CHANNEL,
                                         SRC_CONTROL_STREAM_ID, &params))
        << aeron_errmsg();
    EXPECT_GE(replication_id, 0);

    bool stopped = true;
    ASSERT_EQ(0, aeron_archive_try_stop_replication(&stopped, client_archive_, replication_id + 40'000));
    EXPECT_FALSE(stopped) << "an unknown replication id must answer with an error, not an OK";

    ASSERT_EQ(0, aeron_archive_try_stop_replication(&stopped, client_archive_, replication_id));
    EXPECT_TRUE(stopped);
}

TEST_F(ReplicationInteropTest, replicateRefusesAnUnknownDestination)
{
    aeron_archive_replication_params_t params;
    ASSERT_EQ(0, aeron_archive_replication_params_init(&params));
    params.replication_channel = REPLICATION_CHANNEL;
    params.dst_recording_id = 12'345;

    std::int64_t replication_id = -1;
    EXPECT_EQ(-1, aeron_archive_replicate(&replication_id, client_archive_, 1, SRC_CONTROL_CHANNEL,
                                          SRC_CONTROL_STREAM_ID, &params));
    EXPECT_NE(std::string::npos, std::string{aeron_errmsg()}.find("unknown destination recording id 12345"))
        << aeron_errmsg();
}

TEST_F(ReplicationInteropTest, replicateRefusesAMissingReplicationChannel)
{
    aeron_archive_replication_params_t params;
    ASSERT_EQ(0, aeron_archive_replication_params_init(&params));
    // No channel in the request and none configured on the archive.

    std::int64_t replication_id = -1;
    EXPECT_EQ(-1, aeron_archive_replicate(&replication_id, client_archive_, 1, SRC_CONTROL_CHANNEL,
                                          SRC_CONTROL_STREAM_ID, &params));
    EXPECT_NE(std::string::npos, std::string{aeron_errmsg()}.find("replicationChannel must be set")) << aeron_errmsg();
}

}  // namespace
