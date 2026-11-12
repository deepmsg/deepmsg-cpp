// C4a: the Q6 shell around `StandbySnapshotReplicator` — a node that starts
// with a standby snapshot newer than every local snapshot must warn (and count
// the error) or refuse to start; it never replicates.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "cluster/common/recording_log.h"
#include "cluster/consensus/standby_startup_check.h"

namespace
{

using deepmsg::cluster::common::RecordingLog;
using deepmsg::cluster::consensus::check_standby_snapshots_on_startup;
using deepmsg::cluster::consensus::StandbySnapshotStartupMode;

constexpr std::int32_t SERVICE_COUNT = 1;
constexpr std::int64_t LEADERSHIP_TERM_ID = 1;
constexpr std::int64_t TERM_BASE = 0;
constexpr std::int64_t TIMESTAMP = 1'000;
constexpr const char* ARCHIVE_ENDPOINT = "localhost:8080";

class StandbyStartupCheckTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-standby-check-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;

        auto log = RecordingLog::open(directory_, true);
        ASSERT_TRUE(log) << log.error().message.view();
        log_ = std::make_unique<RecordingLog>(std::move(*log));
    }

    void TearDown() override
    {
        log_.reset();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void append_standby_set(std::int64_t base_recording_id, std::int64_t log_position)
    {
        for (std::int32_t service_id = -1; service_id < SERVICE_COUNT; ++service_id)
        {
            auto appended =
                log_->append_standby_snapshot(base_recording_id + service_id + 1, LEADERSHIP_TERM_ID, TERM_BASE,
                                              log_position, TIMESTAMP, service_id, ARCHIVE_ENDPOINT);
            ASSERT_TRUE(appended) << appended.error().message.view();
        }
    }

    void append_local_set(std::int64_t base_recording_id, std::int64_t log_position)
    {
        for (std::int32_t service_id = -1; service_id < SERVICE_COUNT; ++service_id)
        {
            auto appended = log_->append_snapshot(base_recording_id + service_id + 1, LEADERSHIP_TERM_ID, TERM_BASE,
                                                  log_position, TIMESTAMP, service_id);
            ASSERT_TRUE(appended) << appended.error().message.view();
        }
    }

    std::string directory_;
    std::unique_ptr<RecordingLog> log_;
};

TEST_F(StandbyStartupCheckTest, withNoStandbySnapshotThereIsNothingToReport)
{
    int reported = 0;
    auto checked = check_standby_snapshots_on_startup(*log_, SERVICE_COUNT, StandbySnapshotStartupMode::warn,
                                                      [&reported](const auto&) { ++reported; });

    ASSERT_TRUE(checked);
    EXPECT_EQ(0, reported);
}

TEST_F(StandbyStartupCheckTest, aNewerStandbySnapshotIsReportedAndTheNodeStillStarts)
{
    append_standby_set(100, 500);

    std::vector<std::string> messages;
    auto checked = check_standby_snapshots_on_startup(*log_, SERVICE_COUNT, StandbySnapshotStartupMode::warn,
                                                      [&messages](const auto& error)
                                                      { messages.emplace_back(error.message.view()); });

    ASSERT_TRUE(checked);
    ASSERT_EQ(1U, messages.size());
    EXPECT_NE(std::string::npos, messages[0].find("standby snapshot is newer than the local snapshot"));
    EXPECT_NE(std::string::npos, messages[0].find(ARCHIVE_ENDPOINT));
    EXPECT_NE(std::string::npos, messages[0].find("logPosition=500"));
}

TEST_F(StandbyStartupCheckTest, refuseModeReturnsTheErrorInsteadOfStarting)
{
    append_standby_set(100, 500);

    int reported = 0;
    auto checked = check_standby_snapshots_on_startup(*log_, SERVICE_COUNT, StandbySnapshotStartupMode::refuse,
                                                      [&reported](const auto&) { ++reported; });

    ASSERT_FALSE(checked);
    EXPECT_EQ(0, reported);
    EXPECT_NE(std::string::npos,
              checked.error().message.view().find("aeron.cluster.cpp.standby.snapshot.on.startup=refuse"));
}

TEST_F(StandbyStartupCheckTest, aLocalSnapshotAtTheSamePositionIsNotNewer)
{
    append_standby_set(100, 500);
    append_local_set(200, 500);

    int reported = 0;
    auto checked = check_standby_snapshots_on_startup(*log_, SERVICE_COUNT, StandbySnapshotStartupMode::warn,
                                                      [&reported](const auto&) { ++reported; });

    ASSERT_TRUE(checked);
    EXPECT_EQ(0, reported);
}

TEST_F(StandbyStartupCheckTest, aLocalSnapshotAheadMakesTheStandbySetOld)
{
    append_standby_set(100, 500);
    append_local_set(200, 900);

    int reported = 0;
    auto checked = check_standby_snapshots_on_startup(*log_, SERVICE_COUNT, StandbySnapshotStartupMode::refuse,
                                                      [&reported](const auto&) { ++reported; });

    ASSERT_TRUE(checked);
    EXPECT_EQ(0, reported);
}

}  // namespace
