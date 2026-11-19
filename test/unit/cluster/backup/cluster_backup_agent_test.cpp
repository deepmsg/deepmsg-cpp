// `ClusterBackupAgentTest` (aeron-cluster `:37-96`) by name: the three
// `replayStartPosition` cases. The fourth reference case
// (`shouldRunTerminationHooksOnTermination`) needs a mocked `Aeron` and is
// covered at the process level instead: the backup binary's SIGTERM path exits
// cleanly in `Xc9BackupTest.theBackupStartsAsAReferenceReadableBackup`.

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <vector>

#include "cluster/backup/cluster_backup_agent.h"
#include "cluster/backup/cluster_backup_context.h"
#include "cluster/common/recording_log.h"

namespace
{

using deepmsg::cluster::backup::BackupReplayStart;
using deepmsg::cluster::backup::replay_start_position;
using deepmsg::cluster::backup::StopPositionFn;
using deepmsg::cluster::common::RecordingLog;

constexpr std::int64_t NULL_POSITION = -1;

std::int64_t fake_stop_position(void* context, std::int64_t /*recording_id*/) noexcept
{
    return *static_cast<std::int64_t*>(context);
}

TEST(ClusterBackupAgentTest, shouldReturnReplayStartPositionIfAlreadyExisting)
{
    constexpr std::int64_t expected_start_position = 892374;
    constexpr std::int64_t recording_id = 234;

    RecordingLog::Entry last_term{};
    last_term.recording_id = recording_id;

    std::int64_t stop_position = expected_start_position;
    const std::vector<RecordingLog::Snapshot> snapshots{};

    EXPECT_EQ(expected_start_position, replay_start_position(&last_term, snapshots, BackupReplayStart::beginning,
                                                             &fake_stop_position, &stop_position));
}

TEST(ClusterBackupAgentTest, shouldReturnNullPositionIfLastTermIsNullAndSnapshotsIsEmpty)
{
    const std::vector<RecordingLog::Snapshot> snapshots{};
    EXPECT_EQ(NULL_POSITION,
              replay_start_position(nullptr, snapshots, BackupReplayStart::beginning, &fake_stop_position, nullptr));
}

TEST(ClusterBackupAgentTest, shouldLargestPositionLessThanOrEqualToInitialReplayPosition)
{
    std::vector<RecordingLog::Snapshot> snapshots;
    for (const std::int64_t position : {1000, 2000, 3000, 4000, 5000, 6000})
    {
        RecordingLog::Snapshot snapshot{};
        snapshot.recording_id = 1;
        snapshot.log_position = position;
        snapshot.service_id = 0;
        snapshots.push_back(snapshot);
    }

    EXPECT_EQ(NULL_POSITION,
              replay_start_position(nullptr, snapshots, BackupReplayStart::beginning, &fake_stop_position, nullptr));
    EXPECT_EQ(6000, replay_start_position(nullptr, snapshots, BackupReplayStart::latest_snapshot, &fake_stop_position,
                                          nullptr));
}

}  // namespace
