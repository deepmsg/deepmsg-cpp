#pragma once

#include <cstdint>
#include <span>

#include "cluster/common/cluster_member.h"
#include "cluster/common/recording_log.h"
#include "core/error/error.h"

namespace deepmsg::cluster::backup
{

/// `ClusterBackupEventsListener` (the reference's five callbacks): progress of
/// a backup, for an operator or a test. The reference passes a Java object on
/// `ClusterBackup.Context.eventsListener`; this is the same seam with default
/// no-ops so an implementation overrides only what it needs.
class ClusterBackupEventsListener
{
public:
    virtual ~ClusterBackupEventsListener() = default;

    /// `onBackupQuery()`: the backup entered `BACKUP_QUERY`.
    virtual void on_backup_query() {}

    /// `onPossibleFailure(Exception)`: progress stalled or an operation failed.
    virtual void on_possible_failure(const axc::core::Error& /*error*/) {}

    /// `onBackupResponse(clusterMembers, logSourceMember, snapshotsToRetrieve)`.
    virtual void on_backup_response(std::span<const common::ClusterMember> /*cluster_members*/,
                                    const common::ClusterMember& /*log_source_member*/,
                                    std::span<const common::RecordingLog::Snapshot> /*snapshots_to_retrieve*/)
    {
    }

    /// `onUpdatedRecordingLog(recordingLog, snapshotsRetrieved)`.
    virtual void on_updated_recording_log(const common::RecordingLog& /*recording_log*/,
                                          std::span<const common::RecordingLog::Snapshot> /*snapshots_retrieved*/)
    {
    }

    /// `onLiveLogProgress(recordingId, recordingPosCounterId, logPosition)`.
    virtual void on_live_log_progress(std::int64_t /*recording_id*/, std::int32_t /*recording_pos_counter_id*/,
                                      std::int64_t /*log_position*/)
    {
    }
};

}  // namespace deepmsg::cluster::backup
