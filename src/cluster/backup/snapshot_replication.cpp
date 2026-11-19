#include "cluster/backup/snapshot_replication.h"

#include <utility>

namespace deepmsg::cluster::backup
{

SnapshotReplication::SnapshotReplication(axc::core::DeepmsgClient& client, aeron_archive_t* archive,
                                         std::int64_t archive_id, std::int32_t src_control_stream_id,
                                         std::string src_control_channel, std::string replication_channel,
                                         std::int64_t progress_timeout_ns, std::int64_t progress_interval_ns)
    : client_(&client),
      archive_(archive),
      archive_id_(archive_id),
      src_control_stream_id_(src_control_stream_id),
      src_control_channel_(std::move(src_control_channel)),
      replication_channel_(std::move(replication_channel)),
      progress_timeout_ns_(progress_timeout_ns),
      progress_interval_ns_(progress_interval_ns)
{
}

SnapshotReplication::~SnapshotReplication() = default;

void SnapshotReplication::add_snapshot(const common::RecordingLog::Snapshot& snapshot)
{
    pending_.push_back(Pending{snapshot, -1, -1});
}

bool SnapshotReplication::is_complete() const noexcept
{
    return cursor_ >= pending_.size();
}

bool SnapshotReplication::replicate_current(std::int64_t now_ns)
{
    if (cursor_ >= pending_.size())
    {
        return false;
    }

    const Pending& current = pending_[cursor_];
    replication_ = std::make_unique<consensus::RecordingReplication>(
        *client_, archive_, archive_id_, current.snapshot.recording_id, src_control_channel_, src_control_stream_id_,
        replication_channel_, current.dst_recording_id, current.stop_position, std::nullopt,
        static_cast<std::int32_t>(client_->next_correlation_id()), progress_timeout_ns_, progress_interval_ns_, now_ns);
    return true;
}

std::expected<int, axc::core::Error> SnapshotReplication::poll(std::int64_t now_ns)
{
    if (is_complete())
    {
        return 0;
    }

    int work_count = 0;
    if (nullptr == replication_)
    {
        if (!replicate_current(now_ns))
        {
            return 0;
        }
        if (replication_->failed())
        {
            return std::unexpected(replication_->error());
        }
        work_count++;
    }
    else
    {
        auto polled = replication_->poll(now_ns);
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        work_count += *polled;

        if (replication_->has_replication_ended())
        {
            if (replication_->has_synced())
            {
                completed_[pending_[cursor_].snapshot.recording_id] = replication_->recording_id();
                cursor_++;
                replication_.reset();
            }
            else
            {
                replication_.reset();
                if (!replicate_current(now_ns))
                {
                    return work_count;
                }
            }
            work_count++;
        }
    }
    return work_count;
}

void SnapshotReplication::on_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                                    std::int32_t signal)
{
    if (nullptr != replication_)
    {
        replication_->on_signal(correlation_id, recording_id, position, signal);
    }
}

std::vector<common::RecordingLog::Snapshot> SnapshotReplication::snapshots_retrieved() const
{
    std::vector<common::RecordingLog::Snapshot> snapshots;
    snapshots.reserve(pending_.size());
    for (const Pending& pending : pending_)
    {
        const auto completed = completed_.find(pending.snapshot.recording_id);
        common::RecordingLog::Snapshot retrieved = pending.snapshot;
        if (completed != completed_.end())
        {
            retrieved.recording_id = completed->second;
        }
        snapshots.push_back(retrieved);
    }
    return snapshots;
}

}  // namespace deepmsg::cluster::backup
