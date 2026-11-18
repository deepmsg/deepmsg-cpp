#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <expected>

#include "cluster/common/recording_log.h"
#include "cluster/consensus/recording_replication.h"
#include "core/deepmsg/deepmsg_client.h"

namespace deepmsg::cluster::backup
{

/// `SnapshotReplication`/`MultipleRecordingReplication`
/// (`SnapshotReplication.java:28-143`, `MultipleRecordingReplication.java:28-262`):
/// copies the cluster's snapshot recordings into the backup's own archive, one
/// recording at a time, driven by the archive client's recording signals.
class SnapshotReplication
{
public:
    SnapshotReplication(axc::core::DeepmsgClient& client, aeron_archive_t* archive, std::int64_t archive_id,
                        std::int32_t src_control_stream_id, std::string src_control_channel,
                        std::string replication_channel, std::int64_t progress_timeout_ns,
                        std::int64_t progress_interval_ns);
    SnapshotReplication(const SnapshotReplication&) = delete;
    SnapshotReplication& operator=(const SnapshotReplication&) = delete;
    SnapshotReplication(SnapshotReplication&&) = delete;
    SnapshotReplication& operator=(SnapshotReplication&&) = delete;
    ~SnapshotReplication();

    /// `addSnapshot` (`SnapshotReplication.java:60-64`): the destination is the
    /// next free id on the backup archive and the whole recording is copied.
    void add_snapshot(const common::RecordingLog::Snapshot& snapshot);

    [[nodiscard]] std::expected<int, axc::core::Error> poll(std::int64_t now_ns);

    void on_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position, std::int32_t signal);

    [[nodiscard]] bool is_complete() const noexcept;

    [[nodiscard]] std::vector<common::RecordingLog::Snapshot> snapshots_retrieved() const;

private:
    struct Pending
    {
        common::RecordingLog::Snapshot snapshot{};
        std::int64_t dst_recording_id{-1};
        std::int64_t stop_position{-1};
    };

    [[nodiscard]] bool replicate_current(std::int64_t now_ns);

    axc::core::DeepmsgClient* client_;
    aeron_archive_t* archive_;
    std::int64_t archive_id_;
    std::int32_t src_control_stream_id_;
    std::string src_control_channel_;
    std::string replication_channel_;
    std::int64_t progress_timeout_ns_;
    std::int64_t progress_interval_ns_;
    std::vector<Pending> pending_{};
    std::unordered_map<std::int64_t, std::int64_t> completed_{};
    std::size_t cursor_{0};
    std::unique_ptr<consensus::RecordingReplication> replication_{};
};

}  // namespace deepmsg::cluster::backup
