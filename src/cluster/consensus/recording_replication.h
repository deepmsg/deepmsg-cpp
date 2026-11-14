#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <aeronc.h>

#include <client/aeron_archive.h>

#include "cluster/consensus/consensus_constants.h"
#include "cluster/election/election_host.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `RecordingReplication` (`RecordingReplication.java`): replicates a source
/// archive's log recording into this member's own archive, driven by the
/// recording signals the archive client dispatches
/// (`ConsensusModuleAgent.pollArchiveEvents`).
class RecordingReplication final : public election::RecordingReplication
{
public:
    RecordingReplication(axc::core::DeepmsgClient& client, aeron_archive_t* archive, std::int64_t archive_id,
                         std::int64_t src_recording_id, std::string src_archive_channel,
                         std::int32_t src_control_stream_id, const std::string& replication_channel,
                         std::int64_t dst_recording_id, std::int64_t stop_position,
                         std::optional<std::string> src_response_channel, std::int32_t replication_session_id,
                         std::int64_t progress_check_timeout_ns, std::int64_t progress_check_interval_ns,
                         std::int64_t now_ns);
    ~RecordingReplication() override;

    RecordingReplication(const RecordingReplication&) = delete;
    RecordingReplication& operator=(const RecordingReplication&) = delete;

    /// A replicate request the archive refused at submission time. The
    /// reference throws out of the constructor; the caller turns this into the
    /// `newLogReplication` error instead.
    [[nodiscard]] bool failed() const noexcept
    {
        return axc::core::ErrorCode::none != error_.code;
    }
    [[nodiscard]] const Error& error() const noexcept
    {
        return error_;
    }

    /// `poll(long)` (`:87-127`).
    [[nodiscard]] std::expected<int, Error> poll(std::int64_t now_ns) override;
    /// `onSignal(...)` (`:157-192`).
    void on_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                   std::int32_t signal) override;
    [[nodiscard]] bool has_replication_ended() const override
    {
        return has_replication_ended_;
    }
    [[nodiscard]] bool has_stopped() const override
    {
        return has_stopped_;
    }
    [[nodiscard]] bool has_synced() const override
    {
        return has_synced_;
    }
    [[nodiscard]] std::string_view src_archive_channel() const override
    {
        return src_archive_channel_;
    }
    [[nodiscard]] std::int64_t recording_id() const override
    {
        return recording_id_;
    }
    [[nodiscard]] std::int64_t position() const override
    {
        return position_;
    }
    /// `close()` (`:139-155`).
    void close() override;

private:
    /// `pollDstRecordingPosition()` (`:194-210`).
    [[nodiscard]] bool poll_dst_recording_position();

    axc::core::DeepmsgClient* client_;
    aeron_archive_t* archive_;
    std::int64_t archive_id_;
    std::int64_t replication_id_{NULL_VALUE};
    std::int64_t stop_position_{NULL_VALUE};
    std::int64_t progress_check_timeout_ns_;
    std::int64_t progress_check_interval_ns_;
    std::string src_archive_channel_;

    std::int32_t recording_position_counter_id_{AERON_NULL_COUNTER_ID};
    std::int64_t recording_id_{NULL_VALUE};
    std::int64_t position_{NULL_VALUE};

    std::int64_t progress_deadline_ns_{0};
    std::int64_t progress_check_deadline_ns_{0};
    std::int32_t last_recording_signal_{AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE};

    bool has_replication_ended_{false};
    bool has_synced_{false};
    bool has_stopped_{false};
    /// A DELETE signal: the reference throws from `onSignal`, `poll` reports.
    bool deleted_{false};
    Error error_{};
};

}  // namespace deepmsg::cluster::consensus
