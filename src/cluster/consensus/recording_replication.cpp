#include "cluster/consensus/recording_replication.h"

#include <string>
#include <utility>

namespace deepmsg::cluster::consensus
{

RecordingReplication::RecordingReplication(axc::core::DeepmsgClient& client, aeron_archive_t* archive,
                                           std::int64_t archive_id, std::int64_t src_recording_id,
                                           std::string src_archive_channel, std::int32_t src_control_stream_id,
                                           const std::string& replication_channel, std::int64_t dst_recording_id,
                                           std::int64_t stop_position, std::optional<std::string> src_response_channel,
                                           std::int32_t replication_session_id, std::int64_t progress_check_timeout_ns,
                                           std::int64_t progress_check_interval_ns, std::int64_t now_ns)
    : client_(&client),
      archive_(archive),
      archive_id_(archive_id),
      stop_position_(stop_position),
      progress_check_timeout_ns_(progress_check_timeout_ns),
      progress_check_interval_ns_(progress_check_interval_ns),
      src_archive_channel_(std::move(src_archive_channel))
{
    // `RecordingReplication(...)` (`:52-77`). The reference throws out of the
    // constructor when `replicate` fails; here the caller turns `failed()`
    // into the `newLogReplication` error.
    progress_deadline_ns_ = now_ns + progress_check_timeout_ns_;
    progress_check_deadline_ns_ = now_ns + progress_check_interval_ns_;

    aeron_archive_replication_params_t params{};
    if (0 != aeron_archive_replication_params_init(&params))
    {
        error_ = axc::core::make_error(axc::core::ErrorCode::internal_error, "replication params init failed");
        return;
    }
    params.stop_position = stop_position_;
    params.dst_recording_id = dst_recording_id;
    params.replication_session_id = replication_session_id;
    params.replication_channel = replication_channel.c_str();
    if (src_response_channel.has_value() && !src_response_channel->empty())
    {
        params.src_response_channel = src_response_channel->c_str();
    }

    if (0 != aeron_archive_replicate(&replication_id_, archive_, src_recording_id, src_archive_channel_.c_str(),
                                     src_control_stream_id, &params))
    {
        error_ = axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                       std::string{"replicate failed: "} + std::string{aeron_errmsg()});
    }
}

RecordingReplication::~RecordingReplication()
{
    close();
}

std::expected<int, Error> RecordingReplication::poll(std::int64_t now_ns)
{
    // `poll(long)` (`:87-127`).
    int work_count = 0;

    if (has_replication_ended_)
    {
        return work_count;
    }

    if (deleted_)
    {
        close();
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::protocol_error,
            "recording was deleted during replication: recordingId=" + std::to_string(recording_id_)));
    }

    if (now_ns >= progress_check_deadline_ns_)
    {
        progress_check_deadline_ns_ = now_ns + progress_check_interval_ns_;
        if (poll_dst_recording_position())
        {
            progress_deadline_ns_ = now_ns + progress_check_timeout_ns_;
        }
        work_count++;
    }

    if (now_ns >= progress_deadline_ns_)
    {
        close();
        if (NULL_VALUE == stop_position_ || position_ < stop_position_)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::timed_out, "log replication has not progressed"));
        }
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::timed_out, "log replication failed to stop"));
    }

    return work_count;
}

void RecordingReplication::on_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                                     std::int32_t signal)
{
    // `onSignal(...)` (`:157-192`).
    if (correlation_id != replication_id_)
    {
        return;
    }

    if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND == signal)
    {
        recording_position_counter_id_ = aeron_archive_recording_pos_find_counter_id_by_recording_id(
            client_->counters_reader(), recording_id, archive_id_);
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC == signal)
    {
        has_synced_ = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END == signal)
    {
        has_replication_ended_ = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP == signal)
    {
        if (NULL_VALUE != position)
        {
            position_ = position;
        }
        has_stopped_ = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE == signal)
    {
        // The reference throws; `poll` reports it on the same `doWork` pass.
        deleted_ = true;
    }

    last_recording_signal_ = signal;

    if (NULL_VALUE != recording_id)
    {
        recording_id_ = recording_id;
    }

    if (NULL_VALUE != position)
    {
        position_ = position;
    }
}

bool RecordingReplication::poll_dst_recording_position()
{
    // `pollDstRecordingPosition()` (`:194-210`).
    if (AERON_NULL_COUNTER_ID != recording_position_counter_id_)
    {
        auto* reader = client_->counters_reader();
        auto* address = aeron_counters_reader_addr(reader, recording_position_counter_id_);
        if (nullptr != address)
        {
            const std::int64_t recording_position = __atomic_load_n(address, __ATOMIC_ACQUIRE);
            bool active = false;
            if (0 == aeron_archive_recording_pos_is_active(&active, reader, recording_position_counter_id_,
                                                           recording_id_) &&
                active && recording_position > position_)
            {
                position_ = recording_position;
                return true;
            }
        }
    }

    return false;
}

void RecordingReplication::close()
{
    // `close()` (`:139-155`).
    if (!has_replication_ended_)
    {
        has_replication_ended_ = true;
        if (NULL_VALUE != replication_id_ && nullptr != archive_)
        {
            bool stopped = false;
            (void)aeron_archive_try_stop_replication(&stopped, archive_, replication_id_);
        }
    }
}

}  // namespace deepmsg::cluster::consensus
