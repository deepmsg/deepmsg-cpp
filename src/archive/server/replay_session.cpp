#include "archive/server/replay_session.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "archive/server/segment_files.h"
#include "core/time/clock.h"

namespace deepmsg::archive::server
{
namespace
{

/// `ArchiveException.GENERIC` / `INVALID_POSITION` (`ArchiveException.java:29`, `:109`).
constexpr std::int64_t ERROR_GENERIC = 0;
constexpr std::int64_t ERROR_INVALID_POSITION = 16;

[[nodiscard]] std::int32_t read_int32(const std::uint8_t* address) noexcept
{
    std::int32_t value = 0;
    std::memcpy(&value, address, sizeof(value));
    return value;
}

[[nodiscard]] std::uint16_t read_uint16(const std::uint8_t* address) noexcept
{
    std::uint16_t value = 0;
    std::memcpy(&value, address, sizeof(value));
    return value;
}

}  // namespace

std::int64_t SystemReplayClock::nano_time() const noexcept
{
    return axc::core::NanoClock::now();
}

ReplaySession::ReplaySession(Config config, std::unique_ptr<ReplayPublication> publication,
                             ReplayControlResponses& control, ReplayMetrics& metrics, ReplayClock& clock,
                             const Checksum* checksum, ReplayLimitSource* limit, std::span<std::uint8_t> buffer,
                             ReplayWarningSink* warnings) noexcept
    : config_(std::move(config)),
      publication_(std::move(publication)),
      control_(control),
      metrics_(metrics),
      clock_(clock),
      checksum_(checksum),
      limit_(limit),
      warnings_(warnings),
      buffer_(buffer)
{
    // `ReplaySession` constructor (`ReplaySession.java:162-168`).
    segment_file_base_position_ = ::deepmsg::archive::server::segment_file_base_position(
        config_.start_position, config_.replay_position, config_.term_buffer_length, config_.segment_file_length);
    replay_position_ = config_.replay_position;
    stop_position_ = config_.stop_position;
    replay_limit_ = config_.replay_position + config_.replay_length;
    connect_deadline_ms_ = clock_.time_ms() + config_.connect_timeout_ms;
}

ReplaySession::~ReplaySession()
{
    close();
}

void ReplaySession::close() noexcept
{
    // `ReplaySession.close` (`:174-194`).
    if (closed_)
    {
        return;
    }
    closed_ = true;

    if (publication_)
    {
        if (revoke_publication_)
        {
            publication_->revoke();
        }
        else
        {
            publication_->close();
        }
    }

    close_recording_segment();
}

int ReplaySession::do_work()
{
    // `ReplaySession.doWork` (`:209-243`).
    int work_count = 0;

    if (aborted_)
    {
        revoke_publication_ = true;
        set_state(State::inactive, "replay aborted");
    }

    if (State::init == state_)
    {
        work_count += init();
    }

    // The reference throws out of doWork on error, skipping the INACTIVE
    // cleanup in this pass (`raiseError`, `:532-542`).
    if (!error_message_.empty())
    {
        return work_count;
    }

    if (State::replay == state_)
    {
        work_count += replay();
    }

    if (!error_message_.empty())
    {
        return work_count;
    }

    if (State::inactive == state_)
    {
        close_recording_segment();
        set_state(State::done, "");
    }

    return work_count;
}

void ReplaySession::abort(std::string_view reason) noexcept
{
    // `ReplaySession.abort` (`:248-252`); the reason is traced in the reference.
    abort_reason_.assign(reason);
    aborted_ = true;
}

void ReplaySession::send_pending_error()
{
    // `ReplaySession.sendPendingError` (`:288-295`).
    if (!error_message_.empty())
    {
        control_.send_error(config_.correlation_id, error_code_, error_message_);
    }
}

int ReplaySession::init()
{
    // `ReplaySession.init` (`:303-356`).
    if (fd_ < 0)
    {
        const std::string path =
            config_.archive_dir + "/" + segment_file_name(config_.recording_id, segment_file_base_position_);
        if (0 != ::access(path.c_str(), F_OK))
        {
            if (clock_.time_ms() > connect_deadline_ms_)
            {
                raise_error("recording segment file not created", ERROR_GENERIC);
            }
            return 0;
        }

        const std::int64_t start_term_base_position =
            config_.start_position - (config_.start_position & (config_.term_buffer_length - 1));
        const std::int32_t segment_offset = static_cast<std::int32_t>((replay_position_ - start_term_base_position) &
                                                                      (config_.segment_file_length - 1));
        const std::int32_t term_id =
            publication_->initial_term_id() +
            static_cast<std::int32_t>(replay_position_ >> publication_->position_bits_to_shift());

        if (!open_recording_segment())
        {
            return 0;
        }

        term_offset_ = static_cast<std::int32_t>(replay_position_ & (config_.term_buffer_length - 1));
        term_base_segment_offset_ = segment_offset - term_offset_;

        if (replay_position_ > config_.start_position && replay_position_ != stop_position_)
        {
            if (not_header_aligned(segment_offset, term_offset_, term_id))
            {
                if (error_message_.empty())
                {
                    raise_error("replayPosition=" + frame_position(0) + " does not point to a valid frame",
                                ERROR_INVALID_POSITION);
                }
                return 0;
            }
        }

        // ★ The OK goes out **before** waiting for the connection
        // (`ReplaySession.java:338`; the class comment at `:68` disagrees and the
        // code wins).
        control_.async_send_ok(config_.correlation_id, config_.replay_session_id);
    }

    if (!publication_->is_connected())
    {
        if (clock_.time_ms() > connect_deadline_ms_)
        {
            raise_error("no connection established for replayChannel=" + std::string{publication_->channel()} +
                            ", replayStreamId=" + std::to_string(publication_->stream_id()),
                        ERROR_GENERIC);
        }
        return 0;
    }

    set_state(State::replay, "");
    return 1;
}

int ReplaySession::replay()
{
    // `ReplaySession.replay` (`:359-461`).
    if (!publication_->is_connected())
    {
        revoke_publication_ = true;
        set_state(State::inactive, "publication is not connected");
        return 0;
    }

    if (config_.start_position == stop_position_ && 0 == replay_limit_)
    {
        set_state(State::inactive, "empty replay");
        return 0;
    }

    if (nullptr != limit_ && replay_position_ >= stop_position_ && not_extended(replay_position_, stop_position_))
    {
        return 0;
    }

    if (term_offset_ == config_.term_buffer_length)
    {
        next_term();
        if (!error_message_.empty())
        {
            return 0;
        }
    }

    int work_count = 0;
    if (publication_->available_window() > 0)
    {
        const std::int64_t start_ns = clock_.nano_time();
        const int bytes_read = read_recording(stop_position_ - replay_position_);
        if (bytes_read > 0)
        {
            const std::int32_t session_id = publication_->session_id();
            const std::int32_t stream_id = publication_->stream_id();
            const int remaining = static_cast<int>(
                std::min(replay_limit_ - replay_position_, static_cast<std::int64_t>(TERM_MAX_LENGTH)));
            int batch_offset = 0;
            int padding_frame_length = 0;
            bool unexpected_frame = false;

            while (batch_offset < bytes_read && batch_offset < remaining)
            {
                const std::int32_t frame_length = read_int32(buffer_.data() + batch_offset);
                if (frame_length <= 0)
                {
                    raise_error("unexpected end of recording at position=" + frame_position(batch_offset),
                                ERROR_GENERIC);
                    return 0;
                }

                const std::int32_t frame_type = read_uint16(buffer_.data() + batch_offset + FRAME_TYPE_OFFSET);
                const std::int32_t aligned_length = frame_align(frame_length, FRAME_ALIGNMENT);

                if (DATA_FRAME_TYPE == frame_type)
                {
                    if (batch_offset + aligned_length > bytes_read)
                    {
                        break;
                    }

                    if (nullptr != checksum_)
                    {
                        verify_checksum(batch_offset, aligned_length);
                        if (!error_message_.empty())
                        {
                            return 0;
                        }
                    }

                    // ★ Rewrite the frame header **after** the checksum check
                    // (`:409-423`): the recorded checksum lives in the session id
                    // field.
                    std::memcpy(buffer_.data() + batch_offset + SESSION_ID_OFFSET, &session_id, sizeof(session_id));
                    std::memcpy(buffer_.data() + batch_offset + STREAM_ID_OFFSET, &stream_id, sizeof(stream_id));
                    batch_offset += aligned_length;
                }
                else if (PADDING_FRAME_TYPE == frame_type)
                {
                    // A padding frame ends the batch (`:425-429`).
                    padding_frame_length = frame_length;
                    break;
                }
                else
                {
                    // The reference has no third branch and spins forever
                    // (`:397-430`). Keep "no progress" but bound the stall
                    // (A3b plan §3.3): break out; the next pass re-reads the same
                    // bytes and the deadline below ends the session.
                    unexpected_frame = true;
                    break;
                }
            }

            const std::int64_t read_time_ns = clock_.nano_time() - start_ns;
            metrics_.bytes_read(bytes_read);
            metrics_.read_time_ns(read_time_ns);

            if (unexpected_frame)
            {
                const std::int64_t position = segment_file_base_position_ + term_base_segment_offset_ + term_offset_;
                const std::int64_t now_ns = clock_.nano_time();
                if (stuck_position_ != position)
                {
                    stuck_position_ = position;
                    stuck_deadline_ns_ = now_ns + UNEXPECTED_FRAME_TIMEOUT_NS;
                    if (nullptr != warnings_)
                    {
                        warnings_->unexpected_frame(read_uint16(buffer_.data() + FRAME_TYPE_OFFSET), position);
                    }
                }
                else if (now_ns >= stuck_deadline_ns_)
                {
                    raise_error("unexpected frame type " +
                                    std::to_string(read_uint16(buffer_.data() + FRAME_TYPE_OFFSET)) +
                                    " at position=" + frame_position(0) + " (no progress for " +
                                    std::to_string(UNEXPECTED_FRAME_TIMEOUT_NS) + " ns)",
                                ERROR_GENERIC);
                }
            }

            if (batch_offset > 0)
            {
                const std::int64_t position =
                    publication_->offer_block({buffer_.data(), static_cast<std::size_t>(batch_offset)});
                if (AERON_PUBLICATION_ERROR == position)
                {
                    raise_error(std::string{"failed to offer replay block: "} + aeron_errmsg(), ERROR_GENERIC);
                    return 0;
                }

                if (has_publication_advanced(position, batch_offset))
                {
                    work_count++;
                }
                else
                {
                    padding_frame_length = 0;
                }
            }

            if (padding_frame_length > 0)
            {
                const std::int64_t position = publication_->append_padding(padding_frame_length - DATA_HEADER_LENGTH);
                if (AERON_PUBLICATION_ERROR == position)
                {
                    raise_error(std::string{"failed to append replay padding: "} + aeron_errmsg(), ERROR_GENERIC);
                    return 0;
                }

                if (has_publication_advanced(position, frame_align(padding_frame_length, FRAME_ALIGNMENT)))
                {
                    work_count++;
                }
            }
        }
    }

    return work_count;
}

std::string ReplaySession::frame_position(std::int32_t frame_offset) const
{
    // `ReplaySession.framePosition` (`:463-471`).
    const std::int64_t position = segment_file_base_position_ + term_base_segment_offset_ + term_offset_ + frame_offset;

    return std::to_string(position) + " (segmentFilePosition=" + std::to_string(segment_file_base_position_) +
           ", segmentOffset=" + std::to_string(term_base_segment_offset_) +
           ", termOffset=" + std::to_string(term_offset_) + ", frameOffset=" + std::to_string(frame_offset) + ")";
}

bool ReplaySession::has_publication_advanced(std::int64_t position, std::int32_t aligned_length)
{
    // `ReplaySession.hasPublicationAdvanced` (`:473-494`).
    if (position > 0)
    {
        term_offset_ += aligned_length;
        replay_position_ += aligned_length;
        stuck_position_ = -1;

        if (replay_position_ >= replay_limit_)
        {
            set_state(State::inactive, "position (" + std::to_string(replay_position_) + ") past limit (" +
                                           std::to_string(replay_limit_) + ")");
        }

        return true;
    }

    if (AERON_PUBLICATION_CLOSED == position || AERON_PUBLICATION_NOT_CONNECTED == position)
    {
        revoke_publication_ = true;
        set_state(State::inactive, "stream closed before replay complete");
    }

    return false;
}

void ReplaySession::verify_checksum(std::int32_t frame_offset, std::int32_t aligned_length)
{
    // `ReplaySession.verifyChecksum` (`:496-508`).
    const std::int32_t computed =
        checksum_->compute(buffer_.data(), frame_offset + DATA_HEADER_LENGTH, aligned_length - DATA_HEADER_LENGTH);
    const std::int32_t recorded = read_int32(buffer_.data() + frame_offset + SESSION_ID_OFFSET);

    if (computed != recorded)
    {
        raise_error("CRC checksum mismatch at position=" + frame_position(frame_offset) + ": recorded checksum=" +
                        std::to_string(recorded) + ", computed checksum=" + std::to_string(computed),
                    ERROR_GENERIC);
    }
}

int ReplaySession::read_recording(std::int64_t available_replay)
{
    // `ReplaySession.readRecording` (`:510-530`).
    const std::int64_t limit = std::min({available_replay, static_cast<std::int64_t>(buffer_.size()),
                                         static_cast<std::int64_t>(config_.term_buffer_length - term_offset_)});
    if (limit <= 0)
    {
        return 0;
    }

    if (fd_ < 0)
    {
        raise_error("recording segment is not open", ERROR_GENERIC);
        return 0;
    }

    const std::int64_t position = static_cast<std::int64_t>(term_base_segment_offset_) + term_offset_;
    int total = 0;
    while (total < static_cast<int>(limit))
    {
        const ssize_t bytes_read =
            ::pread(fd_, buffer_.data() + total, static_cast<std::size_t>(static_cast<int>(limit) - total),
                    static_cast<off_t>(position + total));
        if (bytes_read <= 0)
        {
            break;
        }
        total += static_cast<int>(bytes_read);
    }

    // The reference returns the limit even after a short read; the frame loop
    // turns the stale bytes into a `frameLength <= 0` error (`:510-530`).
    return static_cast<int>(limit);
}

bool ReplaySession::not_extended(std::int64_t replay_position, std::int64_t old_stop_position)
{
    // `ReplaySession.notExtended` (`:544-578`).
    const std::int64_t current_limit_position = limit_->value();
    std::int64_t new_stop_position = old_stop_position;

    if (limit_->is_closed())
    {
        if (limit_->registration_matches())
        {
            replay_limit_ = current_limit_position;
            new_stop_position = std::max(old_stop_position, current_limit_position);
        }
        else if (replay_limit_ >= old_stop_position)
        {
            replay_limit_ = old_stop_position;
        }
    }
    else
    {
        new_stop_position = current_limit_position;
    }

    if (replay_position >= replay_limit_)
    {
        set_state(State::inactive, "position (" + std::to_string(replay_position) + ") past limit (" +
                                       std::to_string(replay_limit_) + ") (notExtended)");
    }
    else if (new_stop_position > old_stop_position)
    {
        stop_position_ = new_stop_position;
        return false;
    }

    return true;
}

void ReplaySession::next_term()
{
    // `ReplaySession.nextTerm` (`:580-592`).
    term_offset_ = 0;
    term_base_segment_offset_ += config_.term_buffer_length;

    if (term_base_segment_offset_ == config_.segment_file_length)
    {
        close_recording_segment();
        segment_file_base_position_ += config_.segment_file_length;
        if (!open_recording_segment())
        {
            return;
        }
        term_base_segment_offset_ = 0;
    }
}

void ReplaySession::close_recording_segment() noexcept
{
    // `ReplaySession.closeRecordingSegment` (`:594-599`).
    if (fd_ >= 0)
    {
        (void)::close(fd_);
        fd_ = -1;
    }
}

bool ReplaySession::open_recording_segment()
{
    // `ReplaySession.openRecordingSegment` (`:601-616`).
    const std::string name = segment_file_name(config_.recording_id, segment_file_base_position_);
    const std::string path = config_.archive_dir + "/" + name;

    if (0 != ::access(path.c_str(), F_OK))
    {
        raise_error("recording segment not found", ERROR_GENERIC);
        return false;
    }

    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0)
    {
        raise_error(std::string{"failed to open recording segment: "} + std::strerror(errno), ERROR_GENERIC);
        return false;
    }

    return true;
}

bool ReplaySession::not_header_aligned(std::int32_t segment_offset, std::int32_t term_offset, std::int32_t term_id)
{
    // `ReplaySession.notHeaderAligned` + `isInvalidHeader` (`:618-661`).
    const ssize_t bytes_read = ::pread(fd_, header_.data(), header_.size(), static_cast<off_t>(segment_offset));
    if (static_cast<ssize_t>(header_.size()) != bytes_read)
    {
        raise_error("failed to read fragment header", ERROR_GENERIC);
        return true;
    }

    return read_int32(header_.data() + TERM_OFFSET_FIELD_OFFSET) != term_offset ||
           read_int32(header_.data() + TERM_ID_OFFSET) != term_id ||
           read_int32(header_.data() + STREAM_ID_OFFSET) != config_.stream_id;
}

void ReplaySession::raise_error(std::string_view message, std::int64_t error_code)
{
    // `ReplaySession.raiseError` (`:532-542`).
    revoke_publication_ = true;
    error_code_ = error_code;
    error_message_ = std::string{message} + ", recordingId=" + std::to_string(config_.recording_id) +
                     ", replaySessionId=" + std::to_string(config_.replay_session_id) +
                     ", segmentFile=" + segment_file_name(config_.recording_id, segment_file_base_position_);
    set_state(State::inactive, message);
}

void ReplaySession::set_state(State next, std::string_view reason)
{
    // `ReplaySession.state` (`:636-652`); the tracing event lands with A6.
    (void)reason;
    state_ = next;
}

}  // namespace deepmsg::archive::server
