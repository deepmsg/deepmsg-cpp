#include "archive/server/recording_session.h"

#include <string>
#include <utility>

namespace deepmsg::archive::server
{

// `Aeron.NULL_POSITION`.
namespace
{
constexpr std::int64_t NULL_POSITION = -1;
}

// ---------------------------------------------------------------------------
// AeronRecordingImage
// ---------------------------------------------------------------------------

void AeronRecordingImage::block_handler(void* clientd, const std::uint8_t* buffer, std::size_t length,
                                        std::int32_t session_id, std::int32_t term_id)
{
    auto* self = static_cast<AeronRecordingImage*>(clientd);

    // A block always starts on a frame boundary, so handing it over as
    // `base = block start`, `offset = 0` is what the Java `BlockHandler`
    // expresses with its `termBuffer` + `termOffset` pair.
    if (nullptr != self->writer_ && !self->error_.has_value())
    {
        auto written = self->writer_->on_block(buffer, 0, static_cast<std::int32_t>(length), session_id, term_id);
        if (!written)
        {
            self->error_ = written.error();
        }
    }
}

int AeronRecordingImage::block_poll(RecordingWriter& writer, std::int32_t block_length_limit) noexcept
{
    error_.reset();
    writer_ = &writer;
    const int result = aeron_image_block_poll(image_, &AeronRecordingImage::block_handler, this,
                                              static_cast<std::size_t>(block_length_limit));
    writer_ = nullptr;
    return result;
}

bool AeronRecordingImage::is_end_of_stream() const noexcept
{
    return aeron_image_is_end_of_stream(image_);
}

std::expected<void, axc::core::Error> AeronRecordingImage::take_error()
{
    if (!error_)
    {
        return {};
    }
    const axc::core::Error error = *error_;
    error_.reset();
    return std::unexpected(error);
}

// ---------------------------------------------------------------------------
// RecordingSession
// ---------------------------------------------------------------------------

RecordingSession::RecordingSession(Config config, RecordingWriter writer,
                                   std::unique_ptr<axc::core::CounterTarget> position,
                                   std::unique_ptr<RecordingImage> image) noexcept
    : config_(std::move(config)), writer_(std::move(writer)), position_(std::move(position)), image_(std::move(image))
{
}

RecordingSession::~RecordingSession()
{
    close();
}

int RecordingSession::do_work()
{
    int work_count = 0;

    // `RecordingSession.doWork` (`RecordingSession.java:131-163`).
    if (aborted_)
    {
        set_state(State::inactive, abort_reason_);
    }

    if (State::init == state_)
    {
        work_count += init();
    }

    if (State::recording == state_)
    {
        work_count += record();
    }

    if (State::inactive == state_)
    {
        set_state(State::stopped, "");
        (void)writer_.close();

        if (nullptr != config_.events)
        {
            // `RecordingSession.java:146-156`: after the writer is closed.
            config_.events->stopped(config_.recording_id, config_.join_position, position_->value());
        }

        work_count++;
    }

    return work_count;
}

void RecordingSession::abort(std::string_view reason)
{
    // `RecordingSession.abort` (`RecordingSession.java:108-113`).
    abort_reason_.assign(reason);
    aborted_ = true;
}

void RecordingSession::close()
{
    // `RecordingSession.close` (`RecordingSession.java:118-122`).
    (void)writer_.close();
    position_.reset();
}

std::int64_t RecordingSession::recorded_position() const noexcept
{
    // `RecordingSession.recordedPosition` (`RecordingSession.java:174-181`).
    return position_ ? position_->value() : NULL_POSITION;
}

int RecordingSession::init()
{
    // `RecordingSession.init` (`RecordingSession.java:204-231`).
    if (auto initialized = writer_.init(); !initialized)
    {
        error_message_ = initialized.error().message.view();
        (void)writer_.close();
        set_state(State::stopped, error_message_);
        return 1;
    }

    if (nullptr != config_.events)
    {
        config_.events->started(config_.recording_id, config_.join_position, config_.session_id, config_.stream_id,
                                config_.original_channel, config_.source_identity);
    }

    set_state(State::recording, "");
    return 1;
}

int RecordingSession::record()
{
    // `RecordingSession.record` (`RecordingSession.java:233-278`).
    const int work_count = image_->block_poll(writer_, config_.block_length_limit);

    auto write_error = image_->take_error();
    if (!write_error)
    {
        error_message_ = write_error.error().message.view();
        set_state(State::inactive, error_message_);
        return 1;
    }

    if (work_count > 0)
    {
        position_->set_value(writer_.position());

        if (nullptr != config_.events)
        {
            // `RecordingSession.java:243-254`: only a successful progress event
            // advances `progressEventPosition`.
            const std::int64_t recorded_position = writer_.position();
            if (progress_event_position_ < recorded_position &&
                config_.events->progress(config_.recording_id, config_.join_position, recorded_position))
            {
                progress_event_position_ = recorded_position;
            }
        }

        return work_count;
    }

    if (image_->is_end_of_stream())
    {
        set_state(State::inactive, "image end of stream");
    }

    return 0;
}

void RecordingSession::set_state(State next, std::string_view /*reason*/)
{
    state_ = next;
}

}  // namespace deepmsg::archive::server
