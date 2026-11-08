#include "archive/server/recording_events.h"

#include <string>

#include "io_aeron_archive_codecs/RecordingProgress.h"
#include "io_aeron_archive_codecs/RecordingStarted.h"
#include "io_aeron_archive_codecs/RecordingStopped.h"

namespace deepmsg::archive::server
{
namespace
{

/// `Publication.NOT_CONNECTED` / `CLOSED` / `MAX_POSITION_EXCEEDED`.
constexpr std::int64_t NOT_CONNECTED = -1;
constexpr std::int64_t BACK_PRESSURED = -2;
constexpr std::int64_t ADMIN_ACTION = -3;
constexpr std::int64_t CLOSED = -4;
constexpr std::int64_t MAX_POSITION_EXCEEDED = -5;

constexpr std::int32_t MESSAGE_HEADER_LENGTH = 8;

/// `RecordingEventsProxy.checkResult` (`RecordingEventsProxy.java:135-151`): a
/// closed publication or an exceeded max position stops the retry loop.
[[nodiscard]] bool stop_retrying(std::int64_t result) noexcept
{
    return CLOSED == result || MAX_POSITION_EXCEEDED == result;
}

[[nodiscard]] bool should_retry(std::int64_t result) noexcept
{
    return BACK_PRESSURED == result || ADMIN_ACTION == result;
}

}  // namespace

RecordingEvents::RecordingEvents(ResponsePublication& publication) : publication_(publication), buffer_(4096, 0) {}

void RecordingEvents::started(std::int64_t recording_id, std::int64_t start_position, std::int32_t session_id,
                              std::int32_t stream_id, std::string_view channel,
                              std::string_view source_identity) noexcept
{
    // `RecordingEventsProxy.started` (`RecordingEventsProxy.java:68-96`).
    archive_codecs::RecordingStarted encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.recordingId(recording_id)
        .startPosition(start_position)
        .sessionId(session_id)
        .streamId(stream_id)
        .putChannel(channel.data(), static_cast<std::uint32_t>(channel.size()))
        .putSourceIdentity(source_identity.data(), static_cast<std::uint32_t>(source_identity.size()));

    const auto length = static_cast<std::int32_t>(MESSAGE_HEADER_LENGTH + encoder.encodedLength());

    for (int attempt = 0; attempt < SEND_ATTEMPTS; attempt++)
    {
        const std::int64_t position = publication_.offer({buffer_.data(), static_cast<std::size_t>(length)});
        if (position > 0 || NOT_CONNECTED == position || stop_retrying(position))
        {
            break;
        }
        if (!should_retry(position))
        {
            break;
        }
    }
}

bool RecordingEvents::progress(std::int64_t recording_id, std::int64_t start_position, std::int64_t position) noexcept
{
    // `RecordingEventsProxy.progress` (`RecordingEventsProxy.java:98-119`).
    const std::int32_t length = MESSAGE_HEADER_LENGTH + archive_codecs::RecordingProgress::SBE_BLOCK_LENGTH;

    ResponsePublication::Claim claim;
    if (publication_.try_claim(length, claim) > 0)
    {
        archive_codecs::RecordingProgress encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.buffer.data()), 0, claim.buffer.size());
        encoder.recordingId(recording_id).startPosition(start_position).position(position);
        claim.commit();
        return true;
    }

    return false;
}

void RecordingEvents::stopped(std::int64_t recording_id, std::int64_t start_position,
                              std::int64_t stop_position) noexcept
{
    // `RecordingEventsProxy.stopped` (`RecordingEventsProxy.java:121-152`).
    const std::int32_t length = MESSAGE_HEADER_LENGTH + archive_codecs::RecordingStopped::SBE_BLOCK_LENGTH;

    for (int attempt = 0; attempt < SEND_ATTEMPTS; attempt++)
    {
        ResponsePublication::Claim claim;
        const std::int64_t result = publication_.try_claim(length, claim);
        if (result > 0)
        {
            archive_codecs::RecordingStopped encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.buffer.data()), 0, claim.buffer.size());
            encoder.recordingId(recording_id).startPosition(start_position).stopPosition(stop_position);
            claim.commit();
            break;
        }
        if (NOT_CONNECTED == result || stop_retrying(result))
        {
            break;
        }
        if (!should_retry(result))
        {
            break;
        }
    }
}

}  // namespace deepmsg::archive::server
