#include "archive/server/control_response_proxy.h"

#include <algorithm>
#include <cstring>

namespace deepmsg::archive::server
{

ControlResponseProxy::ControlResponseProxy() : buffer_(4096, 0) {}

bool ControlResponseProxy::send_response(std::int64_t control_session_id, std::int64_t correlation_id,
                                         std::int64_t relevant_id, archive_codecs::ControlResponseCode::Value code,
                                         std::string_view error_message, ResponsePublication& publication,
                                         SessionAbort& session)
{
    // `ControlResponseProxy.sendResponse` (`ControlResponseProxy.java:110-144`).
    archive_codecs::ControlResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.controlSessionId(control_session_id)
        .correlationId(correlation_id)
        .relevantId(relevant_id)
        .code(code)
        .version(PROTOCOL_SEMANTIC_VERSION)
        .putErrorMessage(error_message.data(), static_cast<std::uint32_t>(error_message.size()));

    return send(publication, session, MESSAGE_HEADER_LENGTH + static_cast<std::int32_t>(encoder.encodedLength()));
}

bool ControlResponseProxy::send_challenge(std::int64_t control_session_id, std::int64_t correlation_id,
                                          std::span<const std::uint8_t> encoded_challenge,
                                          ResponsePublication& publication, SessionAbort& session)
{
    // `ControlResponseProxy.sendChallenge` (`ControlResponseProxy.java:146-159`).
    archive_codecs::Challenge encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.controlSessionId(control_session_id).correlationId(correlation_id);
    encoder.putEncodedChallenge(reinterpret_cast<const char*>(encoded_challenge.data()),
                                static_cast<std::uint32_t>(encoded_challenge.size()));

    return send(publication, session, MESSAGE_HEADER_LENGTH + static_cast<std::int32_t>(encoder.encodedLength()));
}

bool ControlResponseProxy::send_ping(std::int64_t control_session_id, ResponsePublication& publication) noexcept
{
    // `ControlResponseProxy.sendPing` (`ControlResponseProxy.java:201-222`).
    const std::int32_t length = MESSAGE_HEADER_LENGTH + archive_codecs::Ping::SBE_BLOCK_LENGTH;

    for (int attempt = 0; attempt < SEND_ATTEMPTS; attempt++)
    {
        ResponsePublication::Claim claim;
        if (publication.try_claim(length, claim) > 0)
        {
            archive_codecs::Ping encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.buffer.data()), 0, claim.buffer.size());
            encoder.controlSessionId(control_session_id);
            last_length_ = length;
            return true;
        }
    }

    return false;
}

bool ControlResponseProxy::send_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                                       std::int64_t recording_id, std::int64_t subscription_id, std::int64_t position,
                                       archive_codecs::RecordingSignal::Value signal,
                                       ResponsePublication& publication) noexcept
{
    // `ControlResponseProxy.sendSignal` (`ControlResponseProxy.java:161-199`).
    const std::int32_t length = MESSAGE_HEADER_LENGTH + archive_codecs::RecordingSignalEvent::SBE_BLOCK_LENGTH;

    for (int attempt = 0; attempt < SEND_ATTEMPTS; attempt++)
    {
        ResponsePublication::Claim claim;
        if (publication.try_claim(length, claim) > 0)
        {
            archive_codecs::RecordingSignalEvent encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.buffer.data()), 0, claim.buffer.size());
            encoder.controlSessionId(control_session_id)
                .correlationId(correlation_id)
                .recordingId(recording_id)
                .subscriptionId(subscription_id)
                .position(position)
                .signal(signal);
            last_length_ = length;
            return true;
        }
    }

    return false;
}

bool ControlResponseProxy::send(ResponsePublication& publication, SessionAbort& session, std::int32_t length)
{
    // `ControlResponseProxy.send` (`ControlResponseProxy.java:224-240`).
    for (int attempt = 0; attempt < SEND_ATTEMPTS; attempt++)
    {
        const std::int64_t position = publication.offer({buffer_.data(), static_cast<std::size_t>(length)});
        if (position > 0)
        {
            last_length_ = length;
            return true;
        }

        check_result(position, session);
    }

    return false;
}

void ControlResponseProxy::check_result(std::int64_t result, SessionAbort& session)
{
    // `ControlResponseProxy.checkResult` (`ControlResponseProxy.java:242-262`):
    // the reference aborts the session and throws; the abort is what the
    // conductor acts on, so the exception becomes a `false` return plus the
    // recorded reason.
    if (NOT_CONNECTED == result)
    {
        session.abort("control response publication is not connected");
    }
    else if (CLOSED == result)
    {
        session.abort("control response publication is closed");
    }
    else if (MAX_POSITION_EXCEEDED == result)
    {
        session.abort("control response publication is at max position");
    }
}

}  // namespace deepmsg::archive::server
