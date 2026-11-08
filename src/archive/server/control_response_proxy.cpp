#include "archive/server/control_response_proxy.h"

#include <algorithm>
#include <cstring>

#include "archive/server/catalog.h"
#include "io_aeron_archive_codecs/RecordingDescriptor.h"
#include "io_aeron_archive_codecs/RecordingSubscriptionDescriptor.h"

namespace deepmsg::archive::server
{

ControlResponseProxy::ControlResponseProxy() : buffer_(64 * 1024, 0) {}

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
            claim.commit();
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
            claim.commit();
            last_length_ = length;
            return true;
        }
    }

    return false;
}

bool ControlResponseProxy::send_descriptor(std::int64_t control_session_id, std::int64_t correlation_id,
                                           std::span<const std::uint8_t> descriptor, ResponsePublication& publication,
                                           SessionAbort& session)
{
    // `ControlResponseProxy.sendDescriptor` (`ControlResponseProxy.java:54-89`):
    // the message is the SBE header + the two ids + the catalog descriptor body
    // (the catalog's own 32-byte `RecordingDescriptorHeader` is dropped).
    // The catalog body carries `controlSessionId` + `correlationId` as its
    // first two (zero) fields; the message replaces them with the archive's
    // values, so the copy starts at `recordingId`
    // (`DESCRIPTOR_CONTENT_OFFSET = RecordingDescriptorHeader.BLOCK_LENGTH +
    // recordingIdEncodingOffset`, `ControlResponseProxy.java:42-46`).
    const std::int32_t content_length = Catalog::descriptor_length(descriptor);
    const auto recording_id_offset =
        static_cast<std::int32_t>(archive_codecs::RecordingDescriptor::recordingIdEncodingOffset());
    const auto source_offset = Catalog::DESCRIPTOR_HEADER_LENGTH + recording_id_offset;
    if (content_length <= recording_id_offset || descriptor.size() < static_cast<std::size_t>(source_offset))
    {
        return false;
    }
    const auto copy_length = static_cast<std::size_t>(content_length - recording_id_offset);

    archive_codecs::RecordingDescriptor encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.controlSessionId(control_session_id).correlationId(correlation_id);

    // `encodedLength()` is the whole fixed block, not the two ids just written;
    // the descriptor body follows the ids directly
    // (`DESCRIPTOR_PREFIX_LENGTH`, `ControlResponseProxy.java:42`).
    const auto content_offset = static_cast<std::size_t>(MESSAGE_HEADER_LENGTH) + 2 * sizeof(std::int64_t);
    const auto length = content_offset + copy_length;
    if (length > buffer_.size())
    {
        return false;
    }

    std::memcpy(buffer_.data() + content_offset, descriptor.data() + source_offset, copy_length);

    return send(publication, session, static_cast<std::int32_t>(length));
}

bool ControlResponseProxy::send_subscription_descriptor(std::int64_t control_session_id, std::int64_t correlation_id,
                                                        std::int64_t subscription_id, std::int32_t stream_id,
                                                        std::string_view channel, ResponsePublication& publication,
                                                        SessionAbort& session)
{
    // `ControlResponseProxy.sendSubscriptionDescriptor` (`ControlResponseProxy.java:91-135`).
    archive_codecs::RecordingSubscriptionDescriptor encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.controlSessionId(control_session_id)
        .correlationId(correlation_id)
        .subscriptionId(subscription_id)
        .streamId(stream_id);
    encoder.putStrippedChannel(channel.data(), static_cast<std::uint32_t>(channel.size()));

    return send(publication, session, MESSAGE_HEADER_LENGTH + static_cast<std::int32_t>(encoder.encodedLength()));
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
