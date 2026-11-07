#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

#include "archive/server/codecs.h"
#include "archive/server/epoch_clock_source.h"
#include "io_aeron_archive_codecs/Challenge.h"
#include "io_aeron_archive_codecs/ControlResponse.h"
#include "io_aeron_archive_codecs/ControlResponseCode.h"
#include "io_aeron_archive_codecs/Ping.h"
#include "io_aeron_archive_codecs/RecordingSignal.h"
#include "io_aeron_archive_codecs/RecordingSignalEvent.h"

namespace deepmsg::archive::server
{

/// Narrow seam over `aeron_exclusive_publication_t` (and the signal path's
/// `aeron_publication_t`) so the control plane can be exercised without a
/// driver. The Aeron-backed implementation lands with the conductor.
class ResponsePublication
{
public:
    using OfferResult = std::int64_t;

    struct Claim
    {
        std::span<std::uint8_t> buffer{};
    };

    ResponsePublication() = default;
    virtual ~ResponsePublication() = default;

    ResponsePublication(const ResponsePublication&) = delete;
    ResponsePublication& operator=(const ResponsePublication&) = delete;

    [[nodiscard]] virtual OfferResult offer(std::span<const std::uint8_t> buffer) = 0;
    [[nodiscard]] virtual OfferResult try_claim(std::int32_t length, Claim& claim) = 0;
    [[nodiscard]] virtual bool is_connected() const = 0;
    [[nodiscard]] virtual std::int32_t max_payload_length() const = 0;
};

/// What the proxy does to a session whose publication cannot take the message
/// (`ControlResponseProxy.checkResult`, `ControlResponseProxy.java:242`).
class SessionAbort
{
public:
    SessionAbort() = default;
    virtual ~SessionAbort() = default;

    SessionAbort(const SessionAbort&) = delete;
    SessionAbort& operator=(const SessionAbort&) = delete;

    virtual void abort(std::string_view reason) = 0;
};

/// `ControlResponseProxy` (`ControlResponseProxy.java:34-273`): encodes a control
/// response (or challenge/ping/signal) and offers it to the session's control
/// publication, retrying `SEND_ATTEMPTS` times before giving up for this pass.
///
/// A `false` return means "not sent now": the caller queues the message and
/// replays it on a later pass (`铁律 1`).
class ControlResponseProxy
{
public:
    /// `ControlResponseProxy.SEND_ATTEMPTS`.
    static constexpr int SEND_ATTEMPTS = 3;
    /// `MessageHeaderEncoder.ENCODED_LENGTH`.
    static constexpr std::int32_t MESSAGE_HEADER_LENGTH = 8;
    /// `AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION` (`AeronArchive.java:2667`).
    static constexpr std::int32_t PROTOCOL_SEMANTIC_VERSION = (1 << 16) | (12 << 8) | 0;

    /// `Publication.NOT_CONNECTED` and friends (`aeron_publication.h:83-93`).
    static constexpr std::int64_t NOT_CONNECTED = -1;
    static constexpr std::int64_t BACK_PRESSURED = -2;
    static constexpr std::int64_t ADMIN_ACTION = -3;
    static constexpr std::int64_t CLOSED = -4;
    static constexpr std::int64_t MAX_POSITION_EXCEEDED = -5;

    ControlResponseProxy();

    [[nodiscard]] bool send_response(std::int64_t control_session_id, std::int64_t correlation_id,
                                     std::int64_t relevant_id, archive_codecs::ControlResponseCode::Value code,
                                     std::string_view error_message, ResponsePublication& publication,
                                     SessionAbort& session);

    [[nodiscard]] bool send_challenge(std::int64_t control_session_id, std::int64_t correlation_id,
                                      std::span<const std::uint8_t> encoded_challenge, ResponsePublication& publication,
                                      SessionAbort& session);

    [[nodiscard]] bool send_ping(std::int64_t control_session_id, ResponsePublication& publication) noexcept;

    [[nodiscard]] bool send_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                                   std::int64_t recording_id, std::int64_t subscription_id, std::int64_t position,
                                   archive_codecs::RecordingSignal::Value signal,
                                   ResponsePublication& publication) noexcept;

    /// Bytes of the last encoded message (test observable).
    [[nodiscard]] std::span<const std::uint8_t> last_message() const noexcept
    {
        return {buffer_.data(), static_cast<std::size_t>(last_length_)};
    }

private:
    [[nodiscard]] bool send(ResponsePublication& publication, SessionAbort& session, std::int32_t length);

    static void check_result(std::int64_t result, SessionAbort& session);

    std::vector<std::uint8_t> buffer_;
    std::int32_t last_length_{0};
};

}  // namespace deepmsg::archive::server
