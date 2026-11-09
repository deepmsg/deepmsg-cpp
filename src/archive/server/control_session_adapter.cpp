#include "archive/server/control_session_adapter.h"

#include <cstring>
#include <string_view>

#include <aeronc.h>

#include "archive/server/channel_uri.h"
#include "io_aeron_archive_codecs/ArchiveIdRequest.h"
#include "io_aeron_archive_codecs/AuthConnectRequest.h"
#include "io_aeron_archive_codecs/BooleanType.h"
#include "io_aeron_archive_codecs/BoundedReplayRequest.h"
#include "io_aeron_archive_codecs/ChallengeResponse.h"
#include "io_aeron_archive_codecs/CloseSessionRequest.h"
#include "io_aeron_archive_codecs/ExtendRecordingRequest2.h"
#include "io_aeron_archive_codecs/FindLastMatchingRecordingRequest.h"
#include "io_aeron_archive_codecs/KeepAliveRequest.h"
#include "io_aeron_archive_codecs/ListRecordingRequest.h"
#include "io_aeron_archive_codecs/ListRecordingSubscriptionsRequest.h"
#include "io_aeron_archive_codecs/MaxRecordedPositionRequest.h"
#include "io_aeron_archive_codecs/MessageHeader.h"
#include "io_aeron_archive_codecs/RecordingPositionRequest.h"
#include "io_aeron_archive_codecs/ReplayRequest.h"
#include "io_aeron_archive_codecs/ReplayTokenRequest.h"
#include "io_aeron_archive_codecs/StartPositionRequest.h"
#include "io_aeron_archive_codecs/StartRecordingRequest.h"
#include "io_aeron_archive_codecs/StartRecordingRequest2.h"
#include "io_aeron_archive_codecs/StopAllReplaysRequest.h"
#include "io_aeron_archive_codecs/StopPositionRequest.h"
#include "io_aeron_archive_codecs/StopRecordingByIdentityRequest.h"
#include "io_aeron_archive_codecs/StopRecordingRequest.h"
#include "io_aeron_archive_codecs/StopRecordingSubscriptionRequest.h"
#include "io_aeron_archive_codecs/StopReplayRequest.h"

namespace deepmsg::archive::server
{
namespace
{

/// `Aeron.NULL_VALUE`.
constexpr std::int64_t NULL_VALUE = -1;

/// Bounds-checked reader for the var-data fields: every length prefix and the
/// bytes behind it must sit inside the fragment. The generated codecs throw on
/// a short buffer, but the request path must turn that away before it decodes
/// (`work-packages` §0: 解码前长度守卫).
class VarDataReader
{
public:
    VarDataReader(const std::uint8_t* buffer, std::int32_t length, std::int32_t offset)
        : buffer_(buffer), length_(length), offset_(offset)
    {
    }

    [[nodiscard]] bool read_string(std::string& out)
    {
        std::int32_t field_length = 0;
        if (!read_length(field_length))
        {
            return false;
        }
        out.assign(reinterpret_cast<const char*>(buffer_ + offset_), static_cast<std::size_t>(field_length));
        offset_ += field_length;
        return true;
    }

    [[nodiscard]] bool read_bytes(std::vector<std::uint8_t>& out)
    {
        std::int32_t field_length = 0;
        if (!read_length(field_length))
        {
            return false;
        }
        out.assign(buffer_ + offset_, buffer_ + offset_ + field_length);
        offset_ += field_length;
        return true;
    }

    [[nodiscard]] bool at_end() const noexcept
    {
        return offset_ == length_;
    }

private:
    [[nodiscard]] bool read_length(std::int32_t& field_length)
    {
        if (offset_ < 0 || offset_ + 4 > length_)
        {
            return false;
        }
        std::memcpy(&field_length, buffer_ + offset_, sizeof(field_length));
        offset_ += 4;
        return field_length >= 0 && offset_ + field_length <= length_;
    }

    const std::uint8_t* buffer_;
    std::int32_t length_;
    std::int32_t offset_;
};

}  // namespace

int ControlSessionAdapter::on_fragment(const std::uint8_t* buffer, std::int32_t length, const void* image)
{
    // `ControlSessionAdapter.onFragment` (`ControlSessionAdapter.java:126-134`).
    if (length < MESSAGE_HEADER_LENGTH)
    {
        handler_.log_warning("control request shorter than the message header");
        return 0;
    }

    archive_codecs::MessageHeader header;
    header.wrap(const_cast<char*>(reinterpret_cast<const char*>(buffer)), 0,
                archive_codecs::MessageHeader::sbeSchemaVersion(), static_cast<std::uint64_t>(length));

    if (SCHEMA_ID != static_cast<std::int32_t>(header.schemaId()))
    {
        handler_.log_warning("expected schemaId=" + std::to_string(SCHEMA_ID) +
                             ", actual=" + std::to_string(header.schemaId()));
        return 0;
    }

    const auto block_length = static_cast<std::int32_t>(header.blockLength());
    if (MESSAGE_HEADER_LENGTH + block_length > length)
    {
        handler_.log_warning("control request shorter than its block length");
        return 0;
    }

    const auto template_id = static_cast<std::int32_t>(header.templateId());
    const auto* body = buffer + MESSAGE_HEADER_LENGTH;

    switch (template_id)
    {
        case TEMPLATE_AUTH_CONNECT:
        {
            // `ControlSessionAdapter.java:766-802`.
            ConnectRequest request;
            request.image = image;

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.response_channel) || !reader.read_bytes(request.encoded_credentials) ||
                !reader.read_string(request.client_info) || !reader.at_end())
            {
                handler_.log_warning("malformed AuthConnectRequest");
                return 0;
            }

            archive_codecs::AuthConnectRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));
            request.correlation_id = decoder.correlationId();
            request.response_stream_id = decoder.responseStreamId();
            request.protocol_version = decoder.version();

            ControlSession* session = handler_.on_connect_request(request);
            if (nullptr != session)
            {
                register_session(session->session_id(), image, session);
                return 1;
            }
            return 0;
        }

        case TEMPLATE_CHALLENGE_RESPONSE:
        {
            // `ControlSessionAdapter.java:804-835`.
            archive_codecs::ChallengeResponse decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            std::vector<std::uint8_t> credentials;
            if (!reader.read_bytes(credentials) || !reader.at_end())
            {
                handler_.log_warning("malformed ChallengeResponse");
                return 0;
            }

            ControlSession* session =
                control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr != session)
            {
                session->on_challenge_response(decoder.correlationId(), credentials);
                return 1;
            }
            return 0;
        }

        case TEMPLATE_START_RECORDING:
        {
            // `ControlSessionAdapter.java:162-186`: the v1 request has no
            // `autoStop` and behaves like `autoStop=false`.
            archive_codecs::StartRecordingRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StartRecordingRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel) || !reader.at_end())
            {
                handler_.log_warning("malformed StartRecordingRequest");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.stream_id = decoder.streamId();
            request.source_location = decoder.sourceLocationRaw();
            request.auto_stop = false;

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_start_recording(request);
            }
            return 1;
        }

        case TEMPLATE_START_RECORDING_2:
        {
            // `ControlSessionAdapter.java:891-915`.
            archive_codecs::StartRecordingRequest2 decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StartRecordingRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel) || !reader.at_end())
            {
                handler_.log_warning("malformed StartRecordingRequest2");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.stream_id = decoder.streamId();
            request.source_location = decoder.sourceLocationRaw();
            request.auto_stop = archive_codecs::BooleanType::TRUE == decoder.autoStop();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_start_recording(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_RECORDING:
        {
            // `ControlSessionAdapter.java:188-206`.
            archive_codecs::StopRecordingRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StopRecordingRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel) || !reader.at_end())
            {
                handler_.log_warning("malformed StopRecordingRequest");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.stream_id = decoder.streamId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_stop_recording(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_RECORDING_SUBSCRIPTION:
        {
            // `ControlSessionAdapter.java:416-434`.
            archive_codecs::StopRecordingSubscriptionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StopRecordingSubscriptionRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.subscription_id = decoder.subscriptionId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_stop_recording_subscription(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_RECORDING_BY_IDENTITY:
        {
            // `ControlSessionAdapter.java:944-962`.
            archive_codecs::StopRecordingByIdentityRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StopRecordingByIdentityRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_stop_recording_by_identity(request);
            }
            return 1;
        }

        case TEMPLATE_RECORDING_POSITION:
        {
            // `ControlSessionAdapter.java:374-393`.
            archive_codecs::RecordingPositionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            RecordingIdRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }
            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_get_recording_position(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_POSITION:
        {
            // `ControlSessionAdapter.java:437-456`.
            archive_codecs::StopPositionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            RecordingIdRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }
            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_get_stop_position(request);
            }
            return 1;
        }

        case TEMPLATE_START_POSITION:
        {
            // `ControlSessionAdapter.java:640-659`.
            archive_codecs::StartPositionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            RecordingIdRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }
            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_get_start_position(request);
            }
            return 1;
        }

        case TEMPLATE_MAX_RECORDED_POSITION:
        {
            // `ControlSessionAdapter.java:1042-1061`.
            archive_codecs::MaxRecordedPositionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            RecordingIdRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }
            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_get_max_recorded_position(request);
            }
            return 1;
        }

        case TEMPLATE_FIND_LAST_MATCHING_RECORDING:
        {
            // `ControlSessionAdapter.java:458-487`.
            archive_codecs::FindLastMatchingRecordingRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            FindLastMatchingRecordingRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel_fragment) || !reader.at_end())
            {
                handler_.log_warning("malformed FindLastMatchingRecordingRequest");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.min_recording_id = decoder.minRecordingId();
            request.session_id = decoder.sessionId();
            request.stream_id = decoder.streamId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_find_last_matching_recording(request);
            }
            return 1;
        }

        case TEMPLATE_LIST_RECORDING:
        {
            // `ControlSessionAdapter.java:225-232`.
            archive_codecs::ListRecordingRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            RecordingIdRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }
            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_list_recording(request);
            }
            return 1;
        }

        case TEMPLATE_LIST_RECORDING_SUBSCRIPTIONS:
        {
            // `ControlSessionAdapter.java:488-508`.
            archive_codecs::ListRecordingSubscriptionsRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            ListRecordingSubscriptionsRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel_fragment) || !reader.at_end())
            {
                handler_.log_warning("malformed ListRecordingSubscriptionsRequest");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.pseudo_index = decoder.pseudoIndex();
            request.subscription_count = decoder.subscriptionCount();
            request.apply_stream_id = archive_codecs::BooleanType::TRUE == decoder.applyStreamId();
            request.stream_id = decoder.streamId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_list_recording_subscriptions(request);
            }
            return 1;
        }

        case TEMPLATE_EXTEND_RECORDING_2:
        {
            // `ControlSessionAdapter.java:917-943`.
            archive_codecs::ExtendRecordingRequest2 decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            ExtendRecordingRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.channel) || !reader.at_end())
            {
                handler_.log_warning("malformed ExtendRecordingRequest2");
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();
            request.stream_id = decoder.streamId();
            request.source_location = decoder.sourceLocationRaw();
            request.auto_stop = archive_codecs::BooleanType::TRUE == decoder.autoStop();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_extend_recording(request);
            }
            return 1;
        }

        case TEMPLATE_REPLAY:
        {
            // `ControlSessionAdapter.java:209-252`.
            archive_codecs::ReplayRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            const auto version = static_cast<std::int32_t>(header.version());
            ReplayRequest request;
            const std::int64_t control_session_id = decoder.controlSessionId();
            const std::int64_t correlation_id = decoder.correlationId();
            request.correlation_id = correlation_id;
            request.recording_id = decoder.recordingId();
            request.position = decoder.position();
            request.length = decoder.length();
            request.file_io_max_length = version >= 7 ? decoder.fileIoMaxLength() : -1;
            request.replay_stream_id = decoder.replayStreamId();
            request.replay_token = version >= 10 ? decoder.replayToken() : -1;

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.replay_channel) || !reader.at_end())
            {
                handler_.log_warning("malformed ReplayRequest");
                return 0;
            }

            request.session =
                setup_session_and_channel_for_replay(request.replay_channel, request.replay_token, request.recording_id,
                                                     correlation_id, control_session_id, template_id, image);
            if (nullptr == request.session)
            {
                return 0;
            }

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_start_replay(request);
            }
            return 1;
        }

        case TEMPLATE_BOUNDED_REPLAY:
        {
            // `ControlSessionAdapter.java:515-561`.
            archive_codecs::BoundedReplayRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            const auto version = static_cast<std::int32_t>(header.version());
            BoundedReplayRequest request;
            const std::int64_t control_session_id = decoder.controlSessionId();
            const std::int64_t correlation_id = decoder.correlationId();
            request.correlation_id = correlation_id;
            request.recording_id = decoder.recordingId();
            request.position = decoder.position();
            request.length = decoder.length();
            request.limit_counter_id = decoder.limitCounterId();
            request.replay_stream_id = decoder.replayStreamId();
            request.file_io_max_length = version >= 7 ? decoder.fileIoMaxLength() : -1;
            request.replay_token = version >= 10 ? decoder.replayToken() : -1;

            VarDataReader reader{buffer, length, MESSAGE_HEADER_LENGTH + block_length};
            if (!reader.read_string(request.replay_channel) || !reader.at_end())
            {
                handler_.log_warning("malformed BoundedReplayRequest");
                return 0;
            }

            request.session =
                setup_session_and_channel_for_replay(request.replay_channel, request.replay_token, request.recording_id,
                                                     correlation_id, control_session_id, template_id, image);
            if (nullptr == request.session)
            {
                return 0;
            }

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_start_bounded_replay(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_REPLAY:
        {
            // `ControlSessionAdapter.java:254-273`.
            archive_codecs::StopReplayRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StopReplayRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.replay_session_id = decoder.replaySessionId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_stop_replay(request);
            }
            return 1;
        }

        case TEMPLATE_STOP_ALL_REPLAYS:
        {
            // `ControlSessionAdapter.java:563-582`.
            archive_codecs::StopAllReplaysRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            StopAllReplaysRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_stop_all_replays(request);
            }
            return 1;
        }

        case TEMPLATE_REPLAY_TOKEN:
        {
            // `ControlSessionAdapter.java:1084-1106`.
            archive_codecs::ReplayTokenRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            ReplayTokenRequest request;
            request.session = control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr == request.session)
            {
                return 0;
            }

            request.correlation_id = decoder.correlationId();
            request.recording_id = decoder.recordingId();

            request.session->attempt_to_activate();
            if (ControlSession::State::active == request.session->state())
            {
                handler_.on_replay_token(request);
            }
            return 1;
        }

        case TEMPLATE_KEEP_ALIVE:
        {
            // `ControlSessionAdapter.java:845-856`.
            archive_codecs::KeepAliveRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            ControlSession* session =
                control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr != session)
            {
                session->on_keep_alive(decoder.correlationId());
                return 1;
            }
            return 0;
        }

        case TEMPLATE_ARCHIVE_ID:
        {
            // `ControlSessionAdapter.java:1063-1084` and
            // `ControlSession.onArchiveId` (`ControlSession.java:541`): the gate
            // first, then activation, then the archive id as the relevant id.
            archive_codecs::ArchiveIdRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            ControlSession* session =
                control_session(decoder.controlSessionId(), image, template_id, decoder.correlationId());
            if (nullptr != session)
            {
                session->attempt_to_activate();
                if (ControlSession::State::active == session->state())
                {
                    session->send_ok_response(decoder.correlationId(), handler_.archive_id());
                }
                return 1;
            }
            return 0;
        }

        case TEMPLATE_CLOSE_SESSION:
        {
            // `ControlSessionAdapter.java:144-160`.
            archive_codecs::CloseSessionRequest decoder;
            decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(body)), 0,
                                  static_cast<std::uint64_t>(block_length), header.version(),
                                  static_cast<std::uint64_t>(length - MESSAGE_HEADER_LENGTH));

            const std::int64_t control_session_id = decoder.controlSessionId();
            const auto found = sessions_.find(control_session_id);
            if (found != sessions_.end() && found->second.image == image)
            {
                found->second.session->abort(ControlSession::SESSION_CLOSED_MSG);
            }
            return 1;
        }

        default:
            // The remaining requests land with A2b (recording), A3a (queries) and
            // A3b (replay); they are named rather than silently dropped.
            handler_.log_warning("unsupported control request templateId=" + std::to_string(template_id));
            return 0;
    }
}

ControlSession* ControlSessionAdapter::setup_session_and_channel_for_replay(
    std::string& channel, std::int64_t replay_token, std::int64_t recording_id, std::int64_t correlation_id,
    std::int64_t control_session_id, std::int32_t template_id, const void* image)
{
    // `ControlSessionAdapter.java:1165-1190`.
    if (is_mds_response_channel(channel) && NULL_VALUE != replay_token)
    {
        ControlSession* session = handler_.replay_session_for_token(replay_token, recording_id);
        if (nullptr == session)
        {
            // The reference throws an ArchiveException, which AgentRunner logs
            // and swallows; the request stays unanswered either way.
            handler_.log_warning("Unknown session or token timeout for replayToken=" + std::to_string(replay_token));
            return nullptr;
        }

        std::int64_t response_correlation_id = 0;
        aeron_image_constants_t constants{};
        if (nullptr != image &&
            0 ==
                aeron_image_constants(const_cast<aeron_image_t*>(static_cast<const aeron_image_t*>(image)), &constants))
        {
            response_correlation_id = constants.correlation_id;
        }

        auto rewritten = with_response_correlation_id(channel, response_correlation_id);
        if (!rewritten)
        {
            handler_.log_warning("failed to rewrite the replay channel: " +
                                 std::string{rewritten.error().message.view()});
            return nullptr;
        }
        channel = std::move(*rewritten);
        return session;
    }

    return control_session(control_session_id, image, template_id, correlation_id);
}

ControlSession* ControlSessionAdapter::control_session(std::int64_t control_session_id, const void* image,
                                                       std::int32_t template_id, std::int64_t correlation_id)
{
    // `ControlSessionAdapter.getControlSession` (`ControlSessionAdapter.java:1186-1230`).
    const auto found = sessions_.find(control_session_id);
    if (found == sessions_.end())
    {
        handler_.log_warning("control request for unknown session: controlSessionId=" +
                             std::to_string(control_session_id) + " templateId=" + std::to_string(template_id));
        return nullptr;
    }

    if (found->second.image != image)
    {
        handler_.log_warning("unauthorised archive action=" + std::to_string(template_id) +
                             " controlSessionId=" + std::to_string(control_session_id));
        return nullptr;
    }

    // The authorisation service (`AuthorisationService`) is A2b's; the default
    // one allows everything.
    (void)correlation_id;
    return found->second.session;
}

void ControlSessionAdapter::register_session(std::int64_t control_session_id, const void* image,
                                             ControlSession* session)
{
    sessions_[control_session_id] = SessionInfo{image, session};
}

void ControlSessionAdapter::unregister_session(std::int64_t control_session_id)
{
    sessions_.erase(control_session_id);
}

void ControlSessionAdapter::abort_sessions_for_image(const void* image, std::string_view reason)
{
    for (auto& [control_session_id, info] : sessions_)
    {
        (void)control_session_id;
        if (info.image == image && nullptr != info.session)
        {
            info.session->abort(reason);
        }
    }
}

}  // namespace deepmsg::archive::server
