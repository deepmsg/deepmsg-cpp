#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "archive/server/codecs.h"
#include "archive/server/control_session.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// The `AuthConnectRequest` fields the conductor needs to build a session
/// (`ControlSessionAdapter.java:766-802`).
struct ConnectRequest
{
    std::int64_t correlation_id{0};
    std::int32_t response_stream_id{0};
    std::int32_t protocol_version{0};
    std::string response_channel;
    std::vector<std::uint8_t> encoded_credentials;
    std::string client_info;
    /// Identity of the image the request arrived on (the reference compares the
    /// `Image` object; we compare the `aeron_image_t*`).
    const void* image{nullptr};
};

/// `StartRecordingRequest` / `StartRecordingRequest2`
/// (`ControlSessionAdapter.java:880-915`).
struct StartRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int32_t stream_id{0};
    /// `SourceLocation.LOCAL` (0) / `REMOTE` (1).
    std::int32_t source_location{0};
    /// `StartRecordingRequest2.autoStop`; always false for the v1 request.
    bool auto_stop{false};
    std::string channel;
    ControlSession* session{nullptr};
};

/// `StopRecordingRequest` (`ControlSessionAdapter.java:188-206`): stop every
/// recording on this `streamId` + `channel`.
struct StopRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int32_t stream_id{0};
    std::string channel;
    ControlSession* session{nullptr};
};

/// `StopRecordingSubscriptionRequest` (`ControlSessionAdapter.java:416-434`).
struct StopRecordingSubscriptionRequest
{
    std::int64_t correlation_id{0};
    std::int64_t subscription_id{0};
    ControlSession* session{nullptr};
};

/// `StopRecordingByIdentityRequest` (`ControlSessionAdapter.java:944-962`).
struct StopRecordingByIdentityRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    ControlSession* session{nullptr};
};

/// What the adapter asks the conductor for.
class ControlSessionHandler
{
public:
    ControlSessionHandler() = default;
    virtual ~ControlSessionHandler() = default;

    ControlSessionHandler(const ControlSessionHandler&) = delete;
    ControlSessionHandler& operator=(const ControlSessionHandler&) = delete;

    /// Create (or refuse) a session for a connect request. Returns nullptr when
    /// the request cannot be served; the adapter then drops it.
    virtual ControlSession* on_connect_request(const ConnectRequest& request) = 0;

    /// `ControlSession.onStartRecording` (`ControlSession.java:340-348`).
    virtual void on_start_recording(const StartRecordingRequest& request) = 0;

    /// `ControlSession.onStopRecording` (`ControlSession.java:318-326`).
    virtual void on_stop_recording(const StopRecordingRequest& request) = 0;

    /// `ControlSession.onStopRecordingSubscription` (`ControlSession.java:328-336`).
    virtual void on_stop_recording_subscription(const StopRecordingSubscriptionRequest& request) = 0;

    /// `ControlSession.onStopRecordingByIdentity` (`ControlSession.java:570-578`).
    virtual void on_stop_recording_by_identity(const StopRecordingByIdentityRequest& request) = 0;

    /// `ArchiveConductor.logWarning` (`ArchiveConductor.java:442`).
    virtual void log_warning(std::string_view message) = 0;

    /// `ArchiveConductor.archiveId` (`ArchiveConductor.java:524`): the id a
    /// session receives for an `ArchiveIdRequest`.
    [[nodiscard]] virtual std::int64_t archive_id() const = 0;
};

/// `ControlSessionAdapter` (`ControlSessionAdapter.java:107-1231`): decodes a
/// control request fragment and dispatches it to its session.
///
/// This card implements the requests needed to connect and authenticate:
/// `AuthConnectRequest` (58), `ChallengeResponse` (60), `KeepAliveRequest` (61)
/// and `CloseSessionRequest` (3). Every other template id is turned away with a
/// warning instead of being silently ignored; they arrive with A2b (recording),
/// A3a (queries) and A3b (replay).
class ControlSessionAdapter
{
public:
    /// `MessageHeaderDecoder.ENCODED_LENGTH`.
    static constexpr std::int32_t MESSAGE_HEADER_LENGTH = 8;
    /// `MessageHeaderDecoder.SCHEMA_ID`.
    static constexpr std::int32_t SCHEMA_ID = 101;  // aeron-archive-codecs.xml, schema id

    // Template ids from `aeron-archive-codecs.xml`.
    static constexpr std::int32_t TEMPLATE_CLOSE_SESSION = 3;
    /// `StartRecordingRequest` (v1, no `autoStop`) and `StartRecordingRequest2`.
    static constexpr std::int32_t TEMPLATE_START_RECORDING = 4;
    static constexpr std::int32_t TEMPLATE_START_RECORDING_2 = 63;
    static constexpr std::int32_t TEMPLATE_STOP_RECORDING = 5;
    static constexpr std::int32_t TEMPLATE_STOP_RECORDING_SUBSCRIPTION = 14;
    static constexpr std::int32_t TEMPLATE_STOP_RECORDING_BY_IDENTITY = 65;
    static constexpr std::int32_t TEMPLATE_ARCHIVE_ID = 68;
    static constexpr std::int32_t TEMPLATE_AUTH_CONNECT = 58;
    static constexpr std::int32_t TEMPLATE_CHALLENGE_RESPONSE = 60;
    static constexpr std::int32_t TEMPLATE_KEEP_ALIVE = 61;

    explicit ControlSessionAdapter(ControlSessionHandler& handler) : handler_(handler) {}

    ControlSessionAdapter(const ControlSessionAdapter&) = delete;
    ControlSessionAdapter& operator=(const ControlSessionAdapter&) = delete;

    /// `ControlSessionAdapter.onFragment` (`ControlSessionAdapter.java:126`).
    /// Returns the number of requests handled (0 or 1).
    int on_fragment(const std::uint8_t* buffer, std::int32_t length, const void* image);

    /// The gate: the session must exist and the request must come from the
    /// image it was created on (`ControlSessionAdapter.java:1186-1230`).
    [[nodiscard]] ControlSession* control_session(std::int64_t control_session_id, const void* image,
                                                  std::int32_t template_id, std::int64_t correlation_id);

    void register_session(std::int64_t control_session_id, const void* image, ControlSession* session);
    void unregister_session(std::int64_t control_session_id);

    /// `ArchiveConductor.onUnavailableImage` (`ArchiveConductor.java:255`): the
    /// session's image went away, so every session created on it is aborted.
    void abort_sessions_for_image(const void* image, std::string_view reason);
    [[nodiscard]] std::size_t session_count() const noexcept
    {
        return sessions_.size();
    }

private:
    struct SessionInfo
    {
        const void* image{nullptr};
        ControlSession* session{nullptr};
    };

    std::unordered_map<std::int64_t, SessionInfo> sessions_;
    ControlSessionHandler& handler_;
};

}  // namespace deepmsg::archive::server
