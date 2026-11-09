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

/// `RecordingPositionRequest` (12), `StopPositionRequest` (15),
/// `StartPositionRequest` (52) and `MaxRecordedPositionRequest` (67) all carry a
/// `recordingId` (`ControlSessionAdapter.java:374-393`, `:437-456`, `:640-659`,
/// `:1042-1061`).
struct RecordingIdRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    ControlSession* session{nullptr};
};

/// `FindLastMatchingRecordingRequest` (`ControlSessionAdapter.java:458-487`).
struct FindLastMatchingRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int64_t min_recording_id{0};
    std::int32_t session_id{0};
    std::int32_t stream_id{0};
    std::string channel_fragment;
    ControlSession* session{nullptr};
};

/// `ListRecordingSubscriptionsRequest` (`ControlSessionAdapter.java:488-508`).
struct ListRecordingSubscriptionsRequest
{
    std::int64_t correlation_id{0};
    std::int32_t pseudo_index{0};
    std::int32_t subscription_count{0};
    bool apply_stream_id{false};
    std::int32_t stream_id{0};
    std::string channel_fragment;
    ControlSession* session{nullptr};
};

/// `ExtendRecordingRequest2` (`ControlSessionAdapter.java:917-943`).
struct ExtendRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::int32_t stream_id{0};
    std::int32_t source_location{0};
    bool auto_stop{false};
    std::string channel;
    ControlSession* session{nullptr};
};

/// `ReplayRequest` (6) / `BoundedReplayRequest` (18) after the version guards
/// (`ControlSessionAdapter.java:209-252`, `:515-561`).
struct ReplayRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    /// `Aeron.NULL_VALUE` ⇒ replay from the recording's start position.
    std::int64_t position{0};
    /// `REPLAY_ALL_AND_FOLLOW` (-1), `REPLAY_ALL_AND_STOP` (-2) or a length.
    std::int64_t length{0};
    std::int32_t file_io_max_length{-1};
    std::int32_t replay_stream_id{0};
    std::int64_t replay_token{-1};
    std::string replay_channel;
    ControlSession* session{nullptr};
};

struct BoundedReplayRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::int64_t position{0};
    std::int64_t length{0};
    std::int32_t limit_counter_id{-1};
    std::int32_t file_io_max_length{-1};
    std::int32_t replay_stream_id{0};
    std::int64_t replay_token{-1};
    std::string replay_channel;
    ControlSession* session{nullptr};
};

/// `StopReplayRequest` (7) and `StopAllReplaysRequest` (19)
/// (`ControlSessionAdapter.java:254-273`, `:563-582`).
struct StopReplayRequest
{
    std::int64_t correlation_id{0};
    std::int64_t replay_session_id{0};
    ControlSession* session{nullptr};
};

struct StopAllReplaysRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{-1};
    ControlSession* session{nullptr};
};

/// `ReplayTokenRequest` (105) (`ControlSessionAdapter.java:1084-1106`).
struct ReplayTokenRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    ControlSession* session{nullptr};
};

/// `TruncateRecordingRequest` (13) (`ControlSessionAdapter.java:395-416`).
struct TruncateRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::int64_t position{0};
    ControlSession* session{nullptr};
};

/// `PurgeRecordingRequest` (104) (`ControlSessionAdapter.java:684-702`).
struct PurgeRecordingRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    ControlSession* session{nullptr};
};

/// `DetachSegmentsRequest` (53) and `PurgeSegmentsRequest` (55)
/// (`ControlSessionAdapter.java:631-702`).
struct SegmentStartPositionRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::int64_t new_start_position{0};
    ControlSession* session{nullptr};
};

/// `MigrateSegmentsRequest` (57) (`ControlSessionAdapter.java:745-767`).
struct MigrateSegmentsRequest
{
    std::int64_t correlation_id{0};
    std::int64_t src_recording_id{0};
    std::int64_t dst_recording_id{0};
    ControlSession* session{nullptr};
};

/// `UpdateChannelRequest` (107) (`ControlSessionAdapter.java:1107-1128`).
struct UpdateChannelRequest
{
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::string channel;
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

    /// The four position queries (`ControlSession.java:436-484`, `:520-568`).
    virtual void on_get_recording_position(const RecordingIdRequest& request) = 0;
    virtual void on_get_stop_position(const RecordingIdRequest& request) = 0;
    virtual void on_get_start_position(const RecordingIdRequest& request) = 0;
    virtual void on_get_max_recorded_position(const RecordingIdRequest& request) = 0;

    /// `ControlSession.onFindLastMatchingRecording` /
    /// `onListRecording` / `onListRecordingSubscriptions`
    /// (`ControlSession.java:486-572`).
    virtual void on_find_last_matching_recording(const FindLastMatchingRecordingRequest& request) = 0;
    virtual void on_list_recording(const RecordingIdRequest& request) = 0;
    virtual void on_list_recording_subscriptions(const ListRecordingSubscriptionsRequest& request) = 0;

    /// `ControlSession.onExtendRecording` (`ControlSession.java:478-486`).
    virtual void on_extend_recording(const ExtendRecordingRequest& request) = 0;

    // Segment management (`ControlSession.java:505-578`, `:637-683`).
    virtual void on_truncate_recording(const TruncateRecordingRequest& request) = 0;
    virtual void on_purge_recording(const PurgeRecordingRequest& request) = 0;
    virtual void on_detach_segments(const SegmentStartPositionRequest& request) = 0;
    virtual void on_delete_detached_segments(const RecordingIdRequest& request) = 0;
    virtual void on_purge_segments(const SegmentStartPositionRequest& request) = 0;
    virtual void on_attach_segments(const RecordingIdRequest& request) = 0;
    virtual void on_update_channel(const UpdateChannelRequest& request) = 0;
    virtual void on_migrate_segments(const MigrateSegmentsRequest& request) = 0;

    /// `ControlSession.onStartReplay` / `onStartBoundedReplay` /
    /// `onStopReplay` / `onStopAllReplays` (`ControlSession.java:412-479`).
    virtual void on_start_replay(const ReplayRequest& request) = 0;
    virtual void on_start_bounded_replay(const BoundedReplayRequest& request) = 0;
    virtual void on_stop_replay(const StopReplayRequest& request) = 0;
    virtual void on_stop_all_replays(const StopAllReplaysRequest& request) = 0;

    /// `ControlSessionAdapter`'s `ReplayTokenRequest` arm: the conductor both
    /// generates the token and sends the OK (`:1084-1106`).
    virtual void on_replay_token(const ReplayTokenRequest& request) = 0;

    /// `ArchiveConductor.generateReplayToken` / `getReplaySession`
    /// (`ArchiveConductor.java:2674-2702`).
    virtual std::int64_t generate_replay_token(ControlSession& session, std::int64_t recording_id) = 0;
    virtual ControlSession* replay_session_for_token(std::int64_t token, std::int64_t recording_id) = 0;

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
    static constexpr std::int32_t TEMPLATE_RECORDING_POSITION = 12;
    static constexpr std::int32_t TEMPLATE_STOP_POSITION = 15;
    static constexpr std::int32_t TEMPLATE_START_POSITION = 52;
    static constexpr std::int32_t TEMPLATE_MAX_RECORDED_POSITION = 67;
    static constexpr std::int32_t TEMPLATE_LIST_RECORDING = 10;
    static constexpr std::int32_t TEMPLATE_LIST_RECORDING_SUBSCRIPTIONS = 17;
    static constexpr std::int32_t TEMPLATE_FIND_LAST_MATCHING_RECORDING = 16;
    static constexpr std::int32_t TEMPLATE_EXTEND_RECORDING_2 = 64;
    static constexpr std::int32_t TEMPLATE_REPLAY = 6;
    static constexpr std::int32_t TEMPLATE_STOP_REPLAY = 7;
    static constexpr std::int32_t TEMPLATE_BOUNDED_REPLAY = 18;
    static constexpr std::int32_t TEMPLATE_STOP_ALL_REPLAYS = 19;
    static constexpr std::int32_t TEMPLATE_REPLAY_TOKEN = 105;
    static constexpr std::int32_t TEMPLATE_ARCHIVE_ID = 68;
    static constexpr std::int32_t TEMPLATE_TRUNCATE_RECORDING = 13;
    static constexpr std::int32_t TEMPLATE_PURGE_RECORDING = 104;
    static constexpr std::int32_t TEMPLATE_DETACH_SEGMENTS = 53;
    static constexpr std::int32_t TEMPLATE_DELETE_DETACHED_SEGMENTS = 54;
    static constexpr std::int32_t TEMPLATE_PURGE_SEGMENTS = 55;
    static constexpr std::int32_t TEMPLATE_ATTACH_SEGMENTS = 56;
    static constexpr std::int32_t TEMPLATE_MIGRATE_SEGMENTS = 57;
    static constexpr std::int32_t TEMPLATE_UPDATE_CHANNEL = 107;
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

    /// `ControlSessionAdapter.setupSessionAndChannelForReplay`
    /// (`ControlSessionAdapter.java:1165-1190`): a response-mode channel with a
    /// replay token resolves the control session through the token and carries
    /// the request image's correlation id.
    [[nodiscard]] ControlSession* setup_session_and_channel_for_replay(std::string& channel, std::int64_t replay_token,
                                                                       std::int64_t recording_id,
                                                                       std::int64_t correlation_id,
                                                                       std::int64_t control_session_id,
                                                                       std::int32_t template_id, const void* image);

    void register_session(std::int64_t control_session_id, const void* image, ControlSession* session);
    void unregister_session(std::int64_t control_session_id);

    /// Drop every session pointer (archive teardown, before the sessions are
    /// destroyed) so a late unavailable-image callback cannot dereference them.
    void clear_sessions() noexcept
    {
        sessions_.clear();
    }

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
