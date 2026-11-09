#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <aeronc.h>

#include "archive/server/archive_config.h"
#include "archive/server/authenticator.h"
#include "archive/server/catalog.h"
#include "archive/server/checksum.h"
#include "archive/server/control_response_proxy.h"
#include "archive/server/control_session.h"
#include "archive/server/control_session_adapter.h"
#include "archive/server/create_replay_publication_session.h"
#include "archive/server/deepmsg_response_publication.h"
#include "archive/server/delete_segments_session.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/recorder.h"
#include "archive/server/recording_events.h"
#include "archive/server/replay_session.h"
#include "archive/server/replayer.h"
#include "archive/server/segment_validation.h"
#include "archive/server/session_worker.h"
#include "core/counters/counter_target.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// The `Archive.Context` values the conductor needs for the control plane
/// (`ArchiveConductor.java` reader of `Archive.Context`).
struct ArchiveConductorContext
{
    std::string archive_dir;
    /// `Archive.Configuration.CONTROL_CHANNEL_DEFAULT`-style channel the archive
    /// listens on for control requests.
    std::string control_channel{"aeron:ipc"};
    std::int32_t control_stream_id{10};
    /// `Archive.LOCAL_CONTROL_CHANNEL_DEFAULT` / `..._STREAM_ID_DEFAULT`
    /// (`Archive.java:484-500`): the privileged local control channel.
    std::string local_control_channel{"aeron:ipc?term-length=64k"};
    std::int32_t local_control_stream_id{10};
    /// Empty ⇒ the Aeron default (`/dev/shm/aeron-<user>`).
    std::string aeron_dir;
    std::int64_t connect_timeout_ms{3000};
    std::int64_t session_liveness_check_interval_ms{1000};
    int fragment_limit{10};
    /// `Archive.Context.controlTermBufferLength()` / `controlMtuLength()` /
    /// `controlTermBufferSparse()` (`Archive.java:2816-2840`).
    std::int32_t control_term_buffer_length{64 * 1024};
    std::int32_t control_mtu_length{1408};
    bool control_term_buffer_sparse{true};
    /// `Aeron.NULL_VALUE` unless the archive was given one (`ctx.archiveId()`).
    std::int64_t archive_id{-1};
    /// nullptr ⇒ `DefaultAuthenticator`.
    Authenticator* authenticator{nullptr};
    /// `Archive.Configuration` recording values (`Archive.java:328-438`).
    RecordingSettings recording;
    /// `Archive.Configuration` replay values (`Archive.java:474-564`, `:676`).
    ReplaySettings replay;
    /// `Archive.Context.replayChecksum()`; nullptr ⇒ no verification.
    const Checksum* replay_checksum{nullptr};
    /// `aeron.archive.catalog.capacity` (`Archive.java:508-516`).
    std::int64_t catalog_capacity{1024 * 1024};
    int catalog_file_sync_level{0};
};

/// `ArchiveConductor` (`ArchiveConductor.java:126-...`), SHARED threading mode:
/// one thread drives the client conductor, the control subscription and the
/// session worker.
///
/// Scope of this card: the control subscription, session creation/teardown and
/// the connect/authenticate/keep-alive/close requests. The recording, query and
/// replay requests, the session counters, the mark file wiring, the
/// `AuthorisationService` and the local-control (privileged) channel arrive with
/// A2b, A3a and A3b.
class ArchiveConductor final : public ControlSessionHandler,
                               public ReplayControlResponses,
                               public ReplayWarningSink,
                               public CreateReplayPublicationSessionHandler,
                               public DeleteSegmentsControl,
                               public DeleteSegmentsResponder
{
public:
    /// `AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION`.
    static constexpr std::int32_t PROTOCOL_MAJOR_VERSION = 1;
    /// `AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION`.
    static constexpr std::int32_t PROTOCOL_SEMANTIC_VERSION = (1 << 16) | (12 << 8) | 0;

    static std::expected<std::unique_ptr<ArchiveConductor>, axc::core::Error> create(
        const ArchiveConductorContext& context);

    ~ArchiveConductor() override;

    ArchiveConductor(const ArchiveConductor&) = delete;
    ArchiveConductor& operator=(const ArchiveConductor&) = delete;

    /// `ArchiveConductor.doWork` (`ArchiveConductor.java:363`).
    int do_work();

    void close();

    ControlSession* on_connect_request(const ConnectRequest& request) override;
    void on_start_recording(const StartRecordingRequest& request) override;
    void on_stop_recording(const StopRecordingRequest& request) override;
    void on_stop_recording_subscription(const StopRecordingSubscriptionRequest& request) override;
    void on_stop_recording_by_identity(const StopRecordingByIdentityRequest& request) override;
    void on_get_recording_position(const RecordingIdRequest& request) override;
    void on_get_stop_position(const RecordingIdRequest& request) override;
    void on_get_start_position(const RecordingIdRequest& request) override;
    void on_get_max_recorded_position(const RecordingIdRequest& request) override;
    void on_find_last_matching_recording(const FindLastMatchingRecordingRequest& request) override;
    void on_list_recording(const RecordingIdRequest& request) override;
    void on_list_recording_subscriptions(const ListRecordingSubscriptionsRequest& request) override;
    void on_extend_recording(const ExtendRecordingRequest& request) override;
    void on_truncate_recording(const TruncateRecordingRequest& request) override;
    void on_purge_recording(const PurgeRecordingRequest& request) override;
    void on_detach_segments(const SegmentStartPositionRequest& request) override;
    void on_delete_detached_segments(const RecordingIdRequest& request) override;
    void on_purge_segments(const SegmentStartPositionRequest& request) override;
    void on_attach_segments(const RecordingIdRequest& request) override;
    void on_update_channel(const UpdateChannelRequest& request) override;
    void on_migrate_segments(const MigrateSegmentsRequest& request) override;
    void on_start_replay(const ReplayRequest& request) override;
    void on_start_bounded_replay(const BoundedReplayRequest& request) override;
    void on_stop_replay(const StopReplayRequest& request) override;
    void on_stop_all_replays(const StopAllReplaysRequest& request) override;
    void on_replay_token(const ReplayTokenRequest& request) override;
    std::int64_t generate_replay_token(ControlSession& session, std::int64_t recording_id) override;
    ControlSession* replay_session_for_token(std::int64_t token, std::int64_t recording_id) override;

    // ReplayControlResponses (`ReplaySession.java:288-295`, `:338`).
    void async_send_ok(std::int64_t control_session_id, std::int64_t correlation_id,
                       std::int64_t replay_session_id) override;
    void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t relevant_id,
                    std::string_view message) override;

    // ReplayWarningSink (`ArchiveConductor.logWarning`).
    void unexpected_frame(std::int32_t frame_type, std::int64_t position) override;

    // CreateReplayPublicationSessionHandler.
    axc::core::AsyncAdd<axc::core::ExPubPtr> add_replay_publication(std::string_view channel,
                                                                    std::int32_t stream_id) override;
    void replay_publication_failed(std::int64_t control_session_id, std::int64_t correlation_id,
                                   std::string_view message) override;
    void new_replay_session(const CreateReplayPublicationSession::Config& config,
                            std::unique_ptr<ReplayPublication> publication) override;

    void log_warning(std::string_view message) override;

    // DeleteSegmentsControl (`ControlSession.archiveConductor()`).
    [[nodiscard]] bool has_in_progress_replays(std::int64_t recording_id) override;
    void remove_delete_segments_session(std::int64_t recording_id) override;
    void log_error(std::string_view message) override;

    // DeleteSegmentsResponder (`ControlSession.sendSignal` / `sendErrorResponse`).
    void send_delete_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                            std::int64_t recording_id) override;
    void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::string_view message) override;

    [[nodiscard]] std::int64_t archive_id() const override
    {
        return context_.archive_id;
    }

    [[nodiscard]] std::size_t session_count() const noexcept
    {
        return sessions_.size();
    }

    /// Live recording sessions (`ctx.numActiveRecordings`).
    [[nodiscard]] std::size_t recording_session_count() const noexcept
    {
        return recorder_.size();
    }

    /// Live replay sessions (`ctx.numActiveReplays`).
    [[nodiscard]] std::size_t replay_session_count() const noexcept
    {
        return replayer_.size();
    }

    [[nodiscard]] std::int64_t recording_sessions_counter_value() const noexcept;

    [[nodiscard]] const std::vector<std::string>& warnings() const noexcept
    {
        return warnings_;
    }

private:
    /// A recording subscription (`ArchiveConductor.recordingSubscriptionByKeyMap`),
    /// plus the start request that created it (the reference captures those in the
    /// `AvailableImageHandler` closure).
    struct RecordingSubscription
    {
        std::string key;
        std::string stripped_channel;
        std::string original_channel;
        /// The channel the subscription was actually added on (`aeron-spy:` for
        /// LOCAL + UDP); the subscription descriptor reports this one
        /// (`ControlResponseProxy.java:91-135`).
        std::string subscription_channel;
        std::int32_t stream_id{0};
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::int64_t registration_id{0};
        bool auto_stop{false};
        /// `ExtendRecordingRequest2`: the image extends this recording instead
        /// of starting a new one (`ArchiveConductor.java:2059-2170`).
        bool extend{false};
        std::int64_t extend_recording_id{0};
        int ref_count{0};
        axc::core::SubPtr subscription;
    };

    /// A subscription add in flight (`AsyncAdd`, C API); the OK carries the
    /// registration id, which only exists once the driver answers.
    struct PendingRecordingSubscription
    {
        std::string key;
        std::string stripped_channel;
        std::string original_channel;
        /// The channel the subscription was actually added on (`aeron-spy:` for
        /// LOCAL + UDP); the subscription descriptor reports this one
        /// (`ControlResponseProxy.java:91-135`).
        std::string subscription_channel;
        std::int32_t stream_id{0};
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        bool auto_stop{false};
        bool extend{false};
        std::int64_t extend_recording_id{0};
        axc::core::AsyncAdd<axc::core::SubPtr> subscription_add;
    };

    struct PollContext
    {
        ArchiveConductor* conductor{nullptr};
        aeron_image_t* image{nullptr};
    };

    explicit ArchiveConductor(const ArchiveConductorContext& context);

    [[nodiscard]] std::expected<void, axc::core::Error> init();

    [[nodiscard]] int poll_control_subscription();
    [[nodiscard]] int poll_pending_publications();
    [[nodiscard]] int poll_pending_recording_subscriptions();
    [[nodiscard]] int poll_pending_control_counters();
    [[nodiscard]] int poll_recording_images();
    [[nodiscard]] int poll_listings();
    void start_replay_request(const ReplayRequest& request, std::int32_t limit_counter_id, bool bounded);
    void close_replay_session(ReplaySession& session);
    void on_replay_start() noexcept;
    void on_replay_end() noexcept;
    [[nodiscard]] std::int64_t next_replay_token();
    void check_replay_tokens();
    void remove_replay_tokens_for_session(std::int64_t control_session_id);

    /// `ArchiveConductor.counterByIdMap` + `Counter.isClosed()`: the stable
    /// per-counter state the replay limit source reads.
    struct CounterState
    {
        std::int64_t registration_id{-1};
        bool closed{false};
        bool client_supplied{false};
    };
    [[nodiscard]] CounterState& counter_state(std::int32_t counter_id, bool client_supplied);
    void reset_counter_state(std::int32_t counter_id, std::int64_t registration_id);
    void mark_counter_closed(std::int32_t counter_id);
    [[nodiscard]] int start_recording_session(RecordingSubscription& subscription, aeron_image_t* image);
    [[nodiscard]] int extend_recording_session(RecordingSubscription& subscription, aeron_image_t* image);
    void close_recording_session(RecordingSession& session);
    void abort_sessions_for_subscription(std::int64_t registration_id, std::string_view reason);
    [[nodiscard]] std::optional<RecordingSubscription> remove_recording_subscription(std::int64_t registration_id);
    [[nodiscard]] std::optional<RecordingSubscription> remove_recording_subscription_by_key(std::string_view key);
    [[nodiscard]] ControlSession* control_session_by_id(std::int64_t id) noexcept;
    void increment_recording_sessions() noexcept;
    void decrement_recording_sessions() noexcept;
    [[nodiscard]] std::expected<axc::core::PositionWriter, axc::core::Error> allocate_counter(
        std::int32_t type_id, std::span<const std::uint8_t> key, std::string_view label);

    static void unavailable_counter_trampoline(void* clientd, aeron_counters_reader_t* reader,
                                               std::int64_t registration_id, std::int32_t counter_id);

    static aeron_controlled_fragment_handler_action_t assembled_trampoline(void* clientd, const std::uint8_t* buffer,
                                                                           size_t length, aeron_header_t* header);
    static void unavailable_image_trampoline(void* clientd, aeron_subscription_t* subscription, aeron_image_t* image);

    ArchiveConductorContext context_;
    axc::core::DeepmsgClient client_;
    axc::core::SubPtr control_subscription_;
    /// `ArchiveConductor.localControlSubscription` (`:238-239`).
    axc::core::SubPtr local_control_subscription_;
    /// Control requests above one MTU arrive as several fragments; the
    /// controlled assembler hands complete messages to the adapter
    /// (`ControlSessionAdapter.java:126` wraps the handler in a
    /// `FragmentAssembler` for the same reason).
    aeron_image_controlled_fragment_assembler_t* control_assembler_{nullptr};
    const void* current_control_image_{nullptr};
    SystemEpochClock epoch_clock_;
    CachedEpochClock cached_clock_;
    DefaultAuthenticator default_authenticator_;
    Authenticator* authenticator_{nullptr};
    ControlResponseProxy proxy_;
    ControlSessionAdapter adapter_;
    SessionWorker<ControlSession> sessions_;
    std::unordered_map<std::int64_t, std::unique_ptr<DeepmsgExclusivePublication>> publications_;
    struct PendingPublication
    {
        std::int64_t control_session_id{0};
        aeron_async_add_exclusive_publication_t* async{nullptr};
    };

    std::unordered_map<std::int64_t, ControlSession*> sessions_by_id_;
    /// `ControlSessionCounter` (type 113): presence/lookup marker per control
    /// session; its value is unused (`ControlSessionCounter.java:63-81`).
    struct PendingControlCounter
    {
        std::int64_t control_session_id{0};
        axc::core::AsyncAdd<axc::core::CounterPtr> add;
    };
    std::vector<PendingControlCounter> pending_control_counters_;
    std::unordered_map<std::int64_t, axc::core::PositionWriter> control_session_counters_;
    std::vector<PendingPublication> pending_publications_;
    std::int64_t next_session_id_{0};
    std::vector<std::string> warnings_;

    // Recording (A2b).
    Catalog catalog_;
    /// `ArchiveConductor.java:224-225`: one events publication for every
    /// recording session (null when `aeron.archive.recording.events.enabled`
    /// is false).
    std::unique_ptr<DeepmsgExclusivePublication> events_publication_;
    std::optional<RecordingEvents> events_;
    Recorder recorder_;
    std::optional<axc::core::PositionWriterTarget> max_write_time_counter_;
    std::optional<axc::core::PositionWriterTarget> total_write_bytes_counter_;
    std::optional<axc::core::PositionWriterTarget> total_write_time_counter_;
    std::optional<axc::core::PositionWriterTarget> control_sessions_counter_;
    std::optional<axc::core::PositionWriterTarget> recording_sessions_counter_;
    std::optional<axc::core::PositionWriterTarget> replay_session_counter_;
    std::optional<axc::core::PositionWriterTarget> total_read_bytes_counter_;
    std::optional<axc::core::PositionWriterTarget> total_read_time_counter_;
    std::optional<axc::core::PositionWriterTarget> max_read_time_counter_;
    std::unordered_map<std::string, RecordingSubscription> recording_subscriptions_;
    /// subscription registration id -> `recording_subscriptions_` key.
    std::unordered_map<std::int64_t, std::string> recording_subscription_keys_;
    std::vector<PendingRecordingSubscription> pending_recording_subscriptions_;
    std::unordered_map<aeron_image_t*, RecordingSession*> sessions_by_image_;

    /// A listing in progress (`ListRecordingByIdSession` /
    /// `ListRecordingSubscriptionsSession`); the control session's
    /// `active listing` points at it.
    struct Listing
    {
        enum class Kind
        {
            recording_by_id,
            subscriptions,
            /// `UpdateChannelSession` (`UpdateChannelSession.java:29-148`): a
            /// one-pass session that occupies the control session's active
            /// listing slot.
            update_channel
        };

        Kind kind{Kind::recording_by_id};
        std::int64_t correlation_id{0};
        std::int64_t control_session_id{0};
        std::int64_t recording_id{0};
        std::int32_t pseudo_index{0};
        std::int32_t subscription_count{0};
        std::int32_t stream_id{0};
        bool apply_stream_id{false};
        std::string channel_fragment;
        /// `update_channel` only: the request's channel and its stripped form.
        std::string original_channel;
        std::string stripped_channel;
        std::int32_t sent{0};
        bool done{false};
    };

    std::vector<Listing> listings_;
    /// `recording_subscription_by_key_map` iteration order (the reference's
    /// `pseudoIndex` walks the map).
    std::vector<std::string> recording_subscription_order_;
    std::unordered_map<std::int64_t, RecordingSession*> sessions_by_recording_id_;

    // Replay (A3b).
    Replayer replayer_;
    SessionWorker<CreateReplayPublicationSession> replay_publication_sessions_;
    std::unordered_map<std::int64_t, ReplaySession*> replay_session_by_id_;
    std::int32_t next_replay_id_{1};
    std::int32_t num_active_replays_{0};
    std::unordered_map<std::int32_t, CounterState> counter_by_id_;
    struct SessionForReplay
    {
        std::int64_t recording_id{0};
        std::int64_t control_session_id{0};
        std::int64_t deadline_ns{0};
    };
    std::unordered_map<std::int64_t, SessionForReplay> replay_tokens_;
    std::uint64_t replay_token_state_{0x9E3779B97F4A7C15ULL};
    /// `ctx.replayBuffer()`: one archive-level buffer, sliced per session.
    std::vector<std::uint8_t> replay_buffer_;
    SystemReplayClock replay_clock_{cached_clock_};

    // Segment management (A4a).
    /// `ArchiveConductor.deleteSegmentsSessionByIdMap` (`:270`): keyed by
    /// **recordingId**, exactly like the reference.
    SessionWorker<DeleteSegmentsSession> delete_sessions_;
    std::unordered_map<std::int64_t, DeleteSegmentsSession*> delete_session_by_recording_id_;

    /// `ArchiveConductor.deleteSegments` (`:2654-2670`): create the session,
    /// send the OK with the file count and emit DELETE itself when the list is
    /// empty (no session would ever close).
    void delete_segments(ControlSession& session, std::int64_t correlation_id, std::int64_t recording_id,
                         std::vector<std::string> files, bool await_replays_stop);

    /// `ArchiveConductor.isDeleteAllowed` (`:1969-1979`).
    [[nodiscard]] bool is_delete_allowed(ControlSession& session, std::int64_t correlation_id,
                                         std::int64_t recording_id);

    /// `ArchiveConductor.stopAllReplays(recordingId)` (`:1048-1062`): abort the
    /// recording's replay sessions; no OK response.
    void stop_all_replays(std::int64_t recording_id);

    /// `RecordingSession.recordedPosition()` for the live session, else
    /// `Aeron.NULL_POSITION`.
    [[nodiscard]] std::int64_t recorded_position_for(std::int64_t recording_id) const;

    /// The live replays of one recording, in the shape `isValidDetach` reads.
    [[nodiscard]] std::vector<ReplayInProgressInfo> replays_for(std::int64_t recording_id) const;
};

}  // namespace deepmsg::archive::server
