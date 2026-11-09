#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "archive/server/codecs.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/replication_source.h"
#include "io_aeron_archive_codecs/RecordingSignal.h"

namespace deepmsg::archive::server
{

/// The two-phase outcome of `ArchiveConductor.extendRecording`
/// (`ArchiveConductor.java:1067-1151`), which the wire handler and a
/// replication session share.
///
/// The reference returns the `Subscription` itself because Java's
/// `addSubscription` blocks; our recording-subscription add is a driver round
/// trip (`AsyncAdd`), so starting one returns a token whose outcome resolves
/// on a later conductor pass. The OK carrying the registration id (or the
/// ERROR) already went out on the control session when this is filled in.
struct ExtendRecordingOutcome
{
    std::int64_t registration_id{0};
    /// Non-empty ⇒ the add failed; the message was already sent on the session.
    std::string error;
};

/// Everything `ArchiveConductor.replicate` passes to a session
/// (`ReplicationSession.java:80-120`).
struct ReplicationConfig
{
    std::int64_t control_session_id{0};
    /// `replicationId` (`:82`): the id the destination's stop finds it by and
    /// the correlation id every signal/error goes out with.
    std::int64_t replication_id{0};
    std::int64_t src_recording_id{0};
    /// `Aeron.NULL_VALUE` ⇒ the session creates the destination recording row.
    std::int64_t dst_recording_id{-1};
    std::int64_t stop_position{-1};
    std::int64_t channel_tag_id{-1};
    std::int64_t subscription_tag_id{-1};
    std::int32_t src_control_stream_id{0};
    std::int32_t file_io_max_length{-1};
    std::int32_t replication_session_id{-1};
    std::string src_control_channel;
    std::string live_destination;
    std::string replication_channel;
    std::string src_response_channel;
    std::string encoded_credentials;
    /// `hasRecording ? recordingSummary : null` (`ArchiveConductor.java:1461`).
    bool has_dst_recording{false};
    std::int64_t dst_start_position{0};
    std::int64_t dst_stop_position{0};
    std::int32_t dst_stream_id{0};
    /// `context.messageTimeoutNs()` (`AeronArchive.Configuration` default 10 s,
    /// `aeron.archive.message.timeout`).
    std::int64_t action_timeout_ms{10'000};
    /// `context.messageRetryAttempts()` (`aeron.archive.message.retry.attempts`):
    /// only the live-join path reads it (a recorded gap), kept for fidelity.
    std::int32_t retry_attempts{3};
    const EpochClockSource* clock{nullptr};
};

/// The conductor side of a replication session: the control responses, the
/// catalog rows and the destination recording subscription. An interface so a
/// unit test can drive the state machine without an archive
/// (`DeleteSegmentsControl` has the same shape).
class ReplicationControl
{
public:
    ReplicationControl() = default;
    ReplicationControl(const ReplicationControl&) = delete;
    ReplicationControl& operator=(const ReplicationControl&) = delete;
    virtual ~ReplicationControl() = default;

    /// `ControlSession.sendSignal(replicationId, recordingId, subscriptionId,
    /// position, signal)` (`ReplicationSession.java:1000-1010`).
    virtual void send_replication_signal(std::int64_t control_session_id, std::int64_t replication_id,
                                         std::int64_t recording_id, std::int64_t subscription_id, std::int64_t position,
                                         archive_codecs::RecordingSignal::Value signal) = 0;

    /// `ControlSession.sendErrorResponse(replicationId, errorCode, msg)`
    /// (`:1000-1010`); the code travels in `relevantId`.
    virtual void send_replication_error(std::int64_t control_session_id, std::int64_t replication_id,
                                        std::int64_t error_code, std::string_view message) = 0;

    /// `ArchiveConductor.stopRecordingSubscription(subscriptionId)`
    /// (`ArchiveConductor.java:625-635`): no response goes out.
    [[nodiscard]] virtual bool stop_recording_subscription(std::int64_t registration_id) = 0;

    /// `Catalog.stopPosition(recordingId)`.
    [[nodiscard]] virtual std::int64_t catalog_stop_position(std::int64_t recording_id) = 0;
    [[nodiscard]] virtual std::expected<std::int64_t, std::string> add_new_recording(
        std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp,
        std::int64_t stop_timestamp, std::int32_t image_initial_term_id, std::int32_t segment_file_length,
        std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id,
        std::string_view stripped_channel, std::string_view original_channel, std::string_view source_identity) = 0;
    [[nodiscard]] virtual std::expected<void, std::string> replace_recording(
        std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position,
        std::int64_t start_timestamp, std::int64_t stop_timestamp, std::int32_t image_initial_term_id,
        std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length,
        std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
        std::string_view original_channel, std::string_view source_identity) = 0;

    /// `ArchiveConductor.extendRecording` (`:1067-1151`) with the
    /// replication's correlation id; success returns the outcome token.
    [[nodiscard]] virtual std::expected<std::int64_t, std::string> start_extend_recording(
        std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t recording_id, std::int32_t stream_id,
        std::int32_t source_location, bool auto_stop, std::string_view original_channel) = 0;
    [[nodiscard]] virtual std::optional<ExtendRecordingOutcome> take_extend_recording_outcome(std::int64_t token) = 0;
    virtual void cancel_extend_recording(std::int64_t token) = 0;

    /// `Subscription.imageBySessionId` on the destination recording
    /// subscription (`ReplicationSession.java:757`).
    [[nodiscard]] virtual aeron_image_t* image_by_session_id(std::int64_t registration_id, std::int32_t session_id) = 0;

    /// `Image.position()` / `isClosed()` / `isEndOfStream()`, kept behind the
    /// interface so a unit test can drive `REPLICATE` without a real image.
    [[nodiscard]] virtual std::int64_t image_position(aeron_image_t* image) = 0;
    [[nodiscard]] virtual bool image_is_closed(aeron_image_t* image) = 0;
    [[nodiscard]] virtual bool image_is_end_of_stream(aeron_image_t* image) = 0;

    /// `Subscription.resolvedEndpoint()` (`:727`).
    [[nodiscard]] virtual std::optional<std::string> subscription_resolved_endpoint(std::int64_t registration_id) = 0;
};

/// `ReplicationSession` (`ReplicationSession.java:60-75`): one archive-to-archive
/// copy, driven a state at a time from the conductor's session worker.
///
/// The judged path is CONNECT → REPLICATE_DESCRIPTOR → EXTEND → REPLAY_TOKEN
/// (pass through) → GET_ARCHIVE_PROXY (pass through) → REPLAY → AWAIT_IMAGE →
/// REPLICATE → DONE; a live destination additionally goes through
/// SRC_RECORDING_POSITION before EXTEND. `CATCHUP` / `ATTEMPT_LIVE_JOIN` (live
/// merge), tagged replication and response-channel replication are not
/// implemented — the request is failed loudly and `docs/compat.md` carries the
/// gap.
class ReplicationSession
{
public:
    enum class State
    {
        connect,
        replicate_descriptor,
        src_recording_position,
        extend,
        replay_token,
        get_archive_proxy,
        replay,
        await_image,
        replicate,
        catchup,
        attempt_live_join,
        done
    };

    ReplicationSession(const ReplicationConfig& config, std::unique_ptr<ReplicationSource> source,
                       ReplicationControl& control);

    ReplicationSession(const ReplicationSession&) = delete;
    ReplicationSession& operator=(const ReplicationSession&) = delete;

    /// `ReplicationSession.doWork` (`:273-280`).
    int do_work();

    [[nodiscard]] bool is_done() const noexcept
    {
        return State::done == state_;
    }

    /// `:200-215`: a stop ends the session; its `close()` sends `REPLICATE_END`.
    void abort(std::string_view reason);

    /// `:196-213`: the destination recording subscription and the source
    /// replay are stopped and `REPLICATE_END` goes out **once**.
    void close();

    [[nodiscard]] std::int64_t session_id() const noexcept
    {
        return config_.replication_id;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] const ReplicationConfig& config() const noexcept
    {
        return config_;
    }

    /// Test observables.
    [[nodiscard]] std::int64_t recording_subscription_id() const noexcept
    {
        return recording_subscription_id_;
    }

    [[nodiscard]] std::int64_t dst_recording_id() const noexcept
    {
        return dst_recording_id_;
    }

    [[nodiscard]] bool closed() const noexcept
    {
        return closed_;
    }

private:
    int connect();
    int replicate_descriptor();
    int src_recording_position();
    int extend();
    int replay_token();
    int get_archive_proxy();
    int replay();
    int await_image();
    int replicate();
    int catchup();
    int attempt_live_join();

    /// `RecordingDescriptorConsumer.onRecordingDescriptor` (`:290-399`).
    void on_recording_descriptor(SourceRecordingDescriptor& descriptor);

    /// `hasResponse` (`:900-915`): an ERROR response fails the session.
    [[nodiscard]] bool has_response(const ReplicationSource::Response& response);

    void error(std::string_view message, std::int64_t error_code);
    [[nodiscard]] bool fail(std::string_view message, std::int64_t error_code);
    void signal(std::int64_t position, archive_codecs::RecordingSignal::Value signal);
    void stop_recording();
    void stop_replay_session();

    /// `state(newState, reason)` (`:975-985`): clears the active correlation
    /// and restarts the action timer.
    void enter(State next, std::string reason = {});

    [[nodiscard]] std::int64_t now_ms() const;
    [[nodiscard]] bool timed_out() const;
    [[nodiscard]] std::int64_t track_action(std::int64_t correlation_id);

    ReplicationConfig config_;
    std::unique_ptr<ReplicationSource> source_;
    ReplicationControl* control_{nullptr};

    State state_{State::connect};
    bool closed_{false};

    std::int64_t active_correlation_id_{-1};
    std::int64_t src_replay_session_id_{-1};
    std::int64_t replay_position_{-1};
    std::int64_t src_stop_position_{-1};
    std::int64_t src_recording_position_{-1};
    std::int64_t dst_stop_position_{-1};
    std::int64_t dst_recording_id_{-1};
    std::int64_t time_of_last_action_ms_{0};
    std::int32_t replay_stream_id_{0};
    std::int32_t replay_session_id_{0};
    bool is_destination_recording_empty_{false};

    /// The destination recording subscription from `extendRecording`.
    std::int64_t recording_subscription_id_{-1};
    std::int64_t extend_token_{0};
    /// The rewritten channel the subscription was added on (the reference's
    /// `recordingSubscription.channel()`).
    std::string extend_channel_;

    aeron_image_t* image_{nullptr};
};

}  // namespace deepmsg::archive::server
