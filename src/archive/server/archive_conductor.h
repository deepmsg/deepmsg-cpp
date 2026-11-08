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
#include "archive/server/control_response_proxy.h"
#include "archive/server/control_session.h"
#include "archive/server/control_session_adapter.h"
#include "archive/server/deepmsg_response_publication.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/recorder.h"
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
class ArchiveConductor final : public ControlSessionHandler
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
    void log_warning(std::string_view message) override;

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
        std::int32_t stream_id{0};
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::int64_t registration_id{0};
        bool auto_stop{false};
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
        std::int32_t stream_id{0};
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        bool auto_stop{false};
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
    [[nodiscard]] int poll_recording_images();
    [[nodiscard]] int start_recording_session(RecordingSubscription& subscription, aeron_image_t* image);
    void close_recording_session(RecordingSession& session);
    void abort_sessions_for_subscription(std::int64_t registration_id, std::string_view reason);
    [[nodiscard]] std::optional<RecordingSubscription> remove_recording_subscription(std::int64_t registration_id);
    [[nodiscard]] std::optional<RecordingSubscription> remove_recording_subscription_by_key(std::string_view key);
    [[nodiscard]] ControlSession* control_session_by_id(std::int64_t id) noexcept;
    void increment_recording_sessions() noexcept;
    void decrement_recording_sessions() noexcept;
    [[nodiscard]] std::expected<axc::core::PositionWriter, axc::core::Error> allocate_counter(
        std::int32_t type_id, std::span<const std::uint8_t> key, std::string_view label);

    static void fragment_trampoline(void* clientd, const std::uint8_t* buffer, size_t length, aeron_header_t* header);
    static void unavailable_image_trampoline(void* clientd, aeron_subscription_t* subscription, aeron_image_t* image);

    ArchiveConductorContext context_;
    axc::core::DeepmsgClient client_;
    axc::core::SubPtr control_subscription_;
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
    std::vector<PendingPublication> pending_publications_;
    std::int64_t next_session_id_{0};
    std::vector<std::string> warnings_;

    // Recording (A2b).
    Catalog catalog_;
    Recorder recorder_;
    std::optional<axc::core::PositionWriterTarget> max_write_time_counter_;
    std::optional<axc::core::PositionWriterTarget> total_write_bytes_counter_;
    std::optional<axc::core::PositionWriterTarget> total_write_time_counter_;
    std::optional<axc::core::PositionWriterTarget> recording_sessions_counter_;
    std::unordered_map<std::string, RecordingSubscription> recording_subscriptions_;
    /// subscription registration id -> `recording_subscriptions_` key.
    std::unordered_map<std::int64_t, std::string> recording_subscription_keys_;
    std::vector<PendingRecordingSubscription> pending_recording_subscriptions_;
    std::unordered_map<aeron_image_t*, RecordingSession*> sessions_by_image_;
    std::unordered_map<std::int64_t, RecordingSession*> sessions_by_recording_id_;
};

}  // namespace deepmsg::archive::server
