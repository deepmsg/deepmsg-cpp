#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <aeronc.h>

#include "archive/server/authenticator.h"
#include "archive/server/control_response_proxy.h"
#include "archive/server/control_session.h"
#include "archive/server/control_session_adapter.h"
#include "archive/server/deepmsg_response_publication.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/session_worker.h"
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
    /// `Aeron.NULL_VALUE` unless the archive was given one (`ctx.archiveId()`).
    std::int64_t archive_id{-1};
    /// nullptr ⇒ `DefaultAuthenticator`.
    Authenticator* authenticator{nullptr};
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
    void log_warning(std::string_view message) override;

    [[nodiscard]] std::int64_t archive_id() const override
    {
        return context_.archive_id;
    }

    [[nodiscard]] std::size_t session_count() const noexcept
    {
        return sessions_.size();
    }

    [[nodiscard]] const std::vector<std::string>& warnings() const noexcept
    {
        return warnings_;
    }

private:
    struct PollContext
    {
        ArchiveConductor* conductor{nullptr};
        aeron_image_t* image{nullptr};
    };

    explicit ArchiveConductor(const ArchiveConductorContext& context);

    [[nodiscard]] std::expected<void, axc::core::Error> init();

    [[nodiscard]] int poll_control_subscription();
    [[nodiscard]] int poll_pending_publications();

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
};

}  // namespace deepmsg::archive::server
