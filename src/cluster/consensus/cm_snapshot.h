#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/timer/timer_service.h"
#include "cluster/consensus/cluster_session.h"
#include "cluster/consensus/pending_service_message_tracker.h"
#include "cluster/service/role.h"
#include "core/error/error.h"
#include "core/error/error_handler.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusModule.Configuration.SNAPSHOT_TYPE_ID` (`ConsensusModule.java:428`).
/// The container's own snapshots use 2.
inline constexpr std::int64_t CM_SNAPSHOT_TYPE_ID = 1;

/// `ConsensusModuleSnapshotAdapter.FRAGMENT_LIMIT` (`:31`).
inline constexpr std::size_t CM_SNAPSHOT_FRAGMENT_LIMIT = 10;

/// The `ExclusivePublication` seam the `ConsensusModuleSnapshotTaker` writes
/// into. The reference takes the publication itself because its tests can mock
/// it; this interface is the same seam, and the product implementation wraps
/// the CM snapshot's exclusive publication (`SnapshotPublication`, C2b).
class SnapshotPublicationSink
{
public:
    SnapshotPublicationSink() = default;
    SnapshotPublicationSink(const SnapshotPublicationSink&) = delete;
    SnapshotPublicationSink& operator=(const SnapshotPublicationSink&) = delete;
    virtual ~SnapshotPublicationSink() = default;

    /// `ExclusivePublication.tryClaim`: a writable claim, or the offer code.
    [[nodiscard]] virtual std::expected<std::span<std::uint8_t>, std::int64_t> try_claim(std::size_t length) = 0;
    /// `BufferClaim.commit` of the last successful `try_claim`.
    [[nodiscard]] virtual std::expected<void, Error> commit() = 0;
    /// `Publication.offer`.
    [[nodiscard]] virtual std::int64_t offer(std::span<const std::uint8_t> payload) = 0;
    [[nodiscard]] virtual std::size_t max_payload_length() const = 0;
    [[nodiscard]] virtual std::int64_t position() const = 0;
    /// `IdleStrategy.idle` plus the client's conductor poll (`SnapshotTaker`).
    virtual void idle() = 0;
};

/// The product sink: an exclusive publication plus the client's conductor pump
/// for `idle()`.
class ExclusivePublicationSink final : public SnapshotPublicationSink
{
public:
    ExclusivePublicationSink(axc::core::DeepmsgClient& client, aeron_exclusive_publication_t* publication) noexcept
        : client_(&client), publication_(publication)
    {
    }

    [[nodiscard]] std::expected<std::span<std::uint8_t>, std::int64_t> try_claim(std::size_t length) override;
    [[nodiscard]] std::expected<void, Error> commit() override;
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> payload) override;
    [[nodiscard]] std::size_t max_payload_length() const override;
    [[nodiscard]] std::int64_t position() const override;
    void idle() override;

private:
    axc::core::DeepmsgClient* client_;
    aeron_exclusive_publication_t* publication_;
    aeron_buffer_claim_t claim_{};
    bool claimed_{false};
};

/// What the adapter does with each record (`ConsensusModuleSnapshotListener`).
class SnapshotLoadHandler
{
public:
    virtual ~SnapshotLoadHandler() = default;

    [[nodiscard]] virtual std::expected<void, Error> on_load_begin_snapshot(std::int32_t app_version,
                                                                            service::TimeUnit time_unit) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_end_snapshot() = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_cluster_session(
        std::int64_t cluster_session_id, std::int64_t correlation_id, std::int64_t opened_log_position,
        std::int64_t time_of_last_activity, codecs::CloseReason::Value close_reason, std::int32_t response_stream_id,
        std::string_view response_channel) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_consensus_module_state(
        std::int64_t next_session_id, std::int64_t next_service_session_id, std::int64_t log_service_session_id,
        std::int32_t pending_message_capacity) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_pending_message_tracker(
        std::int64_t next_service_session_id, std::int64_t log_service_session_id,
        std::int32_t pending_message_capacity, std::int32_t service_id) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_pending_message(std::int64_t cluster_session_id,
                                                                             std::span<const std::uint8_t> message) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_timer(std::int64_t correlation_id,
                                                                   std::int64_t deadline) = 0;
};

/// `ConsensusModuleSnapshotTaker` (`ConsensusModuleSnapshotTaker.java`): the
/// consensus module's own snapshot recording, written between the
/// `SnapshotMarker(BEGIN)` / `SnapshotMarker(END)` markers of type id 1.
///
/// Record order is the reference's (`snapshotState`,
/// `ConsensusModuleAgent.java:3239-3271`): consensus module state, the session
/// table in iteration order, the timer service in its own snapshot order, then
/// one `PendingMessageTracker` header per service followed by that service's
/// queued messages.
class ConsensusModuleSnapshotTaker final : public common::timer::TimerSnapshotTaker
{
public:
    ConsensusModuleSnapshotTaker(SnapshotPublicationSink& publication, service::TimeUnit time_unit,
                                 std::int32_t app_version) noexcept
        : publication_(&publication), time_unit_(time_unit), app_version_(app_version)
    {
    }

    [[nodiscard]] std::expected<void, Error> mark_begin(std::int64_t log_position, std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> mark_end(std::int64_t log_position, std::int64_t leadership_term_id);

    /// `snapshotConsensusModuleState` (`:56-83`).
    [[nodiscard]] std::expected<void, Error> snapshot_consensus_module_state(std::int64_t next_session_id,
                                                                             std::int64_t next_service_session_id,
                                                                             std::int64_t log_service_session_id,
                                                                             std::int32_t pending_message_capacity);

    /// `snapshotSession` (`:85-113`): claim in place when it fits, otherwise
    /// build a record and `offer` it.
    [[nodiscard]] std::expected<void, Error> snapshot_session(const ClusterSession& session);

    /// `snapshotTimer` (`:115-133`). The `TimerSnapshotTaker` seam answers
    /// `void`, so a terminal failure is kept for the caller to pick up with
    /// `take_error()` (a refused claim is idled and retried, as the reference
    /// does).
    void snapshot_timer(std::int64_t correlation_id, std::int64_t deadline) override;

    /// `snapshot(PendingServiceMessageTracker, ErrorHandler)` (`:135-160`),
    /// including `correctNextServiceSessionId`'s "follower has missed service
    /// message(s)" warning (`:176-193`).
    [[nodiscard]] std::expected<void, Error> snapshot_tracker(const PendingServiceMessageTracker& tracker,
                                                              axc::core::ErrorHandler& error_handler);

    /// The first failure kept by the `void`-returning `snapshot_timer`.
    [[nodiscard]] std::optional<Error> take_error() noexcept;

    [[nodiscard]] std::int64_t position() const noexcept
    {
        return publication_->position();
    }

private:
    [[nodiscard]] std::expected<void, Error> mark(std::int32_t mark, std::int64_t log_position,
                                                  std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> claim(std::span<const std::uint8_t> record);
    [[nodiscard]] std::expected<void, Error> offer(std::span<const std::uint8_t> record);
    [[nodiscard]] bool offer_message(std::span<const std::uint8_t> message) noexcept;

    SnapshotPublicationSink* publication_;
    service::TimeUnit time_unit_;
    std::int32_t app_version_;
    std::optional<Error> error_{};
};

/// `ConsensusModuleSnapshotAdapter` (`ConsensusModuleSnapshotAdapter.java`):
/// decodes the consensus module's snapshot recording and stops at `END`.
///
/// `dispatch` is public so a unit test can feed the bytes the taker wrote, as
/// the reference's mocked publication does.
class ConsensusModuleSnapshotAdapter
{
public:
    ConsensusModuleSnapshotAdapter();
    ~ConsensusModuleSnapshotAdapter();

    ConsensusModuleSnapshotAdapter(const ConsensusModuleSnapshotAdapter&) = delete;
    ConsensusModuleSnapshotAdapter& operator=(const ConsensusModuleSnapshotAdapter&) = delete;

    /// `poll()` (`:59-62`): controlled poll of the replay image.
    [[nodiscard]] std::expected<int, Error> poll(aeron_image_t* image, SnapshotLoadHandler& handler);

    /// Bind the handler for `dispatch` (unit tests feed fragments directly).
    void handler(SnapshotLoadHandler& handler) noexcept
    {
        handler_ = &handler;
    }

    /// One complete fragment (the assembler's output).
    [[nodiscard]] std::expected<void, Error> dispatch(std::span<const std::uint8_t> fragment);

    [[nodiscard]] bool is_done() const noexcept
    {
        return done_;
    }

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    SnapshotLoadHandler* handler_{nullptr};
    bool in_snapshot_{false};
    bool done_{false};
};

}  // namespace deepmsg::cluster::consensus
