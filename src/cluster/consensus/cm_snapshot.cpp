#include "cluster/consensus/cm_snapshot.h"

#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/ClusterMembers.h"
#include "io_aeron_cluster_codecs/ClusterSession.h"
#include "io_aeron_cluster_codecs/ConsensusModule.h"
#include "io_aeron_cluster_codecs/PendingMessageTracker.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SnapshotMarker.h"
#include "io_aeron_cluster_codecs/Timer.h"

namespace deepmsg::cluster::consensus
{
namespace
{

constexpr std::int64_t OFFER_BACK_PRESSURED = AERON_PUBLICATION_BACK_PRESSURED;
constexpr std::int64_t OFFER_ADMIN_ACTION = AERON_PUBLICATION_ADMIN_ACTION;

using common::message_header;
using common::MESSAGE_HEADER_LENGTH;
using common::wrap_body;

/// `ClusterClock`'s time unit code (`ClusterTimeUnit`, schema 111). The
/// service library owns the same mapping for the container; the consensus
/// library cannot link it (service depends on consensus), so the two-function
/// mapping lives here too.
[[nodiscard]] std::int32_t cm_time_unit_code(service::TimeUnit unit) noexcept
{
    switch (unit)
    {
        case service::TimeUnit::milliseconds:
            return 0;
        case service::TimeUnit::microseconds:
            return 1;
        case service::TimeUnit::nanoseconds:
            return 2;
    }
    return 0;
}

[[nodiscard]] service::TimeUnit cm_time_unit_from_code(std::int32_t code) noexcept
{
    switch (code)
    {
        case 1:
            return service::TimeUnit::microseconds;
        case 2:
            return service::TimeUnit::nanoseconds;
        default:
            return service::TimeUnit::milliseconds;
    }
}

[[nodiscard]] Error publication_error(std::int64_t code, std::string_view what)
{
    return axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                 std::string{what} + " refused: " + std::to_string(code));
}

}  // namespace

// -- ExclusivePublicationSink -----------------------------------------------

std::expected<std::span<std::uint8_t>, std::int64_t> ExclusivePublicationSink::try_claim(std::size_t length)
{
    claim_ = {};
    const std::int64_t position = aeron_exclusive_publication_try_claim(publication_, length, &claim_);
    if (position < 0)
    {
        return std::unexpected(position);
    }
    claimed_ = true;
    return std::span<std::uint8_t>(claim_.data, claim_.length);
}

std::expected<void, Error> ExclusivePublicationSink::commit()
{
    if (!claimed_)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "commit without a claim"));
    }
    claimed_ = false;
    if (0 != aeron_buffer_claim_commit(&claim_))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "failed to commit the snapshot claim"));
    }
    return {};
}

std::int64_t ExclusivePublicationSink::offer(std::span<const std::uint8_t> payload)
{
    return aeron_exclusive_publication_offer(publication_, payload.data(), payload.size(), nullptr, nullptr);
}

std::size_t ExclusivePublicationSink::max_payload_length() const
{
    aeron_publication_constants_t constants{};
    if (0 != aeron_exclusive_publication_constants(publication_, &constants))
    {
        return 0;
    }
    return constants.max_payload_length;
}

std::int64_t ExclusivePublicationSink::position() const
{
    return aeron_exclusive_publication_position(publication_);
}

void ExclusivePublicationSink::idle()
{
    (void)client_->do_work();
}

// -- ConsensusModuleSnapshotTaker -------------------------------------------

std::expected<void, Error> ConsensusModuleSnapshotTaker::mark(std::int32_t mark, std::int64_t log_position,
                                                              std::int64_t leadership_term_id)
{
    std::uint8_t buffer[MESSAGE_HEADER_LENGTH + codecs::SnapshotMarker::SBE_BLOCK_LENGTH]{};

    codecs::SnapshotMarker encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer), 0, sizeof(buffer));
    encoder.typeId(CM_SNAPSHOT_TYPE_ID)
        .logPosition(log_position)
        .leadershipTermId(leadership_term_id)
        .index(0)
        .mark(static_cast<codecs::SnapshotMark::Value>(mark))
        .timeUnit(static_cast<codecs::ClusterTimeUnit::Value>(cm_time_unit_code(time_unit_)))
        .appVersion(app_version_);

    return claim(std::span<const std::uint8_t>(buffer, sizeof(buffer)));
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::mark_begin(std::int64_t log_position,
                                                                    std::int64_t leadership_term_id)
{
    return mark(static_cast<std::int32_t>(codecs::SnapshotMark::BEGIN), log_position, leadership_term_id);
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::mark_end(std::int64_t log_position,
                                                                  std::int64_t leadership_term_id)
{
    return mark(static_cast<std::int32_t>(codecs::SnapshotMark::END), log_position, leadership_term_id);
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::snapshot_consensus_module_state(
    std::int64_t next_session_id, std::int64_t next_service_session_id, std::int64_t log_service_session_id,
    std::int32_t pending_message_capacity)
{
    std::uint8_t buffer[MESSAGE_HEADER_LENGTH + codecs::ConsensusModule::SBE_BLOCK_LENGTH]{};

    codecs::ConsensusModule encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer), 0, sizeof(buffer));
    encoder.nextSessionId(next_session_id)
        .nextServiceSessionId(next_service_session_id)
        .logServiceSessionId(log_service_session_id)
        .pendingMessageCapacity(pending_message_capacity);

    return claim(std::span<const std::uint8_t>(buffer, sizeof(buffer)));
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::snapshot_session(const ClusterSession& session)
{
    const std::string& response_channel = session.response_channel();
    const std::size_t length = MESSAGE_HEADER_LENGTH + codecs::ClusterSession::SBE_BLOCK_LENGTH +
                               codecs::ClusterSession::responseChannelHeaderLength() + response_channel.size();

    if (length > publication_->max_payload_length())
    {
        std::vector<std::uint8_t> buffer(length, 0);
        codecs::ClusterSession encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.clusterSessionId(session.id())
            .correlationId(session.correlation_id())
            .openedLogPosition(session.opened_log_position())
            .timeOfLastActivity(NULL_VALUE)
            .closeReason(session.close_reason())
            .responseStreamId(session.response_stream_id())
            .putResponseChannel(response_channel);
        return offer(buffer);
    }

    while (true)
    {
        auto claimed = publication_->try_claim(length);
        if (claimed.has_value())
        {
            codecs::ClusterSession encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claimed->data()), 0, claimed->size());
            encoder.clusterSessionId(session.id())
                .correlationId(session.correlation_id())
                .openedLogPosition(session.opened_log_position())
                .timeOfLastActivity(NULL_VALUE)
                .closeReason(session.close_reason())
                .responseStreamId(session.response_stream_id())
                .putResponseChannel(response_channel);
            return publication_->commit();
        }

        if (OFFER_BACK_PRESSURED == claimed.error() || OFFER_ADMIN_ACTION == claimed.error())
        {
            publication_->idle();
            continue;
        }
        return std::unexpected(publication_error(claimed.error(), "snapshot session claim"));
    }
}

void ConsensusModuleSnapshotTaker::snapshot_timer(std::int64_t correlation_id, std::int64_t deadline)
{
    if (error_.has_value())
    {
        return;
    }

    std::uint8_t buffer[MESSAGE_HEADER_LENGTH + codecs::Timer::SBE_BLOCK_LENGTH]{};

    codecs::Timer encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer), 0, sizeof(buffer));
    encoder.correlationId(correlation_id).deadline(deadline);

    auto written = claim(std::span<const std::uint8_t>(buffer, sizeof(buffer)));
    if (!written)
    {
        error_ = written.error();
    }
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::snapshot_tracker(const PendingServiceMessageTracker& tracker,
                                                                          axc::core::ErrorHandler& error_handler)
{
    // `correctNextServiceSessionId` (`:176-193`).
    const std::int64_t next_service_session_id =
        tracker.pending_messages().empty() ? tracker.log_service_session_id() + 1 : tracker.next_service_session_id();
    const std::int64_t missed_service_message_count = next_service_session_id - tracker.next_service_session_id();
    if (0 < missed_service_message_count)
    {
        error_handler.on_error(axc::core::make_error(
            axc::core::ErrorCode::internal_error,
            "Follower has missed " + std::to_string(missed_service_message_count) +
                " service message(s).  Please check service (id=" + std::to_string(tracker.service_id()) +
                ") determinism around the use of Cluster::offer"));
    }

    std::uint8_t buffer[MESSAGE_HEADER_LENGTH + codecs::PendingMessageTracker::SBE_BLOCK_LENGTH]{};

    codecs::PendingMessageTracker encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer), 0, sizeof(buffer));
    encoder.nextServiceSessionId(next_service_session_id)
        .logServiceSessionId(tracker.log_service_session_id())
        .pendingMessageCapacity(static_cast<std::int32_t>(tracker.pending_messages().size()))
        .serviceId(tracker.service_id());

    auto written = claim(std::span<const std::uint8_t>(buffer, sizeof(buffer)));
    if (!written)
    {
        return written;
    }

    std::optional<Error> walk_error{};
    tracker.pending_messages().for_each(
        0,
        [this, &walk_error](std::span<const std::uint8_t> message, std::size_t)
        {
            if (walk_error.has_value())
            {
                return false;
            }
            if (!offer_message(message))
            {
                walk_error =
                    axc::core::make_error(axc::core::ErrorCode::no_capacity, "snapshot pending message offer refused");
                return false;
            }
            return true;
        },
        std::numeric_limits<std::size_t>::max());

    if (walk_error.has_value())
    {
        return std::unexpected(*walk_error);
    }
    return {};
}

std::optional<Error> ConsensusModuleSnapshotTaker::take_error() noexcept
{
    std::optional<Error> error = std::move(error_);
    error_.reset();
    return error;
}

bool ConsensusModuleSnapshotTaker::offer_message(std::span<const std::uint8_t> message) noexcept
{
    while (true)
    {
        const std::int64_t position = publication_->offer(message);
        if (position > 0)
        {
            return true;
        }
        if (OFFER_BACK_PRESSURED == position || OFFER_ADMIN_ACTION == position)
        {
            publication_->idle();
            continue;
        }
        return false;
    }
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::claim(std::span<const std::uint8_t> record)
{
    while (true)
    {
        auto claimed = publication_->try_claim(record.size());
        if (claimed.has_value())
        {
            std::memcpy(claimed->data(), record.data(), record.size());
            return publication_->commit();
        }
        if (OFFER_BACK_PRESSURED == claimed.error() || OFFER_ADMIN_ACTION == claimed.error())
        {
            publication_->idle();
            continue;
        }
        return std::unexpected(publication_error(claimed.error(), "failed to claim snapshot buffer"));
    }
}

std::expected<void, Error> ConsensusModuleSnapshotTaker::offer(std::span<const std::uint8_t> record)
{
    while (true)
    {
        const std::int64_t position = publication_->offer(record);
        if (position > 0)
        {
            return {};
        }
        if (OFFER_BACK_PRESSURED == position || OFFER_ADMIN_ACTION == position)
        {
            publication_->idle();
            continue;
        }
        return std::unexpected(publication_error(position, "failed to offer snapshot buffer"));
    }
}

// -- ConsensusModuleSnapshotAdapter -----------------------------------------

ConsensusModuleSnapshotAdapter::ConsensusModuleSnapshotAdapter()
{
    if (0 !=
        aeron_controlled_fragment_assembler_create(&assembler_, &ConsensusModuleSnapshotAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ConsensusModuleSnapshotAdapter::~ConsensusModuleSnapshotAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

aeron_controlled_fragment_handler_action_t ConsensusModuleSnapshotAdapter::on_fragment(void* clientd,
                                                                                       const std::uint8_t* buffer,
                                                                                       std::size_t length,
                                                                                       aeron_header_t*)
{
    auto* adapter = static_cast<ConsensusModuleSnapshotAdapter*>(clientd);
    auto dispatched = adapter->dispatch(std::span<const std::uint8_t>(buffer, length));
    if (!dispatched)
    {
        return AERON_ACTION_ABORT;
    }
    return adapter->is_done() ? AERON_ACTION_BREAK : AERON_ACTION_CONTINUE;
}

std::expected<int, Error> ConsensusModuleSnapshotAdapter::poll(aeron_image_t* image, SnapshotLoadHandler& handler)
{
    if (done_ || nullptr == assembler_)
    {
        return 0;
    }

    handler_ = &handler;
    const int work = aeron_image_bounded_controlled_poll(image, aeron_controlled_fragment_assembler_handler, assembler_,
                                                         INT64_MAX, CM_SNAPSHOT_FRAGMENT_LIMIT);
    handler_ = nullptr;

    if (work < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "snapshot image poll failed"));
    }
    return work;
}

std::expected<void, Error> ConsensusModuleSnapshotAdapter::dispatch(std::span<const std::uint8_t> fragment)
{
    if (done_ || nullptr == handler_)
    {
        return {};
    }

    auto header = message_header(fragment);
    if (!header)
    {
        return std::unexpected(header.error());
    }

    switch (header->templateId())
    {
        case codecs::SnapshotMarker::SBE_TEMPLATE_ID:
        {
            auto marker = wrap_body<codecs::SnapshotMarker>(fragment, *header);
            if (CM_SNAPSHOT_TYPE_ID != marker.typeId())
            {
                return std::unexpected(
                    axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                          "unexpected snapshot type: " + std::to_string(marker.typeId())));
            }

            if (codecs::SnapshotMark::BEGIN == marker.mark())
            {
                if (in_snapshot_)
                {
                    return std::unexpected(
                        axc::core::make_error(axc::core::ErrorCode::protocol_error, "already in snapshot"));
                }
                in_snapshot_ = true;
                return handler_->on_load_begin_snapshot(
                    marker.appVersion(), cm_time_unit_from_code(static_cast<std::int32_t>(marker.timeUnit())));
            }

            if (codecs::SnapshotMark::END == marker.mark())
            {
                if (!in_snapshot_)
                {
                    return std::unexpected(
                        axc::core::make_error(axc::core::ErrorCode::protocol_error, "missing begin snapshot"));
                }
                done_ = true;
                return handler_->on_load_end_snapshot();
            }
            break;
        }

        case codecs::ClusterSession::SBE_TEMPLATE_ID:
        {
            auto session = wrap_body<codecs::ClusterSession>(fragment, *header);
            return handler_->on_load_cluster_session(session.clusterSessionId(), session.correlationId(),
                                                     session.openedLogPosition(), session.timeOfLastActivity(),
                                                     session.closeReason(), session.responseStreamId(),
                                                     session.getResponseChannelAsString());
        }

        case codecs::Timer::SBE_TEMPLATE_ID:
        {
            auto timer = wrap_body<codecs::Timer>(fragment, *header);
            return handler_->on_load_timer(timer.correlationId(), timer.deadline());
        }

        case codecs::ConsensusModule::SBE_TEMPLATE_ID:
        {
            auto state = wrap_body<codecs::ConsensusModule>(fragment, *header);
            return handler_->on_load_consensus_module_state(state.nextSessionId(), state.nextServiceSessionId(),
                                                            state.logServiceSessionId(),
                                                            state.pendingMessageCapacity());
        }

        case codecs::ClusterMembers::SBE_TEMPLATE_ID:
            // Ignored, as the reference does (`:179-181`).
            break;

        case codecs::PendingMessageTracker::SBE_TEMPLATE_ID:
        {
            auto tracker = wrap_body<codecs::PendingMessageTracker>(fragment, *header);
            return handler_->on_load_pending_message_tracker(tracker.nextServiceSessionId(),
                                                             tracker.logServiceSessionId(),
                                                             tracker.pendingMessageCapacity(), tracker.serviceId());
        }

        case codecs::SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            auto session_header = wrap_body<codecs::SessionMessageHeader>(fragment, *header);
            return handler_->on_load_pending_message(session_header.clusterSessionId(), fragment);
        }

        default:
            break;
    }

    return {};
}

}  // namespace deepmsg::cluster::consensus
