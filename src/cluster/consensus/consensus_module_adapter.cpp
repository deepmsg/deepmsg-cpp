#include "cluster/consensus/consensus_module_adapter.h"

#include <string>
#include <utility>

#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/BooleanType.h"
#include "io_aeron_cluster_codecs/CancelTimer.h"
#include "io_aeron_cluster_codecs/CloseSession.h"
#include "io_aeron_cluster_codecs/ClusterMembersQuery.h"
#include "io_aeron_cluster_codecs/ScheduleTimer.h"
#include "io_aeron_cluster_codecs/ServiceAck.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;
namespace header_codec = deepmsg::cluster::common::codecs;

}  // namespace

ConsensusModuleAdapter::ConsensusModuleAdapter(ServiceControlHandler& handler, std::int32_t fragment_poll_limit)
    : handler_(handler), fragment_poll_limit_(fragment_poll_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &ConsensusModuleAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ConsensusModuleAdapter::~ConsensusModuleAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

void ConsensusModuleAdapter::connect(axc::core::SubPtr subscription)
{
    subscription_ = std::move(subscription);
}

std::expected<int, Error> ConsensusModuleAdapter::poll()
{
    if (!subscription_)
    {
        return 0;
    }
    if (nullptr == assembler_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "service control assembler was not created"));
    }

    error_.reset();
    const int work = aeron_subscription_controlled_poll(
        subscription_.get(), aeron_controlled_fragment_assembler_handler, assembler_, fragment_poll_limit_);
    if (work < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "service control subscription poll failed"));
    }
    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    return work;
}

aeron_controlled_fragment_handler_action_t ConsensusModuleAdapter::on_fragment(void* clientd,
                                                                               const std::uint8_t* buffer,
                                                                               std::size_t length,
                                                                               aeron_header_t* /*header*/)
{
    auto* adapter = static_cast<ConsensusModuleAdapter*>(clientd);
    auto dispatched = adapter->dispatch_message(std::span<const std::uint8_t>{buffer, length});
    if (!dispatched)
    {
        adapter->error_ = dispatched.error();
        return AERON_ACTION_ABORT;
    }
    return *dispatched;
}

std::expected<aeron_controlled_fragment_handler_action_t, Error> ConsensusModuleAdapter::dispatch_message(
    std::span<const std::uint8_t> message)
{
    auto decoded_header = deepmsg::cluster::common::raw_message_header(message);
    if (!decoded_header)
    {
        return std::unexpected(decoded_header.error());
    }

    if (decoded_header->schemaId() != deepmsg::cluster::common::SBE_SCHEMA_ID)
    {
        return std::unexpected(make_error(
            ErrorCode::protocol_error, "expected schemaId=" + std::to_string(deepmsg::cluster::common::SBE_SCHEMA_ID) +
                                           ", actual=" + std::to_string(decoded_header->schemaId())));
    }

    switch (decoded_header->templateId())
    {
        case header_codec::SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::SessionMessageHeader>(message, *decoded_header);
            const std::int64_t cluster_session_id = decoder.clusterSessionId();
            auto handled = handler_.on_service_message(cluster_session_id, message);
            if (!handled)
            {
                return std::unexpected(handled.error());
            }
            return AERON_ACTION_CONTINUE;
        }

        case header_codec::CloseSession::SBE_TEMPLATE_ID:
        {
            auto decoder = deepmsg::cluster::common::wrap_body<header_codec::CloseSession>(message, *decoded_header);
            auto handled = handler_.on_service_close_session(decoder.clusterSessionId());
            if (!handled)
            {
                return std::unexpected(handled.error());
            }
            return AERON_ACTION_CONTINUE;
        }

        case header_codec::ScheduleTimer::SBE_TEMPLATE_ID:
        {
            auto decoder = deepmsg::cluster::common::wrap_body<header_codec::ScheduleTimer>(message, *decoded_header);
            handler_.on_schedule_timer(decoder.correlationId(), decoder.deadline());
            return AERON_ACTION_CONTINUE;
        }

        case header_codec::CancelTimer::SBE_TEMPLATE_ID:
        {
            auto decoder = deepmsg::cluster::common::wrap_body<header_codec::CancelTimer>(message, *decoded_header);
            handler_.on_cancel_timer(decoder.correlationId());
            return AERON_ACTION_CONTINUE;
        }

        case header_codec::ServiceAck::SBE_TEMPLATE_ID:
        {
            auto decoder = deepmsg::cluster::common::wrap_body<header_codec::ServiceAck>(message, *decoded_header);
            auto handled = handler_.on_service_ack(decoder.logPosition(), decoder.timestamp(), decoder.ackId(),
                                                   decoder.relevantId(), decoder.serviceId());
            if (!handled)
            {
                return std::unexpected(handled.error());
            }
            // `ConsensusModuleAdapter.java:134`: the poll stops after an ack.
            return AERON_ACTION_BREAK;
        }

        case header_codec::ClusterMembersQuery::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::ClusterMembersQuery>(message, *decoded_header);
            auto handled = handler_.on_cluster_members_query(decoder.correlationId(),
                                                             header_codec::BooleanType::TRUE == decoder.extended());
            if (!handled)
            {
                return std::unexpected(handled.error());
            }
            return AERON_ACTION_CONTINUE;
        }

        default:
            // The reference has no default arm: an unknown template is
            // silently continued.
            return AERON_ACTION_CONTINUE;
    }
}

}  // namespace deepmsg::cluster::consensus
