#include "cluster/service/service_adapter.h"

#include <utility>

#include "cluster/service/codecs.h"
#include "io_aeron_cluster_codecs/JoinLog.h"
#include "io_aeron_cluster_codecs/RequestServiceAck.h"
#include "io_aeron_cluster_codecs/ServiceTerminationPosition.h"

namespace deepmsg::cluster::service
{

ServiceAdapter::ServiceAdapter(aeron_subscription_t* subscription) : subscription_(subscription)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &ServiceAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ServiceAdapter::~ServiceAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

ServiceAdapter::ServiceAdapter(ServiceAdapter&& other) noexcept
    : subscription_(other.subscription_), assembler_(other.assembler_), events_(other.events_), error_(other.error_)
{
    other.subscription_ = nullptr;
    other.assembler_ = nullptr;
    other.events_ = nullptr;
}

ServiceAdapter& ServiceAdapter::operator=(ServiceAdapter&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != assembler_)
        {
            (void)aeron_controlled_fragment_assembler_delete(assembler_);
        }
        subscription_ = other.subscription_;
        assembler_ = other.assembler_;
        events_ = other.events_;
        error_ = other.error_;
        other.subscription_ = nullptr;
        other.assembler_ = nullptr;
        other.events_ = nullptr;
    }
    return *this;
}

aeron_controlled_fragment_handler_action_t ServiceAdapter::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                       std::size_t length, aeron_header_t* /*header*/)
{
    auto* adapter = static_cast<ServiceAdapter*>(clientd);
    if (nullptr == adapter->events_)
    {
        return AERON_ACTION_CONTINUE;
    }

    auto dispatched = dispatch_message(*adapter->events_, std::span<const std::uint8_t>(buffer, length));
    if (!dispatched)
    {
        adapter->error_ = dispatched.error();
        return AERON_ACTION_ABORT;
    }
    return AERON_ACTION_CONTINUE;
}

std::expected<std::size_t, Error> ServiceAdapter::poll(ServiceEvents& events)
{
    if (nullptr == subscription_ || nullptr == assembler_)
    {
        return std::size_t{0};
    }

    events_ = &events;
    error_.reset();
    const int work = aeron_subscription_controlled_poll(subscription_, aeron_controlled_fragment_assembler_handler,
                                                        assembler_, SERVICE_FRAGMENT_LIMIT);
    events_ = nullptr;

    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    if (work < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "service subscription poll failed"));
    }
    return static_cast<std::size_t>(work);
}

std::expected<void, Error> ServiceAdapter::dispatch_message(ServiceEvents& events,
                                                            std::span<const std::uint8_t> message)
{
    auto header = message_header(message);
    if (!header)
    {
        return std::unexpected(header.error());
    }

    switch (header->templateId())
    {
        case codecs::JoinLog::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::JoinLog>(message, *header);
            JoinLogEvent event;
            event.log_position = decoder.logPosition();
            event.max_log_position = decoder.maxLogPosition();
            event.member_id = decoder.memberId();
            event.log_session_id = decoder.logSessionId();
            event.log_stream_id = decoder.logStreamId();
            event.is_startup = codecs::BooleanType::TRUE == decoder.isStartup();
            event.role = decoder.role();
            event.is_standby = codecs::BooleanType::TRUE == decoder.isStandby();
            event.log_channel = decoder.getLogChannelAsString();
            events.join_log = std::move(event);
            return {};
        }
        case codecs::ServiceTerminationPosition::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::ServiceTerminationPosition>(message, *header);
            events.termination_position =
                std::make_pair(decoder.logPosition(), codecs::BooleanType::TRUE == decoder.ackRequired());
            return {};
        }
        case codecs::RequestServiceAck::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::RequestServiceAck>(message, *header);
            events.requested_ack_position = decoder.logPosition();
            return {};
        }
        default:
            return {};
    }
}

}  // namespace deepmsg::cluster::service
