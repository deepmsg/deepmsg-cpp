#include "cluster/consensus/log_adapter.h"

#include <string>

#include "io_aeron_cluster_codecs/ClusterActionRequest.h"
#include "io_aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "io_aeron_cluster_codecs/SessionCloseEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SessionOpenEvent.h"
#include "io_aeron_cluster_codecs/TimerEvent.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using common::message_header;
using common::wrap_body;

constexpr std::int64_t NULL_VALUE = -1;

}  // namespace

LogAdapter::LogAdapter(LogMessageHandler& handler, std::size_t fragment_limit)
    : handler_(&handler), fragment_limit_(fragment_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &LogAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

LogAdapter::~LogAdapter()
{
    (void)disconnect();
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

std::int64_t LogAdapter::disconnect()
{
    if (nullptr != subscription_ && nullptr != image_)
    {
        (void)aeron_subscription_image_release(subscription_, image_);
    }
    image_ = nullptr;
    subscription_ = nullptr;
    // The C client does not expose the subscription's registration id; the
    // host keeps the handle itself (C6's socket wait will need one).
    return NULL_VALUE;
}

std::int64_t LogAdapter::position() const
{
    return nullptr == image_ ? 0 : aeron_image_position(image_);
}

void LogAdapter::image(aeron_image_t* image)
{
    if (nullptr != image_ && image_ != image && nullptr != subscription_)
    {
        (void)aeron_subscription_image_release(subscription_, image_);
    }
    image_ = image;
}

bool LogAdapter::is_image_closed() const
{
    return nullptr == image_ || aeron_image_is_closed(image_);
}

bool LogAdapter::is_log_end_of_stream() const
{
    return nullptr != image_ && aeron_image_is_end_of_stream(image_);
}

bool LogAdapter::is_log_end_of_stream_at(std::int64_t position) const
{
    return nullptr != image_ && aeron_image_position(image_) >= position && aeron_image_is_end_of_stream(image_);
}

bool LogAdapter::async_remove_destination(std::string_view destination)
{
    if (nullptr == subscription_ || nullptr == client_)
    {
        return false;
    }
    return 0 == aeron_subscription_async_remove_destination(&async_destination_, client_->handle(), subscription_,
                                                            std::string{destination}.c_str());
}

std::expected<int, Error> LogAdapter::poll(std::int64_t bound_position)
{
    if (nullptr == image_ || nullptr == assembler_)
    {
        return 0;
    }

    error_.reset();
    const int work = aeron_image_bounded_controlled_poll(image_, aeron_controlled_fragment_assembler_handler,
                                                         assembler_, bound_position, fragment_limit_);
    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    if (work < 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "log image poll failed"));
    }
    return work;
}

aeron_controlled_fragment_handler_action_t LogAdapter::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                   std::size_t length, aeron_header_t* header)
{
    auto* adapter = static_cast<LogAdapter*>(clientd);
    return adapter->dispatch(std::span<const std::uint8_t>(buffer, length), aeron_header_position(header));
}

aeron_controlled_fragment_handler_action_t LogAdapter::dispatch(std::span<const std::uint8_t> message,
                                                                std::int64_t log_position)
{
    if (nullptr == handler_)
    {
        return AERON_ACTION_CONTINUE;
    }

    auto header = message_header(message);
    if (!header)
    {
        error_ = header.error();
        return AERON_ACTION_ABORT;
    }

    switch (header->templateId())
    {
        case codecs::SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionMessageHeader>(message, *header);
            handler_->on_replay_session_message(decoder.clusterSessionId(), decoder.timestamp());
            return AERON_ACTION_CONTINUE;
        }

        case codecs::TimerEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::TimerEvent>(message, *header);
            handler_->on_replay_timer_event(decoder.correlationId());
            return AERON_ACTION_CONTINUE;
        }

        case codecs::SessionOpenEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionOpenEvent>(message, *header);
            handler_->on_replay_session_open(log_position, decoder.correlationId(), decoder.clusterSessionId(),
                                             decoder.timestamp(), decoder.responseStreamId(),
                                             decoder.getResponseChannelAsString());
            return AERON_ACTION_CONTINUE;
        }

        case codecs::SessionCloseEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionCloseEvent>(message, *header);
            handler_->on_replay_session_close(decoder.clusterSessionId(), decoder.closeReason());
            return AERON_ACTION_CONTINUE;
        }

        case codecs::ClusterActionRequest::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::ClusterActionRequest>(message, *header);
            const std::int32_t flags =
                codecs::ClusterActionRequest::flagsNullValue() != decoder.flags() ? decoder.flags() : 0;
            auto handled = handler_->on_replay_cluster_action(decoder.leadershipTermId(), decoder.logPosition(),
                                                              decoder.timestamp(),
                                                              static_cast<std::int32_t>(decoder.action()), flags);
            if (!handled)
            {
                error_ = handled.error();
                return AERON_ACTION_ABORT;
            }
            return AERON_ACTION_BREAK;
        }

        case codecs::NewLeadershipTermEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::NewLeadershipTermEvent>(message, *header);
            auto handled = handler_->on_replay_new_leadership_term_event(
                decoder.leadershipTermId(), decoder.logPosition(), decoder.timestamp(), decoder.termBaseLogPosition(),
                static_cast<std::int32_t>(decoder.timeUnit()), decoder.appVersion());
            if (!handled)
            {
                error_ = handled.error();
                return AERON_ACTION_ABORT;
            }
            return AERON_ACTION_CONTINUE;
        }

        default:
            return AERON_ACTION_CONTINUE;
    }
}

}  // namespace deepmsg::cluster::consensus
