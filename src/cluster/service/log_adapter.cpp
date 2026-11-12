#include "cluster/service/log_adapter.h"

#include <utility>

#include "cluster/service/codecs.h"
#include "io_aeron_cluster_codecs/ClusterActionRequest.h"
#include "io_aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "io_aeron_cluster_codecs/SessionCloseEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SessionOpenEvent.h"
#include "io_aeron_cluster_codecs/TimerEvent.h"

namespace deepmsg::cluster::service
{

BoundedLogAdapter::BoundedLogAdapter(std::size_t fragment_limit) : fragment_limit_(fragment_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &BoundedLogAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

BoundedLogAdapter::~BoundedLogAdapter()
{
    clear_image();
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

BoundedLogAdapter::BoundedLogAdapter(BoundedLogAdapter&& other) noexcept
    : subscription_(other.subscription_),
      image_(other.image_),
      max_log_position_(other.max_log_position_),
      fragment_limit_(other.fragment_limit_),
      assembler_(other.assembler_),
      handler_(other.handler_),
      error_(other.error_)
{
    other.subscription_ = nullptr;
    other.image_ = nullptr;
    other.assembler_ = nullptr;
    other.handler_ = nullptr;
}

BoundedLogAdapter& BoundedLogAdapter::operator=(BoundedLogAdapter&& other) noexcept
{
    if (this != &other)
    {
        clear_image();
        if (nullptr != assembler_)
        {
            (void)aeron_controlled_fragment_assembler_delete(assembler_);
        }
        subscription_ = other.subscription_;
        image_ = other.image_;
        max_log_position_ = other.max_log_position_;
        fragment_limit_ = other.fragment_limit_;
        assembler_ = other.assembler_;
        handler_ = other.handler_;
        error_ = other.error_;
        other.subscription_ = nullptr;
        other.image_ = nullptr;
        other.assembler_ = nullptr;
        other.handler_ = nullptr;
    }
    return *this;
}

void BoundedLogAdapter::set_image(aeron_subscription_t* subscription, aeron_image_t* image) noexcept
{
    clear_image();
    subscription_ = subscription;
    image_ = image;
}

void BoundedLogAdapter::clear_image() noexcept
{
    if (nullptr != image_ && nullptr != subscription_)
    {
        (void)aeron_subscription_image_release(subscription_, image_);
    }
    image_ = nullptr;
    subscription_ = nullptr;
}

bool BoundedLogAdapter::is_done() const noexcept
{
    if (nullptr == image_)
    {
        return true;
    }
    return aeron_image_position(image_) >= max_log_position_ || aeron_image_is_end_of_stream(image_) ||
           aeron_image_is_closed(image_);
}

aeron_controlled_fragment_handler_action_t BoundedLogAdapter::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                          std::size_t length, aeron_header_t* header)
{
    auto* adapter = static_cast<BoundedLogAdapter*>(clientd);
    if (nullptr == adapter->handler_)
    {
        return AERON_ACTION_CONTINUE;
    }

    auto decoded = decode(std::span<const std::uint8_t>(buffer, length));
    if (!decoded)
    {
        adapter->error_ = decoded.error();
        return AERON_ACTION_ABORT;
    }

    LogEvent event;
    event.position = aeron_header_position(header);
    aeron_header_values_t values{};
    if (0 == aeron_header_values(header, &values))
    {
        event.session_id = values.frame.session_id;
        event.stream_id = values.frame.stream_id;
    }
    event.message = std::move(*decoded);

    // The payload of a session message is the business bytes after the 32-byte
    // session header; every other message has none.
    if (LogMessage::Kind::session_message == event.message.kind)
    {
        constexpr std::size_t SESSION_MESSAGE_HEADER_LENGTH =
            MESSAGE_HEADER_LENGTH + codecs::SessionMessageHeader::SBE_BLOCK_LENGTH;
        if (length < SESSION_MESSAGE_HEADER_LENGTH)
        {
            adapter->error_ = axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                    "a session message is shorter than its header");
            return AERON_ACTION_ABORT;
        }
        event.payload = std::span<const std::uint8_t>(buffer + SESSION_MESSAGE_HEADER_LENGTH,
                                                      length - SESSION_MESSAGE_HEADER_LENGTH);
    }

    auto handled = adapter->handler_->on_log_event(event);
    if (!handled)
    {
        adapter->error_ = handled.error();
        return AERON_ACTION_ABORT;
    }
    return AERON_ACTION_CONTINUE;
}

std::expected<std::size_t, Error> BoundedLogAdapter::poll(std::int64_t limit, LogHandler& handler)
{
    if (nullptr == image_ || nullptr == assembler_)
    {
        return std::size_t{0};
    }

    const std::int64_t bound = limit < max_log_position_ ? limit : max_log_position_;
    handler_ = &handler;
    error_.reset();

    const int work = aeron_image_bounded_controlled_poll(image_, aeron_controlled_fragment_assembler_handler,
                                                         assembler_, bound, fragment_limit_);
    handler_ = nullptr;

    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    if (work < 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "log image poll failed"));
    }
    return static_cast<std::size_t>(work);
}

std::expected<LogMessage, Error> BoundedLogAdapter::decode(std::span<const std::uint8_t> message)
{
    auto header = message_header(message);
    if (!header)
    {
        return std::unexpected(header.error());
    }

    LogMessage decoded;
    switch (header->templateId())
    {
        case codecs::SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionMessageHeader>(message, *header);
            decoded.kind = LogMessage::Kind::session_message;
            decoded.cluster_session_id = decoder.clusterSessionId();
            decoded.timestamp = decoder.timestamp();
            return decoded;
        }
        case codecs::TimerEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::TimerEvent>(message, *header);
            decoded.kind = LogMessage::Kind::timer_event;
            decoded.correlation_id = decoder.correlationId();
            decoded.timestamp = decoder.timestamp();
            return decoded;
        }
        case codecs::SessionOpenEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionOpenEvent>(message, *header);
            decoded.kind = LogMessage::Kind::session_open;
            decoded.leadership_term_id = decoder.leadershipTermId();
            decoded.cluster_session_id = decoder.clusterSessionId();
            decoded.timestamp = decoder.timestamp();
            decoded.response_stream_id = decoder.responseStreamId();
            decoded.response_channel = decoder.getResponseChannelAsString();
            const std::string principal = decoder.getEncodedPrincipalAsString();
            decoded.encoded_principal.assign(principal.begin(), principal.end());
            return decoded;
        }
        case codecs::SessionCloseEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::SessionCloseEvent>(message, *header);
            decoded.kind = LogMessage::Kind::session_close;
            decoded.leadership_term_id = decoder.leadershipTermId();
            decoded.cluster_session_id = decoder.clusterSessionId();
            decoded.timestamp = decoder.timestamp();
            decoded.close_reason = decoder.closeReason();
            return decoded;
        }
        case codecs::ClusterActionRequest::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::ClusterActionRequest>(message, *header);
            decoded.kind = LogMessage::Kind::cluster_action;
            decoded.leadership_term_id = decoder.leadershipTermId();
            decoded.log_position = decoder.logPosition();
            decoded.timestamp = decoder.timestamp();
            decoded.action = static_cast<std::int32_t>(decoder.action());
            decoded.flags = decoder.flags();
            return decoded;
        }
        case codecs::NewLeadershipTermEvent::SBE_TEMPLATE_ID:
        {
            auto decoder = wrap_body<codecs::NewLeadershipTermEvent>(message, *header);
            decoded.kind = LogMessage::Kind::new_leadership_term;
            decoded.leadership_term_id = decoder.leadershipTermId();
            decoded.log_position = decoder.logPosition();
            decoded.timestamp = decoder.timestamp();
            decoded.term_base_log_position = decoder.termBaseLogPosition();
            decoded.leader_member_id = decoder.leaderMemberId();
            decoded.log_session_id = decoder.logSessionId();
            decoded.time_unit_code = static_cast<std::int32_t>(decoder.timeUnit());
            decoded.app_version = decoder.appVersion();
            return decoded;
        }
        default:
            // The reference's switch has no default arm.
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::protocol_error, "unknown cluster log template"));
    }
}

}  // namespace deepmsg::cluster::service
