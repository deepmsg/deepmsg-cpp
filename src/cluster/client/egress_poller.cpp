#include "cluster/client/egress_poller.h"

#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::client
{
namespace
{

namespace codecs = common::codecs;

constexpr std::int32_t SESSION_MESSAGE_HEADER_TEMPLATE_ID = codecs::SessionMessageHeader::sbeTemplateId();
constexpr std::int32_t SESSION_EVENT_TEMPLATE_ID = codecs::SessionEvent::sbeTemplateId();
constexpr std::int32_t NEW_LEADER_EVENT_TEMPLATE_ID = codecs::NewLeaderEvent::sbeTemplateId();
constexpr std::int32_t CHALLENGE_TEMPLATE_ID = codecs::Challenge::sbeTemplateId();

}  // namespace

EgressPoller::EgressPoller(std::size_t fragment_limit) : fragment_limit_(fragment_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &EgressPoller::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

EgressPoller::~EgressPoller()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

bool EgressPoller::is_challenged() const noexcept
{
    return CHALLENGE_TEMPLATE_ID == template_id_;
}

int EgressPoller::poll(aeron_subscription_t* subscription)
{
    if (is_poll_complete_)
    {
        reset_slot();
    }

    if (nullptr == subscription || nullptr == assembler_)
    {
        return 0;
    }

    return aeron_subscription_controlled_poll(subscription, &EgressPoller::on_fragment, this, fragment_limit_);
}

void EgressPoller::reset_slot()
{
    is_poll_complete_ = false;
    cluster_session_id_ = NULL_VALUE;
    correlation_id_ = NULL_VALUE;
    leadership_term_id_ = NULL_VALUE;
    leader_member_id_ = NULL_VALUE;
    template_id_ = NULL_VALUE;
    version_ = 0;
    leader_heartbeat_timeout_ns_ = NULL_VALUE;
    event_code_.reset();
    detail_.clear();
    encoded_challenge_.clear();
}

aeron_controlled_fragment_handler_action_t EgressPoller::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                     std::size_t length, aeron_header_t* header)
{
    auto* poller = static_cast<EgressPoller*>(clientd);
    return poller->on_message(std::span<const std::uint8_t>(buffer, length),
                              static_cast<aeron_image_t*>(aeron_header_context(header)));
}

aeron_controlled_fragment_handler_action_t EgressPoller::on_message(std::span<const std::uint8_t> message,
                                                                    aeron_image_t* image)
{
    // `EP:239-242`: a full slot backs the publisher off.
    if (is_poll_complete_)
    {
        return AERON_ACTION_ABORT;
    }

    auto header = common::message_header(message);
    if (!header)
    {
        // A short or foreign-schema message is stepped over (`EP:246-250`); the
        // length guard is ours, the schema check is the reference's.
        return AERON_ACTION_CONTINUE;
    }

    template_id_ = header->templateId();
    switch (template_id_)
    {
        case SESSION_MESSAGE_HEADER_TEMPLATE_ID:
        {
            // `EP:255-265`.
            auto decoder = common::wrap_body<codecs::SessionMessageHeader>(message, *header);
            leadership_term_id_ = decoder.leadershipTermId();
            cluster_session_id_ = decoder.clusterSessionId();
            is_poll_complete_ = true;
            return AERON_ACTION_BREAK;
        }

        case SESSION_EVENT_TEMPLATE_ID:
        {
            // `EP:267-284`.
            auto decoder = common::wrap_body<codecs::SessionEvent>(message, *header);
            cluster_session_id_ = decoder.clusterSessionId();
            correlation_id_ = decoder.correlationId();
            leadership_term_id_ = decoder.leadershipTermId();
            leader_member_id_ = decoder.leaderMemberId();
            event_code_ = decoder.code();
            version_ = decoder.version();
            const std::int64_t heartbeat = decoder.leaderHeartbeatTimeoutNs();
            leader_heartbeat_timeout_ns_ =
                heartbeat == codecs::SessionEvent::leaderHeartbeatTimeoutNsNullValue() ? NULL_VALUE : heartbeat;
            detail_ = decoder.getDetailAsString();
            is_poll_complete_ = true;
            egress_image_ = image;
            return AERON_ACTION_BREAK;
        }

        case NEW_LEADER_EVENT_TEMPLATE_ID:
        {
            // `EP:286-299`.
            auto decoder = common::wrap_body<codecs::NewLeaderEvent>(message, *header);
            cluster_session_id_ = decoder.clusterSessionId();
            leadership_term_id_ = decoder.leadershipTermId();
            leader_member_id_ = decoder.leaderMemberId();
            // `ingressEndpoints` is a length-prefixed var-data field, not a C
            // string: build the detail from the encoded length.
            detail_ = decoder.getIngressEndpointsAsString();
            is_poll_complete_ = true;
            egress_image_ = image;
            return AERON_ACTION_BREAK;
        }

        case CHALLENGE_TEMPLATE_ID:
        {
            // `EP:301-316`.
            auto decoder = common::wrap_body<codecs::Challenge>(message, *header);
            const std::uint32_t length = decoder.encodedChallengeLength();
            encoded_challenge_.resize(length);
            if (0 != length)
            {
                (void)decoder.getEncodedChallenge(reinterpret_cast<char*>(encoded_challenge_.data()), length);
            }
            cluster_session_id_ = decoder.clusterSessionId();
            correlation_id_ = decoder.correlationId();
            is_poll_complete_ = true;
            return AERON_ACTION_BREAK;
        }

        default:
            break;
    }

    // `EP:319`: an unknown template is stepped over.
    return AERON_ACTION_CONTINUE;
}

}  // namespace deepmsg::cluster::client
