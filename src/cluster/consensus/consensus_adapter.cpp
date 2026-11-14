#include "cluster/consensus/consensus_adapter.h"

#include "io_aeron_cluster_codecs/AppendPosition.h"
#include "io_aeron_cluster_codecs/CanvassPosition.h"
#include "io_aeron_cluster_codecs/CatchupPosition.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"
#include "io_aeron_cluster_codecs/CommitPosition.h"
#include "io_aeron_cluster_codecs/NewLeadershipTerm.h"
#include "io_aeron_cluster_codecs/RequestVote.h"
#include "io_aeron_cluster_codecs/StopCatchup.h"
#include "io_aeron_cluster_codecs/TerminationAck.h"
#include "io_aeron_cluster_codecs/TerminationPosition.h"
#include "io_aeron_cluster_codecs/Vote.h"

namespace deepmsg::cluster::consensus
{
namespace
{

namespace codecs = ::io::aeron::cluster::codecs;

using common::message_header;
using common::wrap_body;

}  // namespace

ConsensusAdapter::ConsensusAdapter(std::size_t fragment_limit) : fragment_limit_(fragment_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &ConsensusAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ConsensusAdapter::~ConsensusAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

std::expected<int, Error> ConsensusAdapter::poll(aeron_subscription_t* subscription, ConsensusMessageHandler& handler)
{
    if (nullptr == subscription || nullptr == assembler_)
    {
        return 0;
    }

    handler_ = &handler;
    const int work = aeron_subscription_controlled_poll(subscription, aeron_controlled_fragment_assembler_handler,
                                                        assembler_, fragment_limit_);
    handler_ = nullptr;
    if (work < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "consensus subscription poll failed"));
    }
    return work;
}

aeron_controlled_fragment_handler_action_t ConsensusAdapter::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                         std::size_t length, aeron_header_t*)
{
    auto* adapter = static_cast<ConsensusAdapter*>(clientd);
    auto dispatched = adapter->dispatch(std::span<const std::uint8_t>(buffer, length));
    return dispatched ? AERON_ACTION_CONTINUE : AERON_ACTION_ABORT;
}

std::expected<void, Error> ConsensusAdapter::dispatch(std::span<const std::uint8_t> message)
{
    if (nullptr == handler_)
    {
        return {};
    }

    auto header = message_header(message);
    if (!header)
    {
        return std::unexpected(header.error());
    }

    switch (header->templateId())
    {
        case codecs::CanvassPosition::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::CanvassPosition>(message, *header);
            handler_->on_canvass_position(m.logLeadershipTermId(), m.logPosition(), m.leadershipTermId(),
                                          m.followerMemberId(), m.protocolVersion());
            return {};
        }
        case codecs::RequestVote::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::RequestVote>(message, *header);
            handler_->on_request_vote(m.logLeadershipTermId(), m.logPosition(), m.candidateTermId(),
                                      m.candidateMemberId(), m.protocolVersion());
            return {};
        }
        case codecs::Vote::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::Vote>(message, *header);
            handler_->on_vote(m.candidateTermId(), m.logLeadershipTermId(), m.logPosition(), m.candidateMemberId(),
                              m.followerMemberId(), codecs::BooleanType::TRUE == m.vote());
            return {};
        }
        case codecs::NewLeadershipTerm::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::NewLeadershipTerm>(message, *header);
            handler_->on_new_leadership_term(m.logLeadershipTermId(), m.nextLeadershipTermId(),
                                             m.nextTermBaseLogPosition(), m.nextLogPosition(), m.leadershipTermId(),
                                             m.termBaseLogPosition(), m.logPosition(), m.commitPosition(),
                                             m.leaderRecordingId(), m.timestamp(), m.leaderMemberId(), m.logSessionId(),
                                             m.appVersion(), codecs::BooleanType::TRUE == m.isStartup());
            return {};
        }
        case codecs::AppendPosition::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::AppendPosition>(message, *header);
            handler_->on_append_position(m.leadershipTermId(), m.logPosition(), m.followerMemberId(),
                                         static_cast<std::int16_t>(m.flags()));
            return {};
        }
        case codecs::CommitPosition::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::CommitPosition>(message, *header);
            handler_->on_commit_position(m.leadershipTermId(), m.logPosition(), m.leaderMemberId());
            return {};
        }
        case codecs::CatchupPosition::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::CatchupPosition>(message, *header);
            handler_->on_catchup_position(m.leadershipTermId(), m.logPosition(), m.followerMemberId(),
                                          m.getCatchupEndpointAsString());
            return {};
        }
        case codecs::StopCatchup::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::StopCatchup>(message, *header);
            handler_->on_stop_catchup(m.leadershipTermId(), m.followerMemberId());
            return {};
        }
        case codecs::TerminationPosition::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::TerminationPosition>(message, *header);
            handler_->on_termination_position(m.leadershipTermId(), m.logPosition());
            return {};
        }
        case codecs::TerminationAck::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::TerminationAck>(message, *header);
            handler_->on_termination_ack(m.leadershipTermId(), m.logPosition(), m.memberId());
            return {};
        }
        case codecs::ChallengeResponse::SBE_TEMPLATE_ID:
        {
            auto m = wrap_body<codecs::ChallengeResponse>(message, *header);
            handler_->on_consensus_challenge_response(m.correlationId(), m.clusterSessionId(),
                                                      m.getEncodedCredentialsAsStringView());
            return {};
        }
        default:
            handler_->on_unhandled_template(header->templateId());
            return {};
    }
}

}  // namespace deepmsg::cluster::consensus
