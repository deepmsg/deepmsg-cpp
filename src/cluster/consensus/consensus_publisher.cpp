#include "cluster/consensus/consensus_publisher.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/consensus_constants.h"
#include "core/time/clock.h"
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

constexpr std::size_t EXTRA_LENGTH = 256;

template <class Encoder, class Fill>
std::vector<std::uint8_t> encode(Fill&& fill)
{
    std::vector<std::uint8_t> buffer(common::MESSAGE_HEADER_LENGTH + Encoder::SBE_BLOCK_LENGTH + EXTRA_LENGTH, 0);
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    fill(encoder);
    buffer.resize(common::MESSAGE_HEADER_LENGTH + encoder.encodedLength());
    return buffer;
}

}  // namespace

ConsensusPublisher::ConsensusPublisher(axc::core::DeepmsgClient& client, std::string consensus_channel,
                                       std::int32_t consensus_stream_id, std::string control_endpoint,
                                       bool enable_control)
    : client_(&client),
      consensus_channel_(std::move(consensus_channel)),
      consensus_stream_id_(consensus_stream_id),
      control_endpoint_(std::move(control_endpoint)),
      enable_control_(enable_control)
{
}

ConsensusPublisher::~ConsensusPublisher()
{
    close_publications();
}

std::string ConsensusPublisher::member_channel(const common::ClusterMember& member) const
{
    // An IPC channel has no endpoint; the reference's member endpoints are for
    // the UDP/MDC consensus channel.
    if (member.consensus_endpoint.empty() || 0 == consensus_channel_.rfind("aeron:ipc", 0))
    {
        return consensus_channel_;
    }
    std::string channel = consensus_channel_;
    channel.push_back(std::string::npos == channel.find('?') ? '?' : '|');
    channel.append("endpoint=");
    channel.append(member.consensus_endpoint);

    // `ClusterMember.setControlEndpoint` (`ClusterMember.java:1271-1285`): the
    // sender binds its own control endpoint (port 0) or the receiver's
    // status/NAK cannot come back.
    if (enable_control_ && 0 == channel.rfind("aeron:udp", 0))
    {
        if (const auto control = common::replace_port_with_wildcard(control_endpoint_); control.has_value())
        {
            channel.append("|control=").append(*control);
        }
    }
    return channel;
}

std::expected<void, Error> ConsensusPublisher::add_publication(const common::ClusterMember& member)
{
    if (publications_.end() != publications_.find(member.id))
    {
        return {};
    }

    const std::string channel = member_channel(member);
    auto add = client_->add_publication(channel, consensus_stream_id_);
    const std::int64_t deadline = axc::core::NanoClock::now() + 5'000'000'000;
    while (true)
    {
        (void)client_->do_work();
        auto added = add.poll();
        if (!added)
        {
            return std::unexpected(added.error());
        }
        if (added->has_value())
        {
            publications_.emplace(member.id, std::move(**added));
            return {};
        }
        if (axc::core::NanoClock::now() >= deadline)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::no_capacity, "consensus publication add timed out"));
        }
    }
}

bool ConsensusPublisher::has_publication(std::int32_t member_id) const
{
    return publications_.end() != publications_.find(member_id);
}

void ConsensusPublisher::close_publications()
{
    publications_.clear();
}

bool ConsensusPublisher::offer(std::int32_t member_id, std::span<const std::uint8_t> message) const
{
    const auto it = publications_.find(member_id);
    if (publications_.end() == it)
    {
        return false;
    }
    const std::int64_t result =
        aeron_publication_offer(it->second.get(), message.data(), message.size(), nullptr, nullptr);
    if (result <= 0)
    {
    }
    return result > 0;
}

void ConsensusPublisher::canvass_position(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                          std::int64_t log_position, std::int64_t leadership_term_id,
                                          std::int32_t follower_member_id)
{
    const auto buffer = encode<codecs::CanvassPosition>(
        [&](codecs::CanvassPosition& encoder)
        {
            encoder.logLeadershipTermId(log_leadership_term_id)
                .logPosition(log_position)
                .leadershipTermId(leadership_term_id)
                .followerMemberId(follower_member_id)
                .protocolVersion(CLUSTER_PROTOCOL_SEMANTIC_VERSION);
        });
    (void)offer(member_id, buffer);
}

bool ConsensusPublisher::request_vote(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                      std::int64_t log_position, std::int64_t candidate_term_id,
                                      std::int32_t candidate_member_id)
{
    const auto buffer = encode<codecs::RequestVote>(
        [&](codecs::RequestVote& encoder)
        {
            encoder.logLeadershipTermId(log_leadership_term_id)
                .logPosition(log_position)
                .candidateTermId(candidate_term_id)
                .candidateMemberId(candidate_member_id)
                .protocolVersion(CLUSTER_PROTOCOL_SEMANTIC_VERSION);
        });
    return offer(member_id, buffer);
}

void ConsensusPublisher::place_vote(std::int32_t member_id, std::int64_t candidate_term_id,
                                    std::int64_t log_leadership_term_id, std::int64_t log_position,
                                    std::int32_t candidate_member_id, std::int32_t follower_member_id, bool vote)
{
    const auto buffer = encode<codecs::Vote>(
        [&](codecs::Vote& encoder)
        {
            encoder.candidateTermId(candidate_term_id)
                .logLeadershipTermId(log_leadership_term_id)
                .logPosition(log_position)
                .candidateMemberId(candidate_member_id)
                .followerMemberId(follower_member_id)
                .vote(vote ? codecs::BooleanType::TRUE : codecs::BooleanType::FALSE);
        });
    (void)offer(member_id, buffer);
}

void ConsensusPublisher::new_leadership_term(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                             std::int64_t next_leadership_term_id,
                                             std::int64_t next_term_base_log_position, std::int64_t next_log_position,
                                             std::int64_t leadership_term_id, std::int64_t term_base_log_position,
                                             std::int64_t log_position, std::int64_t commit_position,
                                             std::int64_t leader_recording_id, std::int64_t timestamp,
                                             std::int32_t leader_member_id, std::int32_t log_session_id,
                                             std::int32_t app_version, bool is_startup)
{
    const auto buffer = encode<codecs::NewLeadershipTerm>(
        [&](codecs::NewLeadershipTerm& encoder)
        {
            encoder.logLeadershipTermId(log_leadership_term_id)
                .nextLeadershipTermId(next_leadership_term_id)
                .nextTermBaseLogPosition(next_term_base_log_position)
                .nextLogPosition(next_log_position)
                .leadershipTermId(leadership_term_id)
                .termBaseLogPosition(term_base_log_position)
                .logPosition(log_position)
                .commitPosition(commit_position)
                .leaderRecordingId(leader_recording_id)
                .timestamp(timestamp)
                .leaderMemberId(leader_member_id)
                .logSessionId(log_session_id)
                .appVersion(app_version)
                .isStartup(is_startup ? codecs::BooleanType::TRUE : codecs::BooleanType::FALSE);
        });
    (void)offer(member_id, buffer);
}

bool ConsensusPublisher::append_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                         std::int64_t log_position, std::int32_t follower_member_id, std::int16_t flags)
{
    const auto buffer = encode<codecs::AppendPosition>(
        [&](codecs::AppendPosition& encoder)
        {
            encoder.leadershipTermId(leadership_term_id)
                .logPosition(log_position)
                .followerMemberId(follower_member_id)
                .flags(static_cast<std::uint8_t>(flags));
        });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::catchup_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                          std::int64_t log_position, std::int32_t follower_member_id,
                                          std::string_view catchup_endpoint)
{
    const auto buffer = encode<codecs::CatchupPosition>(
        [&](codecs::CatchupPosition& encoder)
        {
            encoder.leadershipTermId(leadership_term_id)
                .logPosition(log_position)
                .followerMemberId(follower_member_id)
                .putCatchupEndpoint(catchup_endpoint);
        });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::challenge_response(std::int32_t member_id, std::int64_t next_correlation_id,
                                            std::int64_t cluster_session_id,
                                            std::span<const std::uint8_t> encoded_credentials)
{
    const auto buffer = encode<codecs::ChallengeResponse>(
        [&](codecs::ChallengeResponse& encoder)
        {
            encoder.correlationId(next_correlation_id)
                .clusterSessionId(cluster_session_id)
                .putEncodedCredentials(reinterpret_cast<const char*>(encoded_credentials.data()),
                                       static_cast<std::uint32_t>(encoded_credentials.size()));
        });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::stop_catchup(std::int32_t member_id, std::int64_t leadership_term_id,
                                      std::int32_t follower_member_id)
{
    const auto buffer = encode<codecs::StopCatchup>(
        [&](codecs::StopCatchup& encoder)
        { encoder.leadershipTermId(leadership_term_id).followerMemberId(follower_member_id); });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::commit_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                         std::int64_t log_position, std::int32_t leader_member_id)
{
    const auto buffer = encode<codecs::CommitPosition>(
        [&](codecs::CommitPosition& encoder)
        { encoder.leadershipTermId(leadership_term_id).logPosition(log_position).leaderMemberId(leader_member_id); });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::termination_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                              std::int64_t log_position)
{
    const auto buffer = encode<codecs::TerminationPosition>(
        [&](codecs::TerminationPosition& encoder)
        { encoder.leadershipTermId(leadership_term_id).logPosition(log_position); });
    return offer(member_id, buffer);
}

bool ConsensusPublisher::termination_ack(std::int32_t member_id, std::int64_t leadership_term_id,
                                         std::int64_t log_position, std::int32_t member_id_ack)
{
    const auto buffer = encode<codecs::TerminationAck>(
        [&](codecs::TerminationAck& encoder)
        { encoder.leadershipTermId(leadership_term_id).logPosition(log_position).memberId(member_id_ack); });
    return offer(member_id, buffer);
}

}  // namespace deepmsg::cluster::consensus
