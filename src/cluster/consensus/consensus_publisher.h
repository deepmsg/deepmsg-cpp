#pragma once

#include <cstdint>
#include <expected>
#include <map>
#include <span>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/cluster_member.h"
#include "cluster/election/election_host.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusPublisher` (`ConsensusPublisher.java`, Aeron 1.54.0): the
/// per-member consensus publications. The election sees the six
/// `ConsensusSender` methods; the agent also sends `commitPosition` (`:273`),
/// `terminationPosition` (`:383`) and `terminationAck` (`:417`) through it.
///
/// The publication is named by the member id; the channel is the configured
/// consensus channel with the member's `consensusEndpoint` as its endpoint
/// (`ClusterMember.addConsensusPublications`).
class ConsensusPublisher final : public election::ConsensusSender
{
public:
    /// `control_endpoint` is this member's consensus endpoint:
    /// `ClusterMember.setControlEndpoint` binds it with port 0 on every
    /// publication when `enable_control` is set.
    ConsensusPublisher(axc::core::DeepmsgClient& client, std::string consensus_channel,
                       std::int32_t consensus_stream_id, std::string control_endpoint, bool enable_control);
    ~ConsensusPublisher() override;

    ConsensusPublisher(const ConsensusPublisher&) = delete;
    ConsensusPublisher& operator=(const ConsensusPublisher&) = delete;

    /// Blocking add, as the reference's `addConsensusPublications` is: the
    /// election cannot start before its publications exist.
    [[nodiscard]] std::expected<void, Error> add_publication(const common::ClusterMember& member);
    [[nodiscard]] bool has_publication(std::int32_t member_id) const;
    void close_publications();

    // ---- `ConsensusSender` (`ConsensusPublisher.java:53-308`) ----------

    void canvass_position(std::int32_t member_id, std::int64_t log_leadership_term_id, std::int64_t log_position,
                          std::int64_t leadership_term_id, std::int32_t follower_member_id) override;

    [[nodiscard]] bool request_vote(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                    std::int64_t log_position, std::int64_t candidate_term_id,
                                    std::int32_t candidate_member_id) override;

    void place_vote(std::int32_t member_id, std::int64_t candidate_term_id, std::int64_t log_leadership_term_id,
                    std::int64_t log_position, std::int32_t candidate_member_id, std::int32_t follower_member_id,
                    bool vote) override;

    void new_leadership_term(std::int32_t member_id, std::int64_t log_leadership_term_id,
                             std::int64_t next_leadership_term_id, std::int64_t next_term_base_log_position,
                             std::int64_t next_log_position, std::int64_t leadership_term_id,
                             std::int64_t term_base_log_position, std::int64_t log_position,
                             std::int64_t commit_position, std::int64_t leader_recording_id, std::int64_t timestamp,
                             std::int32_t leader_member_id, std::int32_t log_session_id, std::int32_t app_version,
                             bool is_startup) override;

    [[nodiscard]] bool append_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                       std::int64_t log_position, std::int32_t follower_member_id,
                                       std::int16_t flags) override;

    [[nodiscard]] bool catchup_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                        std::int64_t log_position, std::int32_t follower_member_id,
                                        std::string_view catchup_endpoint) override;

    /// `challengeResponse` (`ConsensusPublisher.java:558-580`): a follower
    /// forwards a client's `ChallengeResponse` to the leader.
    [[nodiscard]] bool challenge_response(std::int32_t member_id, std::int64_t next_correlation_id,
                                          std::int64_t cluster_session_id,
                                          std::span<const std::uint8_t> encoded_credentials);

    /// `stopCatchup` (`ConsensusPublisher.java:351-380`).
    [[nodiscard]] bool stop_catchup(std::int32_t member_id, std::int64_t leadership_term_id,
                                    std::int32_t follower_member_id);

    // ---- the agent's own sends (`:273`, `:383`, `:417`) ----------------

    [[nodiscard]] bool commit_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                       std::int64_t log_position, std::int32_t leader_member_id);
    [[nodiscard]] bool termination_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                            std::int64_t log_position);
    [[nodiscard]] bool termination_ack(std::int32_t member_id, std::int64_t leadership_term_id,
                                       std::int64_t log_position, std::int32_t member_id_ack);

private:
    [[nodiscard]] std::string member_channel(const common::ClusterMember& member) const;
    [[nodiscard]] bool offer(std::int32_t member_id, std::span<const std::uint8_t> message) const;

    axc::core::DeepmsgClient* client_;
    std::string consensus_channel_;
    std::int32_t consensus_stream_id_;
    std::string control_endpoint_;
    bool enable_control_{true};
    std::map<std::int32_t, axc::core::PubPtr> publications_{};
};

}  // namespace deepmsg::cluster::consensus
