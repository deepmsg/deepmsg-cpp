#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/cluster_codecs.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusAdapter.FRAGMENT_LIMIT` (`ConsensusAdapter.java:35`).
inline constexpr std::size_t CONSENSUS_FRAGMENT_LIMIT = 10;

/// Where the ten consensus-channel messages go (`ConsensusAdapter.java:80-282`).
/// Eight are the election's inbound handlers; the two termination messages are
/// the agent's; `BackupQuery` is C9's and is reported, never handled silently.
class ConsensusMessageHandler
{
public:
    ConsensusMessageHandler() = default;
    ConsensusMessageHandler(const ConsensusMessageHandler&) = delete;
    ConsensusMessageHandler& operator=(const ConsensusMessageHandler&) = delete;
    virtual ~ConsensusMessageHandler() = default;

    virtual void on_canvass_position(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                     std::int64_t leadership_term_id, std::int32_t follower_member_id,
                                     std::int32_t protocol_version) = 0;
    virtual void on_request_vote(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                 std::int64_t candidate_term_id, std::int32_t candidate_member_id,
                                 std::int32_t protocol_version) = 0;
    virtual void on_vote(std::int64_t candidate_term_id, std::int64_t log_leadership_term_id, std::int64_t log_position,
                         std::int32_t candidate_member_id, std::int32_t follower_member_id, bool vote) = 0;
    virtual void on_new_leadership_term(std::int64_t log_leadership_term_id, std::int64_t next_leadership_term_id,
                                        std::int64_t next_term_base_log_position, std::int64_t next_log_position,
                                        std::int64_t leadership_term_id, std::int64_t term_base_log_position,
                                        std::int64_t log_position, std::int64_t commit_position,
                                        std::int64_t leader_recording_id, std::int64_t timestamp,
                                        std::int32_t leader_member_id, std::int32_t log_session_id,
                                        std::int32_t app_version, bool is_startup) = 0;
    virtual void on_append_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                    std::int32_t follower_member_id, std::int16_t flags) = 0;
    virtual void on_commit_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                    std::int32_t leader_member_id) = 0;
    virtual void on_catchup_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                     std::int32_t follower_member_id, std::string_view catchup_endpoint) = 0;
    virtual void on_stop_catchup(std::int64_t leadership_term_id, std::int32_t follower_member_id) = 0;
    virtual void on_termination_position(std::int64_t leadership_term_id, std::int64_t log_position) = 0;
    virtual void on_termination_ack(std::int64_t leadership_term_id, std::int64_t log_position,
                                    std::int32_t member_id) = 0;
    /// `ChallengeResponse` (`ConsensusAdapter.java:243`): a follower forwards a
    /// user session's answer to the leader (`onConsensusChallengeResponse`).
    virtual void on_consensus_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                                 std::string_view encoded_credentials) = 0;
    /// `BackupQuery` (`ConsensusAdapter.java:250`), C9's.
    virtual void on_unhandled_template(std::int32_t template_id) = 0;
};

/// `ConsensusAdapter` (`ConsensusAdapter.java`): the consensus channel's
/// inbound messages, reassembled and dispatched.
class ConsensusAdapter
{
public:
    explicit ConsensusAdapter(std::size_t fragment_limit);
    ~ConsensusAdapter();

    ConsensusAdapter(const ConsensusAdapter&) = delete;
    ConsensusAdapter& operator=(const ConsensusAdapter&) = delete;

    /// `poll()` (`:69-78`).
    [[nodiscard]] std::expected<int, Error> poll(aeron_subscription_t* subscription, ConsensusMessageHandler& handler);

    /// One complete message, for unit tests.
    [[nodiscard]] std::expected<void, Error> dispatch(std::span<const std::uint8_t> message);

    void handler(ConsensusMessageHandler& handler) noexcept
    {
        handler_ = &handler;
    }

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    ConsensusMessageHandler* handler_{nullptr};
    std::size_t fragment_limit_;
};

}  // namespace deepmsg::cluster::consensus
