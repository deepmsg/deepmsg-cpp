#include "cluster/election/election.h"

#include <cstdio>

#include <cstdio>

#include <cstdio>

#include <algorithm>
#include <string>
#include <utility>

#include "cluster/common/node_state_file.h"
#include "cluster/common/recording_log.h"
#include "cluster/service/role.h"

namespace deepmsg::cluster::election
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;
using common::ClusterMember;
using common::ClusterMembers;
using service::Role;

/// One day in nanoseconds: `Election`'s initial "no update has ever happened"
/// timestamp (`Election.java:141`).
constexpr std::int64_t ONE_DAY_NS = 24LL * 60 * 60 * 1'000'000'000;

/// `Aeron.NULL_VALUE` in the state-change trace's leader field.
constexpr std::int32_t NULL_MEMBER_ID = -1;

[[nodiscard]] bool ends_with_zero_port(std::string_view endpoint) noexcept
{
    return endpoint.size() >= 2 && endpoint.substr(endpoint.size() - 2) == ":0";
}

}  // namespace

std::string_view election_state_name(ElectionState state) noexcept
{
    switch (state)
    {
        case ElectionState::init:
            return "INIT";
        case ElectionState::canvass:
            return "CANVASS";
        case ElectionState::nominate:
            return "NOMINATE";
        case ElectionState::candidate_ballot:
            return "CANDIDATE_BALLOT";
        case ElectionState::follower_ballot:
            return "FOLLOWER_BALLOT";
        case ElectionState::leader_log_replication:
            return "LEADER_LOG_REPLICATION";
        case ElectionState::leader_replay:
            return "LEADER_REPLAY";
        case ElectionState::leader_init:
            return "LEADER_INIT";
        case ElectionState::leader_ready:
            return "LEADER_READY";
        case ElectionState::follower_log_replication:
            return "FOLLOWER_LOG_REPLICATION";
        case ElectionState::follower_replay:
            return "FOLLOWER_REPLAY";
        case ElectionState::follower_catchup_init:
            return "FOLLOWER_CATCHUP_INIT";
        case ElectionState::follower_catchup_await:
            return "FOLLOWER_CATCHUP_AWAIT";
        case ElectionState::follower_catchup:
            return "FOLLOWER_CATCHUP";
        case ElectionState::follower_log_init:
            return "FOLLOWER_LOG_INIT";
        case ElectionState::follower_log_await:
            return "FOLLOWER_LOG_AWAIT";
        case ElectionState::follower_ready:
            return "FOLLOWER_READY";
        case ElectionState::closed:
            return "CLOSED";
    }
    return "<unknown>";
}

std::int64_t ElectionContext::timestamp_from_nanos(std::int64_t nanos) const noexcept
{
    switch (time_unit)
    {
        case service::TimeUnit::milliseconds:
            return nanos / 1'000'000;
        case service::TimeUnit::microseconds:
            return nanos / 1'000;
        case service::TimeUnit::nanoseconds:
            return nanos;
    }
    return nanos;
}

Election::Election(bool is_node_startup, std::int32_t graceful_closed_leader_id, std::int64_t leadership_term_id,
                   std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t append_position,
                   ClusterMembers& cluster_members, ClusterMember& this_member, ConsensusSender& sender,
                   ElectionHost& host, ElectionContext context)
    : cluster_members_(cluster_members),
      this_member_(this_member),
      sender_(sender),
      host_(host),
      context_(std::move(context)),
      is_node_startup_(is_node_startup),
      log_position_(log_position),
      append_position_(append_position),
      leadership_term_id_(leadership_term_id),
      log_leadership_term_id_(leadership_term_id),
      candidate_term_id_(leadership_term_id),
      graceful_closed_leader_id_(graceful_closed_leader_id),
      is_extended_canvass_(is_node_startup)
{
    // `Election.java:120-150`. The five-tuple is set from the arguments; the
    // "never updated" timestamps are one day in the past so the first interval
    // check always fires.
    initial_log_leadership_term_id_ = leadership_term_id;
    initial_term_base_log_position_ = term_base_log_position;

    const std::int64_t now = context_.now_ns();
    initial_time_of_last_update_ns_ = now - ONE_DAY_NS;
    time_of_last_update_ns_ = initial_time_of_last_update_ns_;
    time_of_last_commit_position_update_ns_ = initial_time_of_last_update_ns_;

    context_.set_election_state(ElectionState::init);
    context_.increment_election_counter();
}

std::int64_t Election::timestamp_to_nanos(std::int64_t timestamp) const noexcept
{
    // `timestampToNanos(ctx, timestamp)` (`Election.java:1606-1609`).
    switch (context_.time_unit)
    {
        case service::TimeUnit::milliseconds:
            return timestamp * 1'000'000;
        case service::TimeUnit::microseconds:
            return timestamp * 1'000;
        case service::TimeUnit::nanoseconds:
            return timestamp;
    }
    return timestamp;
}

common::ClusterMember* Election::member_by_id(std::int32_t member_id) noexcept
{
    return common::find_member(cluster_members_, member_id);
}

std::expected<int, Error> Election::do_work(std::int64_t now_ns)
{
    // `Election.java:180-257`.
    switch (state_)
    {
        case ElectionState::init:
            return init(now_ns);
        case ElectionState::canvass:
            return canvass(now_ns);
        case ElectionState::nominate:
            return nominate(now_ns);
        case ElectionState::candidate_ballot:
            return candidate_ballot(now_ns);
        case ElectionState::follower_ballot:
            return follower_ballot(now_ns);
        case ElectionState::leader_log_replication:
            return leader_log_replication(now_ns);
        case ElectionState::leader_replay:
            return leader_replay(now_ns);
        case ElectionState::leader_init:
            return leader_init(now_ns);
        case ElectionState::leader_ready:
            return leader_ready(now_ns);
        case ElectionState::follower_log_replication:
            return follower_log_replication(now_ns);
        case ElectionState::follower_replay:
            return follower_replay(now_ns);
        case ElectionState::follower_catchup_init:
            return follower_catchup_init(now_ns);
        case ElectionState::follower_catchup_await:
            return follower_catchup_await(now_ns);
        case ElectionState::follower_catchup:
            return follower_catchup(now_ns);
        case ElectionState::follower_log_init:
            return follower_log_init(now_ns);
        case ElectionState::follower_log_await:
            return follower_log_await(now_ns);
        case ElectionState::follower_ready:
            return follower_ready(now_ns);
        case ElectionState::closed:
            return 0;
    }
    return 0;
}

std::expected<void, Error> Election::handle_error(std::int64_t now_ns, const Error& error)
{
    // `Election.java:259-267`.
    log_position_ = context_.commit_position();
    return state(ElectionState::init, now_ns, error.message.view());
}

void Election::on_recording_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                                   std::int32_t signal)
{
    // `Election.java:269-282`.
    if (ElectionState::init == state_ || nullptr == log_replication_)
    {
        return;
    }

    log_replication_->on_signal(correlation_id, recording_id, position, signal);
    host_.set_log_recording_id(log_replication_->recording_id());
}

// ---------------------------------------------------------------------------
// The eight inbound handlers (`Election.java:284-646`).
// ---------------------------------------------------------------------------

std::expected<void, Error> Election::on_canvass_position(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                                         std::int64_t leadership_term_id,
                                                         std::int32_t follower_member_id, std::int32_t protocol_version)
{
    // `Election.java:284-332`.
    (void)protocol_version;
    if (ElectionState::init == state_)
    {
        return {};
    }

    if (follower_member_id == graceful_closed_leader_id_)
    {
        graceful_closed_leader_id_ = NULL_MEMBER_ID;
    }

    ClusterMember* follower = member_by_id(follower_member_id);
    if (nullptr != follower && this_member_.id != follower_member_id)
    {
        host_.update_member_log_position(*follower, log_leadership_term_id, log_position);

        if (log_leadership_term_id < leadership_term_id_)
        {
            if (Role::leader == host_.role())
            {
                const std::int64_t now = now_ns();
                publish_new_leadership_term(*follower, log_leadership_term_id,
                                            host_.quorum_position_bounded_by_leader_log(append_position_, now),
                                            nanos_to_timestamp(now));
            }
        }
        else if (log_leadership_term_id > leadership_term_id_)
        {
            switch (state_)
            {
                case ElectionState::leader_log_replication:
                case ElectionState::leader_ready:
                    return std::unexpected(make_error(ErrorCode::internal_error, "potential new election in progress"));

                default:
                    break;
            }
        }
    }
    return {};
}

std::expected<void, Error> Election::on_request_vote(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                                     std::int64_t candidate_term_id, std::int32_t candidate_id,
                                                     std::int32_t protocol_version)
{
    // `Election.java:334-381`.
    (void)protocol_version;
    if (ElectionState::init == state_)
    {
        return {};
    }

    if (candidate_id == this_member_.id)
    {
        return {};
    }

    if (candidate_term_id <= candidate_term_id_)
    {
        place_vote(candidate_term_id, candidate_id, false);
    }
    else if (common::compare_log(log_leadership_term_id_, append_position_, log_leadership_term_id, log_position) > 0)
    {
        auto proposed = context_.node_state_file->propose_max_candidate_term_id(candidate_term_id, log_position,
                                                                                context_.epoch_ms());
        if (!proposed)
        {
            return std::unexpected(proposed.error());
        }
        candidate_term_id_ = *proposed;

        place_vote(candidate_term_id, candidate_id, false);

        ClusterMember* candidate_member = member_by_id(candidate_id);
        if (nullptr != candidate_member && Role::leader == host_.role())
        {
            const std::int64_t now = now_ns();
            publish_new_leadership_term(*candidate_member, log_leadership_term_id,
                                        host_.quorum_position_bounded_by_leader_log(append_position_, now),
                                        nanos_to_timestamp(now));
        }
    }
    else if (ElectionState::canvass == state_ || ElectionState::nominate == state_ ||
             ElectionState::candidate_ballot == state_ || ElectionState::follower_ballot == state_)
    {
        const std::int64_t now = now_ns();
        auto proposed = context_.node_state_file->propose_max_candidate_term_id(candidate_term_id, log_position,
                                                                                context_.timestamp_from_nanos(now));
        if (!proposed)
        {
            return std::unexpected(proposed.error());
        }
        candidate_term_id_ = *proposed;
        place_vote(candidate_term_id, candidate_id, true);
        return state(ElectionState::follower_ballot, now, "");
    }
    return {};
}

std::expected<void, Error> Election::on_vote(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                             std::int64_t candidate_term_id, std::int32_t candidate_id,
                                             std::int32_t voter_id, bool vote)
{
    // `Election.java:383-410`.
    if (ElectionState::init == state_)
    {
        return {};
    }

    if (ElectionState::candidate_ballot == state_ && candidate_term_id == candidate_term_id_ &&
        candidate_id == this_member_.id)
    {
        ClusterMember* follower = member_by_id(voter_id);
        if (nullptr != follower)
        {
            follower->candidate_term_id = candidate_term_id;
            follower->vote = vote;
            host_.update_member_log_position(*follower, log_leadership_term_id, log_position);
        }
    }
    return {};
}

std::expected<void, Error> Election::on_new_leadership_term(
    std::int64_t log_leadership_term_id, std::int64_t next_leadership_term_id, std::int64_t next_term_base_log_position,
    std::int64_t next_log_position, std::int64_t leadership_term_id, std::int64_t term_base_log_position,
    std::int64_t log_position, std::int64_t commit_position, std::int64_t leader_recording_id, std::int64_t timestamp,
    std::int32_t leader_member_id, std::int32_t log_session_id, bool is_startup)
{
    // `Election.java:412-533`.
    (void)timestamp;
    if (ElectionState::init == state_)
    {
        return {};
    }

    ClusterMember* leader = member_by_id(leader_member_id);
    if (nullptr == leader || (leader_member_id == this_member_.id && leadership_term_id == leadership_term_id_))
    {
        return {};
    }

    if (leader_member_id == graceful_closed_leader_id_)
    {
        graceful_closed_leader_id_ = NULL_MEMBER_ID;
    }

    if (((ElectionState::follower_ballot == state_ || ElectionState::candidate_ballot == state_) &&
         leadership_term_id == candidate_term_id_) ||
        ElectionState::canvass == state_)
    {
        if (log_leadership_term_id == log_leadership_term_id_)
        {
            if (NULL_POSITION != next_term_base_log_position && next_term_base_log_position < append_position_)
            {
                auto truncated =
                    on_truncate_log_entry(this_member_.id, state_, log_leadership_term_id_, leadership_term_id_,
                                          candidate_term_id_, context_.commit_position(), log_position_,
                                          append_position_, append_position_, next_term_base_log_position);
                if (!truncated)
                {
                    return std::unexpected(truncated.error());
                }
            }

            leader_member_ = leader;
            is_leader_startup_ = is_startup;
            leadership_term_id_ = leadership_term_id;
            candidate_term_id_ = std::max(leadership_term_id, candidate_term_id_);
            log_session_id_ = log_session_id;
            leader_recording_id_ = leader_recording_id;
            catchup_join_position_ = append_position_ < log_position ? log_position : NULL_POSITION;
            notified_commit_position_ = std::max(notified_commit_position_, commit_position);

            if (append_position_ < term_base_log_position)
            {
                if (NULL_VALUE != next_leadership_term_id)
                {
                    if (append_position_ < next_term_base_log_position)
                    {
                        replication_leadership_term_id_ = log_leadership_term_id;
                        replication_stop_position_ = next_term_base_log_position;
                        // An open but uncommitted term means the base position is
                        // already known; writing it back through the recording
                        // log would only echo it.
                        replication_term_base_log_position_ = NULL_VALUE;
                        auto changed = state(ElectionState::follower_log_replication, now_ns(), "");
                        if (!changed)
                        {
                            return std::unexpected(changed.error());
                        }
                    }
                    else if (append_position_ == next_term_base_log_position)
                    {
                        if (NULL_POSITION != next_log_position)
                        {
                            replication_leadership_term_id_ = next_leadership_term_id;
                            replication_stop_position_ = next_log_position;
                            replication_term_base_log_position_ = next_term_base_log_position;
                            auto changed = state(ElectionState::follower_log_replication, now_ns(), "");
                            if (!changed)
                            {
                                return std::unexpected(changed.error());
                            }
                        }
                    }
                }
                else
                {
                    return std::unexpected(make_error(
                        ErrorCode::protocol_error,
                        "invalid newLeadershipTerm - this.appendPosition=" + std::to_string(append_position_) +
                            " < termBaseLogPosition=" + std::to_string(term_base_log_position) +
                            " and nextLeadershipTermId=" + std::to_string(next_leadership_term_id) +
                            ", logLeadershipTermId=" + std::to_string(log_leadership_term_id) +
                            ", nextTermBaseLogPosition=" + std::to_string(next_term_base_log_position) +
                            ", nextLogPosition=" + std::to_string(next_log_position) +
                            ", leadershipTermId=" + std::to_string(leadership_term_id) +
                            ", termBaseLogPosition=" + std::to_string(term_base_log_position)));
                }
            }
            else
            {
                auto changed = state(ElectionState::follower_replay, now_ns(), "");
                if (!changed)
                {
                    return std::unexpected(changed.error());
                }
            }
        }
        else
        {
            auto changed = state(ElectionState::canvass, now_ns(), "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
    }

    if (ElectionState::follower_log_replication == state_ && log_leadership_term_id == log_leadership_term_id_ &&
        nullptr != leader_member_ && leader_member_->id == leader_member_id)
    {
        replication_deadline_ns_ = now_ns() + context_.leader_heartbeat_timeout_ns;
    }
    return {};
}

std::expected<void, Error> Election::on_append_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                                        std::int32_t follower_member_id, std::int16_t flags)
{
    // `Election.java:535-555`.
    if (ElectionState::init == state_)
    {
        return {};
    }

    if (leadership_term_id <= leadership_term_id_)
    {
        ClusterMember* follower = member_by_id(follower_member_id);
        if (nullptr != follower)
        {
            host_.update_member_log_position(*follower, leadership_term_id, log_position);
            auto tracked = host_.track_catchup_completion(*follower, leadership_term_id, flags);
            if (!tracked)
            {
                return std::unexpected(tracked.error());
            }
        }
    }
    return {};
}

std::expected<void, Error> Election::on_commit_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                                        std::int32_t leader_member_id)
{
    // `Election.java:557-588`. `leadershipTermId` is deliberately not compared
    // with ours: prior to the fix the leader sent a `leadershipTermId` that did
    // not match its own `NewLeadershipTerm`.
    const ElectionState current = state_;
    if (ElectionState::init == current)
    {
        return {};
    }

    if (nullptr != leader_member_ && leader_member_->id == leader_member_id)
    {
        notified_commit_position_ = std::max(notified_commit_position_, log_position);
        if (ElectionState::follower_log_replication == current)
        {
            replication_deadline_ns_ = now_ns() + context_.leader_heartbeat_timeout_ns;
        }
    }
    else if (leadership_term_id > leadership_term_id_ && ElectionState::leader_ready == current)
    {
        return std::unexpected(
            make_error(ErrorCode::internal_error,
                       "new leader detected due to commit position - memberId=" + std::to_string(this_member_.id) +
                           " this.leadershipTermId=" + std::to_string(leadership_term_id_) + " this.leaderMemberId=" +
                           std::to_string(nullptr != leader_member_ ? leader_member_->id : NULL_MEMBER_ID) +
                           " this.logPosition=" + std::to_string(log_position_) +
                           " newLeadershipTermId=" + std::to_string(leadership_term_id) + " newLeaderMemberId=" +
                           std::to_string(leader_member_id) + " newCommitPosition=" + std::to_string(log_position)));
    }
    return {};
}

std::expected<void, Error> Election::on_replay_new_leadership_term_event(std::int64_t leadership_term_id,
                                                                         std::int64_t log_position,
                                                                         std::int64_t timestamp,
                                                                         std::int64_t term_base_log_position)
{
    // `Election.java:590-608`.
    if (ElectionState::init == state_)
    {
        return {};
    }

    if (ElectionState::follower_catchup == state_ || ElectionState::follower_replay == state_)
    {
        const std::int64_t now = timestamp_to_nanos(timestamp);
        auto ensured = ensure_recording_log_coherent(leadership_term_id, term_base_log_position, NULL_POSITION, now);
        if (!ensured)
        {
            return std::unexpected(ensured.error());
        }
        log_position_ = log_position;
        log_leadership_term_id_ = leadership_term_id;
    }
    return {};
}

std::expected<void, Error> Election::on_truncate_log_entry(std::int32_t member_id, ElectionState state,
                                                           std::int64_t log_leadership_term_id,
                                                           std::int64_t leadership_term_id,
                                                           std::int64_t candidate_term_id, std::int64_t commit_position,
                                                           std::int64_t log_position, std::int64_t append_position,
                                                           std::int64_t old_position, std::int64_t new_position)
{
    // `Election.java:610-646`. The truncation happens, and then the reference
    // always raises a `ClusterEvent`: it is an abnormal path that unwinds
    // through `handleError`. The error return *is* that event here.
    auto truncated = host_.truncate_log_entry(log_leadership_term_id, new_position);
    if (!truncated)
    {
        return std::unexpected(truncated.error());
    }
    append_position_ = new_position;

    return std::unexpected(make_error(
        ErrorCode::internal_error,
        "Truncating Cluster Log - memberId=" + std::to_string(member_id) +
            " state=" + std::string{election_state_name(state)} + " this.logLeadershipTermId=" +
            std::to_string(log_leadership_term_id) + " this.leadershipTermId=" + std::to_string(leadership_term_id) +
            " this.candidateTermId=" + std::to_string(candidate_term_id) + " this.commitPosition=" +
            std::to_string(commit_position) + " this.logPosition=" + std::to_string(log_position) +
            " this.appendPosition=" + std::to_string(append_position) + " oldPosition=" + std::to_string(old_position) +
            " newPosition=" + std::to_string(new_position)));
}

// ---------------------------------------------------------------------------
// The states (`Election.java:653-1180`).
// ---------------------------------------------------------------------------

std::expected<int, Error> Election::init(std::int64_t now_ns)
{
    // `Election.java:653-696`.
    if (is_first_init_)
    {
        is_first_init_ = false;
        if (!is_node_startup_)
        {
            auto prepared = prepare_for_new_leadership(now_ns);
            if (!prepared)
            {
                return std::unexpected(prepared.error());
            }
        }
    }
    else
    {
        stop_log_replication();
        auto caught_up = stop_catchup();
        if (!caught_up)
        {
            return std::unexpected(caught_up.error());
        }

        auto prepared = prepare_for_new_leadership(now_ns);
        if (!prepared)
        {
            return std::unexpected(prepared.error());
        }
        log_session_id_ = NULL_SESSION_ID;
        stop_replay();

        if (NULL_VALUE != log_subscription_registration_id_)
        {
            const std::int64_t registration_id = log_subscription_registration_id_;
            auto removed = host_.remove_log_subscription(registration_id);
            if (!removed)
            {
                return std::unexpected(removed.error());
            }
            // The subscription is gone. The id is the C++ "do we have one"
            // marker, so it is cleared before the socket wait: a failed wait
            // (`Election.java:674-676` retries on a closed object) must retry
            // the wait, not ask the agent to remove the subscription again.
            log_subscription_registration_id_ = NULL_VALUE;
            auto awaited = host_.await_local_sockets_closed(registration_id);
            if (!awaited)
            {
                return std::unexpected(awaited.error());
            }
        }
    }

    notified_commit_position_ = 0;
    candidate_term_id_ = std::max(context_.node_state_file->candidate_term().candidate_term_id, leadership_term_id_);

    if (cluster_members_.size() == 1 && this_member_.id == cluster_members_[0].id)
    {
        // Short-circuit nominating self to a candidate and winning the ballot
        // (`Election.java:683-691`).
        candidate_term_id_ += 1;
        auto updated =
            context_.node_state_file->update_candidate_term_id(candidate_term_id_, log_position_, context_.epoch_ms());
        if (!updated)
        {
            return std::unexpected(updated.error());
        }

        leader_member_ = &this_member_;
        leadership_term_id_ = candidate_term_id_;

        auto changed = state(ElectionState::leader_log_replication, now_ns, "single-node cluster leader");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }
    else
    {
        auto changed = state(ElectionState::canvass, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }

    return 1;
}

std::expected<int, Error> Election::canvass(std::int64_t now_ns)
{
    // `Election.java:700-730`.
    int work_count = 0;
    const std::int64_t deadline_ns = is_extended_canvass_
                                         ? time_of_last_state_change_ns_ + context_.startup_canvass_timeout_ns
                                         : host_.time_of_last_leader_update_ns() + context_.leader_heartbeat_timeout_ns;

    if (has_update_interval_expired(now_ns, context_.election_status_interval_ns))
    {
        time_of_last_update_ns_ = now_ns;
        auto published = publish_canvass_position();
        if (!published)
        {
            return std::unexpected(published.error());
        }
        work_count++;
    }

    if (context_.appointed_leader_id != NULL_MEMBER_ID && context_.appointed_leader_id != this_member_.id)
    {
        return work_count;
    }

    if (common::is_unanimous_candidate(cluster_members_, this_member_, graceful_closed_leader_id_) ||
        (now_ns >= deadline_ns && common::is_quorum_candidate(cluster_members_, this_member_)))
    {
        const auto delay_ns =
            static_cast<std::int64_t>(context_.random() * static_cast<double>(context_.election_timeout_ns >> 1));
        nomination_deadline_ns_ = now_ns + delay_ns;
        auto changed = state(ElectionState::nominate, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
        work_count++;
    }

    return work_count;
}

std::expected<int, Error> Election::nominate(std::int64_t now_ns)
{
    // `Election.java:732-752`.
    if (now_ns >= nomination_deadline_ns_)
    {
        auto proposed = context_.node_state_file->propose_max_candidate_term_id(candidate_term_id_ + 1, log_position_,
                                                                                context_.epoch_ms());
        if (!proposed)
        {
            return std::unexpected(proposed.error());
        }
        candidate_term_id_ = *proposed;
        common::become_candidate(cluster_members_, candidate_term_id_, this_member_.id);
        auto changed = state(ElectionState::candidate_ballot, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
        return 1;
    }

    if (has_update_interval_expired(now_ns, context_.election_status_interval_ns))
    {
        time_of_last_update_ns_ = now_ns;
        auto published = publish_canvass_position();
        if (!published)
        {
            return std::unexpected(published.error());
        }
        return 1;
    }

    return 0;
}

std::expected<int, Error> Election::candidate_ballot(std::int64_t now_ns)
{
    // `Election.java:754-794`.
    int work_count = 0;

    if (common::is_unanimous_leader(cluster_members_, candidate_term_id_, graceful_closed_leader_id_))
    {
        leader_member_ = &this_member_;
        leadership_term_id_ = candidate_term_id_;
        auto changed = state(ElectionState::leader_log_replication, now_ns, "unanimous leader");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
        work_count++;
    }
    else if (now_ns >= (time_of_last_state_change_ns_ + context_.election_timeout_ns))
    {
        if (common::is_quorum_leader(cluster_members_, candidate_term_id_))
        {
            leader_member_ = &this_member_;
            leadership_term_id_ = candidate_term_id_;
            auto changed = state(ElectionState::leader_log_replication, now_ns, "quorum leader");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
        else
        {
            auto changed = state(ElectionState::canvass, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
        work_count++;
    }
    else
    {
        for (ClusterMember& member : cluster_members_)
        {
            if (member.is_ballot_sent)
            {
                continue;
            }
            work_count++;
            member.is_ballot_sent = sender_.request_vote(member.id, log_leadership_term_id_, append_position_,
                                                         candidate_term_id_, this_member_.id);
        }
    }

    return work_count;
}

std::expected<int, Error> Election::follower_ballot(std::int64_t now_ns)
{
    // `Election.java:796-807`.
    if (now_ns >= (time_of_last_state_change_ns_ + context_.election_timeout_ns))
    {
        auto changed = state(ElectionState::canvass, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
        return 1;
    }
    return 0;
}

std::expected<int, Error> Election::leader_log_replication(std::int64_t now_ns)
{
    // `Election.java:809-826`.
    int work_count = 0;

    sync_this_member_log(leadership_term_id_, append_position_, now_ns, true);

    const std::int64_t quorum_position = host_.quorum_position_bounded_by_leader_log(append_position_, now_ns);
    auto new_terms = publish_new_leadership_term_on_interval(quorum_position, now_ns);
    if (!new_terms)
    {
        return std::unexpected(new_terms.error());
    }
    work_count += *new_terms;

    auto commits = publish_commit_position_on_interval(quorum_position, now_ns);
    if (!commits)
    {
        return std::unexpected(commits.error());
    }
    work_count += *commits;

    if (quorum_position >= append_position_)
    {
        work_count++;
        auto changed = state(ElectionState::leader_replay, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }

    return work_count;
}

std::expected<int, Error> Election::leader_replay(std::int64_t now_ns)
{
    // `Election.java:828-866`.
    int work_count = 0;

    if (nullptr == log_replay_)
    {
        if (log_position_ < append_position_)
        {
            auto replay = host_.new_log_replay(log_position_, append_position_);
            if (!replay)
            {
                return std::unexpected(replay.error());
            }
            log_replay_ = std::move(*replay);
        }
        else
        {
            auto changed = state(ElectionState::leader_init, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }

        work_count++;
        is_leader_startup_ = is_node_startup_;
        sync_this_member_log(leadership_term_id_, append_position_, now_ns, true);
    }
    else
    {
        work_count += log_replay_->do_work();
        if (log_replay_->is_done())
        {
            stop_replay();
            log_position_ = append_position_;
            auto changed = state(ElectionState::leader_init, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
    }

    const std::int64_t quorum_position = host_.quorum_position_bounded_by_leader_log(append_position_, now_ns);
    auto new_terms = publish_new_leadership_term_on_interval(quorum_position, now_ns);
    if (!new_terms)
    {
        return std::unexpected(new_terms.error());
    }
    work_count += *new_terms;

    auto commits = publish_commit_position_on_interval(quorum_position, now_ns);
    if (!commits)
    {
        return std::unexpected(commits.error());
    }
    work_count += *commits;

    return work_count;
}

std::expected<int, Error> Election::leader_init(std::int64_t now_ns)
{
    // `Election.java:868-875`.
    auto joined = host_.join_log_as_leader(leadership_term_id_, log_position_, log_session_id_, is_leader_startup_);
    if (!joined)
    {
        return std::unexpected(joined.error());
    }

    auto updated = update_recording_log(now_ns);
    if (!updated)
    {
        return std::unexpected(updated.error());
    }

    auto changed = state(ElectionState::leader_ready, now_ns, "");
    if (!changed)
    {
        return std::unexpected(changed.error());
    }
    return 1;
}

std::expected<int, Error> Election::leader_ready(std::int64_t now_ns)
{
    // `Election.java:877-895`.
    const std::int64_t quorum_position = host_.quorum_position_bounded_by_leader_log(append_position_, now_ns);
    auto updated = host_.update_leader_position(now_ns, append_position_, quorum_position);
    if (!updated)
    {
        return std::unexpected(updated.error());
    }
    int work_count = *updated;

    auto new_terms = publish_new_leadership_term_on_interval(quorum_position, now_ns);
    if (!new_terms)
    {
        return std::unexpected(new_terms.error());
    }
    work_count += *new_terms;

    if (common::has_quorum_at_position(cluster_members_, leadership_term_id_, log_position_, now_ns,
                                       context_.leader_heartbeat_timeout_ns))
    {
        auto appended = host_.append_new_leadership_term_event(now_ns);
        if (!appended)
        {
            return std::unexpected(appended.error());
        }
        if (*appended)
        {
            const std::int32_t leader_id = nullptr != leader_member_ ? leader_member_->id : NULL_MEMBER_ID;
            auto completed = host_.election_complete(leadership_term_id_, leader_id, log_position_, now_ns);
            if (!completed)
            {
                return std::unexpected(completed.error());
            }

            auto changed = state(ElectionState::closed, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
            work_count++;
        }
    }

    return work_count;
}

std::expected<int, Error> Election::follower_log_replication(std::int64_t now_ns)
{
    // `Election.java:897-958`.
    int work_count = 0;

    if (nullptr == log_replication_)
    {
        if (append_position_ < replication_stop_position_)
        {
            auto replication =
                host_.new_log_replication(leader_member_->archive_endpoint, leader_member_->archive_response_endpoint,
                                          leader_recording_id_, replication_stop_position_, now_ns);
            if (!replication)
            {
                return std::unexpected(replication.error());
            }
            log_replication_ = std::move(*replication);
            replication_deadline_ns_ = now_ns + context_.leader_heartbeat_timeout_ns;
            work_count++;
        }
        else
        {
            auto updated = update_recording_log_for_replication(replication_leadership_term_id_,
                                                                replication_term_base_log_position_,
                                                                replication_stop_position_, now_ns);
            if (!updated)
            {
                return std::unexpected(updated.error());
            }
            auto changed = state(ElectionState::canvass, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
    }
    else
    {
        auto polled = host_.poll_archive_events();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        work_count += *polled;
        auto replicated = log_replication_->poll(now_ns);
        if (!replicated)
        {
            return std::unexpected(replicated.error());
        }
        work_count += *replicated;
        const bool replication_done = log_replication_->has_replication_ended() && log_replication_->has_stopped();
        // Log replication runs concurrently; publishing after the completion
        // check makes the last position at the end of the leadership the one
        // that goes out as an `AppendPosition`.
        auto published = publish_follower_replication_position(now_ns);
        if (!published)
        {
            return std::unexpected(published.error());
        }
        work_count += *published;

        if (replication_done)
        {
            if (notified_commit_position_ >= append_position_)
            {
                append_position_ = log_replication_->position();
                stop_log_replication();
                auto updated = update_recording_log_for_replication(replication_leadership_term_id_,
                                                                    replication_term_base_log_position_,
                                                                    replication_stop_position_, now_ns);
                if (!updated)
                {
                    return std::unexpected(updated.error());
                }
                auto changed = state(ElectionState::canvass, now_ns, "");
                if (!changed)
                {
                    return std::unexpected(changed.error());
                }
                work_count++;
            }
            else if (now_ns >= replication_deadline_ns_)
            {
                return std::unexpected(make_error(ErrorCode::timed_out, "timeout awaiting commit position"));
            }
        }
    }

    return work_count;
}

std::expected<int, Error> Election::follower_replay(std::int64_t now_ns)
{
    // `Election.java:960-1018`.
    int work_count = 0;

    if (nullptr == log_replay_)
    {
        if (log_position_ < append_position_)
        {
            if (0 == notified_commit_position_)
            {
                auto published = publish_follower_append_position(now_ns);
                if (!published)
                {
                    return std::unexpected(published.error());
                }
                return *published;
            }

            if (log_position_ >= notified_commit_position_)
            {
                auto changed = state(ElectionState::canvass, now_ns,
                                     "log replay rejected: logPosition=" + std::to_string(log_position_) + " is " +
                                         (log_position_ > notified_commit_position_ ? "greater than" : "equal to") +
                                         " quorumPosition=" + std::to_string(notified_commit_position_));
                if (!changed)
                {
                    return std::unexpected(changed.error());
                }
                return work_count;
            }

            auto replay = host_.new_log_replay(log_position_, std::min(append_position_, notified_commit_position_));
            if (!replay)
            {
                return std::unexpected(replay.error());
            }
            log_replay_ = std::move(*replay);
            work_count++;
        }
        else
        {
            auto changed = state(NULL_POSITION != catchup_join_position_ ? ElectionState::follower_catchup_init
                                                                         : ElectionState::follower_log_init,
                                 now_ns, "nothing to replay");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
            work_count++;
        }
    }
    else
    {
        work_count += log_replay_->do_work();
        if (log_replay_->is_done())
        {
            log_position_ = log_replay_->position();
            stop_replay();

            if (log_position_ == append_position_)
            {
                auto changed = state(NULL_POSITION != catchup_join_position_ ? ElectionState::follower_catchup_init
                                                                             : ElectionState::follower_log_init,
                                     now_ns, "log replay done");
                if (!changed)
                {
                    return std::unexpected(changed.error());
                }
            }
            else
            {
                auto changed = state(ElectionState::canvass, now_ns,
                                     "incomplete log replay: logPosition=" + std::to_string(log_position_) +
                                         " appendPosition=" + std::to_string(append_position_));
                if (!changed)
                {
                    return std::unexpected(changed.error());
                }
            }
        }
    }

    return work_count;
}

std::expected<int, Error> Election::follower_catchup_init(std::int64_t now_ns)
{
    // `Election.java:1020-1056`.
    if (NULL_VALUE == log_subscription_registration_id_)
    {
        auto subscribed = add_follower_subscription();
        if (!subscribed)
        {
            return std::unexpected(subscribed.error());
        }
        auto destination = add_catchup_log_destination();
        if (!destination)
        {
            return std::unexpected(destination.error());
        }
    }

    std::optional<std::string> catchup_endpoint;
    const std::string& endpoint = this_member_.catchup_endpoint;
    if (ends_with_zero_port(endpoint))
    {
        auto resolved = host_.log_subscription_resolved_endpoint(log_subscription_registration_id_);
        if (resolved.has_value() && !resolved->empty())
        {
            const std::size_t colon = resolved->rfind(':');
            if (std::string::npos != colon)
            {
                catchup_endpoint = endpoint.substr(0, endpoint.size() - 2) + resolved->substr(colon);
            }
        }
    }
    else
    {
        catchup_endpoint = endpoint;
    }

    if (catchup_endpoint.has_value() && send_catchup_position(*catchup_endpoint))
    {
        time_of_last_update_ns_ = now_ns;
        auto initiated = host_.catchup_initiated(now_ns);
        if (!initiated)
        {
            return std::unexpected(initiated.error());
        }
        auto changed = state(ElectionState::follower_catchup_await, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }
    else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
    {
        return std::unexpected(make_error(ErrorCode::timed_out, "failed to send catchup position"));
    }

    return 1;
}

std::expected<int, Error> Election::follower_catchup_await(std::int64_t now_ns)
{
    // `Election.java:1058-1087`.
    int work_count = 0;

    auto image = host_.log_image_by_session(log_subscription_registration_id_, log_session_id_);
    if (image.has_value())
    {
        auto verified = verify_log_join_position("followerCatchupAwait", image->join_position);
        if (!verified)
        {
            return std::unexpected(verified.error());
        }

        auto joined = host_.try_join_log_as_follower(image->image, is_leader_startup_, now_ns);
        if (!joined)
        {
            return std::unexpected(joined.error());
        }
        if (*joined)
        {
            auto changed = state(ElectionState::follower_catchup, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
            work_count++;
        }
        else if (host_.log_subscription_channel_errored(log_subscription_registration_id_))
        {
            return std::unexpected(make_error(ErrorCode::internal_error, "failed to add catchup log as follower - " +
                                                                             std::string{host_.log_subscription_channel(
                                                                                 log_subscription_registration_id_)}));
        }
        else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
        {
            return std::unexpected(make_error(ErrorCode::timed_out, "failed to join catchup log as follower"));
        }
    }
    else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
    {
        return std::unexpected(make_error(ErrorCode::timed_out, "failed to join catchup log"));
    }

    return work_count;
}

std::expected<int, Error> Election::follower_catchup(std::int64_t now_ns)
{
    // `Election.java:1089-1113`.
    auto polled = host_.catchup_poll(notified_commit_position_, now_ns);
    if (!polled)
    {
        return std::unexpected(polled.error());
    }
    int work_count = *polled;

    if (host_.live_log_destination().empty() &&
        host_.is_catchup_near_live(std::max(catchup_join_position_, notified_commit_position_)))
    {
        auto added = add_live_log_destination();
        if (!added)
        {
            return std::unexpected(added.error());
        }
        work_count++;
    }

    const std::int64_t position = context_.commit_position();
    if (position >= catchup_join_position_ && position >= notified_commit_position_ &&
        host_.catchup_log_destination().empty() && ModuleState::snapshot != host_.state())
    {
        append_position_ = position;
        log_position_ = position;
        auto changed = state(ElectionState::follower_log_init, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
        work_count++;
    }

    return work_count;
}

std::expected<int, Error> Election::follower_log_init(std::int64_t now_ns)
{
    // `Election.java:1115-1132`.
    if (NULL_VALUE == log_subscription_registration_id_)
    {
        if (NULL_SESSION_ID != log_session_id_)
        {
            auto subscribed = add_follower_subscription();
            if (!subscribed)
            {
                return std::unexpected(subscribed.error());
            }
            auto added = add_live_log_destination();
            if (!added)
            {
                return std::unexpected(added.error());
            }
            auto changed = state(ElectionState::follower_log_await, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
        }
    }
    else
    {
        auto changed = state(ElectionState::follower_ready, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }

    return 1;
}

std::expected<int, Error> Election::follower_log_await(std::int64_t now_ns)
{
    // `Election.java:1134-1164`.
    int work_count = 0;

    auto image = host_.log_image_by_session(log_subscription_registration_id_, log_session_id_);
    if (image.has_value())
    {
        auto verified = verify_log_join_position("followerLogAwait", image->join_position);
        if (!verified)
        {
            return std::unexpected(verified.error());
        }

        auto joined = host_.try_join_log_as_follower(image->image, is_leader_startup_, now_ns);
        if (!joined)
        {
            return std::unexpected(joined.error());
        }
        if (*joined)
        {
            auto updated = update_recording_log(now_ns);
            if (!updated)
            {
                return std::unexpected(updated.error());
            }
            auto changed = state(ElectionState::follower_ready, now_ns, "");
            if (!changed)
            {
                return std::unexpected(changed.error());
            }
            work_count++;
        }
        else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
        {
            return std::unexpected(make_error(ErrorCode::timed_out, "failed to join live log as follower"));
        }
    }
    else if (host_.log_subscription_channel_errored(log_subscription_registration_id_))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "failed to add live log as follower - " +
                                                                         std::string{host_.log_subscription_channel(
                                                                             log_subscription_registration_id_)}));
    }
    else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
    {
        return std::unexpected(make_error(ErrorCode::timed_out, "failed to join live log"));
    }

    return work_count;
}

std::expected<int, Error> Election::follower_ready(std::int64_t now_ns)
{
    // `Election.java:1166-1180`.
    if (sender_.append_position(leader_member_->id, leadership_term_id_, log_position_, this_member_.id,
                                APPEND_POSITION_FLAG_NONE))
    {
        auto completed = host_.election_complete(leadership_term_id_, leader_member_->id, log_position_, now_ns);
        if (!completed)
        {
            return std::unexpected(completed.error());
        }
        auto changed = state(ElectionState::closed, now_ns, "");
        if (!changed)
        {
            return std::unexpected(changed.error());
        }
    }
    else if (now_ns >= (time_of_last_state_change_ns_ + context_.leader_heartbeat_timeout_ns))
    {
        return std::unexpected(make_error(ErrorCode::timed_out, "ready follower failed to notify leader"));
    }

    return 1;
}

std::expected<void, Error> Election::state(ElectionState new_state, std::int64_t now_ns, std::string_view reason)
{
    // `Election.java:1385-1438`.
    if (new_state == state_)
    {
        return {};
    }

    if (ElectionState::canvass == state_)
    {
        is_extended_canvass_ = false;
    }

    switch (new_state)
    {
        case ElectionState::canvass:
            reset_members();
            host_.set_role(Role::follower);
            break;

        case ElectionState::candidate_ballot:
            host_.set_role(Role::candidate);
            break;

        case ElectionState::leader_log_replication:
        {
            host_.set_role(Role::leader);
            auto added = host_.add_log_publication(append_position_);
            if (!added)
            {
                return std::unexpected(added.error());
            }
            log_session_id_ = *added;
            break;
        }

        case ElectionState::follower_log_replication:
        case ElectionState::follower_replay:
            host_.set_role(Role::follower);
            break;

        default:
            break;
    }

    log_state_change(state_, new_state, nullptr != leader_member_ ? leader_member_->id : NULL_MEMBER_ID, reason);

    state_ = new_state;
    context_.set_election_state(new_state);
    time_of_last_state_change_ns_ = now_ns;
    time_of_last_update_ns_ = initial_time_of_last_update_ns_;
    time_of_last_commit_position_update_ns_ = initial_time_of_last_update_ns_;
    return {};
}

// ---------------------------------------------------------------------------
// Leader and canvass helpers (`Election.java:1182-1383`).
// ---------------------------------------------------------------------------

std::expected<int, Error> Election::publish_new_leadership_term_on_interval(std::int64_t quorum_position,
                                                                            std::int64_t now_ns)
{
    // `Election.java:1198-1210`.
    if (!has_update_interval_expired(now_ns, context_.leader_heartbeat_interval_ns))
    {
        return 0;
    }

    time_of_last_update_ns_ = now_ns;
    publish_new_leadership_term(quorum_position, nanos_to_timestamp(now_ns));
    return 1;
}

std::expected<int, Error> Election::publish_commit_position_on_interval(std::int64_t quorum_position,
                                                                        std::int64_t now_ns)
{
    // `Election.java:1212-1230`. We would stop sending `commitPosition` if
    // `quorumPosition` regresses during the election; that is fine because the
    // NewLeadershipTerm keeps going out on its own interval.
    if (!(last_published_commit_position_ < quorum_position ||
          (last_published_commit_position_ == quorum_position &&
           has_interval_expired(now_ns, time_of_last_commit_position_update_ns_,
                                context_.leader_heartbeat_interval_ns))))
    {
        return 0;
    }

    time_of_last_commit_position_update_ns_ = now_ns;
    last_published_commit_position_ = quorum_position;
    // Use the election's `leadershipTermId`, i.e. match the value the
    // NewLeadershipTerm event carries.
    auto published = host_.publish_commit_position(quorum_position, leadership_term_id_);
    if (!published)
    {
        return std::unexpected(published.error());
    }
    return 1;
}

void Election::publish_new_leadership_term(std::int64_t quorum_position, std::int64_t timestamp)
{
    // `Election.java:1232-1258`.
    for (ClusterMember& member : cluster_members_)
    {
        publish_new_leadership_term(member, log_leadership_term_id_, quorum_position, timestamp);
    }
}

void Election::publish_new_leadership_term(ClusterMember& member, std::int64_t log_leadership_term_id,
                                           std::int64_t quorum_position, std::int64_t timestamp)
{
    // `Election.java:1260-1299`.
    if (member.id == this_member_.id || NULL_SESSION_ID == log_session_id_)
    {
        return;
    }

    const common::RecordingLog::Entry* log_next_term_entry =
        context_.recording_log->find_term_entry(log_leadership_term_id + 1);

    const std::int64_t next_leadership_term_id =
        nullptr != log_next_term_entry ? log_next_term_entry->leadership_term_id : leadership_term_id_;
    const std::int64_t next_term_base_log_position =
        nullptr != log_next_term_entry ? log_next_term_entry->term_base_log_position : append_position_;
    const std::int64_t next_log_position =
        nullptr != log_next_term_entry
            ? (NULL_POSITION != log_next_term_entry->log_position ? log_next_term_entry->log_position
                                                                  : append_position_)
            : NULL_POSITION;

    sender_.new_leadership_term(member.id, log_leadership_term_id, next_leadership_term_id, next_term_base_log_position,
                                next_log_position, leadership_term_id_, append_position_, append_position_,
                                quorum_position, host_.log_recording_id(), timestamp, this_member_.id, log_session_id_,
                                context_.app_version, is_leader_startup_);
}

void Election::place_vote(std::int64_t candidate_term_id, std::int32_t candidate_id, bool vote)
{
    // `Election.java:1182-1196`.
    ClusterMember* candidate = member_by_id(candidate_id);
    if (nullptr != candidate)
    {
        sender_.place_vote(candidate->id, candidate_term_id, log_leadership_term_id_, append_position_, candidate_id,
                           this_member_.id, vote);
    }
}

std::expected<void, Error> Election::publish_canvass_position()
{
    // `Election.java:1232-1250`.
    for (ClusterMember& member : cluster_members_)
    {
        if (member.id == this_member_.id)
        {
            continue;
        }

        if (!host_.member_has_publication(member.id))
        {
            // `ClusterMember.tryAddPublication` — an error here is the next
            // canvass interval's work, not this pass's.
            auto added = host_.try_add_member_publication(member.id);
            if (!added)
            {
                return std::unexpected(added.error());
            }
        }

        sender_.canvass_position(member.id, log_leadership_term_id_, append_position_, leadership_term_id_,
                                 this_member_.id);
    }
    return {};
}

std::expected<int, Error> Election::publish_follower_replication_position(std::int64_t now_ns)
{
    // `Election.java:1300-1316`.
    const std::int64_t position = log_replication_->position();
    if (position > append_position_ ||
        (position == append_position_ && has_update_interval_expired(now_ns, context_.leader_heartbeat_interval_ns)))
    {
        if (sender_.append_position(leader_member_->id, leadership_term_id_, position, this_member_.id,
                                    APPEND_POSITION_FLAG_NONE))
        {
            append_position_ = position;
            time_of_last_update_ns_ = now_ns;
            return 1;
        }
    }
    return 0;
}

std::expected<int, Error> Election::publish_follower_append_position(std::int64_t now_ns)
{
    // `Election.java:1318-1336`.
    if (last_published_append_position_ != append_position_ ||
        has_update_interval_expired(now_ns, context_.leader_heartbeat_interval_ns))
    {
        if (sender_.append_position(leader_member_->id, leadership_term_id_, append_position_, this_member_.id,
                                    APPEND_POSITION_FLAG_NONE))
        {
            last_published_append_position_ = append_position_;
            time_of_last_update_ns_ = now_ns;
            return 1;
        }
    }
    return 0;
}

bool Election::send_catchup_position(std::string_view catchup_endpoint)
{
    // `Election.java:1338-1343`.
    return sender_.catchup_position(leader_member_->id, leadership_term_id_, log_position_, this_member_.id,
                                    catchup_endpoint);
}

std::expected<void, Error> Election::add_catchup_log_destination()
{
    // `Election.java:1345-1349`.
    const std::string destination = common::create_destination_uri(context_.log_channel, this_member_.catchup_endpoint);
    auto added = host_.add_log_destination(log_subscription_registration_id_, destination);
    if (!added)
    {
        return std::unexpected(added.error());
    }
    return host_.set_catchup_log_destination(destination);
}

std::expected<void, Error> Election::add_live_log_destination()
{
    // `Election.java:1351-1364`.
    const std::string destination =
        context_.log_is_mdc ? common::create_destination_uri(context_.log_channel, this_member_.log_endpoint)
                            : context_.log_channel;
    auto added = host_.add_log_destination(log_subscription_registration_id_, destination);
    if (!added)
    {
        return std::unexpected(added.error());
    }
    return host_.set_live_log_destination(destination);
}

std::expected<void, Error> Election::add_follower_subscription()
{
    // `Election.java:1366-1383`.
    auto added = host_.add_log_subscription(log_session_id_);
    if (!added)
    {
        return std::unexpected(added.error());
    }
    log_subscription_registration_id_ = *added;
    return {};
}

// ---------------------------------------------------------------------------
// INIT and recording helpers (`Election.java:1400-1609`).
// ---------------------------------------------------------------------------

std::expected<void, Error> Election::prepare_for_new_leadership(std::int64_t now_ns)
{
    // `Election.java:1589-1596`.
    auto last_append_position = host_.prepare_for_new_leadership(log_position_, now_ns);
    if (!last_append_position)
    {
        return std::unexpected(last_append_position.error());
    }
    if (NULL_POSITION != *last_append_position)
    {
        append_position_ = *last_append_position;
    }
    return {};
}

std::expected<void, Error> Election::stop_catchup()
{
    // `Election.java:1440-1444`.
    auto stopped = host_.stop_all_catchups();
    if (!stopped)
    {
        return std::unexpected(stopped.error());
    }
    catchup_join_position_ = NULL_POSITION;
    return {};
}

void Election::stop_log_replication() noexcept
{
    // `Election.java:1460-1469`.
    if (nullptr != log_replication_)
    {
        log_replication_->close();
        log_replication_.reset();
    }
    replication_deadline_ns_ = 0;
    last_published_commit_position_ = 0;
}

void Election::stop_replay() noexcept
{
    // `Election.java:1453-1461`.
    if (nullptr != log_replay_)
    {
        log_replay_->close();
        log_replay_.reset();
    }
    last_published_append_position_ = 0;
}

void Election::sync_this_member_log(std::int64_t leadership_term_id, std::int64_t log_position, std::int64_t time_ns,
                                    bool set_time) noexcept
{
    this_member_.leadership_term_id = leadership_term_id;
    this_member_.log_position = log_position;
    if (set_time)
    {
        this_member_.time_of_last_append_position_ns = time_ns;
    }
    common::ClusterMember* self = common::find_member(cluster_members_, this_member_.id);
    if (nullptr != self)
    {
        self->leadership_term_id = leadership_term_id;
        self->log_position = log_position;
        if (set_time)
        {
            self->time_of_last_append_position_ns = time_ns;
        }
    }
}

void Election::reset_members() noexcept
{
    // `Election.java:1446-1451`.
    common::reset_members(cluster_members_);
    sync_this_member_log(leadership_term_id_, append_position_, 0, false);
    leader_member_ = nullptr;
}

std::expected<void, Error> Election::update_recording_log(std::int64_t now_ns)
{
    // `Election.java:1502-1511`.
    auto ensured = ensure_recording_log_coherent(leadership_term_id_, log_position_, NULL_VALUE, now_ns);
    if (!ensured)
    {
        return std::unexpected(ensured.error());
    }
    log_leadership_term_id_ = leadership_term_id_;
    return {};
}

std::expected<void, Error> Election::update_recording_log_for_replication(std::int64_t leadership_term_id,
                                                                          std::int64_t term_base_log_position,
                                                                          std::int64_t log_position,
                                                                          std::int64_t now_ns)
{
    // `Election.java:1513-1521`.
    auto ensured = ensure_recording_log_coherent(leadership_term_id, term_base_log_position, log_position, now_ns);
    if (!ensured)
    {
        return std::unexpected(ensured.error());
    }
    log_leadership_term_id_ = leadership_term_id;
    return {};
}

std::expected<void, Error> Election::ensure_recording_log_coherent(std::int64_t leadership_term_id,
                                                                   std::int64_t term_base_log_position,
                                                                   std::int64_t log_position, std::int64_t now_ns)
{
    // `Election.java:1470-1481`.
    return ensure_recording_log_coherent(context_, host_.log_recording_id(), initial_log_leadership_term_id_,
                                         initial_term_base_log_position_, leadership_term_id, term_base_log_position,
                                         log_position, now_ns);
}

std::expected<void, Error> Election::ensure_recording_log_coherent(
    const ElectionContext& context, std::int64_t recording_id, std::int64_t initial_log_leadership_term_id,
    std::int64_t initial_term_base_log_position, std::int64_t leadership_term_id, std::int64_t term_base_log_position,
    std::int64_t log_position, std::int64_t now_ns)
{
    // `Election.java:1483-1512`.
    if (NULL_VALUE == recording_id)
    {
        // This can happen during a log replication if the initial
        // appendPosition != 0 and nextTermLogPosition == appendPosition.
        return {};
    }

    const std::int64_t timestamp = context.timestamp_from_nanos(now_ns);
    return context.recording_log->ensure_coherent(
        recording_id, initial_log_leadership_term_id, initial_term_base_log_position, leadership_term_id,
        NULL_VALUE != term_base_log_position ? term_base_log_position : initial_term_base_log_position, log_position,
        now_ns, timestamp, context.file_sync_level);
}

std::expected<void, Error> Election::verify_log_join_position(std::string_view state, std::int64_t join_position) const
{
    // `Election.java:1523-1538`.
    if (join_position != log_position_)
    {
        const std::string_view inequality = join_position < log_position_ ? " less " : " greater ";
        return std::unexpected(make_error(ErrorCode::internal_error,
                                          std::string{state} + " - joinPosition=" + std::to_string(join_position) +
                                              std::string{inequality} +
                                              "than logPosition=" + std::to_string(log_position_)));
    }
    return {};
}

void Election::log_state_change(ElectionState old_state, ElectionState new_state, std::int32_t leader_id,
                                std::string_view reason) const
{
    // `Election.java:1540-1570`: `ClusterTracing.traceElectionStateChange`.
    ElectionStateChange change;
    change.member_id = this_member_.id;
    change.old_state = old_state;
    change.new_state = new_state;
    change.leader_id = leader_id;
    change.candidate_term_id = candidate_term_id_;
    change.leadership_term_id = leadership_term_id_;
    change.log_position = log_position_;
    change.log_leadership_term_id = log_leadership_term_id_;
    change.append_position = append_position_;
    change.catchup_position = catchup_join_position_;
    change.reason = reason;
    context_.trace_state_change(change);
}

bool Election::has_update_interval_expired(std::int64_t now_ns, std::int64_t interval_ns) const noexcept
{
    // `Election.java:1567-1570`.
    return has_interval_expired(now_ns, time_of_last_update_ns_, interval_ns);
}

bool Election::has_interval_expired(std::int64_t now_ns, std::int64_t previous_ns, std::int64_t interval_ns) noexcept
{
    return (now_ns - previous_ns) >= interval_ns;
}

}  // namespace deepmsg::cluster::election
