#include "cluster/election/election.h"

#include <algorithm>
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

/// `ElectionState.name()` for traces and messages.
[[nodiscard]] std::string_view state_name(ElectionState state) noexcept
{
    return election_state_name(state);
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

    const std::int64_t now_ns = context_.now_ns();
    initial_time_of_last_update_ns_ = now_ns - ONE_DAY_NS;
    time_of_last_update_ns_ = initial_time_of_last_update_ns_;
    time_of_last_commit_position_update_ns_ = initial_time_of_last_update_ns_;

    context_.set_election_state(ElectionState::init);
    context_.increment_election_counter();
}

std::expected<int, Error> Election::do_work(std::int64_t now_ns)
{
    // `Election.java:180-257`. The states C5b has not transcribed yet answer
    // with a loud error: a multi-member cluster cannot be elected by C5a, and
    // pretending otherwise would hide that.
    switch (state_)
    {
        case ElectionState::init:
            return init(now_ns);
        case ElectionState::leader_log_replication:
            return leader_log_replication(now_ns);
        case ElectionState::leader_replay:
            return leader_replay(now_ns);
        case ElectionState::leader_init:
            return leader_init(now_ns);
        case ElectionState::leader_ready:
            return leader_ready(now_ns);
        case ElectionState::closed:
            return 0;

        case ElectionState::canvass:
        case ElectionState::nominate:
        case ElectionState::candidate_ballot:
        case ElectionState::follower_ballot:
        case ElectionState::follower_log_replication:
        case ElectionState::follower_replay:
        case ElectionState::follower_catchup_init:
        case ElectionState::follower_catchup_await:
        case ElectionState::follower_catchup:
        case ElectionState::follower_log_init:
        case ElectionState::follower_log_await:
        case ElectionState::follower_ready:
            break;
    }

    return std::unexpected(
        make_error(ErrorCode::internal_error, std::string{state_name(state_)} + " is not transcribed yet (C5b)"));
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

    log_state_change(state_, new_state, nullptr != leader_member_ ? leader_member_->id : -1, reason);

    state_ = new_state;
    context_.set_election_state(new_state);
    time_of_last_state_change_ns_ = now_ns;
    time_of_last_update_ns_ = initial_time_of_last_update_ns_;
    time_of_last_commit_position_update_ns_ = initial_time_of_last_update_ns_;
    return {};
}

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

std::expected<int, Error> Election::leader_log_replication(std::int64_t now_ns)
{
    // `Election.java:809-826`.
    int work_count = 0;

    this_member_.log_position = append_position_;
    this_member_.time_of_last_append_position_ns = now_ns;

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
        this_member_.leadership_term_id = leadership_term_id_;
        this_member_.log_position = append_position_;
        this_member_.time_of_last_append_position_ns = now_ns;
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
            const std::int32_t leader_id = nullptr != leader_member_ ? leader_member_->id : -1;
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

void Election::reset_members() noexcept
{
    // `Election.java:1446-1451`.
    common::reset_members(cluster_members_);
    this_member_.leadership_term_id = leadership_term_id_;
    this_member_.log_position = append_position_;
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

void Election::log_state_change(ElectionState old_state, ElectionState new_state, std::int32_t leader_id,
                                std::string_view reason) const
{
    // `Election.java:1540-1570`: `ClusterTracing.traceElectionStateChange`.
    // The trace is a hook, so the election does not depend on the CFB tracing
    // layer.
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
