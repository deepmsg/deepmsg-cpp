#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>

#include "cluster/common/cluster_member.h"
#include "cluster/election/election_host.h"
#include "cluster/election/election_state.h"
#include "core/error/error.h"

namespace deepmsg::cluster::election
{

using axc::core::Error;

/// `Election` (`Election.java:95-1649`): the 18-state machine that settles the
/// leadership term and catches the log up.
///
/// All 18 states and the eight inbound handlers are transcribed (C5a carried
/// `INIT`, the single-member short circuit and the `LEADER_*` chain; C5b the
/// rest).
///
/// Transcribed deviations, all from `election_host.h`'s:
/// * the member table is referenced, not copied; publications are named by
///   member id and live behind the host;
/// * every reference `throw` is an `std::expected` error, and a `ClusterEvent`
///   is `ErrorCode::internal_error` carrying the reference's message — the
///   reference's `handleError` seen from the CM's side;
/// * the log subscription is a registration id the host owns, not an object.
class Election
{
public:
    Election(bool is_node_startup, std::int32_t graceful_closed_leader_id, std::int64_t leadership_term_id,
             std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t append_position,
             common::ClusterMembers& cluster_members, common::ClusterMember& this_member, ConsensusSender& sender,
             ElectionHost& host, ElectionContext context);

    Election(const Election&) = delete;
    Election& operator=(const Election&) = delete;
    Election(Election&&) = delete;
    Election& operator=(Election&&) = delete;
    ~Election() = default;

    /// `doWork(long)` (`Election.java:180-257`).
    [[nodiscard]] std::expected<int, Error> do_work(std::int64_t now_ns);

    /// `handleError(long, Throwable)` (`Election.java:259-267`): counted, then
    /// back to `INIT` with the commit position as the log position.
    [[nodiscard]] std::expected<void, Error> handle_error(std::int64_t now_ns, const Error& error);

    /// `onRecordingSignal(...)` (`Election.java:269-282`).
    void on_recording_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                             std::int32_t signal);

    // ---- the eight inbound handlers (`Election.java:284-646`) -----------

    /// `onCanvassPosition(...)` (`Election.java:284-332`).
    [[nodiscard]] std::expected<void, Error> on_canvass_position(std::int64_t log_leadership_term_id,
                                                                 std::int64_t log_position,
                                                                 std::int64_t leadership_term_id,
                                                                 std::int32_t follower_member_id,
                                                                 std::int32_t protocol_version);

    /// `onRequestVote(...)` (`Election.java:334-381`).
    [[nodiscard]] std::expected<void, Error> on_request_vote(std::int64_t log_leadership_term_id,
                                                             std::int64_t log_position, std::int64_t candidate_term_id,
                                                             std::int32_t candidate_id, std::int32_t protocol_version);

    /// `onVote(...)` (`Election.java:383-410`).
    [[nodiscard]] std::expected<void, Error> on_vote(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                                     std::int64_t candidate_term_id, std::int32_t candidate_id,
                                                     std::int32_t voter_id, bool vote);

    /// `onNewLeadershipTerm(...)` (`Election.java:412-533`).
    // NOLINTNEXTLINE(readability-function-size): the wire message has thirteen fields
    [[nodiscard]] std::expected<void, Error> on_new_leadership_term(
        std::int64_t log_leadership_term_id, std::int64_t next_leadership_term_id,
        std::int64_t next_term_base_log_position, std::int64_t next_log_position, std::int64_t leadership_term_id,
        std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t commit_position,
        std::int64_t leader_recording_id, std::int64_t timestamp, std::int32_t leader_member_id,
        std::int32_t log_session_id, bool is_startup);

    /// `onAppendPosition(...)` (`Election.java:535-555`).
    [[nodiscard]] std::expected<void, Error> on_append_position(std::int64_t leadership_term_id,
                                                                std::int64_t log_position,
                                                                std::int32_t follower_member_id, std::int16_t flags);

    /// `onCommitPosition(...)` (`Election.java:557-588`).
    [[nodiscard]] std::expected<void, Error> on_commit_position(std::int64_t leadership_term_id,
                                                                std::int64_t log_position,
                                                                std::int32_t leader_member_id);

    /// `onReplayNewLeadershipTermEvent(...)` (`Election.java:590-608`).
    [[nodiscard]] std::expected<void, Error> on_replay_new_leadership_term_event(std::int64_t leadership_term_id,
                                                                                 std::int64_t log_position,
                                                                                 std::int64_t timestamp,
                                                                                 std::int64_t term_base_log_position);

    /// `onTruncateLogEntry(...)` (`Election.java:610-646`). Always ends in a
    /// `ClusterEvent` in the reference — the error *is* the return value here.
    [[nodiscard]] std::expected<void, Error> on_truncate_log_entry(
        std::int32_t member_id, ElectionState state, std::int64_t log_leadership_term_id,
        std::int64_t leadership_term_id, std::int64_t candidate_term_id, std::int64_t commit_position,
        std::int64_t log_position, std::int64_t append_position, std::int64_t old_position, std::int64_t new_position);

    // ---- accessors the tests and the CM's completion path read ----------

    [[nodiscard]] ElectionState state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] common::ClusterMember* leader() const noexcept
    {
        return leader_member_;
    }

    [[nodiscard]] std::int32_t this_member_id() const noexcept
    {
        return this_member_.id;
    }

    [[nodiscard]] std::int32_t log_session_id() const noexcept
    {
        return log_session_id_;
    }

    [[nodiscard]] std::int64_t leadership_term_id() const noexcept
    {
        return leadership_term_id_;
    }

    [[nodiscard]] std::int64_t log_position() const noexcept
    {
        return log_position_;
    }

    [[nodiscard]] bool is_leader_startup() const noexcept
    {
        return is_leader_startup_;
    }

    /// `notifiedCommitPosition()` (`Election.java:645-650`).
    [[nodiscard]] std::int64_t notified_commit_position() const noexcept
    {
        return notified_commit_position_;
    }

    /// `Election.ensureRecordingLogCoherent(...)` (`Election.java:1470-1512`),
    /// package-private in the reference and reached by its test directly.
    [[nodiscard]] static std::expected<void, Error> ensure_recording_log_coherent(
        const ElectionContext& context, std::int64_t recording_id, std::int64_t initial_log_leadership_term_id,
        std::int64_t initial_term_base_log_position, std::int64_t leadership_term_id,
        std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t now_ns);

private:
    // ---- the states (`Election.java:653-1180`) --------------------------
    [[nodiscard]] std::expected<int, Error> init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> canvass(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> nominate(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> candidate_ballot(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_ballot(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_log_replication(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_replay(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_ready(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_log_replication(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_replay(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_catchup_init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_catchup_await(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_catchup(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_log_init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_log_await(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> follower_ready(std::int64_t now_ns);

    // ---- state() and its side effects (`Election.java:1385-1438`) ------
    [[nodiscard]] std::expected<void, Error> state(ElectionState new_state, std::int64_t now_ns,
                                                   std::string_view reason);

    // ---- leader helpers (`Election.java:1190-1299`) --------------------
    [[nodiscard]] std::expected<int, Error> publish_new_leadership_term_on_interval(std::int64_t quorum_position,
                                                                                    std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> publish_commit_position_on_interval(std::int64_t quorum_position,
                                                                                std::int64_t now_ns);
    void publish_new_leadership_term(common::ClusterMember& member, std::int64_t log_leadership_term_id,
                                     std::int64_t quorum_position, std::int64_t timestamp);
    void publish_new_leadership_term(std::int64_t quorum_position, std::int64_t timestamp);
    void place_vote(std::int64_t candidate_term_id, std::int32_t candidate_id, bool vote);

    // ---- canvass / follower helpers (`Election.java:1232-1383`) --------
    [[nodiscard]] std::expected<void, Error> publish_canvass_position();
    [[nodiscard]] std::expected<int, Error> publish_follower_replication_position(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> publish_follower_append_position(std::int64_t now_ns);
    [[nodiscard]] bool send_catchup_position(std::string_view catchup_endpoint);
    [[nodiscard]] std::expected<void, Error> add_catchup_log_destination();
    [[nodiscard]] std::expected<void, Error> add_live_log_destination();
    [[nodiscard]] std::expected<void, Error> add_follower_subscription();

    // ---- INIT / recording helpers ---------------------------------------
    [[nodiscard]] std::expected<void, Error> prepare_for_new_leadership(std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> stop_catchup();
    void stop_log_replication() noexcept;
    void stop_replay() noexcept;
    void reset_members() noexcept;
    /// The C++ `this_member_` is a copy, while the reference's array element is
    /// the same object; every write must go to both.
    void sync_this_member_log(std::int64_t leadership_term_id, std::int64_t log_position, std::int64_t time_ns,
                              bool set_time) noexcept;
    [[nodiscard]] std::expected<void, Error> update_recording_log(std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> update_recording_log_for_replication(std::int64_t leadership_term_id,
                                                                                  std::int64_t term_base_log_position,
                                                                                  std::int64_t log_position,
                                                                                  std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> ensure_recording_log_coherent(std::int64_t leadership_term_id,
                                                                           std::int64_t term_base_log_position,
                                                                           std::int64_t log_position,
                                                                           std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> verify_log_join_position(std::string_view state,
                                                                      std::int64_t join_position) const;
    void log_state_change(ElectionState old_state, ElectionState new_state, std::int32_t leader_id,
                          std::string_view reason) const;

    [[nodiscard]] common::ClusterMember* member_by_id(std::int32_t member_id) noexcept;
    [[nodiscard]] bool has_update_interval_expired(std::int64_t now_ns, std::int64_t interval_ns) const noexcept;
    [[nodiscard]] static bool has_interval_expired(std::int64_t now_ns, std::int64_t previous_ns,
                                                   std::int64_t interval_ns) noexcept;
    [[nodiscard]] std::int64_t nanos_to_timestamp(std::int64_t now_ns) const noexcept
    {
        return context_.timestamp_from_nanos(now_ns);
    }
    [[nodiscard]] std::int64_t timestamp_to_nanos(std::int64_t timestamp) const noexcept;
    [[nodiscard]] std::int64_t now_ns() const noexcept
    {
        return context_.now_ns();
    }

    // ---- the reference's fields (`Election.java:97-150`) -----------------
    common::ClusterMembers& cluster_members_;
    common::ClusterMember& this_member_;
    ConsensusSender& sender_;
    ElectionHost& host_;
    ElectionContext context_;

    std::int64_t initial_log_leadership_term_id_{NULL_VALUE};
    std::int64_t initial_time_of_last_update_ns_{0};
    std::int64_t initial_term_base_log_position_{NULL_POSITION};
    bool is_node_startup_{false};

    std::int64_t time_of_last_state_change_ns_{0};
    std::int64_t time_of_last_update_ns_{0};
    std::int64_t time_of_last_commit_position_update_ns_{0};
    std::int64_t nomination_deadline_ns_{0};
    std::int64_t log_position_{NULL_POSITION};
    std::int64_t append_position_{NULL_POSITION};
    std::int64_t notified_commit_position_{0};
    std::int64_t catchup_join_position_{NULL_POSITION};
    std::int64_t replication_leadership_term_id_{NULL_VALUE};
    std::int64_t replication_stop_position_{NULL_POSITION};
    std::int64_t replication_deadline_ns_{0};
    std::int64_t replication_term_base_log_position_{0};
    std::int64_t leader_recording_id_{NULL_VALUE};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::int64_t log_leadership_term_id_{NULL_VALUE};
    std::int64_t candidate_term_id_{NULL_VALUE};
    std::int64_t last_published_commit_position_{0};
    std::int64_t last_published_append_position_{0};
    std::int32_t log_session_id_{NULL_SESSION_ID};
    std::int32_t graceful_closed_leader_id_{NULL_VALUE};
    bool is_first_init_{true};
    bool is_leader_startup_{false};
    bool is_extended_canvass_{false};
    common::ClusterMember* leader_member_{nullptr};
    ElectionState state_{ElectionState::init};
    /// The follower's log subscription (`logSubscription`), held by the host.
    std::int64_t log_subscription_registration_id_{NULL_VALUE};
    std::unique_ptr<LogReplay> log_replay_{};
    std::unique_ptr<RecordingReplication> log_replication_{};
};

}  // namespace deepmsg::cluster::election
