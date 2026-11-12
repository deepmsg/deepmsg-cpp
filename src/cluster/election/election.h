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
/// **C5a scope** (the card): the interface above is frozen, and `INIT`, the
/// single-member short circuit and the `LEADER_*` states are transcribed. Every
/// other state is a `not_implemented` error until C5b — it fails loudly rather
/// than pretending to elect.
///
/// Transcribed deviations, all from `election_host.h`'s:
/// * the member table is the host's and is referenced, not copied;
/// * `ClusterMember` is named by reference, publications by member id;
/// * every reference `throw` is an `std::expected` error, and a `ClusterEvent`
///   is `ErrorCode::internal_error` with the reference's message until C5b
///   needs to distinguish it.
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
    // ---- the states C5a transcribes -------------------------------------
    [[nodiscard]] std::expected<int, Error> init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_log_replication(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_replay(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_init(std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> leader_ready(std::int64_t now_ns);

    // ---- state() and its side effects (`Election.java:1385-1438`) ------
    [[nodiscard]] std::expected<void, Error> state(ElectionState new_state, std::int64_t now_ns,
                                                   std::string_view reason);

    // ---- leader helpers ---------------------------------------------------
    [[nodiscard]] std::expected<int, Error> publish_new_leadership_term_on_interval(std::int64_t quorum_position,
                                                                                    std::int64_t now_ns);
    [[nodiscard]] std::expected<int, Error> publish_commit_position_on_interval(std::int64_t quorum_position,
                                                                                std::int64_t now_ns);
    void publish_new_leadership_term(common::ClusterMember& member, std::int64_t log_leadership_term_id,
                                     std::int64_t quorum_position, std::int64_t timestamp);
    void publish_new_leadership_term(std::int64_t quorum_position, std::int64_t timestamp);

    // ---- INIT helpers -----------------------------------------------------
    [[nodiscard]] std::expected<void, Error> prepare_for_new_leadership(std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> stop_catchup();
    void stop_log_replication() noexcept;
    void stop_replay() noexcept;
    void reset_members() noexcept;
    [[nodiscard]] std::expected<void, Error> update_recording_log(std::int64_t now_ns);
    [[nodiscard]] std::expected<void, Error> ensure_recording_log_coherent(std::int64_t leadership_term_id,
                                                                           std::int64_t term_base_log_position,
                                                                           std::int64_t log_position,
                                                                           std::int64_t now_ns);
    void log_state_change(ElectionState old_state, ElectionState new_state, std::int32_t leader_id,
                          std::string_view reason) const;

    [[nodiscard]] bool has_update_interval_expired(std::int64_t now_ns, std::int64_t interval_ns) const noexcept;
    [[nodiscard]] static bool has_interval_expired(std::int64_t now_ns, std::int64_t previous_ns,
                                                   std::int64_t interval_ns) noexcept;
    [[nodiscard]] std::int64_t nanos_to_timestamp(std::int64_t now_ns) const noexcept
    {
        return context_.timestamp_from_nanos(now_ns);
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
    /// C5b's ballot and follower states. `[[maybe_unused]]` until then: the
    /// reference has them from the constructor on, but an unread field is a
    /// gate error here.
    [[maybe_unused]] std::int64_t nomination_deadline_ns_{0};
    std::int64_t log_position_{NULL_POSITION};
    std::int64_t append_position_{NULL_POSITION};
    std::int64_t notified_commit_position_{0};
    std::int64_t catchup_join_position_{NULL_POSITION};
    [[maybe_unused]] std::int64_t replication_leadership_term_id_{NULL_VALUE};
    [[maybe_unused]] std::int64_t replication_stop_position_{NULL_POSITION};
    std::int64_t replication_deadline_ns_{0};
    [[maybe_unused]] std::int64_t replication_term_base_log_position_{0};
    [[maybe_unused]] std::int64_t leader_recording_id_{NULL_VALUE};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::int64_t log_leadership_term_id_{NULL_VALUE};
    std::int64_t candidate_term_id_{NULL_VALUE};
    std::int64_t last_published_commit_position_{0};
    std::int64_t last_published_append_position_{0};
    std::int32_t log_session_id_{NULL_SESSION_ID};
    [[maybe_unused]] std::int32_t graceful_closed_leader_id_{NULL_VALUE};
    bool is_first_init_{true};
    bool is_leader_startup_{false};
    bool is_extended_canvass_{false};
    common::ClusterMember* leader_member_{nullptr};
    ElectionState state_{ElectionState::init};
    std::unique_ptr<LogReplay> log_replay_{};
    std::unique_ptr<RecordingReplication> log_replication_{};
};

}  // namespace deepmsg::cluster::election
