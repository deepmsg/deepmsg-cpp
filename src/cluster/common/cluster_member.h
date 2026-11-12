#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/error/error.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

/// A cluster member and its election bookkeeping.
///
/// Transcribed from `ClusterMember.java` (Aeron 1.54.0). The eight endpoint
/// attributes are parsed; only the seventh (`archiveResponseEndpoint`) has
/// consumers in the reference (`Election.java:907`,
/// `ClusterBackupAgent.java:583-588`). The eighth (`egressResponseEndpoint`)
/// has none: parse, keep, and do not use.
struct ClusterMember
{
    static constexpr std::int64_t NULL_VALUE = -1;
    static constexpr std::int64_t NULL_POSITION = -1;

    std::int32_t id{static_cast<std::int32_t>(NULL_VALUE)};
    std::string ingress_endpoint{};
    std::string consensus_endpoint{};
    std::string log_endpoint{};
    std::string catchup_endpoint{};
    std::string archive_endpoint{};
    std::optional<std::string> archive_response_endpoint{};
    std::optional<std::string> egress_response_endpoint{};
    std::string endpoints{};

    bool is_ballot_sent{false};
    bool is_leader{false};
    bool has_terminated{false};
    /// `null` = no vote received (`Boolean` in the reference).
    std::optional<bool> vote{};
    std::int64_t candidate_term_id{NULL_VALUE};
    std::int64_t leadership_term_id{NULL_VALUE};
    std::int64_t log_position{NULL_POSITION};
    std::int64_t catchup_replay_session_id{NULL_VALUE};
    std::int64_t catchup_replay_correlation_id{NULL_VALUE};
    std::int64_t change_correlation_id{NULL_VALUE};
    std::int64_t time_of_last_append_position_ns{NULL_VALUE};

    /// `ClusterMember.reset()`: state only, endpoints unchanged.
    void reset() noexcept;

    /// `isActive`: `timeOfLastAppendPositionNs + timeoutNs > nowNs`.
    [[nodiscard]] bool is_active(std::int64_t now_ns, std::int64_t timeout_ns) const noexcept;

    /// `hasReachedPosition` (`ClusterMember.java`).
    [[nodiscard]] bool has_reached_position(std::int64_t leadership_term_id, std::int64_t position, std::int64_t now_ns,
                                            std::int64_t timeout_ns) const noexcept;

    /// `willVoteFor`: deliberately does not check activity.
    [[nodiscard]] bool will_vote_for(const ClusterMember& candidate) const noexcept;
};

using ClusterMembers = std::vector<ClusterMember>;

/// `ClusterMember.parse` with 6–8 comma-separated attributes per member.
[[nodiscard]] std::expected<ClusterMembers, Error> parse_cluster_members(std::string_view value);

/// `ClusterMember.parseEndpoints`: exactly 5 attributes.
[[nodiscard]] std::expected<ClusterMember, Error> parse_member_endpoints(std::int32_t id, std::string_view endpoints);

/// `ClusterMember.encodeAsString`.
[[nodiscard]] std::string encode_cluster_members(std::span<const ClusterMember> members);

[[nodiscard]] ClusterMember* find_member(ClusterMembers& members, std::int32_t member_id) noexcept;
[[nodiscard]] const ClusterMember* find_member(const ClusterMembers& members, std::int32_t member_id) noexcept;

/// `ClusterMember.determineMember`: resolve this node from `aeron.cluster.members`
/// (or from `aeron.cluster.member.endpoints` when there is no membership).
[[nodiscard]] std::expected<ClusterMember, Error> determine_member(const ClusterMembers& members,
                                                                   std::int32_t member_id,
                                                                   std::string_view member_endpoints);

[[nodiscard]] bool are_same_endpoints(const ClusterMember& lhs, const ClusterMember& rhs) noexcept;
[[nodiscard]] std::expected<void, Error> validate_member_endpoints(const ClusterMember& member,
                                                                   std::string_view member_endpoints);

[[nodiscard]] std::string ingress_endpoints(std::span<const ClusterMember> members);

[[nodiscard]] constexpr std::int32_t quorum_threshold(std::int32_t member_count) noexcept
{
    return (member_count >> 1) + 1;
}

/// Insertion-ranked quorum position; `ranked_positions` must be sized to
/// `quorum_threshold(members.size())` and is zeroed by the call.
[[nodiscard]] std::int64_t quorum_position(std::span<const ClusterMember> members,
                                           std::span<std::int64_t> ranked_positions, std::int64_t now_ns,
                                           std::int64_t timeout_ns) noexcept;

[[nodiscard]] bool has_active_quorum(std::span<const ClusterMember> members, std::int64_t now_ns,
                                     std::int64_t timeout_ns) noexcept;

[[nodiscard]] bool has_quorum_at_position(std::span<const ClusterMember> members, std::int64_t leadership_term_id,
                                          std::int64_t position, std::int64_t now_ns, std::int64_t timeout_ns) noexcept;

[[nodiscard]] bool is_quorum_leader(std::span<const ClusterMember> members, std::int64_t candidate_term_id) noexcept;
[[nodiscard]] bool is_unanimous_leader(std::span<const ClusterMember> members, std::int64_t candidate_term_id,
                                       std::int32_t graceful_closed_leader_id) noexcept;
[[nodiscard]] bool is_quorum_candidate(std::span<const ClusterMember> members, const ClusterMember& candidate) noexcept;
[[nodiscard]] bool is_unanimous_candidate(std::span<const ClusterMember> members, const ClusterMember& candidate,
                                          std::int32_t graceful_closed_leader_id) noexcept;

[[nodiscard]] std::int32_t compare_log(std::int64_t lhs_log_leadership_term_id, std::int64_t lhs_log_position,
                                       std::int64_t rhs_log_leadership_term_id, std::int64_t rhs_log_position) noexcept;
[[nodiscard]] std::int32_t compare_log(const ClusterMember& lhs, const ClusterMember& rhs) noexcept;

void reset_members(ClusterMembers& members) noexcept;
void become_candidate(ClusterMembers& members, std::int64_t candidate_term_id,
                      std::int32_t candidate_member_id) noexcept;
void set_is_leader(ClusterMembers& members, std::int32_t leader_member_id) noexcept;

/// Java `String.split(delimiter)`: drops trailing empty fields (`""` -> no
/// fields, `"a,"` -> `{"a"}`).
[[nodiscard]] std::vector<std::string> split_java(std::string_view value, char delimiter);

}  // namespace deepmsg::cluster::common
