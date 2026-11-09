#include "cluster/common/cluster_member.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <string>
#include <utility>

namespace deepmsg::cluster::common
{

using axc::core::ErrorCode;
using axc::core::make_error;

namespace
{

/// `Integer.parseInt`: optional sign, decimal digits, nothing else.
[[nodiscard]] std::expected<std::int32_t, Error> parse_int32(std::string_view value)
{
    if (value.empty())
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "invalid cluster member id"));
    }

    std::string_view digits = value;
    if ('+' == digits.front())
    {
        digits.remove_prefix(1);
    }
    if (digits.empty())
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "invalid cluster member id"));
    }

    std::int32_t parsed = 0;
    const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), parsed);
    if (std::errc{} != result.ec || result.ptr != digits.data() + digits.size())
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "invalid cluster member id"));
    }

    return parsed;
}

[[nodiscard]] std::string join_endpoints(const std::vector<std::string>& attributes, std::size_t start)
{
    std::string endpoints = attributes[start];
    for (std::size_t i = start + 1; i < attributes.size(); ++i)
    {
        endpoints.push_back(',');
        endpoints.append(attributes[i]);
    }
    return endpoints;
}

}  // namespace

std::vector<std::string> split_java(std::string_view value, char delimiter)
{
    std::vector<std::string> parts;
    if (value.empty())
    {
        parts.emplace_back();
        return parts;
    }

    std::size_t start = 0;
    for (std::size_t i = 0; i < value.size(); ++i)
    {
        if (value[i] == delimiter)
        {
            parts.emplace_back(value.substr(start, i - start));
            start = i + 1;
        }
    }
    parts.emplace_back(value.substr(start));

    while (!parts.empty() && parts.back().empty())
    {
        parts.pop_back();
    }

    return parts;
}

void ClusterMember::reset() noexcept
{
    is_ballot_sent = false;
    is_leader = false;
    has_terminated = false;
    vote.reset();
    candidate_term_id = NULL_VALUE;
    leadership_term_id = NULL_VALUE;
    log_position = NULL_POSITION;
}

bool ClusterMember::is_active(std::int64_t now_ns, std::int64_t timeout_ns) const noexcept
{
    return time_of_last_append_position_ns + timeout_ns > now_ns;
}

bool ClusterMember::has_reached_position(std::int64_t expected_leadership_term_id, std::int64_t position,
                                         std::int64_t now_ns, std::int64_t timeout_ns) const noexcept
{
    return is_active(now_ns, timeout_ns) && expected_leadership_term_id == leadership_term_id &&
           log_position >= position;
}

bool ClusterMember::will_vote_for(const ClusterMember& candidate) const noexcept
{
    // Deliberately no `is_active()` check: doing so breaks the nomination phase
    // (`ClusterMember.java:1210-1216`).
    return NULL_POSITION != log_position && compare_log(*this, candidate) <= 0;
}

std::expected<ClusterMembers, Error> parse_cluster_members(std::string_view value)
{
    if (value.empty())
    {
        return ClusterMembers{};
    }

    const std::vector<std::string> member_values = split_java(value, '|');
    ClusterMembers members;
    members.reserve(member_values.size());

    for (const std::string& id_and_endpoints : member_values)
    {
        const std::vector<std::string> attributes = split_java(id_and_endpoints, ',');
        if (attributes.size() < 6 || attributes.size() > 8)
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument, "invalid member value: " + id_and_endpoints +
                                                                               " within: " + std::string{value}));
        }

        auto cluster_member_id = parse_int32(attributes[0]);
        if (!cluster_member_id)
        {
            return std::unexpected(
                make_error(ErrorCode::invalid_argument, "invalid cluster member id, must be an integer value"));
        }

        ClusterMember member;
        member.id = *cluster_member_id;
        member.ingress_endpoint = attributes[1];
        member.consensus_endpoint = attributes[2];
        member.log_endpoint = attributes[3];
        member.catchup_endpoint = attributes[4];
        member.archive_endpoint = attributes[5];

        if (attributes.size() > 6)
        {
            member.archive_response_endpoint = attributes[6];
        }
        if (attributes.size() > 7)
        {
            member.egress_response_endpoint = attributes[7];
        }

        member.endpoints = join_endpoints(attributes, 1);
        members.push_back(std::move(member));
    }

    return members;
}

std::expected<ClusterMember, Error> parse_member_endpoints(std::int32_t id, std::string_view endpoints)
{
    const std::vector<std::string> attributes = split_java(endpoints, ',');
    if (attributes.size() != 5)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "invalid member value: " + std::string{endpoints}));
    }

    ClusterMember member;
    member.id = id;
    member.ingress_endpoint = attributes[0];
    member.consensus_endpoint = attributes[1];
    member.log_endpoint = attributes[2];
    member.catchup_endpoint = attributes[3];
    member.archive_endpoint = attributes[4];
    member.endpoints = join_endpoints(attributes, 0);
    return member;
}

std::string encode_cluster_members(std::span<const ClusterMember> members)
{
    if (members.empty())
    {
        return {};
    }

    std::string encoded;
    for (std::size_t i = 0; i < members.size(); ++i)
    {
        const ClusterMember& member = members[i];
        encoded.append(std::to_string(member.id));
        encoded.push_back(',');
        encoded.append(member.endpoints);

        if (members.size() - 1 != i)
        {
            encoded.push_back('|');
        }
    }

    return encoded;
}

ClusterMember* find_member(ClusterMembers& members, std::int32_t member_id) noexcept
{
    for (ClusterMember& member : members)
    {
        if (member_id == member.id)
        {
            return &member;
        }
    }
    return nullptr;
}

const ClusterMember* find_member(const ClusterMembers& members, std::int32_t member_id) noexcept
{
    for (const ClusterMember& member : members)
    {
        if (member_id == member.id)
        {
            return &member;
        }
    }
    return nullptr;
}

std::expected<ClusterMember, Error> determine_member(const ClusterMembers& members, std::int32_t member_id,
                                                     std::string_view member_endpoints)
{
    const ClusterMember* member = ClusterMember::NULL_VALUE != member_id ? find_member(members, member_id) : nullptr;

    if (members.empty() && nullptr == member)
    {
        return parse_member_endpoints(static_cast<std::int32_t>(ClusterMember::NULL_VALUE), member_endpoints);
    }

    if (nullptr == member)
    {
        return std::unexpected(
            make_error(ErrorCode::not_found, "memberId=" + std::to_string(member_id) + " not found in clusterMembers"));
    }

    if (!member_endpoints.empty())
    {
        auto validated = validate_member_endpoints(*member, member_endpoints);
        if (!validated)
        {
            return std::unexpected(validated.error());
        }
    }

    return *member;
}

bool are_same_endpoints(const ClusterMember& lhs, const ClusterMember& rhs) noexcept
{
    return lhs.ingress_endpoint == rhs.ingress_endpoint && lhs.consensus_endpoint == rhs.consensus_endpoint &&
           lhs.log_endpoint == rhs.log_endpoint && lhs.catchup_endpoint == rhs.catchup_endpoint &&
           lhs.archive_endpoint == rhs.archive_endpoint;
}

std::expected<void, Error> validate_member_endpoints(const ClusterMember& member, std::string_view member_endpoints)
{
    auto endpoints = parse_member_endpoints(static_cast<std::int32_t>(ClusterMember::NULL_VALUE), member_endpoints);
    if (!endpoints)
    {
        return std::unexpected(endpoints.error());
    }

    if (!are_same_endpoints(member, *endpoints))
    {
        return std::unexpected(make_error(
            ErrorCode::invalid_argument,
            "clusterMembers and endpoints differ: " + member.endpoints + " != " + std::string{member_endpoints}));
    }

    return {};
}

std::string ingress_endpoints(std::span<const ClusterMember> members)
{
    std::string endpoints;
    for (std::size_t i = 0; i < members.size(); ++i)
    {
        if (0 != i)
        {
            endpoints.push_back(',');
        }
        endpoints.append(std::to_string(members[i].id));
        endpoints.push_back('=');
        endpoints.append(members[i].ingress_endpoint);
    }
    return endpoints;
}

std::int64_t quorum_position(std::span<const ClusterMember> members, std::span<std::int64_t> ranked_positions,
                             std::int64_t now_ns, std::int64_t timeout_ns) noexcept
{
    for (std::int64_t& ranked_position : ranked_positions)
    {
        ranked_position = 0;
    }

    for (const ClusterMember& member : members)
    {
        if (member.is_active(now_ns, timeout_ns))
        {
            std::int64_t new_position = member.log_position;
            for (std::int64_t& ranked_position : ranked_positions)
            {
                if (new_position > ranked_position)
                {
                    const std::int64_t old_ranked_position = ranked_position;
                    ranked_position = new_position;
                    new_position = old_ranked_position;
                }
            }
        }
    }

    return ranked_positions.back();
}

bool has_active_quorum(std::span<const ClusterMember> members, std::int64_t now_ns, std::int64_t timeout_ns) noexcept
{
    std::int32_t threshold = quorum_threshold(static_cast<std::int32_t>(members.size()));

    for (const ClusterMember& member : members)
    {
        if (member.is_active(now_ns, timeout_ns))
        {
            if (--threshold <= 0)
            {
                return true;
            }
        }
    }

    return false;
}

bool has_quorum_at_position(std::span<const ClusterMember> members, std::int64_t leadership_term_id,
                            std::int64_t position, std::int64_t now_ns, std::int64_t timeout_ns) noexcept
{
    std::int32_t votes = 0;

    for (const ClusterMember& member : members)
    {
        if (member.has_reached_position(leadership_term_id, position, now_ns, timeout_ns))
        {
            ++votes;
        }
    }

    return votes >= quorum_threshold(static_cast<std::int32_t>(members.size()));
}

bool is_quorum_leader(std::span<const ClusterMember> members, std::int64_t candidate_term_id) noexcept
{
    std::int32_t votes = 0;

    for (const ClusterMember& member : members)
    {
        if (candidate_term_id == member.candidate_term_id)
        {
            if (member.vote.has_value() && !*member.vote)
            {
                return false;
            }

            if (member.vote.has_value() && *member.vote)
            {
                ++votes;
            }
        }
    }

    return votes >= quorum_threshold(static_cast<std::int32_t>(members.size()));
}

bool is_unanimous_leader(std::span<const ClusterMember> members, std::int64_t candidate_term_id,
                         std::int32_t graceful_closed_leader_id) noexcept
{
    std::int32_t votes = 0;

    for (const ClusterMember& member : members)
    {
        if (member.id == graceful_closed_leader_id)
        {
            continue;
        }

        if (candidate_term_id != member.candidate_term_id || !member.vote.has_value() || !*member.vote)
        {
            return false;
        }

        ++votes;
    }

    return votes >= quorum_threshold(static_cast<std::int32_t>(members.size()));
}

bool is_quorum_candidate(std::span<const ClusterMember> members, const ClusterMember& candidate) noexcept
{
    std::int32_t possible_votes = 0;

    for (const ClusterMember& member : members)
    {
        if (member.will_vote_for(candidate))
        {
            ++possible_votes;
        }
    }

    return possible_votes >= quorum_threshold(static_cast<std::int32_t>(members.size()));
}

bool is_unanimous_candidate(std::span<const ClusterMember> members, const ClusterMember& candidate,
                            std::int32_t graceful_closed_leader_id) noexcept
{
    std::int32_t possible_votes = 0;

    for (const ClusterMember& member : members)
    {
        if (member.id == graceful_closed_leader_id)
        {
            continue;
        }

        if (!member.will_vote_for(candidate))
        {
            return false;
        }

        ++possible_votes;
    }

    return possible_votes >= quorum_threshold(static_cast<std::int32_t>(members.size()));
}

std::int32_t compare_log(std::int64_t lhs_log_leadership_term_id, std::int64_t lhs_log_position,
                         std::int64_t rhs_log_leadership_term_id, std::int64_t rhs_log_position) noexcept
{
    if (lhs_log_leadership_term_id > rhs_log_leadership_term_id)
    {
        return 1;
    }
    if (lhs_log_leadership_term_id < rhs_log_leadership_term_id)
    {
        return -1;
    }
    if (lhs_log_position > rhs_log_position)
    {
        return 1;
    }
    if (lhs_log_position < rhs_log_position)
    {
        return -1;
    }

    return 0;
}

std::int32_t compare_log(const ClusterMember& lhs, const ClusterMember& rhs) noexcept
{
    return compare_log(lhs.leadership_term_id, lhs.log_position, rhs.leadership_term_id, rhs.log_position);
}

void reset_members(ClusterMembers& members) noexcept
{
    for (ClusterMember& member : members)
    {
        member.reset();
    }
}

void become_candidate(ClusterMembers& members, std::int64_t candidate_term_id,
                      std::int32_t candidate_member_id) noexcept
{
    for (ClusterMember& member : members)
    {
        if (member.id == candidate_member_id)
        {
            member.vote = true;
            member.candidate_term_id = candidate_term_id;
            member.is_ballot_sent = true;
        }
        else
        {
            member.vote.reset();
            member.candidate_term_id = ClusterMember::NULL_VALUE;
            member.is_ballot_sent = false;
        }
    }
}

void set_is_leader(ClusterMembers& members, std::int32_t leader_member_id) noexcept
{
    for (ClusterMember& member : members)
    {
        member.is_leader = member.id == leader_member_id;
    }
}

}  // namespace deepmsg::cluster::common
