#pragma once

#include <cstdint>
#include <string_view>

namespace deepmsg::cluster::election
{

/// `ElectionState` (`ElectionState.java:30-115`): the 18 states of the
/// election, written to counter 207 by `Election::state(...)`
/// (`Election.java:1433`).
///
/// The codes are a contract: `ClusterTool` and the counters reader take them
/// by number, so the order is the reference's, not alphabetical.
enum class ElectionState : std::int32_t
{
    init = 0,
    canvass = 1,
    nominate = 2,
    candidate_ballot = 3,
    follower_ballot = 4,
    leader_log_replication = 5,
    leader_replay = 6,
    leader_init = 7,
    leader_ready = 8,
    follower_log_replication = 9,
    follower_replay = 10,
    follower_catchup_init = 11,
    follower_catchup_await = 12,
    follower_catchup = 13,
    follower_log_init = 14,
    follower_log_await = 15,
    follower_ready = 16,
    closed = 17
};

/// The code the counter carries (`ElectionState.java:30-115`).
[[nodiscard]] constexpr std::int32_t election_state_code(ElectionState state) noexcept
{
    return static_cast<std::int32_t>(state);
}

/// The reference's enum constant name, for state-change traces and test
/// failure messages (`ElectionState.java:30-115`).
[[nodiscard]] std::string_view election_state_name(ElectionState state) noexcept;

}  // namespace deepmsg::cluster::election
