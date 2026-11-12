#pragma once

#include <cstdint>
#include <string_view>

namespace deepmsg::cluster::consensus
{

/// `Aeron.NULL_VALUE`.
inline constexpr std::int64_t NULL_VALUE = -1;
/// `AeronArchive.NULL_POSITION`.
inline constexpr std::int64_t NULL_POSITION = -1;

/// `AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION` / semantic version
/// (`AeronCluster.java:1209-1228`): 0.3.0.
inline constexpr std::int32_t CLUSTER_PROTOCOL_MAJOR_VERSION = 0;
inline constexpr std::int32_t CLUSTER_PROTOCOL_SEMANTIC_VERSION = (0 << 16) | (3 << 8) | 0;

/// `ConsensusModule.Configuration.SESSION_LIMIT_MSG`,
/// `SESSION_REJECTED_MSG`, `SESSION_INVALID_VERSION_MSG`
/// (`ConsensusModule.java:578-591`).
inline constexpr std::string_view SESSION_LIMIT_MSG = "concurrent session limit";
inline constexpr std::string_view SESSION_REJECTED_MSG = "session failed authentication";
inline constexpr std::string_view SESSION_INVALID_VERSION_MSG = "invalid client version";

}  // namespace deepmsg::cluster::consensus
