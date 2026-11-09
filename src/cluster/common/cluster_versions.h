#pragma once

#include <cstdint>

namespace deepmsg::cluster::common
{

/// `ClusterMarkFile` semantic version, shared by `node-state.dat` (C1a) and
/// `cluster-mark*.dat` (C1b).
///
/// Reference: `ClusterMarkFile.java:56-68` (0.3.0) and Agrona
/// `SemanticVersion.compose` / `SemanticVersion.major`.
inline constexpr std::int32_t CLUSTER_MARK_MAJOR_VERSION = 0;
inline constexpr std::int32_t CLUSTER_MARK_MINOR_VERSION = 3;
inline constexpr std::int32_t CLUSTER_MARK_PATCH_VERSION = 0;

[[nodiscard]] constexpr std::int32_t compose_semantic_version(std::int32_t major, std::int32_t minor,
                                                              std::int32_t patch) noexcept
{
    return (major << 16) | (minor << 8) | patch;
}

[[nodiscard]] constexpr std::int32_t semantic_major(std::int32_t version) noexcept
{
    return version >> 16;
}

inline constexpr std::int32_t CLUSTER_MARK_SEMANTIC_VERSION =
    compose_semantic_version(CLUSTER_MARK_MAJOR_VERSION, CLUSTER_MARK_MINOR_VERSION, CLUSTER_MARK_PATCH_VERSION);

}  // namespace deepmsg::cluster::common
