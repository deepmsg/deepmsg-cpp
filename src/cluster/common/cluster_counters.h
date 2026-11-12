#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"

namespace deepmsg::cluster::common
{

/// Cluster counter type ids, from `AeronCounters.java:860-1106`.
///
/// Only types with an allocated counter in this implementation are marked as
/// "used"; the premium standby/transition ids are still named here so the
/// number table is complete and the disposition is explicit.
inline constexpr std::int32_t CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;
inline constexpr std::int32_t CLUSTER_NODE_ROLE_TYPE_ID = 201;
inline constexpr std::int32_t CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;
inline constexpr std::int32_t CLUSTER_COMMIT_POSITION_TYPE_ID = 203;
inline constexpr std::int32_t CLUSTER_RECOVERY_STATE_TYPE_ID = 204;
inline constexpr std::int32_t CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;
inline constexpr std::int32_t CLUSTER_ELECTION_STATE_TYPE_ID = 207;
inline constexpr std::int32_t CLUSTER_BACKUP_STATE_TYPE_ID = 208;
inline constexpr std::int32_t CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;
inline constexpr std::int32_t CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;
inline constexpr std::int32_t CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;
inline constexpr std::int32_t CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;
inline constexpr std::int32_t CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;
inline constexpr std::int32_t CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;
inline constexpr std::int32_t CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;
inline constexpr std::int32_t CLUSTER_MAX_CYCLE_TIME_TYPE_ID = 216;
inline constexpr std::int32_t CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 217;
inline constexpr std::int32_t CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID = 218;
inline constexpr std::int32_t CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 219;
inline constexpr std::int32_t CLUSTER_STANDBY_STATE_TYPE_ID = 220;
inline constexpr std::int32_t CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID = 221;
inline constexpr std::int32_t CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID = 222;
inline constexpr std::int32_t CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID = 223;
inline constexpr std::int32_t TRANSITION_MODULE_STATE_TYPE_ID = 224;
inline constexpr std::int32_t TRANSITION_MODULE_ERROR_COUNT_TYPE_ID = 226;
inline constexpr std::int32_t CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID = 227;
inline constexpr std::int32_t CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 228;
inline constexpr std::int32_t TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID = 229;
inline constexpr std::int32_t TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 230;
inline constexpr std::int32_t CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID = 231;
inline constexpr std::int32_t CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID = 232;
inline constexpr std::int32_t NODE_CONTROL_TOGGLE_TYPE_ID = 233;
inline constexpr std::int32_t CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID = 234;
inline constexpr std::int32_t CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 235;
inline constexpr std::int32_t CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID = 236;
inline constexpr std::int32_t CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 237;
inline constexpr std::int32_t CLUSTER_ELECTION_COUNT_TYPE_ID = 238;
inline constexpr std::int32_t CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID = 239;
inline constexpr std::int32_t CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID = 240;
inline constexpr std::int32_t CLUSTER_SESSION_TYPE_ID = 241;

inline constexpr std::string_view CLUSTER_ID_LABEL_SUFFIX = " - clusterId=";
inline constexpr std::string_view SERVICE_ID_LABEL_SUFFIX = " serviceId=";

/// `ClusterCounters.allocate`: key is `clusterId` as a 4-byte LE int.
[[nodiscard]] std::array<std::uint8_t, sizeof(std::int32_t)> cluster_counter_key(std::int32_t cluster_id) noexcept;

/// `ClusterCounters.allocateServiceCounter`: key is `clusterId` then
/// `serviceId`, both 4-byte LE ints.
[[nodiscard]] std::array<std::uint8_t, 2 * sizeof(std::int32_t)> service_counter_key(std::int32_t cluster_id,
                                                                                     std::int32_t service_id) noexcept;

[[nodiscard]] std::string cluster_counter_label(std::string_view name, std::int32_t cluster_id);
[[nodiscard]] std::string service_counter_label(std::string_view name, std::int32_t cluster_id,
                                                std::int32_t service_id);

/// `AeronCounters.appendVersionInfo`: a space, then `version=… commit=…`.
[[nodiscard]] std::string version_info(std::string_view version, std::string_view commit_hash);

[[nodiscard]] axc::core::AsyncAdd<axc::core::CounterPtr> allocate(axc::core::DeepmsgClient& client,
                                                                  std::string_view name, std::int32_t type_id,
                                                                  std::int32_t cluster_id);

[[nodiscard]] axc::core::AsyncAdd<axc::core::CounterPtr> allocate_versioned(axc::core::DeepmsgClient& client,
                                                                            std::string_view name, std::int32_t type_id,
                                                                            std::int32_t cluster_id,
                                                                            std::string_view version,
                                                                            std::string_view commit_hash);

[[nodiscard]] axc::core::AsyncAdd<axc::core::CounterPtr> allocate_service_counter(axc::core::DeepmsgClient& client,
                                                                                  std::string_view name,
                                                                                  std::int32_t type_id,
                                                                                  std::int32_t cluster_id,
                                                                                  std::int32_t service_id);

[[nodiscard]] axc::core::AsyncAdd<axc::core::CounterPtr> allocate_service_error_counter(
    axc::core::DeepmsgClient& client, std::int32_t cluster_id, std::int32_t service_id, std::string_view version,
    std::string_view commit_hash);

/// `ClusterCounters.find`: first allocated counter with the type id whose key
/// starts with `clusterId`. Returns `AERON_NULL_COUNTER_ID` when absent.
[[nodiscard]] std::int32_t find_cluster_counter(aeron_counters_reader_t* reader, std::int32_t type_id,
                                                std::int32_t cluster_id) noexcept;

}  // namespace deepmsg::cluster::common
