#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/recording_log.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

/// `RecoveryState` (`io.aeron.cluster.service.RecoveryState`, Aeron 1.54.0):
/// the counter (type id 204) the clustered service container reads at startup
/// to learn which snapshot recordings it must load.
///
/// The payload lives in the counter's **key**, not its value:
///
/// ```
/// [leadershipTermId int64][logPosition int64][timestamp int64]
/// [clusterId int32][serviceCount int32][snapshotRecordingId int64 × serviceCount]
/// ```
///
/// `find_counter_id`/the getters are for the container side (and tools); the
/// consensus module only allocates and closes it.
class RecoveryState
{
public:
    /// `RECOVERY_STATE_TYPE_ID` (`AeronCounters.java:884`).
    static constexpr std::int32_t TYPE_ID = CLUSTER_RECOVERY_STATE_TYPE_ID;
    /// `NAME` (`RecoveryState.java:69`).
    static constexpr std::string_view NAME = "Cluster recovery: leadershipTermId=";
    /// Field offsets (`RecoveryState.java:74-94`).
    static constexpr std::size_t LEADERSHIP_TERM_ID_OFFSET = 0;
    static constexpr std::size_t LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + sizeof(std::int64_t);
    static constexpr std::size_t TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + sizeof(std::int64_t);
    static constexpr std::size_t CLUSTER_ID_OFFSET = TIMESTAMP_OFFSET + sizeof(std::int64_t);
    static constexpr std::size_t SERVICE_COUNT_OFFSET = CLUSTER_ID_OFFSET + sizeof(std::int32_t);
    static constexpr std::size_t SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + sizeof(std::int32_t);

    /// `RecoveryState.allocate(aeron, leadershipTermId, logPosition, timestamp,
    /// clusterId, snapshotRecordingIds...)` (`:111-150`). The label is the
    /// reference's `Cluster recovery: leadershipTermId=… logPosition=…
    /// clusterId=…`.
    [[nodiscard]] static std::expected<axc::core::AsyncAdd<axc::core::CounterPtr>, Error> allocate(
        axc::core::DeepmsgClient& client, std::int64_t leadership_term_id, std::int64_t log_position,
        std::int64_t timestamp, std::int32_t cluster_id, std::span<const std::int64_t> snapshot_recording_ids);

    /// `addRecoveryStateCounter` (`ConsensusModuleAgent.java:2868-2893`): the
    /// first snapshot of a plan is the consensus module's own (service id
    /// `NULL_VALUE`); the rest are indexed by service id. With no snapshot the
    /// counter carries `leadership_term_id`, a zero position and timestamp and
    /// no recording ids.
    [[nodiscard]] static std::expected<axc::core::AsyncAdd<axc::core::CounterPtr>, Error> allocate(
        axc::core::DeepmsgClient& client, const RecordingLog::RecoveryPlan& plan, std::int32_t cluster_id,
        std::int64_t leadership_term_id);

    /// `RecoveryState.findCounterId` (`:159-180`).
    [[nodiscard]] static std::int32_t find_counter_id(aeron_counters_reader_t* reader,
                                                      std::int32_t cluster_id) noexcept;

    /// `getLeadershipTermId` (`:189-200`), `NULL_VALUE` when absent.
    [[nodiscard]] static std::int64_t get_leadership_term_id(aeron_counters_reader_t* reader,
                                                             std::int32_t counter_id) noexcept;
    /// `getLogPosition` (`:209-220`), `NULL_VALUE` when absent.
    [[nodiscard]] static std::int64_t get_log_position(aeron_counters_reader_t* reader,
                                                       std::int32_t counter_id) noexcept;
    /// `getTimestamp` (`:229-240`), `NULL_VALUE` when absent.
    [[nodiscard]] static std::int64_t get_timestamp(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept;
    /// `getSnapshotRecordingId` (`:250-270`); `NULL_VALUE` when the counter is
    /// absent, an out-of-range service id is `std::nullopt`.
    [[nodiscard]] static std::optional<std::int64_t> get_snapshot_recording_id(aeron_counters_reader_t* reader,
                                                                               std::int32_t counter_id,
                                                                               std::int32_t service_id) noexcept;

private:
    [[nodiscard]] static const std::uint8_t* key_of(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept;
    [[nodiscard]] static bool is_allocated(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept;
};

}  // namespace deepmsg::cluster::common
