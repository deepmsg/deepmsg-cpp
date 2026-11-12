#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "cluster/consensus/consensus_constants.h"
#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusModule.State` (`ConsensusModule.java:140-200`): the counter code
/// is the enum's ordinal.
enum class ConsensusModuleState : std::int32_t
{
    init = 0,
    active = 1,
    suspended = 2,
    snapshot = 3,
    quitting = 4,
    terminating = 5,
    closed = 6
};

[[nodiscard]] constexpr std::int32_t state_code(ConsensusModuleState state) noexcept
{
    return static_cast<std::int32_t>(state);
}

[[nodiscard]] std::string_view state_name(ConsensusModuleState state) noexcept;

/// `Cluster.Role` (`Cluster.java:51-136`). The service container has the same
/// enum in `cluster::service`; the consensus module does not depend on the
/// container library, so the three codes are spelled out here as well.
enum class ClusterRole : std::int32_t
{
    follower = 0,
    candidate = 1,
    leader = 2
};

[[nodiscard]] constexpr std::int32_t role_code(ClusterRole role) noexcept
{
    return static_cast<std::int32_t>(role);
}

/// The Q6 shell: `StandbySnapshotReplicator` is not implemented, but a node
/// starting with a newer standby snapshot must not do so silently.
enum class StandbySnapshotStartupMode
{
    warn,
    refuse
};

/// `ConsensusModule.Context` for the C4a skeleton: launch, main loop, slow
/// tick, counters, archive connection. The reference reads 41 names owned by
/// `ConsensusModule.java`; the ones C4b/C4c will consume are parsed and stored
/// here so one property file configures the whole node.
struct ConsensusModuleConfig
{
    static constexpr std::int32_t MAX_SERVICE_COUNT = 10;

    // Identity and membership.
    std::int32_t cluster_id{0};
    std::int32_t member_id{0};
    std::int32_t appointed_leader_id{static_cast<std::int32_t>(NULL_VALUE)};
    std::string members{};  // required
    std::string member_endpoints{};
    std::string consensus_endpoints{};

    // Channels and stream ids.
    std::string ingress_channel{};  // required
    std::int32_t ingress_stream_id{101};
    std::int32_t ingress_fragment_limit{50};
    bool ingress_ipc_allowed{false};
    std::string egress_channel{};  // optional
    std::int32_t egress_stream_id{102};
    std::string log_channel{"aeron:udp?term-length=64m"};
    std::int32_t log_stream_id{100};
    std::int32_t log_fragment_limit{50};
    std::string consensus_channel{"aeron:udp"};
    std::int32_t consensus_stream_id{108};
    std::string replication_channel{};  // required
    std::string follower_catchup_channel{"aeron:udp"};
    std::string leader_archive_control_channel{"aeron:udp?term-length=64k"};
    std::string control_channel{"aeron:ipc?term-length=128k"};
    std::int32_t service_stream_id{104};
    std::int32_t consensus_module_stream_id{105};

    // Directories.
    std::string aeron_dir{};
    std::string cluster_dir{"aeron-cluster"};
    std::string services_dir{};
    std::optional<std::string> mark_file_dir{};

    // Timeouts and limits.
    std::int32_t service_count{1};
    std::int32_t max_concurrent_sessions{10};
    std::int64_t session_timeout_ns{10'000'000'000};
    std::int64_t leader_heartbeat_timeout_ns{10'000'000'000};
    std::int64_t leader_heartbeat_interval_ns{200'000'000};
    std::int64_t startup_canvass_timeout_ns{60'000'000'000};
    std::int64_t election_timeout_ns{1'000'000'000};
    std::int64_t election_status_interval_ns{100'000'000};
    std::int64_t termination_timeout_ns{10'000'000'000};
    std::int64_t cycle_threshold_ns{100'000'000};
    std::int64_t total_snapshot_duration_threshold_ns{1'000'000'000};
    std::int32_t file_sync_level{0};
    std::int32_t error_buffer_length{1024 * 1024};
    std::int64_t wheel_tick_resolution_ns{8'000'000};
    std::int32_t ticks_per_wheel{128};
    std::string timer_service_supplier{"io.aeron.cluster.WheelTimerServiceSupplier"};
    std::string idle_strategy{"org.agrona.concurrent.BackoffIdleStrategy"};
    std::string agent_role_name{};
    bool delete_dir_on_start{false};

    // Local archive (the reference's `AeronArchive.Context` defaults,
    // `AeronArchive.java:2722-2764`). The response stream id is completed in
    // `conclude()` as `clusterId * 100 + 100 + 20`.
    std::string archive_local_control_channel{"aeron:ipc?term-length=64k"};
    std::int32_t archive_local_control_stream_id{10};
    std::int32_t archive_control_response_stream_id{20};

    /// The C++ shell's own switch (Q6); not a reference name.
    StandbySnapshotStartupMode standby_snapshot_on_startup{StandbySnapshotStartupMode::warn};

    /// Tests inject clocks; empty means the real ones
    /// (`ClusterClock` / `SystemEpochClock` in the reference).
    std::function<std::int64_t()> nano_clock{};
    std::function<std::int64_t()> epoch_clock_ms{};

    /// `ConsensusModule.Context.conclude()`, in the reference's order and with
    /// the reference's messages where the rule applies to C4a.
    [[nodiscard]] std::expected<void, Error> conclude();

    [[nodiscard]] bool is_concluded() const noexcept
    {
        return concluded_;
    }

    [[nodiscard]] std::string mark_file_path() const;
    [[nodiscard]] std::string mark_file_link_name() const;

private:
    bool concluded_{false};
};

/// Reads every `aeron.cluster.*` name the CM owns with the reference's
/// defaults, then validates with `Conclude()`. Unknown names are left to
/// `common::cluster_name_table()`, as the container does.
[[nodiscard]] std::expected<ConsensusModuleConfig, Error> consensus_module_config_from_properties(
    const axc::core::PropertySource& properties);

/// `CommonContext.threadName`: classic is
/// `consensus-module-<clusterId>-<memberId>`; the new scheme is
/// `aeron-cl-cm` (`ConsensusModuleAgent.AERON_CLUSTER_CONSENSUS_THREAD_NAME`).
[[nodiscard]] std::string consensus_module_thread_name(const ConsensusModuleConfig& config);

}  // namespace deepmsg::cluster::consensus
