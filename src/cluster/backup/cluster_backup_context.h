#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/common/termination_hook.h"
#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::cluster::backup
{

using axc::core::Error;

/// `ClusterBackup.State` (`ClusterBackup.java:102-200`): the counter code is
/// the enum's ordinal.
enum class ClusterBackupState : std::int32_t
{
    backup_query = 0,
    snapshot_retrieve = 1,
    live_log_record = 2,
    live_log_replay = 3,
    update_recording_log = 4,
    backing_up = 5,
    reset_backup = 6,
    closed = 7
};

[[nodiscard]] constexpr std::int32_t backup_state_code(ClusterBackupState state) noexcept
{
    return static_cast<std::int32_t>(state);
}

[[nodiscard]] std::string_view backup_state_name(ClusterBackupState state) noexcept;

/// `ClusterBackup.SourceType` (`ClusterBackup.java:205-221`).
enum class BackupSourceType
{
    any,
    leader,
    follower
};

[[nodiscard]] std::expected<BackupSourceType, Error> parse_backup_source_type(std::string_view value);
[[nodiscard]] std::string_view backup_source_type_name(BackupSourceType type) noexcept;

/// `ClusterBackup.Configuration.ReplayStart` (`ClusterBackup.java:230-260`).
enum class BackupReplayStart
{
    beginning,
    latest_snapshot
};

/// `ClusterBackup.Context` (`ClusterBackup.java:584-2092`), transcribed for the
/// C9 slices: identity and membership, channels and stream ids, the backup
/// intervals, the local and cluster archive contexts, and the two termination
/// hooks. Property names are the reference's (see `notes-c9-audit.md` §3).
struct ClusterBackupConfig
{
    // Identity and membership (`aeron.cluster.id`, `.member.id`, `.members`).
    std::int32_t cluster_id{0};
    std::int32_t member_id{-1};
    std::string members{};
    std::string member_endpoints{};
    /// Derived in `conclude()` from the member entry (`Configuration.consensusChannel`).
    std::string consensus_endpoints{};

    // Channels and stream ids.
    std::string consensus_channel{"aeron:udp"};
    std::int32_t consensus_stream_id{108};
    std::int32_t consensus_module_snapshot_stream_id{106};
    std::int32_t service_snapshot_stream_id{106};
    std::int32_t log_stream_id{100};
    /// `aeron.cluster.backup.catchup.endpoint` — required, derived from the
    /// member entry when unset (`ClusterBackup.java:411-431`).
    std::string catchup_endpoint{};
    std::string catchup_channel{"aeron:udp?endpoint=localhost:0"};

    // Intervals (`ClusterBackup.java:353-400`).
    std::int64_t backup_interval_ns{3'600'000'000'000};
    std::int64_t response_timeout_ns{5'000'000'000};
    std::int64_t progress_timeout_ns{10'000'000'000};
    std::int64_t cooldown_interval_ns{30'000'000'000};

    // Source selection and replay start (`:402-408`, `:561-571`).
    BackupSourceType source_type{BackupSourceType::any};
    BackupReplayStart initial_replay_start{BackupReplayStart::latest_snapshot};

    // Directories.
    std::string aeron_dir{};
    std::string cluster_dir{"aeron-cluster"};
    std::optional<std::string> mark_file_dir{};
    bool delete_dir_on_start{false};

    // Misc.
    std::int32_t error_buffer_length{1024 * 1024};
    std::string idle_strategy{"org.agrona.concurrent.BackoffIdleStrategy"};

    // Stream ids written into the backup's mark file (`:2075-2087`).
    std::int32_t service_stream_id{104};
    std::int32_t consensus_module_stream_id{105};
    std::int32_t ingress_stream_id{101};

    // Local archive (`AeronArchive.Context` defaults, `AeronArchive.java:2722-2764`).
    std::string archive_local_control_channel{"aeron:ipc?term-length=64k"};
    std::int32_t archive_local_control_stream_id{10};
    std::int32_t archive_control_response_stream_id{20};

    // The cluster member's archive (endpoint substitution happens per member).
    std::string cluster_archive_control_channel{"aeron:udp?endpoint=localhost:8010"};
    std::string cluster_archive_control_response_channel{};
    std::int32_t cluster_archive_control_stream_id{10};

    // Replication progress (`ConsensusModule.Configuration.replication.*`).
    std::int64_t replication_progress_timeout_ns{10'000'000'000};
    std::int64_t replication_progress_interval_ns{1'000'000'000};

    /// `CredentialsSupplier` (`ClusterBackup.Context.credentialsSupplier`):
    /// the default sends empty credentials and answers a challenge with an
    /// empty response (`NullCredentialsSupplier`).
    std::function<std::vector<std::uint8_t>()> encoded_credentials = [] { return std::vector<std::uint8_t>{}; };
    std::function<std::vector<std::uint8_t>(std::span<const std::uint8_t>)> on_challenge =
        [](std::span<const std::uint8_t>) { return std::vector<std::uint8_t>{}; };

    // Injected by tests / the binary.
    common::TerminationHook termination_hook{};
    common::ExtendedTerminationHook extended_termination_hook{};
    std::function<std::int64_t()> epoch_clock_ms{};
    std::function<std::int64_t()> nano_clock{};

    [[nodiscard]] std::expected<void, Error> conclude();
    [[nodiscard]] bool is_concluded() const noexcept
    {
        return concluded_;
    }

    [[nodiscard]] std::string mark_file_path() const;
    [[nodiscard]] static constexpr std::string_view mark_file_link_name() noexcept
    {
        return "cluster-mark.lnk";
    }

private:
    bool concluded_{false};
};

/// Reads every `aeron.cluster.backup.*` name (and the `aeron.cluster.*` /
/// `aeron.archive.*` names the backup context borrows) with the reference's
/// defaults, then validates like `Context.conclude()`.
[[nodiscard]] std::expected<ClusterBackupConfig, Error> cluster_backup_config_from_properties(
    const axc::core::PropertySource& properties);

/// `Configuration.threadName` for the backup agent: `cluster-backup` in the
/// classic scheme, `aeron-cl-bu` in the new one (`ClusterBackup.java:243-244`).
[[nodiscard]] std::string_view backup_thread_name(const ClusterBackupConfig& config);

}  // namespace deepmsg::cluster::backup
