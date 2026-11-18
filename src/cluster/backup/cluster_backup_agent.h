#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/backup/cluster_backup_context.h"
#include "cluster/backup/publication_group.h"
#include "cluster/backup/snapshot_replication.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_member.h"
#include "cluster/common/recording_log.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"
#include "core/error/error_handler.h"

namespace deepmsg::cluster::backup
{

using axc::core::Error;

/// `ClusterBackupAgent` (`ClusterBackupAgent.java`): the agent which backs up a
/// remote cluster by replicating its log and polling for snapshots.
///
/// The state machine follows `doWork` (`:240-315`): a slow tick, the consensus
/// subscription, then the current state's work. The reference throws out of a
/// failed pass (`AgentTerminationException` for storage exhaustion); the C++
/// agent reports through `on_error` and returns, and the caller decides.
class ClusterBackupAgent final : public axc::core::ErrorHandler
{
public:
    explicit ClusterBackupAgent(ClusterBackupConfig config);
    ClusterBackupAgent(const ClusterBackupAgent&) = delete;
    ClusterBackupAgent& operator=(const ClusterBackupAgent&) = delete;
    ClusterBackupAgent(ClusterBackupAgent&&) = delete;
    ClusterBackupAgent& operator=(ClusterBackupAgent&&) = delete;
    ~ClusterBackupAgent() noexcept override;

    // `axc::core::Agent` surface, driven by `AgentRunner`/tests.
    [[nodiscard]] std::expected<void, Error> on_start();
    [[nodiscard]] int do_work() noexcept;
    void on_close() noexcept;
    [[nodiscard]] std::string_view role_name() const noexcept;

    // `axc::core::ErrorHandler`: the runner hands agent-loop errors here.
    void on_error(const Error& error) noexcept override;

    [[nodiscard]] const ClusterBackupConfig& config() const noexcept
    {
        return config_;
    }

    [[nodiscard]] ClusterBackupState state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] std::int64_t now_ms() const noexcept;

    /// The `recording.log` the agent maintains (`onStart`, `:188`).
    [[nodiscard]] const common::RecordingLog* recording_log() const noexcept;

private:
    [[nodiscard]] std::expected<void, Error> open_mark_file();
    [[nodiscard]] std::expected<void, Error> allocate_counters();
    [[nodiscard]] std::expected<void, Error> open_recording_log();
    [[nodiscard]] std::expected<void, Error> connect_backup_archive();
    [[nodiscard]] std::expected<void, Error> open_consensus_subscription();

    void reset();
    void stop_replay() noexcept;
    void stop_recording() noexcept;
    [[nodiscard]] std::int64_t start_log_recording() noexcept;
    [[nodiscard]] int poll_backup_archive_events() noexcept;

    static void on_recording_signal(aeron_archive_recording_signal_t* signal, void* clientd) noexcept;
    /// `onUnavailableCounter` (`ClusterBackupAgent.java:394-404`): the live
    /// log's `RecordingPos` counter disappeared with its process.
    static void unavailable_counter_trampoline(void* clientd, aeron_counters_reader_t* reader,
                                               std::int64_t registration_id, std::int32_t counter_id) noexcept;
    void on_unavailable_counter(std::int64_t registration_id, std::int32_t counter_id);
    void handle_recording_signal(std::int32_t signal, std::int64_t correlation_id, std::int64_t recording_id,
                                 std::int64_t position);

    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);
    [[nodiscard]] bool dispatch(std::span<const std::uint8_t> message);
    void on_backup_response(std::span<const std::uint8_t> message);
    void on_challenge(std::span<const std::uint8_t> message);
    void on_session_event(std::span<const std::uint8_t> message);

    [[nodiscard]] int slow_tick(std::int64_t now_ms) noexcept;
    [[nodiscard]] int backup_query_work(std::int64_t now_ms);
    [[nodiscard]] int snapshot_retrieve_work(std::int64_t now_ms);
    [[nodiscard]] int live_log_record_work(std::int64_t now_ms);
    [[nodiscard]] int live_log_replay_work(std::int64_t now_ms);
    [[nodiscard]] int update_recording_log_work(std::int64_t now_ms);
    [[nodiscard]] int backing_up_work(std::int64_t now_ms);
    [[nodiscard]] int reset_backup_work(std::int64_t now_ms);
    [[nodiscard]] bool has_progress_stalled(std::int64_t now_ms) const noexcept;

    [[nodiscard]] bool source_acceptable(std::int32_t leader_member_id, std::int32_t member_id) const noexcept;
    [[nodiscard]] std::int64_t replay_start_position() const;

    void state(ClusterBackupState new_state, std::int64_t now_ms);

    ClusterBackupConfig config_{};
    axc::core::DeepmsgClient client_{};
    common::ClusterMarkFile mark_file_{};
    bool mark_file_open_{false};
    std::optional<common::RecordingLog> recording_log_{};

    axc::core::CounterPtr error_counter_{};
    axc::core::CounterPtr state_counter_{};
    axc::core::CounterPtr live_log_position_counter_{};
    axc::core::CounterPtr next_query_deadline_ms_counter_{};
    axc::core::CounterPtr snapshot_retrieve_counter_{};

    // Consensus side.
    std::unique_ptr<PublicationGroup> consensus_publications_{};
    axc::core::SubPtr consensus_subscription_{};
    aeron_controlled_fragment_assembler_t* consensus_assembler_{nullptr};
    std::vector<common::ClusterMember> cluster_members_{};
    std::optional<common::ClusterMember> log_supplier_member_{};
    std::optional<common::RecordingLog::Entry> leader_log_entry_{};
    std::optional<common::RecordingLog::Entry> leader_last_term_entry_{};
    std::vector<common::RecordingLog::Snapshot> snapshots_to_retrieve_{};
    std::vector<common::RecordingLog::Snapshot> snapshots_retrieved_{};
    std::int64_t correlation_id_{-1};
    std::int64_t cluster_log_recording_id_{-1};
    std::int64_t time_of_last_backup_query_ms_{0};

    // Archive side.
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_t* backup_archive_{nullptr};
    aeron_archive_context_t* cluster_archive_context_{nullptr};
    aeron_archive_t* cluster_archive_{nullptr};
    aeron_archive_async_connect_t* cluster_archive_async_connect_{nullptr};
    std::int64_t backup_archive_id_{-1};
    std::unique_ptr<SnapshotReplication> snapshot_replication_{};
    std::int64_t time_of_last_progress_ms_{0};
    std::int64_t cool_down_deadline_ms_{-1};

    // Live log (S5).
    std::int64_t live_log_recording_subscription_id_{-1};
    std::int64_t live_log_recording_id_{-1};
    std::int64_t live_log_replay_session_id_{-1};
    std::int64_t live_log_recording_session_id_{-1};
    std::int32_t leader_commit_position_counter_id_{-1};
    std::int32_t live_log_recording_counter_id_{-1};
    axc::core::SubPtr recording_subscription_{};
    std::string replay_channel_{};
    std::string recording_channel_{};

    ClusterBackupState state_{ClusterBackupState::backup_query};
    std::int64_t slow_tick_deadline_ms_{0};
    std::int64_t mark_file_update_deadline_ms_{0};
};

}  // namespace deepmsg::cluster::backup
