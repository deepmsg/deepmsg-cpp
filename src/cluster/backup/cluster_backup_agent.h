#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>

#include "cluster/backup/cluster_backup_context.h"
#include "cluster/common/cluster_mark_file.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"
#include "core/error/error_handler.h"

namespace deepmsg::cluster::backup
{

using axc::core::Error;

/// `ClusterBackupAgent` (`ClusterBackupAgent.java`): the agent which backs up a
/// remote cluster by replicating its log and polling for snapshots.
///
/// S2 delivers the lifecycle: the `cluster-mark.dat` (component `BACKUP`), the
/// four backup counters, the activity timestamp, and the closed state. The
/// state machine's transitions land in S3-S5 with the reference's method
/// names and `file:line` comments.
class ClusterBackupAgent final : public axc::core::ErrorHandler
{
public:
    explicit ClusterBackupAgent(ClusterBackupConfig config);
    ClusterBackupAgent(const ClusterBackupAgent&) = delete;
    ClusterBackupAgent& operator=(const ClusterBackupAgent&) = delete;
    ClusterBackupAgent(ClusterBackupAgent&&) = delete;
    ClusterBackupAgent& operator=(ClusterBackupAgent&&) = delete;
    ~ClusterBackupAgent() noexcept override;

    /// `axc::core::Agent` surface, driven by `AgentRunner`/tests.
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

private:
    [[nodiscard]] std::expected<void, Error> open_mark_file();
    [[nodiscard]] std::expected<void, Error> allocate_counters();
    void on_slow_tick(std::int64_t now_ms) noexcept;
    void state(ClusterBackupState new_state) noexcept;

    ClusterBackupConfig config_{};
    axc::core::DeepmsgClient client_{};
    common::ClusterMarkFile mark_file_{};
    bool mark_file_open_{false};

    axc::core::CounterPtr error_counter_{};
    axc::core::CounterPtr state_counter_{};
    axc::core::CounterPtr live_log_position_counter_{};
    axc::core::CounterPtr next_query_deadline_ms_counter_{};
    axc::core::CounterPtr snapshot_retrieve_counter_{};

    ClusterBackupState state_{ClusterBackupState::backup_query};
    std::int64_t slow_tick_deadline_ms_{0};
    std::int64_t mark_file_update_deadline_ms_{0};
};

}  // namespace deepmsg::cluster::backup
