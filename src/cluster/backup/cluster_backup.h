#pragma once

#include <expected>
#include <memory>

#include "cluster/backup/cluster_backup_agent.h"
#include "core/agent/agent_runner.h"
#include "core/agent/idle_strategy.h"
#include "core/error/error.h"

namespace deepmsg::cluster::backup
{

using axc::core::Error;

/// `ClusterBackup` (`ClusterBackup.java:222-322`): `launch()` concludes the
/// context, creates the agent and starts it on its own thread. Tests drive
/// `ClusterBackupAgent::on_start`/`do_work` directly (the reference's
/// invoker mode); the binary uses this handle.
class ClusterBackup
{
public:
    static std::expected<ClusterBackup, Error> launch(ClusterBackupConfig config);

    ClusterBackup() = default;
    ClusterBackup(const ClusterBackup&) = delete;
    ClusterBackup& operator=(const ClusterBackup&) = delete;
    ClusterBackup(ClusterBackup&& other) noexcept = default;
    ClusterBackup& operator=(ClusterBackup&& other) noexcept = default;
    ~ClusterBackup() noexcept;

    void close() noexcept;

    [[nodiscard]] ClusterBackupAgent& agent() noexcept
    {
        return *agent_;
    }

    [[nodiscard]] const ClusterBackupConfig& config() const noexcept
    {
        return agent_->config();
    }

    [[nodiscard]] bool is_running() const noexcept
    {
        return nullptr != runner_ && runner_->is_running();
    }

private:
    // Declared agent-first so that reverse destruction closes the runner before
    // the agent it drives.
    std::unique_ptr<ClusterBackupAgent> agent_{};
    std::unique_ptr<axc::core::AgentRunner<ClusterBackupAgent>> runner_{};
    axc::core::IdleStrategy idle_{};
};

}  // namespace deepmsg::cluster::backup
