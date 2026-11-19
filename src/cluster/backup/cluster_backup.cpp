#include "cluster/backup/cluster_backup.h"

#include <utility>

namespace deepmsg::cluster::backup
{

std::expected<ClusterBackup, Error> ClusterBackup::launch(ClusterBackupConfig config)
{
    // `ClusterBackup(final Context ctx)` calls `ctx.conclude()`
    // (`ClusterBackup.java:222-250`). Configs read from properties are already
    // concluded; configs built by tests are not.
    if (!config.is_concluded())
    {
        auto concluded = config.conclude();
        if (!concluded)
        {
            return std::unexpected(concluded.error());
        }
    }

    auto idle = axc::core::IdleStrategy::by_agrona_name(config.idle_strategy);
    if (!idle)
    {
        return std::unexpected(idle.error());
    }

    ClusterBackup backup;
    backup.idle_ = std::move(*idle);
    backup.agent_ = std::make_unique<ClusterBackupAgent>(std::move(config));
    backup.runner_ =
        std::make_unique<axc::core::AgentRunner<ClusterBackupAgent>>(*backup.agent_, backup.idle_, *backup.agent_);

    auto started = backup.runner_->start();
    if (!started)
    {
        backup.runner_.reset();
        backup.agent_.reset();
        return std::unexpected(started.error());
    }
    return backup;
}

ClusterBackup::~ClusterBackup() noexcept
{
    close();
}

void ClusterBackup::close() noexcept
{
    if (nullptr != runner_)
    {
        runner_->close();
        runner_.reset();
    }
    agent_.reset();
}

}  // namespace deepmsg::cluster::backup
