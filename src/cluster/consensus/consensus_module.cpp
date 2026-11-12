#include "cluster/consensus/consensus_module.h"

#include <utility>

namespace deepmsg::cluster::consensus
{

std::expected<ConsensusModule, Error> ConsensusModule::launch(ConsensusModuleConfig config)
{
    // `ConsensusModule(final Context ctx)` calls `ctx.conclude()`
    // (`ConsensusModule.java:288-291`). Configs read from properties are
    // already concluded; configs built by tests are not.
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

    ConsensusModule module;
    module.idle_ = std::move(*idle);
    module.agent_ = std::make_unique<ConsensusModuleAgent>(std::move(config));
    module.runner_ =
        std::make_unique<axc::core::AgentRunner<ConsensusModuleAgent>>(*module.agent_, module.idle_, *module.agent_);

    auto started = module.runner_->start();
    if (!started)
    {
        module.runner_.reset();
        module.agent_.reset();
        return std::unexpected(started.error());
    }
    return module;
}

ConsensusModule::~ConsensusModule() noexcept
{
    close();
}

void ConsensusModule::close() noexcept
{
    if (nullptr != runner_)
    {
        runner_->close();
        runner_.reset();
    }
    agent_.reset();
}

}  // namespace deepmsg::cluster::consensus
