#pragma once

#include <expected>
#include <memory>

#include "cluster/consensus/consensus_module_agent.h"
#include "core/agent/agent_runner.h"
#include "core/agent/idle_strategy.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusModule` (`ConsensusModule.java:326-390`): `launch()` concludes the
/// context, creates the agent and starts it on its own thread.
///
/// The C++ agent has no `useAgentInvoker` switch on the handle: tests drive
/// `ConsensusModuleAgent::on_start`/`do_work` directly on the test thread (the
/// reference's invoker mode), and `launch()` is the thread mode the binary will
/// use in C4f. Both run the same transcribed loop.
class ConsensusModule
{
public:
    static std::expected<ConsensusModule, Error> launch(ConsensusModuleConfig config);

    ConsensusModule() = default;
    ConsensusModule(const ConsensusModule&) = delete;
    ConsensusModule& operator=(const ConsensusModule&) = delete;
    ConsensusModule(ConsensusModule&& other) noexcept = default;
    ConsensusModule& operator=(ConsensusModule&& other) noexcept = default;
    ~ConsensusModule() noexcept;

    /// Stops the conductor (the runner calls `on_close` on its thread) and
    /// releases the agent.
    void close() noexcept;

    [[nodiscard]] ConsensusModuleAgent& agent() noexcept
    {
        return *agent_;
    }

    [[nodiscard]] const ConsensusModuleConfig& config() const noexcept
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
    std::unique_ptr<ConsensusModuleAgent> agent_{};
    std::unique_ptr<axc::core::AgentRunner<ConsensusModuleAgent>> runner_{};
    axc::core::IdleStrategy idle_{};
};

}  // namespace deepmsg::cluster::consensus
