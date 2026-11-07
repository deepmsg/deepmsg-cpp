#pragma once

#include <expected>

#include "core/agent/agent.h"

namespace axc::core
{

/// Runs an agent on the caller's thread (archive INVOKER mode, and how the C
/// client conductor is driven inside archive/CM agents).
template <Agent A>
class AgentInvoker
{
public:
    explicit AgentInvoker(A& agent) noexcept : agent_(agent) {}

    [[nodiscard]] std::expected<void, Error> start()
    {
        if (started_)
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument, "agent invoker already started"));
        }

        auto result = agent_.on_start();
        if (result)
        {
            started_ = true;
        }
        return result;
    }

    [[nodiscard]] int invoke() noexcept
    {
        return agent_.do_work();
    }

    void close() noexcept
    {
        if (started_)
        {
            agent_.on_close();
            started_ = false;
        }
    }

    [[nodiscard]] bool started() const noexcept
    {
        return started_;
    }

private:
    A& agent_;
    bool started_{false};
};

}  // namespace axc::core
