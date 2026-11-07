#pragma once

#include <expected>
#include <string_view>
#include <tuple>

#include "core/agent/agent.h"

namespace axc::core
{

/// Runs several agents sequentially on one thread (archive SHARED mode).
template <Agent... Agents>
class CompositeAgent
{
public:
    explicit CompositeAgent(Agents&... agents) noexcept : agents_(agents...) {}

    [[nodiscard]] std::expected<void, Error> on_start()
    {
        std::expected<void, Error> result{};

        std::apply(
            [&result](auto&... agents)
            {
                const auto start_one = [&result](auto& agent)
                {
                    if (!result)
                    {
                        return;
                    }
                    auto started = agent.on_start();
                    if (!started)
                    {
                        result = std::unexpected(started.error());
                    }
                };
                (start_one(agents), ...);
            },
            agents_);

        return result;
    }

    [[nodiscard]] int do_work() noexcept
    {
        return std::apply(
            [](auto&... agents)
            {
                int work = 0;
                ((work += agents.do_work()), ...);
                return work;
            },
            agents_);
    }

    void on_close() noexcept
    {
        std::apply([](auto&... agents) { (agents.on_close(), ...); }, agents_);
    }

    [[nodiscard]] std::string_view role_name() noexcept
    {
        return "composite-agent";
    }

private:
    std::tuple<Agents&...> agents_;
};

}  // namespace axc::core
