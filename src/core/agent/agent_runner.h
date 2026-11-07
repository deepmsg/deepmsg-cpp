#pragma once

#include <pthread.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <expected>
#include <thread>

#include "core/agent/agent.h"
#include "core/agent/cpu_affinity.h"
#include "core/agent/idle_strategy.h"
#include "core/error/error_handler.h"

namespace axc::core
{

/// Owns one thread running one agent.
///
/// **The only place in the code base that creates a thread.** Everything else
/// (conductor, recorder, replayer, CM, containers) is an Agent handed to a
/// runner; `std::thread` outside this header is rejected in review.
template <Agent A>
class AgentRunner
{
public:
    AgentRunner(A& agent, IdleStrategy idle, ErrorHandler& error_handler, CpuAffinity affinity = CpuAffinity::none())
        : agent_(agent), idle_(idle), error_handler_(error_handler), affinity_(affinity)
    {
    }

    AgentRunner(const AgentRunner&) = delete;
    AgentRunner& operator=(const AgentRunner&) = delete;

    ~AgentRunner()
    {
        close();
    }

    [[nodiscard]] std::expected<void, Error> start()
    {
        if (thread_.joinable())
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument, "agent runner already started"));
        }

        auto started = agent_.on_start();
        if (!started)
        {
            return started;
        }

        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
        return {};
    }

    void close() noexcept
    {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    [[nodiscard]] bool is_running() const noexcept
    {
        return running_.load(std::memory_order_acquire);
    }

private:
    void run() noexcept
    {
        char name[16] = {};
        const std::string_view role = agent_.role_name();
        const std::size_t length = std::min(role.size(), sizeof(name) - 1);
        std::memcpy(name, role.data(), length);

        if (0 != ::pthread_setname_np(::pthread_self(), name))
        {
            error_handler_.on_error(make_error(ErrorCode::internal_error, "thread name could not be set"));
        }

        if (affinity_.any() && !affinity_.apply_to_current_thread())
        {
            error_handler_.on_error(make_error(ErrorCode::config_error, "cpu affinity could not be applied"));
        }

        while (running_.load(std::memory_order_acquire))
        {
            const int work = agent_.do_work();
            idle_.idle(work);
        }

        agent_.on_close();
    }

    A& agent_;
    IdleStrategy idle_;
    ErrorHandler& error_handler_;
    CpuAffinity affinity_;
    std::thread thread_;
    std::atomic<bool> running_{false};
};

}  // namespace axc::core
