#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <string_view>
#include <thread>

#include "core/agent/agent_invoker.h"
#include "core/agent/agent_runner.h"
#include "core/agent/composite_agent.h"

namespace
{

using axc::core::Error;
using axc::core::ErrorCode;

class CountingAgent
{
public:
    std::expected<void, Error> on_start()
    {
        started.store(true, std::memory_order_release);
        return {};
    }

    int do_work() noexcept
    {
        count.fetch_add(1, std::memory_order_relaxed);
        return 1;
    }

    void on_close() noexcept
    {
        closed.store(true, std::memory_order_release);
    }

    std::string_view role_name() noexcept
    {
        return "counting-agent";
    }

    std::atomic<int> count{0};
    std::atomic<bool> started{false};
    std::atomic<bool> closed{false};
};

class FailingAgent
{
public:
    std::expected<void, Error> on_start()
    {
        return std::unexpected(axc::core::make_error(ErrorCode::config_error, "refused"));
    }

    int do_work() noexcept
    {
        return 0;
    }

    void on_close() noexcept
    {
        closed = true;
    }

    std::string_view role_name() noexcept
    {
        return "failing-agent";
    }

    bool closed{false};
};

template <class Value>
bool wait_until(Value& value, int target)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < target && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::yield();
    }
    return value.load(std::memory_order_acquire) >= target;
}

axc::core::IdleStrategy yield_strategy()
{
    const auto strategy = axc::core::IdleStrategy::by_agrona_name("yield");
    return *strategy;
}

}  // namespace

static_assert(axc::core::Agent<CountingAgent>);
static_assert(axc::core::Agent<axc::core::CompositeAgent<CountingAgent, CountingAgent>>);

TEST(AgentRunner, RunsUntilClosed)
{
    CountingAgent agent;
    axc::core::NoOpErrorHandler errors;
    axc::core::AgentRunner<CountingAgent> runner(agent, yield_strategy(), errors);

    ASSERT_TRUE(runner.start().has_value());
    EXPECT_TRUE(agent.started.load(std::memory_order_acquire));
    EXPECT_TRUE(wait_until(agent.count, 1000));

    runner.close();
    EXPECT_FALSE(runner.is_running());
    EXPECT_TRUE(agent.closed.load(std::memory_order_acquire));
}

TEST(AgentRunner, RefusesToStartTwice)
{
    CountingAgent agent;
    axc::core::NoOpErrorHandler errors;
    axc::core::AgentRunner<CountingAgent> runner(agent, yield_strategy(), errors);

    ASSERT_TRUE(runner.start().has_value());
    EXPECT_FALSE(runner.start().has_value());
    runner.close();
}

TEST(AgentRunner, PropagatesOnStartFailure)
{
    FailingAgent agent;
    axc::core::NoOpErrorHandler errors;
    axc::core::AgentRunner<FailingAgent> runner(agent, yield_strategy(), errors);

    const auto started = runner.start();
    ASSERT_FALSE(started.has_value());
    EXPECT_EQ(ErrorCode::config_error, started.error().code);
    EXPECT_FALSE(agent.closed);
    EXPECT_FALSE(runner.is_running());
}

TEST(CompositeAgent, SumsWorkAndClosesAll)
{
    CountingAgent first;
    CountingAgent second;
    axc::core::CompositeAgent<CountingAgent, CountingAgent> composite(first, second);

    ASSERT_TRUE(composite.on_start().has_value());
    EXPECT_GT(composite.do_work(), 0);
    EXPECT_EQ(1, first.count.load());
    EXPECT_EQ(1, second.count.load());

    composite.on_close();
    EXPECT_TRUE(first.closed.load());
    EXPECT_TRUE(second.closed.load());
}

TEST(AgentInvoker, StartsInvokesAndCloses)
{
    CountingAgent agent;
    axc::core::AgentInvoker<CountingAgent> invoker(agent);

    ASSERT_TRUE(invoker.start().has_value());
    EXPECT_FALSE(invoker.start().has_value());

    EXPECT_EQ(1, invoker.invoke());
    EXPECT_EQ(1, agent.count.load());

    invoker.close();
    EXPECT_TRUE(agent.closed.load());
    EXPECT_FALSE(invoker.started());
}
