#include <gtest/gtest.h>

#include <string_view>
#include <utility>

#include "core/agent/idle_strategy.h"

using axc::core::ErrorCode;
using axc::core::IdleKind;
using axc::core::IdleStrategy;

TEST(IdleStrategy, AcceptsAliasesAndClassNames)
{
    const std::pair<std::string_view, IdleKind> cases[] = {
        {"spin", IdleKind::busy_spin},
        {"org.agrona.concurrent.BusySpinIdleStrategy", IdleKind::busy_spin},
        {"yield", IdleKind::yielding},
        {"org.agrona.concurrent.YieldingIdleStrategy", IdleKind::yielding},
        {"sleep-ns", IdleKind::sleeping_ns},
        {"org.agrona.concurrent.SleepingIdleStrategy", IdleKind::sleeping_ns},
        {"sleep-ms", IdleKind::sleeping_ms},
        {"org.agrona.concurrent.SleepingMillisIdleStrategy", IdleKind::sleeping_ms},
        {"backoff", IdleKind::backoff},
        {"org.agrona.concurrent.BackoffIdleStrategy", IdleKind::backoff},
        {"noop", IdleKind::noop},
        {"org.agrona.concurrent.NoOpIdleStrategy", IdleKind::noop},
    };

    for (const auto& [name, kind] : cases)
    {
        const auto strategy = IdleStrategy::by_agrona_name(name);
        ASSERT_TRUE(strategy.has_value()) << name;
        EXPECT_EQ(kind, strategy->kind) << name;
    }
}

TEST(IdleStrategy, BackoffUsesDriverConstants)
{
    const auto strategy = IdleStrategy::by_agrona_name("backoff");
    ASSERT_TRUE(strategy.has_value());

    EXPECT_EQ(10, strategy->max_spins);
    EXPECT_EQ(20, strategy->max_yields);
    EXPECT_EQ(1000, strategy->min_park_ns);
    EXPECT_EQ(1'000'000, strategy->max_park_ns);
}

TEST(IdleStrategy, RejectsUnknownNames)
{
    const auto strategy = IdleStrategy::by_agrona_name("nope");
    ASSERT_FALSE(strategy.has_value());
    EXPECT_EQ(ErrorCode::config_error, strategy.error().code);
}

TEST(IdleStrategy, ControllableIsRefusedLoudly)
{
    for (const std::string_view name : {"controllable", "org.agrona.concurrent.ControllableIdleStrategy"})
    {
        const auto strategy = IdleStrategy::by_agrona_name(name);
        ASSERT_FALSE(strategy.has_value()) << name;
        EXPECT_NE(std::string_view::npos, strategy.error().message.view().find("StatusIndicator"));
    }
}

TEST(IdleStrategy, IdlesWithoutWorkWithoutHanging)
{
    for (const std::string_view name : {"spin", "yield", "sleep-ns", "sleep-ms", "backoff", "noop"})
    {
        auto strategy = IdleStrategy::by_agrona_name(name);
        ASSERT_TRUE(strategy.has_value()) << name;
        for (int i = 0; i < 5; ++i)
        {
            strategy->idle(0);
        }
        strategy->idle(1);
    }
}
