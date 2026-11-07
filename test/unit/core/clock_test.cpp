#include <gtest/gtest.h>

#include "core/time/clock.h"

using axc::core::CachedNanoClock;
using axc::core::EpochClock;
using axc::core::NanoClock;

TEST(Clocks, MonotonicIsNonDecreasing)
{
    const std::int64_t first = NanoClock::now();
    const std::int64_t second = NanoClock::now();

    EXPECT_GT(first, 0);
    EXPECT_LE(first, second);
}

TEST(Clocks, EpochIsInMilliseconds)
{
    // After 2020-09-13; catches a seconds/microseconds mix-up.
    EXPECT_GT(EpochClock::now_ms(), 1'600'000'000'000LL);
}

TEST(Clocks, CachedClockOnlyMovesOnUpdate)
{
    CachedNanoClock clock;
    const std::int64_t first = clock.time();

    EXPECT_EQ(first, clock.time());
    clock.update();
    EXPECT_GE(clock.time(), first);
}
