// Built with AXC_STAGE_TIMING=1: exercises the enabled path in isolation.

#include <gtest/gtest.h>

#include <cstdint>

#include "core/trace/stage_timer.h"

using axc::core::Stage;
using axc::core::StageTimer;

TEST(StageTimer, MeasuresEnabledStages)
{
    EXPECT_GT(StageTimer::read_cycles(), 0U);

    StageTimer timer;
    timer.begin(Stage::archive_recorder_fsync);
    std::uint64_t sink = 0;
    for (int index = 0; index < 1000; ++index)
    {
        sink += StageTimer::read_cycles();
    }
    timer.end(Stage::archive_recorder_fsync);

    EXPECT_GT(sink, 0U);

    const auto stats = timer.stats(Stage::archive_recorder_fsync);
    EXPECT_EQ(1U, stats.count);
    EXPECT_GT(stats.total_cycles, 0U);
    EXPECT_GE(stats.max_cycles, stats.min_cycles);

    timer.reset();
    EXPECT_EQ(0U, timer.stats(Stage::archive_recorder_fsync).count);
}

TEST(StageTimer, IgnoresMismatchedEnd)
{
    StageTimer timer;
    timer.begin(Stage::cluster_ingress_to_log);
    timer.end(Stage::cluster_service_to_egress);
    EXPECT_EQ(0U, timer.stats(Stage::cluster_ingress_to_log).count);
}
