#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "core/trace/stage_timer.h"
#include "core/trace/trace.h"

using axc::core::Stage;
using axc::core::StageTimer;
using axc::core::TraceRecord;
using axc::core::TraceRing;

TEST(TraceRing, RecordsAndDrainsInOrder)
{
    auto& ring = TraceRing::instance();
    ring.clear();

    ring.record(1, 10, 20);
    ring.record(2, 30, 40);

    std::vector<TraceRecord> records;
    const std::size_t drained = ring.drain([&records](const TraceRecord& record) { records.push_back(record); });

    ASSERT_EQ(2U, drained);
    ASSERT_EQ(2U, records.size());
    EXPECT_EQ(1, records[0].code);
    EXPECT_EQ(10, records[0].value_a);
    EXPECT_EQ(20, records[0].value_b);
    EXPECT_EQ(2, records[1].code);

    EXPECT_EQ(0U, ring.drain([](const TraceRecord&) {}));
    ring.clear();
}

TEST(TraceRing, BoundsUnreadRecordsToCapacity)
{
    auto& ring = TraceRing::instance();
    ring.clear();

    for (std::size_t index = 0; index < TraceRing::CAPACITY + 10; ++index)
    {
        ring.record(static_cast<std::int32_t>(index), 0, 0);
    }

    std::size_t count = 0;
    const std::size_t drained = ring.drain([&count](const TraceRecord&) { ++count; });

    EXPECT_EQ(TraceRing::CAPACITY, drained);
    EXPECT_EQ(TraceRing::CAPACITY, count);
    ring.clear();
}

TEST(StageTimer, IsZeroWhenTimingIsCompiledOut)
{
#if defined(AXC_STAGE_TIMING)
    GTEST_SKIP() << "timing enabled";
#else
    StageTimer timer;
    timer.begin(Stage::archive_recorder_write);
    timer.end(Stage::archive_recorder_write);

    const auto stats = timer.stats(Stage::archive_recorder_write);
    EXPECT_EQ(0U, stats.count);
    EXPECT_EQ(0U, stats.total_cycles);
    EXPECT_EQ(0U, StageTimer::read_cycles());
#endif
}
