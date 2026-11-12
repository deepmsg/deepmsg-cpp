// Unit cases for `WheelTimerService` and the Agrona `DeadlineTimerWheel` under
// it (Aeron 1.54.0). The reference has no standalone wheel unit test -- its
// wheel coverage is the full-cluster `ClusterTimerTest`, which needs C2/C4 --
// so these cases pin the wheel behaviour the reference relies on, plus the
// engine invariants from the bytecode.

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "cluster/common/timer/deadline_timer_wheel.h"
#include "cluster/common/timer/wheel_timer_service.h"

namespace
{

using deepmsg::cluster::common::timer::ClusterTimeUnit;
using deepmsg::cluster::common::timer::DeadlineTimerWheel;
using deepmsg::cluster::common::timer::TimerHandler;
using deepmsg::cluster::common::timer::TimerSnapshotTaker;
using deepmsg::cluster::common::timer::WheelTimerService;

class IdHandler final : public TimerHandler
{
public:
    explicit IdHandler(bool result = true) : result_(result) {}

    bool on_timer_event(std::int64_t correlation_id) override
    {
        ids.push_back(correlation_id);
        return result_;
    }

    std::vector<std::int64_t> ids;

private:
    bool result_;
};

class Recorder final : public TimerSnapshotTaker
{
public:
    void snapshot_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        entries.emplace_back(correlation_id, deadline);
    }

    std::vector<std::pair<std::int64_t, std::int64_t>> entries;
};

TEST(DeadlineTimerWheelTest, aTimerScheduledForThePastIsClampedToTheCurrentTick)
{
    DeadlineTimerWheel wheel{0, 8, 128};
    const std::int64_t timer_id = wheel.schedule_timer(-1);

    EXPECT_EQ(0U, DeadlineTimerWheel::tick_for_timer_id(timer_id));
    EXPECT_EQ(-1, wheel.deadline(timer_id));
}

TEST(DeadlineTimerWheelTest, aFullTickBlockGrowsTheAllocationAndKeepsTheOldSlots)
{
    DeadlineTimerWheel wheel{0, 8, 128};
    std::vector<std::int64_t> ids;

    for (std::size_t i = 0; i < DeadlineTimerWheel::INITIAL_TICK_ALLOCATION; ++i)
    {
        const std::int64_t timer_id = wheel.schedule_timer(100);
        EXPECT_EQ(i, DeadlineTimerWheel::index_in_tick_array(timer_id));
        ids.push_back(timer_id);
    }

    const std::int64_t grown = wheel.schedule_timer(100);
    EXPECT_EQ(DeadlineTimerWheel::INITIAL_TICK_ALLOCATION, DeadlineTimerWheel::index_in_tick_array(grown));

    ids.push_back(grown);
    for (const std::int64_t timer_id : ids)
    {
        EXPECT_EQ(100, wheel.deadline(timer_id));
    }
    EXPECT_EQ(ids.size(), wheel.timer_count());
}

TEST(DeadlineTimerWheelTest, rejectsANonPowerOfTwoResolutionOrWheelSize)
{
    EXPECT_THROW(DeadlineTimerWheel(0, 7, 128), std::invalid_argument);
    EXPECT_THROW(DeadlineTimerWheel(0, 0, 128), std::invalid_argument);
    EXPECT_THROW(DeadlineTimerWheel(0, 8, 3), std::invalid_argument);
    EXPECT_THROW(DeadlineTimerWheel(0, 8, 1), std::invalid_argument);
}

TEST(DeadlineTimerWheelTest, currentTickTimeMovesForwardOnly)
{
    DeadlineTimerWheel wheel{1000, 8, 128};
    EXPECT_EQ(1008, wheel.current_tick_time());

    wheel.set_current_tick_time(1017);
    EXPECT_EQ(1024, wheel.current_tick_time());

    wheel.set_current_tick_time(1000);
    EXPECT_EQ(1024, wheel.current_tick_time());
}

TEST(WheelTimerServiceTest, theServicePollWalksTheTicksInOrderWithinOneCall)
{
    IdHandler handler;
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 10);
    service.schedule_timer_for_correlation_id(3, 20);

    EXPECT_EQ(3, service.poll(25, handler));
    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 3}), handler.ids);
    EXPECT_EQ(0, service.poll(25, handler));
}

TEST(WheelTimerServiceTest, aTimerIsKeptWhenTheHandlerReturnsFalse)
{
    IdHandler handler(false);
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    service.schedule_timer_for_correlation_id(1, 10);

    EXPECT_EQ(0, service.poll(100, handler));
    ASSERT_EQ(1U, handler.ids.size());

    IdHandler accepting;
    EXPECT_EQ(1, service.poll(100, accepting));
    EXPECT_EQ((std::vector<std::int64_t>{1}), accepting.ids);
}

TEST(WheelTimerServiceTest, aRescheduleIsARemoveAndReinsert)
{
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    service.schedule_timer_for_correlation_id(7, 100);
    std::int64_t first = 0;
    ASSERT_TRUE(service.timer_id_for_correlation_id(7, first));

    service.schedule_timer_for_correlation_id(7, 100);
    std::int64_t second = 0;
    ASSERT_TRUE(service.timer_id_for_correlation_id(7, second));
    EXPECT_EQ(first, second);
    EXPECT_EQ(1U, service.wheel().timer_count());

    service.schedule_timer_for_correlation_id(7, 200);
    std::int64_t third = 0;
    ASSERT_TRUE(service.timer_id_for_correlation_id(7, third));
    EXPECT_NE(first, third);
    EXPECT_EQ(1U, service.wheel().timer_count());
}

TEST(WheelTimerServiceTest, cancellingAnUnknownCorrelationIdReturnsFalse)
{
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    EXPECT_FALSE(service.cancel_timer_by_correlation_id(1));

    service.schedule_timer_for_correlation_id(1, 10);
    EXPECT_TRUE(service.cancel_timer_by_correlation_id(1));
    EXPECT_FALSE(service.cancel_timer_by_correlation_id(1));
    EXPECT_EQ(0U, service.wheel().timer_count());
}

TEST(WheelTimerServiceTest, currentTimeAdvancesTheTicksAndExpiresOnTheNextPoll)
{
    IdHandler handler;
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    service.schedule_timer_for_correlation_id(1, 100);

    service.current_time(96);
    EXPECT_EQ(0, service.poll(96, handler));

    EXPECT_EQ(1, service.poll(104, handler));
    EXPECT_EQ((std::vector<std::int64_t>{1}), handler.ids);
}

TEST(WheelTimerServiceTest, snapshotIsEmptyWithNoTimers)
{
    WheelTimerService service{ClusterTimeUnit::millis, 0, 8, 128};
    Recorder recorder;
    service.snapshot(recorder);
    EXPECT_TRUE(recorder.entries.empty());
}

}  // namespace
