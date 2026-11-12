// Port of `PriorityHeapTimerServiceTest` (Aeron 1.54.0). The null-handler case
// has no C++ counterpart (`TimerHandler&` cannot be null); the case names are
// otherwise the reference's.

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

#include "cluster/common/timer/priority_heap_timer_service.h"

namespace
{

using deepmsg::cluster::common::timer::POLL_LIMIT;
using deepmsg::cluster::common::timer::PriorityHeapTimerService;
using deepmsg::cluster::common::timer::TimerHandler;
using deepmsg::cluster::common::timer::TimerSnapshotTaker;

class RecordingHandler final : public TimerHandler
{
public:
    explicit RecordingHandler(bool result = true) : result_(result) {}

    bool on_timer_event(std::int64_t correlation_id) override
    {
        ids.push_back(correlation_id);
        return result_;
    }

    std::vector<std::int64_t> ids;

private:
    bool result_;
};

class RecordingSnapshotTaker final : public TimerSnapshotTaker
{
public:
    void snapshot_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        entries.emplace_back(correlation_id, deadline);
    }

    std::vector<std::pair<std::int64_t, std::int64_t>> entries;
};

TEST(PriorityHeapTimerServiceTest, pollIsANoOpWhenNoTimersWhereScheduled)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;

    EXPECT_EQ(0, service.poll(INT64_MIN, handler));
    EXPECT_TRUE(handler.ids.empty());
}

TEST(PriorityHeapTimerServiceTest, pollIsANoOpWhenNoScheduledTimersAreExpired)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);

    EXPECT_EQ(0, service.poll(7, handler));
    EXPECT_TRUE(handler.ids.empty());
}

TEST(PriorityHeapTimerServiceTest, pollShouldNotExpireTimerIfHandlerReturnsFalse)
{
    RecordingHandler handler(false);
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);

    EXPECT_EQ(0, service.poll(INT64_MAX, handler));
    ASSERT_EQ(1U, handler.ids.size());
    EXPECT_EQ(1, handler.ids[0]);
}

TEST(PriorityHeapTimerServiceTest, pollShouldExpireSingleTimer)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);

    EXPECT_EQ(1, service.poll(200, handler));
    EXPECT_EQ(0, service.poll(200, handler));
    ASSERT_EQ(1U, handler.ids.size());
    EXPECT_EQ(1, handler.ids[0]);
}

TEST(PriorityHeapTimerServiceTest, pollShouldRemovedExpiredTimers)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(2, 200);
    service.schedule_timer_for_correlation_id(3, 300);
    service.schedule_timer_for_correlation_id(4, 400);
    service.schedule_timer_for_correlation_id(5, 500);

    EXPECT_EQ(2, service.poll(200, handler));
    EXPECT_EQ(3, service.poll(500, handler));
    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 3, 4, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, pollShouldExpireTimersInOrderOfDeadlineButWithinTheDeadlineOrderIsUndefined)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(4, 3);
    service.schedule_timer_for_correlation_id(2, 0);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(5, 0);
    service.schedule_timer_for_correlation_id(6, 11);

    EXPECT_EQ(5, service.poll(10, handler));
    EXPECT_EQ((std::vector<std::int64_t>{2, 5, 3, 4, 1}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered)
{
    PriorityHeapTimerService service;
    EXPECT_FALSE(service.cancel_timer_by_correlation_id(100));
}

TEST(PriorityHeapTimerServiceTest, cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId)
{
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);

    EXPECT_FALSE(service.cancel_timer_by_correlation_id(3));
}

TEST(PriorityHeapTimerServiceTest, cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);
    service.schedule_timer_for_correlation_id(2, 90);

    EXPECT_TRUE(service.cancel_timer_by_correlation_id(7));
    EXPECT_EQ(2, service.poll(111, handler));
    EXPECT_EQ((std::vector<std::int64_t>{2, 1}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheLastTimer)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(7, 50);
    service.schedule_timer_for_correlation_id(2, 90);

    EXPECT_TRUE(service.cancel_timer_by_correlation_id(1));
    EXPECT_EQ(2, service.poll(111, handler));
    EXPECT_EQ((std::vector<std::int64_t>{7, 2}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, cancelTimerByCorrelationIdAfterPoll)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 30);
    service.schedule_timer_for_correlation_id(5, 15);
    service.schedule_timer_for_correlation_id(2, 20);
    service.schedule_timer_for_correlation_id(4, 40);

    EXPECT_EQ(1, service.poll(19, handler));
    EXPECT_TRUE(service.cancel_timer_by_correlation_id(1));
    EXPECT_EQ(2, service.poll(40, handler));
    EXPECT_EQ((std::vector<std::int64_t>{5, 2, 4}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, scheduleTimerForAnExistingCorrelationIdIsANoOpIfDeadlineDoesNotChange)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(5, 50);
    service.schedule_timer_for_correlation_id(7, 70);
    service.schedule_timer_for_correlation_id(5, 50);

    EXPECT_EQ(3, service.poll(70, handler));
    EXPECT_EQ((std::vector<std::int64_t>{3, 5, 7}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 10);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 30);
    service.schedule_timer_for_correlation_id(5, 50);
    service.schedule_timer_for_correlation_id(5, 10);

    EXPECT_EQ(3, service.poll(10, handler));
    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 10);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 30);
    service.schedule_timer_for_correlation_id(5, 50);
    service.schedule_timer_for_correlation_id(1, 30);

    EXPECT_EQ(4, service.poll(30, handler));
    EXPECT_EQ((std::vector<std::int64_t>{2, 4, 1, 3}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, pollShouldStopAfterPollLimitIsReached)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    for (std::int64_t i = 0; i < POLL_LIMIT * 2; ++i)
    {
        service.schedule_timer_for_correlation_id(i, i);
    }

    EXPECT_EQ(POLL_LIMIT, service.poll(INT64_MAX, handler));
    EXPECT_EQ(static_cast<std::size_t>(POLL_LIMIT), handler.ids.size());
}

TEST(PriorityHeapTimerServiceTest, snapshotProcessesAllScheduledTimers)
{
    RecordingHandler handler;
    RecordingSnapshotTaker taker;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 10);
    service.schedule_timer_for_correlation_id(2, 14);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 29);
    service.schedule_timer_for_correlation_id(5, 15);

    EXPECT_EQ(2, service.poll(14, handler));
    service.snapshot(taker);

    EXPECT_EQ((std::vector<std::pair<std::int64_t, std::int64_t>>{{5, 15}, {4, 29}, {3, 30}}), taker.entries);
}

TEST(PriorityHeapTimerServiceTest, expireThanCancelTimer)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 100);
    service.schedule_timer_for_correlation_id(2, 2);
    service.schedule_timer_for_correlation_id(3, 30);
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 50);

    EXPECT_EQ(2, service.poll(5, handler));
    EXPECT_TRUE(service.cancel_timer_by_correlation_id(1));
    EXPECT_EQ(2, service.poll(1000, handler));
    EXPECT_EQ((std::vector<std::int64_t>{2, 4, 3, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, moveUpAnExistingTimerAndCancelAnotherOne)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 1);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(5, 1);

    EXPECT_TRUE(service.cancel_timer_by_correlation_id(3));
    EXPECT_EQ(4, service.poll(5, handler));
    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 5, 4}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, moveDownAnExistingTimerAndCancelAnotherOne)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 1);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(1, 5);

    EXPECT_TRUE(service.cancel_timer_by_correlation_id(3));
    EXPECT_EQ(4, service.poll(5, handler));
    EXPECT_EQ((std::vector<std::int64_t>{2, 4, 1, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, cancelExpiredTimerIsANoOp)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 1);
    service.schedule_timer_for_correlation_id(3, 3);

    EXPECT_EQ(2, service.poll(1, handler));
    EXPECT_FALSE(service.cancel_timer_by_correlation_id(1));
    EXPECT_FALSE(service.cancel_timer_by_correlation_id(2));
}

TEST(PriorityHeapTimerServiceTest, scheduleMustRetainOrderBetweenDeadlines)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);

    EXPECT_EQ(4, service.poll(5, handler));
    EXPECT_EQ((std::vector<std::int64_t>{6, 3, 4, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, shouldReuseExpiredEntriesFromAFreeList)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 2);
    service.schedule_timer_for_correlation_id(3, 3);
    EXPECT_EQ(3U, service.allocated_timer_count());

    EXPECT_EQ(2, service.poll(2, handler));
    handler.ids.clear();
    service.schedule_timer_for_correlation_id(4, 4);
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);
    EXPECT_EQ(4U, service.allocated_timer_count());

    EXPECT_EQ(4, service.poll(5, handler));
    EXPECT_EQ((std::vector<std::int64_t>{6, 3, 4, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, shouldReuseCanceledTimerEntriesFromAFreeList)
{
    RecordingHandler handler;
    PriorityHeapTimerService service;
    service.schedule_timer_for_correlation_id(1, 1);
    service.schedule_timer_for_correlation_id(2, 2);
    service.schedule_timer_for_correlation_id(3, 3);
    service.schedule_timer_for_correlation_id(4, 4);
    EXPECT_EQ(4U, service.allocated_timer_count());

    EXPECT_TRUE(service.cancel_timer_by_correlation_id(2));
    EXPECT_TRUE(service.cancel_timer_by_correlation_id(4));
    service.schedule_timer_for_correlation_id(5, 5);
    service.schedule_timer_for_correlation_id(6, 0);
    EXPECT_EQ(4U, service.allocated_timer_count());

    EXPECT_EQ(4, service.poll(5, handler));
    EXPECT_EQ((std::vector<std::int64_t>{6, 1, 3, 5}), handler.ids);
}

TEST(PriorityHeapTimerServiceTest, manyRandomOperations)
{
    struct Harness final : TimerHandler
    {
        PriorityHeapTimerService service{};
        std::unordered_map<std::int64_t, std::int64_t> deadlines{};
        std::mt19937 random{1};
        std::int64_t next_correlation_id{1};
        std::int64_t time{1000};

        bool on_timer_event(std::int64_t correlation_id) override
        {
            const auto found = deadlines.find(correlation_id);
            if (deadlines.end() == found)
            {
                ADD_FAILURE() << "expired unknown correlationId=" << correlation_id;
                return true;
            }
            const std::int64_t deadline = found->second;
            EXPECT_GE(deadline, 1000);
            EXPECT_LE(deadline, time) << "correlationId=" << correlation_id;
            deadlines.erase(found);
            return true;
        }

        std::int64_t random_int(std::int64_t bound)
        {
            return std::uniform_int_distribution<std::int64_t>(0, bound - 1)(random);
        }

        void schedule_timer()
        {
            const std::int64_t correlation_id = next_correlation_id++;
            const std::int64_t deadline = time + random_int(20);
            service.schedule_timer_for_correlation_id(correlation_id, deadline);
            deadlines[correlation_id] = deadline;
        }

        std::int64_t random_scheduled_correlation_id()
        {
            const std::size_t index = static_cast<std::size_t>(random_int(static_cast<std::int64_t>(deadlines.size())));
            auto it = deadlines.begin();
            std::advance(it, static_cast<std::ptrdiff_t>(index));
            return it->first;
        }

        void cancel_random_timer()
        {
            const std::int64_t correlation_id = random_scheduled_correlation_id();
            EXPECT_EQ(1U, deadlines.erase(correlation_id));
            EXPECT_TRUE(service.cancel_timer_by_correlation_id(correlation_id));
        }

        void reschedule_random_timer()
        {
            const std::int64_t correlation_id = random_scheduled_correlation_id();
            const std::int64_t deadline = time + random_int(20);
            service.schedule_timer_for_correlation_id(correlation_id, deadline);
            EXPECT_EQ(1U, deadlines.erase(correlation_id));
            deadlines[correlation_id] = deadline;
        }

        void increment_time_and_poll()
        {
            time += random_int(2);
            const int expired = service.poll(time, *this);
            (void)expired;
        }

        void validate_ordering()
        {
            std::size_t next_index = 0;
            std::vector<PriorityHeapTimerService::HeapTimer> entries;
            entries.reserve(deadlines.size());
            service.for_each(
                [&](const PriorityHeapTimerService::HeapTimer& timer)
                {
                    EXPECT_EQ(static_cast<std::int32_t>(next_index), timer.index);
                    ++next_index;
                    entries.push_back(timer);
                    if (timer.index > 0)
                    {
                        const auto parent_index = static_cast<std::size_t>((timer.index - 1) / 2);
                        EXPECT_LE(entries[parent_index].deadline, timer.deadline);
                    }
                });
            EXPECT_EQ(deadlines.size(), entries.size());
        }

        void do_random_operation()
        {
            if (deadlines.size() < 5)
            {
                schedule_timer();
            }
            else if (deadlines.size() > 20)
            {
                increment_time_and_poll();
            }
            else
            {
                switch (random_int(5))
                {
                    case 0:
                        increment_time_and_poll();
                        break;
                    case 1:
                        cancel_random_timer();
                        break;
                    case 2:
                        reschedule_random_timer();
                        break;
                    default:
                        schedule_timer();
                }
            }
            validate_ordering();
        }
    };

    Harness harness;
    for (int i = 0; i < 10000; ++i)
    {
        harness.do_random_operation();
    }
}

}  // namespace
