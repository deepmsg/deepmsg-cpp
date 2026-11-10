// `core::DutyCycleTracker` (`io.aeron.driver.status.DutyCycleStallTracker`).
//
// The two cases are the contract the archive conductor depends on: a tracker
// must be anchored with `update()` before its first `measure_and_update()`, or
// that first measurement is the raw clock. The conductor did not anchor its
// tracker, and the benchmark grid read 6.1e13 ns (the machine's uptime) as
// `archive-conductor max cycle time` against the reference's ~2.5e7
// (`analysis/cpp/records/a6a.md`, finding 8).
#include <gtest/gtest.h>

#include <cstdint>

#include "core/counters/duty_cycle_tracker.h"

namespace
{

class FakeCounter final : public axc::core::CounterTarget
{
public:
    void set_value(std::int64_t value) noexcept override
    {
        value_ = value;
    }
    [[nodiscard]] std::int64_t value() const noexcept override
    {
        return value_;
    }

private:
    std::int64_t value_{0};
};

}  // namespace

TEST(DutyCycleTracker, AnchoredCycleReportsTheCycleAndCountsExceededOnes)
{
    FakeCounter max_cycle_time;
    FakeCounter threshold_exceeded;
    axc::core::DutyCycleTracker tracker(&max_cycle_time, &threshold_exceeded, 1'000);

    // `ArchiveConductor.onStart` (`ArchiveConductor.java:252`).
    tracker.update(1'000);

    tracker.measure_and_update(1'250);
    EXPECT_EQ(250, max_cycle_time.value());
    EXPECT_EQ(0, threshold_exceeded.value());

    tracker.measure_and_update(3'000);  // 1,750 ns, over the threshold
    EXPECT_EQ(1'750, max_cycle_time.value());
    EXPECT_EQ(1, threshold_exceeded.value());

    tracker.measure_and_update(3'100);  // 100 ns, under it, and not a new max
    EXPECT_EQ(1'750, max_cycle_time.value());
    EXPECT_EQ(1, threshold_exceeded.value());
}

TEST(DutyCycleTracker, UnanchoredFirstMeasurementIsTheRawClock)
{
    FakeCounter max_cycle_time;
    FakeCounter threshold_exceeded;
    axc::core::DutyCycleTracker tracker(&max_cycle_time, &threshold_exceeded, 1'000);

    // No `update()`: the first measurement is `now - 0`. This is the shape the
    // conductor had, and the reason the counter read as the machine's uptime.
    tracker.measure_and_update(6'100'000'000'000);
    EXPECT_EQ(6'100'000'000'000, max_cycle_time.value());
}
