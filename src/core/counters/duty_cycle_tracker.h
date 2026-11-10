#pragma once

#include <cstdint>

#include "core/counters/counter_target.h"

namespace axc::core
{

/// `io.aeron.driver.status.DutyCycleStallTracker`
/// (`DutyCycleStallTracker.java:76-90`): track the longest duty cycle and count
/// the cycles that exceed a threshold.
///
/// One tracker per agent thread; `measure_and_update` runs at the top of that
/// agent's `do_work` pass (`ArchiveConductor.java:373`,
/// `DedicatedModeArchiveConductor.java:205,333`).
class DutyCycleTracker
{
public:
    DutyCycleTracker() noexcept = default;

    DutyCycleTracker(CounterTarget* max_cycle_time, CounterTarget* threshold_exceeded,
                     std::int64_t threshold_ns) noexcept
        : max_cycle_time_(max_cycle_time), threshold_exceeded_(threshold_exceeded), threshold_ns_(threshold_ns)
    {
    }

    /// `DutyCycleTracker.update` (`DutyCycleTracker.java:51-57`).
    void update(std::int64_t now_ns) noexcept
    {
        last_ns_ = now_ns;
    }

    /// `DutyCycleTracker.measureAndUpdate` (`:59-70`).
    void measure_and_update(std::int64_t now_ns) noexcept
    {
        const std::int64_t duration_ns = now_ns - last_ns_;
        last_ns_ = now_ns;

        if (nullptr == max_cycle_time_)
        {
            return;
        }

        // `reportMeasurement` (`DutyCycleStallTracker.java:76-90`):
        // `proposeMaxRelease` then the threshold count.
        if (duration_ns > max_value_)
        {
            max_value_ = duration_ns;
            max_cycle_time_->set_value(duration_ns);
        }

        if (duration_ns > threshold_ns_ && nullptr != threshold_exceeded_)
        {
            threshold_exceeded_->set_value(threshold_exceeded_->value() + 1);
        }
    }

    [[nodiscard]] std::int64_t max_cycle_time_ns() const noexcept
    {
        return max_value_;
    }

    [[nodiscard]] std::int64_t threshold_ns() const noexcept
    {
        return threshold_ns_;
    }

private:
    CounterTarget* max_cycle_time_{nullptr};
    CounterTarget* threshold_exceeded_{nullptr};
    std::int64_t threshold_ns_{0};
    std::int64_t last_ns_{0};
    std::int64_t max_value_{0};
};

}  // namespace axc::core
