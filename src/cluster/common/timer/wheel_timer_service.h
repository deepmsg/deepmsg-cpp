#pragma once

#include <cstdint>

#include "cluster/common/timer/deadline_timer_wheel.h"
#include "cluster/common/timer/timer_service.h"
#include "core/collections/long2long_hash_map.h"

namespace deepmsg::cluster::common::timer
{

/// `WheelTimerService` (`WheelTimerService.java`).
///
/// The snapshot order is the **entry-iterator** order of
/// `timerIdByCorrelationIdMap` (`Long2LongHashMap`), with each deadline read
/// back out of the wheel. Do not replace the map or reorder the walk.
class WheelTimerService final : public TimerService
{
public:
    WheelTimerService(ClusterTimeUnit time_unit, std::int64_t start_time, std::int64_t tick_resolution,
                      std::size_t ticks_per_wheel);

    [[nodiscard]] int poll(std::int64_t now, TimerHandler& handler) override;
    void schedule_timer_for_correlation_id(std::int64_t correlation_id, std::int64_t deadline) override;
    [[nodiscard]] bool cancel_timer_by_correlation_id(std::int64_t correlation_id) override;
    void snapshot(TimerSnapshotTaker& taker) override;
    void current_time(std::int64_t now) override;

    [[nodiscard]] const DeadlineTimerWheel& wheel() const noexcept
    {
        return wheel_;
    }

    [[nodiscard]] bool timer_id_for_correlation_id(std::int64_t correlation_id, std::int64_t& timer_id) const noexcept;

private:
    [[nodiscard]] bool on_timer_expiry(std::int64_t timer_id, TimerHandler& handler) noexcept;

    class ExpiryHandler;

    ClusterTimeUnit time_unit_;
    DeadlineTimerWheel wheel_;
    axc::core::Long2LongHashMap timer_id_by_correlation_id_;
    axc::core::Long2LongHashMap correlation_id_by_timer_id_;
    bool abort_{false};
};

}  // namespace deepmsg::cluster::common::timer
