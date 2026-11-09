#include "cluster/common/timer/timer_supplier.h"

#include <string>

#include "cluster/common/timer/priority_heap_timer_service.h"
#include "cluster/common/timer/wheel_timer_service.h"

namespace deepmsg::cluster::common::timer
{

std::expected<TimerServiceKind, Error> timer_service_kind(std::string_view supplier_name)
{
    if (TIMER_SERVICE_SUPPLIER_WHEEL == supplier_name)
    {
        return TimerServiceKind::wheel;
    }
    if (TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP == supplier_name)
    {
        return TimerServiceKind::priority_heap;
    }

    return std::unexpected(axc::core::make_error(
        axc::core::ErrorCode::config_error, "invalid TimerServiceSupplier: " + std::string{supplier_name} +
                                                " (this build knows " + std::string{TIMER_SERVICE_SUPPLIER_WHEEL} +
                                                " and " + std::string{TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP} + ")"));
}

std::int64_t wheel_tick_resolution(std::int64_t tick_resolution_ns, ClusterTimeUnit time_unit) noexcept
{
    std::int64_t converted = tick_resolution_ns;
    switch (time_unit)
    {
        case ClusterTimeUnit::millis:
            converted = tick_resolution_ns / 1'000'000;
            break;
        case ClusterTimeUnit::micros:
            converted = tick_resolution_ns / 1'000;
            break;
        case ClusterTimeUnit::nanos:
            break;
    }

    std::int64_t rounded = 1;
    while (rounded < converted)
    {
        rounded <<= 1;
    }
    return rounded;
}

std::unique_ptr<TimerService> make_timer_service(TimerServiceKind kind, ClusterTimeUnit time_unit,
                                                 std::int64_t start_time, std::int64_t tick_resolution,
                                                 std::int32_t ticks_per_wheel)
{
    if (TimerServiceKind::priority_heap == kind)
    {
        return std::make_unique<PriorityHeapTimerService>();
    }

    return std::make_unique<WheelTimerService>(time_unit, start_time, tick_resolution,
                                               static_cast<std::size_t>(ticks_per_wheel));
}

}  // namespace deepmsg::cluster::common::timer
