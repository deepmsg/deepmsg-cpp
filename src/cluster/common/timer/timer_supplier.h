#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string_view>

#include "cluster/common/timer/timer_service.h"
#include "core/error/error.h"

namespace deepmsg::cluster::common::timer
{

using axc::core::Error;

inline constexpr std::string_view TIMER_SERVICE_SUPPLIER_WHEEL = "io.aeron.cluster.WheelTimerServiceSupplier";
inline constexpr std::string_view TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP =
    "io.aeron.cluster.PriorityHeapTimerServiceSupplier";
inline constexpr std::string_view TIMER_SERVICE_SUPPLIER_DEFAULT = TIMER_SERVICE_SUPPLIER_WHEEL;

/// `aeron.cluster.wheel.tick.resolution` default is 8 ms, expressed in ns.
inline constexpr std::int64_t WHEEL_TICK_RESOLUTION_DEFAULT_NS = 8'000'000;
/// `aeron.cluster.ticks.per.wheel` default.
inline constexpr std::int32_t TICKS_PER_WHEEL_DEFAULT = 128;

enum class TimerServiceKind
{
    wheel,
    priority_heap
};

/// `ConsensusModule.Configuration.timerServiceSupplier`: a supplier class name.
[[nodiscard]] std::expected<TimerServiceKind, Error> timer_service_kind(std::string_view supplier_name);

/// `WheelTimerServiceSupplier`: convert the configured resolution into the
/// clock's unit, then round up to a power of two.
[[nodiscard]] std::int64_t wheel_tick_resolution(std::int64_t tick_resolution_ns, ClusterTimeUnit time_unit) noexcept;

[[nodiscard]] std::unique_ptr<TimerService> make_timer_service(TimerServiceKind kind, ClusterTimeUnit time_unit,
                                                               std::int64_t start_time, std::int64_t tick_resolution,
                                                               std::int32_t ticks_per_wheel);

}  // namespace deepmsg::cluster::common::timer
