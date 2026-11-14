#pragma once

#include <cstdint>
#include <expected>
#include <string>

#include <aeronc.h>

#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ClusterControl.ToggleState` (`ClusterControl.java:43-170`): the code is
/// the enum's ordinal and the counter holds that code.
enum class ClusterControlState : std::int32_t
{
    inactive = 0,
    neutral = 1,
    suspend = 2,
    resume = 3,
    snapshot = 4,
    shutdown = 5,
    abort = 6,
    standby_snapshot = 7
};

[[nodiscard]] constexpr std::int32_t cluster_control_code(ClusterControlState state) noexcept
{
    return static_cast<std::int32_t>(state);
}

/// `ClusterControl.ToggleState.get` (`ClusterControl.java:180-205`): a closed
/// counter or a value outside the enum is an error, as the reference's
/// `ClusterException`. C++ has no exception here, so callers report and stop.
[[nodiscard]] inline std::expected<ClusterControlState, Error> cluster_control_state(std::int64_t value) noexcept
{
    if (value < cluster_control_code(ClusterControlState::inactive) ||
        value > cluster_control_code(ClusterControlState::standby_snapshot))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "invalid cluster control toggle value: " + std::to_string(value)));
    }
    return static_cast<ClusterControlState>(value);
}

[[nodiscard]] inline std::int64_t cluster_control_value(const axc::core::CounterPtr& counter) noexcept
{
    if (!counter)
    {
        return cluster_control_code(ClusterControlState::inactive);
    }
    std::int64_t* address = aeron_counter_addr(counter.get());
    return nullptr == address ? cluster_control_code(ClusterControlState::inactive)
                              : __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

[[nodiscard]] inline bool cluster_control_compare_and_set(std::int64_t* address, std::int32_t expected,
                                                          std::int32_t desired) noexcept
{
    if (nullptr == address)
    {
        return false;
    }
    std::int64_t current = expected;
    return __atomic_compare_exchange_n(address, &current, desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

[[nodiscard]] inline bool cluster_control_compare_and_set(const axc::core::CounterPtr& counter, std::int32_t expected,
                                                          std::int32_t desired) noexcept
{
    return counter ? cluster_control_compare_and_set(aeron_counter_addr(counter.get()), expected, desired) : false;
}

inline void cluster_control_store(std::int64_t* address, ClusterControlState state) noexcept
{
    if (nullptr != address)
    {
        __atomic_store_n(address, static_cast<std::int64_t>(cluster_control_code(state)), __ATOMIC_RELEASE);
    }
}

inline void cluster_control_set(const axc::core::CounterPtr& counter, ClusterControlState state) noexcept
{
    if (counter)
    {
        cluster_control_store(aeron_counter_addr(counter.get()), state);
    }
}

inline void cluster_control_reset(const axc::core::CounterPtr& counter) noexcept
{
    cluster_control_set(counter, ClusterControlState::neutral);
}

inline void cluster_control_activate(const axc::core::CounterPtr& counter) noexcept
{
    cluster_control_set(counter, ClusterControlState::neutral);
}

inline void cluster_control_deactivate(const axc::core::CounterPtr& counter) noexcept
{
    cluster_control_set(counter, ClusterControlState::inactive);
}

/// `ClusterControl.ToggleState.toggle` (`ClusterControl.java:118-126`): from
/// NEUTRAL to the target, and RESUME additionally from SUSPEND.
[[nodiscard]] inline bool cluster_control_toggle(std::int64_t* address, ClusterControlState target) noexcept
{
    if (nullptr == address)
    {
        return false;
    }
    if (ClusterControlState::resume == target &&
        cluster_control_code(ClusterControlState::suspend) == __atomic_load_n(address, __ATOMIC_ACQUIRE))
    {
        return cluster_control_compare_and_set(address, cluster_control_code(ClusterControlState::suspend),
                                               cluster_control_code(ClusterControlState::resume));
    }
    return cluster_control_compare_and_set(address, cluster_control_code(ClusterControlState::neutral),
                                           cluster_control_code(target));
}

[[nodiscard]] inline bool cluster_control_toggle(const axc::core::CounterPtr& counter,
                                                 ClusterControlState target) noexcept
{
    return counter ? cluster_control_toggle(aeron_counter_addr(counter.get()), target) : false;
}

}  // namespace deepmsg::cluster::consensus
