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

/// `NodeControl.ToggleState` (`NodeControl.java:38-79`): the code is the
/// enum's ordinal and the counter holds that code.
enum class NodeControlState : std::int32_t
{
    inactive = 0,
    neutral = 1,
    replicate_standby_snapshot = 2,
    validate_recording_log = 3
};

[[nodiscard]] constexpr std::int32_t node_control_code(NodeControlState state) noexcept
{
    return static_cast<std::int32_t>(state);
}

/// `NodeControl.ToggleState.get` (`NodeControl.java:127-141`).
[[nodiscard]] inline std::expected<NodeControlState, Error> node_control_state(std::int64_t value) noexcept
{
    if (value < node_control_code(NodeControlState::inactive) ||
        value > node_control_code(NodeControlState::validate_recording_log))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "invalid node control toggle value: " + std::to_string(value)));
    }
    return static_cast<NodeControlState>(value);
}

[[nodiscard]] inline std::int64_t node_control_value(const axc::core::CounterPtr& counter) noexcept
{
    if (!counter)
    {
        return node_control_code(NodeControlState::inactive);
    }
    std::int64_t* address = aeron_counter_addr(counter.get());
    return nullptr == address ? node_control_code(NodeControlState::inactive)
                              : __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

[[nodiscard]] inline bool node_control_compare_and_set(std::int64_t* address, std::int32_t expected,
                                                       std::int32_t desired) noexcept
{
    if (nullptr == address)
    {
        return false;
    }
    std::int64_t current = expected;
    return __atomic_compare_exchange_n(address, &current, desired, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

[[nodiscard]] inline bool node_control_compare_and_set(const axc::core::CounterPtr& counter, std::int32_t expected,
                                                       std::int32_t desired) noexcept
{
    return counter ? node_control_compare_and_set(aeron_counter_addr(counter.get()), expected, desired) : false;
}

inline void node_control_store(std::int64_t* address, NodeControlState state) noexcept
{
    if (nullptr != address)
    {
        __atomic_store_n(address, static_cast<std::int64_t>(node_control_code(state)), __ATOMIC_RELEASE);
    }
}

inline void node_control_set(const axc::core::CounterPtr& counter, NodeControlState state) noexcept
{
    if (counter)
    {
        node_control_store(aeron_counter_addr(counter.get()), state);
    }
}

inline void node_control_reset(const axc::core::CounterPtr& counter) noexcept
{
    node_control_set(counter, NodeControlState::neutral);
}

inline void node_control_activate(const axc::core::CounterPtr& counter) noexcept
{
    node_control_set(counter, NodeControlState::neutral);
}

/// `NodeControl.ToggleState.toggle` (`NodeControl.java:86-90`): only from
/// NEUTRAL.
[[nodiscard]] inline bool node_control_toggle(std::int64_t* address, NodeControlState target) noexcept
{
    return node_control_compare_and_set(address, node_control_code(NodeControlState::neutral),
                                        node_control_code(target));
}

[[nodiscard]] inline bool node_control_toggle(const axc::core::CounterPtr& counter, NodeControlState target) noexcept
{
    return counter ? node_control_toggle(aeron_counter_addr(counter.get()), target) : false;
}

}  // namespace deepmsg::cluster::consensus
