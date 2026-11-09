#pragma once

#include <cstdint>
#include <expected>
#include <string>

#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// `Cluster.Role` (`Cluster.java:51-136`): the code space is its ordinal.
enum class Role : std::int32_t
{
    follower = 0,
    candidate = 1,
    leader = 2
};

[[nodiscard]] std::expected<Role, Error> role_from_code(std::int32_t code) noexcept;

[[nodiscard]] constexpr std::int32_t role_code(Role role) noexcept
{
    return static_cast<std::int32_t>(role);
}

/// The unit of the cluster clock (`ClusterTimeUnit`, schema 111).
enum class TimeUnit
{
    milliseconds,
    microseconds,
    nanoseconds
};

/// `ClusterClock.map(ClusterTimeUnit)`: `NULL_VAL` means milliseconds.
[[nodiscard]] TimeUnit time_unit_from_code(std::int32_t code) noexcept;
[[nodiscard]] std::int32_t time_unit_code(TimeUnit unit) noexcept;

[[nodiscard]] constexpr std::int64_t time_to_micros(TimeUnit unit, std::int64_t time) noexcept
{
    switch (unit)
    {
        case TimeUnit::milliseconds:
            return time * 1'000;
        case TimeUnit::microseconds:
            return time;
        case TimeUnit::nanoseconds:
            return time / 1'000;
    }
    return time;
}

[[nodiscard]] constexpr std::int64_t time_to_nanos(TimeUnit unit, std::int64_t time) noexcept
{
    switch (unit)
    {
        case TimeUnit::milliseconds:
            return time * 1'000'000;
        case TimeUnit::microseconds:
            return time * 1'000;
        case TimeUnit::nanoseconds:
            return time;
    }
    return time;
}

}  // namespace deepmsg::cluster::service
