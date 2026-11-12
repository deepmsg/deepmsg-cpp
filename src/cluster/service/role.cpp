#include "cluster/service/role.h"

#include <string>

namespace deepmsg::cluster::service
{

std::expected<Role, Error> role_from_code(std::int32_t code) noexcept
{
    switch (code)
    {
        case 0:
            return Role::follower;
        case 1:
            return Role::candidate;
        case 2:
            return Role::leader;
        default:
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                         "Invalid role counter code: " + std::to_string(code)));
    }
}

TimeUnit time_unit_from_code(std::int32_t code) noexcept
{
    switch (code)
    {
        case 1:
            return TimeUnit::microseconds;
        case 2:
            return TimeUnit::nanoseconds;
        default:
            return TimeUnit::milliseconds;
    }
}

std::int32_t time_unit_code(TimeUnit unit) noexcept
{
    switch (unit)
    {
        case TimeUnit::milliseconds:
            return 0;
        case TimeUnit::microseconds:
            return 1;
        case TimeUnit::nanoseconds:
            return 2;
    }
    return 0;
}

}  // namespace deepmsg::cluster::service
