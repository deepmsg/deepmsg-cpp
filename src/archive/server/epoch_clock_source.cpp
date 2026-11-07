#include "archive/server/epoch_clock_source.h"

#include "core/time/clock.h"

namespace deepmsg::archive::server
{

std::int64_t SystemEpochClock::time_ms() const noexcept
{
    return axc::core::EpochClock::now_ms();
}

}  // namespace deepmsg::archive::server
