#include "core/time/clock.h"

#include <ctime>

namespace axc::core
{

std::int64_t NanoClock::now() noexcept
{
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000 + static_cast<std::int64_t>(ts.tv_nsec);
}

std::int64_t EpochClock::now_ms() noexcept
{
    timespec ts{};
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1'000 + static_cast<std::int64_t>(ts.tv_nsec) / 1'000'000;
}

}  // namespace axc::core
