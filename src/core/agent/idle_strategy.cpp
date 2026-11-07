#include "core/agent/idle_strategy.h"

#include <sched.h>
#include <time.h>
#include <algorithm>

namespace axc::core
{
namespace
{

void pause_instruction() noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}

void park_nanos(std::int64_t nanos) noexcept
{
    if (nanos <= 0)
    {
        return;
    }

    timespec duration{};
    duration.tv_sec = static_cast<time_t>(nanos / 1'000'000'000);
    duration.tv_nsec = static_cast<long>(nanos % 1'000'000'000);
    ::nanosleep(&duration, nullptr);
}

bool name_is(std::string_view name, std::string_view alias, std::string_view class_name) noexcept
{
    return name == alias || name == class_name;
}

}  // namespace

std::expected<IdleStrategy, Error> IdleStrategy::by_agrona_name(std::string_view name) noexcept
{
    IdleStrategy strategy;

    if (name_is(name, "spin", "org.agrona.concurrent.BusySpinIdleStrategy"))
    {
        strategy.kind = IdleKind::busy_spin;
    }
    else if (name_is(name, "yield", "org.agrona.concurrent.YieldingIdleStrategy"))
    {
        strategy.kind = IdleKind::yielding;
    }
    else if (name_is(name, "sleep-ns", "org.agrona.concurrent.SleepingIdleStrategy"))
    {
        strategy.kind = IdleKind::sleeping_ns;
    }
    else if (name_is(name, "sleep-ms", "org.agrona.concurrent.SleepingMillisIdleStrategy"))
    {
        strategy.kind = IdleKind::sleeping_ms;
    }
    else if (name_is(name, "backoff", "org.agrona.concurrent.BackoffIdleStrategy"))
    {
        strategy.kind = IdleKind::backoff;
    }
    else if (name_is(name, "noop", "org.agrona.concurrent.NoOpIdleStrategy"))
    {
        strategy.kind = IdleKind::noop;
    }
    else if (name_is(name, "controllable", "org.agrona.concurrent.ControllableIdleStrategy"))
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "controllable idle strategy needs a StatusIndicator (driver-only)"));
    }
    else
    {
        return std::unexpected(make_error(ErrorCode::config_error, "unknown idle strategy name"));
    }

    return strategy;
}

void IdleStrategy::idle(int work_count) noexcept
{
    switch (kind)
    {
        case IdleKind::busy_spin:
        case IdleKind::noop:
            break;

        case IdleKind::yielding:
            if (work_count < 1)
            {
                ::sched_yield();
            }
            break;

        case IdleKind::sleeping_ns:
            if (work_count < 1)
            {
                park_nanos(sleep_period_ns);
            }
            break;

        case IdleKind::sleeping_ms:
            if (work_count < 1)
            {
                park_nanos(sleep_period_ms * 1'000'000);
            }
            break;

        case IdleKind::backoff:
            if (work_count > 0)
            {
                reset();
            }
            else
            {
                backoff_idle();
            }
            break;
    }
}

void IdleStrategy::reset() noexcept
{
    state_ = 0;
    spins_ = 0;
    yields_ = 0;
    park_period_ns_ = min_park_ns;
}

void IdleStrategy::backoff_idle() noexcept
{
    switch (state_)
    {
        case 0:  // not idle
            state_ = 1;
            ++spins_;
            break;

        case 1:  // spinning
            pause_instruction();
            if (++spins_ > max_spins)
            {
                state_ = 2;
                yields_ = 0;
            }
            break;

        case 2:  // yielding
            if (++yields_ > max_yields)
            {
                state_ = 3;
                park_period_ns_ = min_park_ns;
            }
            else
            {
                ::sched_yield();
            }
            break;

        default:  // parking
            park_nanos(park_period_ns_);
            park_period_ns_ = std::min(park_period_ns_ << 1, max_park_ns);
            break;
    }
}

}  // namespace axc::core
