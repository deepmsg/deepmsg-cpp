#include "cluster/common/timer/deadline_timer_wheel.h"

#include <stdexcept>
#include <string>

namespace deepmsg::cluster::common::timer
{
namespace
{

[[nodiscard]] constexpr bool is_power_of_two(std::int64_t value) noexcept
{
    return value > 0 && 0 == (static_cast<std::uint64_t>(value) & (static_cast<std::uint64_t>(value) - 1));
}

[[nodiscard]] constexpr std::uint32_t trailing_zeros(std::size_t value) noexcept
{
    std::uint32_t count = 0;
    while (0 == (value & 1U))
    {
        value >>= 1U;
        ++count;
    }
    return count;
}

}  // namespace

DeadlineTimerWheel::DeadlineTimerWheel(std::int64_t start_time, std::int64_t tick_resolution,
                                       std::size_t ticks_per_wheel)
    : tick_resolution_(tick_resolution),
      start_time_(start_time),
      ticks_per_wheel_(ticks_per_wheel),
      tick_mask_(ticks_per_wheel - 1),
      resolution_bits_to_shift_(0),
      allocation_bits_to_shift_(trailing_zeros(INITIAL_TICK_ALLOCATION))
{
    if (!is_power_of_two(tick_resolution))
    {
        throw std::invalid_argument("tick resolution must be a power of two: " + std::to_string(tick_resolution));
    }

    if (ticks_per_wheel < 2 || 0 != (ticks_per_wheel & (ticks_per_wheel - 1)))
    {
        throw std::invalid_argument("ticks per wheel must be a power of two >= 2: " + std::to_string(ticks_per_wheel));
    }

    resolution_bits_to_shift_ = trailing_zeros(static_cast<std::size_t>(tick_resolution));
    wheel_.assign(ticks_per_wheel * tick_allocation_, NULL_DEADLINE);
}

std::int64_t DeadlineTimerWheel::current_tick_time() const noexcept
{
    return start_time_ + ((current_tick_ + 1) << resolution_bits_to_shift_);
}

void DeadlineTimerWheel::set_current_tick_time(std::int64_t now) noexcept
{
    const std::int64_t tick = (now - start_time_) >> resolution_bits_to_shift_;
    if (tick > current_tick_)
    {
        current_tick_ = tick;
    }
}

std::int64_t DeadlineTimerWheel::schedule_timer(std::int64_t deadline)
{
    const std::int64_t deadline_tick = (deadline - start_time_) >> resolution_bits_to_shift_;
    const auto tick = static_cast<std::size_t>(deadline_tick > current_tick_ ? deadline_tick : current_tick_);
    const std::size_t tick_index = tick & tick_mask_;
    const std::size_t base = tick_index << allocation_bits_to_shift_;

    for (std::size_t slot = 0; slot < tick_allocation_; ++slot)
    {
        const std::size_t index = base + slot;
        if (NULL_DEADLINE == wheel_[index])
        {
            wheel_[index] = deadline;
            ++timer_count_;
            return timer_id_for_slot(tick_index, slot);
        }
    }

    return increase_capacity(deadline, tick_index);
}

bool DeadlineTimerWheel::cancel_timer(std::int64_t timer_id) noexcept
{
    const std::size_t index = wheel_index(timer_id);
    if (NULL_DEADLINE == wheel_[index])
    {
        return false;
    }

    wheel_[index] = NULL_DEADLINE;
    --timer_count_;
    return true;
}

std::int64_t DeadlineTimerWheel::deadline(std::int64_t timer_id) const noexcept
{
    const std::size_t tick_index = tick_for_timer_id(timer_id);
    const std::size_t slot = index_in_tick_array(timer_id);
    if (tick_index >= ticks_per_wheel_ || slot >= tick_allocation_)
    {
        return NULL_DEADLINE;
    }
    return wheel_[(tick_index << allocation_bits_to_shift_) + slot];
}

void DeadlineTimerWheel::clear() noexcept
{
    for (std::int64_t& slot : wheel_)
    {
        slot = NULL_DEADLINE;
    }
    timer_count_ = 0;
    current_tick_ = 0;
    poll_index_ = 0;
}

std::int64_t DeadlineTimerWheel::increase_capacity(std::int64_t deadline, std::size_t tick_index)
{
    const std::size_t old_allocation = tick_allocation_;
    const std::size_t new_allocation = old_allocation * 2;
    const std::uint32_t new_shift = trailing_zeros(new_allocation);

    std::vector<std::int64_t> new_wheel(ticks_per_wheel_ * new_allocation, NULL_DEADLINE);

    for (std::size_t tick = 0; tick < ticks_per_wheel_; ++tick)
    {
        const std::size_t old_base = tick << allocation_bits_to_shift_;
        const std::size_t new_base = tick << new_shift;
        for (std::size_t slot = 0; slot < old_allocation; ++slot)
        {
            new_wheel[new_base + slot] = wheel_[old_base + slot];
        }
    }

    wheel_ = std::move(new_wheel);
    tick_allocation_ = new_allocation;
    allocation_bits_to_shift_ = new_shift;

    const std::size_t index = (tick_index << allocation_bits_to_shift_) + old_allocation;
    wheel_[index] = deadline;
    ++timer_count_;
    return timer_id_for_slot(tick_index, old_allocation);
}

std::size_t DeadlineTimerWheel::wheel_index(std::int64_t timer_id) const noexcept
{
    return (tick_for_timer_id(timer_id) << allocation_bits_to_shift_) + index_in_tick_array(timer_id);
}

}  // namespace deepmsg::cluster::common::timer
