#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace deepmsg::cluster::common::timer
{

/// Agrona `DeadlineTimerWheel` (2.7.0), the engine under `WheelTimerService`.
///
/// `wheel` is a flat array of `ticks_per_wheel` blocks with `tick_allocation`
/// deadline slots each; a timer id names `(tick, slot)` as
/// `(tick << 32) | slot`. Every rule here was read out of `agrona-2.7.0.jar`
/// (`javap -p -c`) and is also pinned by the reference's own expiry order.
class DeadlineTimerWheel
{
public:
    /// An empty slot, and never an expiry.
    static constexpr std::int64_t NULL_DEADLINE = INT64_MAX;
    static constexpr std::size_t INITIAL_TICK_ALLOCATION = 16;

    /// Throws `std::invalid_argument` when the resolution or wheel size is not
    /// a power of two in the reference's accepted range.
    DeadlineTimerWheel(std::int64_t start_time, std::int64_t tick_resolution, std::size_t ticks_per_wheel);

    [[nodiscard]] std::int64_t tick_resolution() const noexcept
    {
        return tick_resolution_;
    }

    [[nodiscard]] std::size_t timer_count() const noexcept
    {
        return timer_count_;
    }

    [[nodiscard]] std::int64_t current_tick_time() const noexcept;
    void set_current_tick_time(std::int64_t now) noexcept;
    [[nodiscard]] std::int64_t current_tick() const noexcept
    {
        return current_tick_;
    }

    [[nodiscard]] std::int64_t schedule_timer(std::int64_t deadline);
    [[nodiscard]] bool cancel_timer(std::int64_t timer_id) noexcept;
    [[nodiscard]] std::int64_t deadline(std::int64_t timer_id) const noexcept;
    void clear() noexcept;

    /// `poll(now, handler, limit)`; `handler` receives the timer id and answers
    /// whether the expiry was handled.
    template <class H>
    int poll(std::int64_t now, H& handler, std::size_t limit) noexcept
    {
        int expired = 0;

        if (timer_count_ > 0)
        {
            const auto tick_index = static_cast<std::size_t>(current_tick_) & tick_mask_;
            const std::size_t tick_allocation = tick_allocation_;
            std::size_t i = 0;

            while (i < tick_allocation && static_cast<std::size_t>(expired) < limit)
            {
                const std::size_t wheel_index = (tick_index << allocation_bits_to_shift_) + poll_index_;
                const std::int64_t slot_deadline = wheel_[wheel_index];

                if (now >= slot_deadline)
                {
                    wheel_[wheel_index] = NULL_DEADLINE;
                    --timer_count_;
                    ++expired;

                    const std::int64_t timer_id = timer_id_for_slot(tick_index, poll_index_);
                    if (!handler.on_timer_event(timer_id))
                    {
                        wheel_[wheel_index] = slot_deadline;
                        ++timer_count_;
                        --expired;
                        return expired;
                    }
                }

                poll_index_ = poll_index_ + 1 < tick_allocation ? poll_index_ + 1 : 0;
                ++i;
            }

            if (static_cast<std::size_t>(expired) < limit && now >= current_tick_time())
            {
                ++current_tick_;
                poll_index_ = 0;
            }
            else if (poll_index_ >= tick_allocation)
            {
                poll_index_ = 0;
            }
        }
        else if (now >= current_tick_time())
        {
            ++current_tick_;
            poll_index_ = 0;
        }

        return expired;
    }

    [[nodiscard]] static constexpr std::int64_t timer_id_for_slot(std::size_t tick, std::size_t index) noexcept
    {
        return (static_cast<std::int64_t>(tick) << 32) | static_cast<std::int64_t>(index);
    }

    [[nodiscard]] static constexpr std::size_t tick_for_timer_id(std::int64_t timer_id) noexcept
    {
        return static_cast<std::size_t>((timer_id >> 32) & 0xFFFF'FFFFLL);
    }

    [[nodiscard]] static constexpr std::size_t index_in_tick_array(std::int64_t timer_id) noexcept
    {
        return static_cast<std::size_t>(static_cast<std::int32_t>(timer_id));
    }

private:
    [[nodiscard]] std::int64_t increase_capacity(std::int64_t deadline, std::size_t tick_index);
    [[nodiscard]] std::size_t wheel_index(std::int64_t timer_id) const noexcept;

    std::int64_t tick_resolution_;
    std::int64_t start_time_;
    std::int64_t current_tick_{0};
    std::size_t timer_count_{0};
    std::size_t ticks_per_wheel_;
    std::size_t tick_mask_;
    std::uint32_t resolution_bits_to_shift_;
    std::size_t tick_allocation_{INITIAL_TICK_ALLOCATION};
    std::uint32_t allocation_bits_to_shift_;
    std::size_t poll_index_{0};
    std::vector<std::int64_t> wheel_;
};

}  // namespace deepmsg::cluster::common::timer
