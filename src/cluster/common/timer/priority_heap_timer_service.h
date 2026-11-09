#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include "cluster/common/timer/timer_service.h"

namespace deepmsg::cluster::common::timer
{

/// `PriorityHeapTimerService` (`PriorityHeapTimerService.java`).
///
/// A binary min-heap keyed on deadline only; equal deadlines are unordered.
/// The snapshot writes the heap array in **level order**, so every shift below
/// is the reference's, in the reference's order.
class PriorityHeapTimerService final : public TimerService
{
public:
    struct HeapTimer
    {
        std::int64_t correlation_id{-1};
        std::int64_t deadline{-1};
        std::int32_t index{-1};
    };

    static constexpr std::size_t MIN_CAPACITY = 8;

    PriorityHeapTimerService() = default;

    [[nodiscard]] int poll(std::int64_t now, TimerHandler& handler) override;
    void schedule_timer_for_correlation_id(std::int64_t correlation_id, std::int64_t deadline) override;
    [[nodiscard]] bool cancel_timer_by_correlation_id(std::int64_t correlation_id) override;
    void snapshot(TimerSnapshotTaker& taker) override;
    void current_time(std::int64_t now) override;

    /// `forEach`: level order over the live portion of the array.
    void for_each(const std::function<void(const HeapTimer&)>& consumer) const;

    [[nodiscard]] std::size_t allocated_timer_count() const noexcept
    {
        return allocated_;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return size_;
    }

private:
    void ensure_capacity(std::size_t required_capacity);
    void add_to_free_list(HeapTimer timer);
    void place(std::size_t index, HeapTimer timer);
    void shift_up(std::size_t start_index, HeapTimer timer);
    void shift_down(std::size_t heap_size, std::size_t start_index, HeapTimer timer);

    std::unordered_map<std::int64_t, std::int32_t> timer_by_correlation_id_{};
    std::vector<std::optional<HeapTimer>> timers_{};
    std::vector<HeapTimer> free_timers_{};
    std::size_t size_{0};
    std::size_t allocated_{0};
};

}  // namespace deepmsg::cluster::common::timer
