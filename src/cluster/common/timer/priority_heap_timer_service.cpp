#include "cluster/common/timer/priority_heap_timer_service.h"

#include <utility>

namespace deepmsg::cluster::common::timer
{
namespace
{

constexpr std::int64_t NULL_VALUE = -1;

}  // namespace

int PriorityHeapTimerService::poll(std::int64_t now, TimerHandler& handler)
{
    int expired_timers = 0;
    auto& timers = timers_;

    while (size_ > 0 && expired_timers < POLL_LIMIT)
    {
        const HeapTimer timer = timers[0].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        if (timer.deadline > now)
        {
            break;
        }

        if (!handler.on_timer_event(timer.correlation_id))
        {
            break;
        }

        ++expired_timers;

        --size_;
        const std::size_t last_index = size_;
        std::optional<HeapTimer> last_timer = timers[last_index];
        timers[last_index].reset();
        timers[0].reset();

        if (0 != last_index)
        {
            shift_down(last_index, 0,
                       last_timer.value());  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        }

        timer_by_correlation_id_.erase(timer.correlation_id);
        add_to_free_list(timer);
    }

    return expired_timers;
}

void PriorityHeapTimerService::schedule_timer_for_correlation_id(std::int64_t correlation_id, std::int64_t deadline)
{
    const auto found = timer_by_correlation_id_.find(correlation_id);
    if (timer_by_correlation_id_.end() != found)
    {
        const auto index = static_cast<std::size_t>(found->second);
        HeapTimer existing = timers_[index].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        timers_[index].reset();

        if (deadline < existing.deadline)
        {
            existing.deadline = deadline;
            shift_up(static_cast<std::size_t>(existing.index), existing);
        }
        else if (deadline > existing.deadline)
        {
            existing.deadline = deadline;
            shift_down(size_, static_cast<std::size_t>(existing.index), existing);
        }
        else
        {
            timers_[index] = existing;
        }
        return;
    }

    ensure_capacity(size_ + 1);

    const auto index = static_cast<std::int32_t>(size_);
    ++size_;

    HeapTimer timer;
    if (!free_timers_.empty())
    {
        timer = free_timers_.back();
        free_timers_.pop_back();
        timer.correlation_id = correlation_id;
        timer.deadline = deadline;
        timer.index = index;
    }
    else
    {
        ++allocated_;
        timer.correlation_id = correlation_id;
        timer.deadline = deadline;
        timer.index = index;
    }

    timer_by_correlation_id_[correlation_id] = index;
    shift_up(static_cast<std::size_t>(index), timer);
}

bool PriorityHeapTimerService::cancel_timer_by_correlation_id(std::int64_t correlation_id)
{
    const auto found = timer_by_correlation_id_.find(correlation_id);
    if (timer_by_correlation_id_.end() == found)
    {
        return false;
    }

    const auto removed_index = static_cast<std::size_t>(found->second);
    timer_by_correlation_id_.erase(found);
    const HeapTimer removed =
        timers_[removed_index].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
    timers_[removed_index].reset();

    --size_;
    const std::size_t last_index = size_;
    std::optional<HeapTimer> last_timer = timers_[last_index];
    timers_[last_index].reset();

    if (last_index != removed_index)
    {
        const std::int64_t last_correlation_id =
            last_timer.value().correlation_id;  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        shift_down(last_index, removed_index,
                   last_timer.value());  // NOLINT(bugprone-unchecked-optional-access): heap invariant

        const bool still_at_start = timers_[removed_index].value_or(HeapTimer{}).correlation_id == last_correlation_id;
        if (still_at_start)
        {
            HeapTimer displaced = timers_[removed_index].value_or(HeapTimer{});
            timers_[removed_index].reset();
            shift_up(removed_index, displaced);
        }
    }

    add_to_free_list(removed);
    return true;
}

void PriorityHeapTimerService::snapshot(TimerSnapshotTaker& taker)
{
    for (std::size_t i = 0; i < size_; ++i)
    {
        const HeapTimer& timer = timers_[i].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        taker.snapshot_timer(timer.correlation_id, timer.deadline);
    }
}

void PriorityHeapTimerService::current_time(std::int64_t now)
{
    (void)now;
}

void PriorityHeapTimerService::for_each(const std::function<void(const HeapTimer&)>& consumer) const
{
    for (std::size_t i = 0; i < size_; ++i)
    {
        consumer(timers_[i].value());  // NOLINT(bugprone-unchecked-optional-access): heap invariant
    }
}

void PriorityHeapTimerService::ensure_capacity(std::size_t required_capacity)
{
    const std::size_t current_capacity = timers_.size();

    if (required_capacity > current_capacity)
    {
        if (0 == current_capacity)
        {
            timers_.resize(MIN_CAPACITY);
            free_timers_.reserve(MIN_CAPACITY);
        }
        else
        {
            const std::size_t new_capacity = current_capacity + (current_capacity >> 1);
            timers_.resize(new_capacity);
            free_timers_.reserve(new_capacity);
        }
    }
}

void PriorityHeapTimerService::add_to_free_list(HeapTimer timer)
{
    timer.correlation_id = NULL_VALUE;
    timer.deadline = NULL_VALUE;
    timer.index = -1;
    free_timers_.push_back(timer);
}

void PriorityHeapTimerService::place(std::size_t index, HeapTimer timer)
{
    timer.index = static_cast<std::int32_t>(index);
    timer_by_correlation_id_[timer.correlation_id] = timer.index;
    timers_[index] = timer;
}

void PriorityHeapTimerService::shift_up(std::size_t start_index, HeapTimer timer)
{
    std::size_t index = start_index;

    while (index > 0)
    {
        const std::size_t prev_index = (index - 1) >> 1;
        HeapTimer prev = timers_[prev_index].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        timers_[prev_index].reset();

        if (timer.deadline >= prev.deadline)
        {
            timers_[prev_index] = prev;
            break;
        }

        place(index, prev);
        index = prev_index;
    }

    place(index, timer);
}

void PriorityHeapTimerService::shift_down(std::size_t heap_size, std::size_t start_index, HeapTimer timer)
{
    const std::size_t half = heap_size >> 1;
    std::size_t index = start_index;

    while (index < half)
    {
        std::size_t next_index = (index << 1) + 1;
        const std::size_t right = next_index + 1;
        HeapTimer next = timers_[next_index].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        timers_[next_index].reset();

        if (right < heap_size &&
            next.deadline >
                timers_[right].value().deadline)  // NOLINT(bugprone-unchecked-optional-access): heap invariant
        {
            timers_[next_index] = next;
            next_index = right;
            next = timers_[right].value();  // NOLINT(bugprone-unchecked-optional-access): heap invariant
            timers_[right].reset();
        }

        if (timer.deadline < next.deadline)
        {
            timers_[next_index] = next;
            break;
        }

        place(index, next);
        index = next_index;
    }

    place(index, timer);
}

}  // namespace deepmsg::cluster::common::timer
