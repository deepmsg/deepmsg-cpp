#include "cluster/common/timer/wheel_timer_service.h"

namespace deepmsg::cluster::common::timer
{

class WheelTimerService::ExpiryHandler
{
public:
    ExpiryHandler(WheelTimerService& service, TimerHandler& handler) noexcept : service_(service), handler_(handler) {}

    [[nodiscard]] bool on_timer_event(std::int64_t timer_id) noexcept
    {
        return service_.on_timer_expiry(timer_id, handler_);
    }

private:
    WheelTimerService& service_;
    TimerHandler& handler_;
};

WheelTimerService::WheelTimerService(ClusterTimeUnit time_unit, std::int64_t start_time, std::int64_t tick_resolution,
                                     std::size_t ticks_per_wheel)
    : time_unit_(time_unit),
      wheel_(start_time, tick_resolution, ticks_per_wheel),
      timer_id_by_correlation_id_(axc::core::Long2LongHashMap::MIN_CAPACITY,
                                  axc::core::Long2LongHashMap::DEFAULT_LOAD_FACTOR, DeadlineTimerWheel::NULL_DEADLINE),
      correlation_id_by_timer_id_(axc::core::Long2LongHashMap::MIN_CAPACITY,
                                  axc::core::Long2LongHashMap::DEFAULT_LOAD_FACTOR, DeadlineTimerWheel::NULL_DEADLINE)
{
    (void)time_unit_;
}

bool WheelTimerService::on_timer_expiry(std::int64_t timer_id, TimerHandler& handler) noexcept
{
    const std::int64_t correlation_id = correlation_id_by_timer_id_.get(timer_id);

    if (handler.on_timer_event(correlation_id))
    {
        (void)correlation_id_by_timer_id_.remove(timer_id);
        (void)timer_id_by_correlation_id_.remove(correlation_id);
        return true;
    }

    abort_ = true;
    return false;
}

int WheelTimerService::poll(std::int64_t now, TimerHandler& handler)
{
    int expired = 0;
    abort_ = false;

    do
    {
        ExpiryHandler on_expiry{*this, handler};
        expired += wheel_.poll(now, on_expiry, static_cast<std::size_t>(POLL_LIMIT));

        if (abort_)
        {
            break;
        }
    } while (expired < POLL_LIMIT && wheel_.current_tick_time() < now);

    return expired;
}

void WheelTimerService::schedule_timer_for_correlation_id(std::int64_t correlation_id, std::int64_t deadline)
{
    (void)cancel_timer_by_correlation_id(correlation_id);

    const std::int64_t timer_id = wheel_.schedule_timer(deadline);
    (void)timer_id_by_correlation_id_.put(correlation_id, timer_id);
    (void)correlation_id_by_timer_id_.put(timer_id, correlation_id);
}

bool WheelTimerService::cancel_timer_by_correlation_id(std::int64_t correlation_id)
{
    const std::int64_t timer_id = timer_id_by_correlation_id_.get(correlation_id);
    if (DeadlineTimerWheel::NULL_DEADLINE != timer_id)
    {
        (void)timer_id_by_correlation_id_.remove(correlation_id);
        (void)wheel_.cancel_timer(timer_id);
        (void)correlation_id_by_timer_id_.remove(timer_id);
        return true;
    }

    return false;
}

void WheelTimerService::snapshot(TimerSnapshotTaker& taker)
{
    timer_id_by_correlation_id_.for_each([this, &taker](std::int64_t correlation_id, std::int64_t timer_id)
                                         { taker.snapshot_timer(correlation_id, wheel_.deadline(timer_id)); });
}

void WheelTimerService::current_time(std::int64_t now)
{
    wheel_.set_current_tick_time(now);
}

bool WheelTimerService::timer_id_for_correlation_id(std::int64_t correlation_id, std::int64_t& timer_id) const noexcept
{
    timer_id = timer_id_by_correlation_id_.get(correlation_id);
    return DeadlineTimerWheel::NULL_DEADLINE != timer_id;
}

}  // namespace deepmsg::cluster::common::timer
