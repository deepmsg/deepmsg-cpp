#pragma once

#include <cstdint>

namespace deepmsg::cluster::common::timer
{

/// `TimerService.POLL_LIMIT` (`TimerService.java:57`): the most timers one
/// `poll` may expire.
inline constexpr std::int32_t POLL_LIMIT = 20;

/// Clock unit of the cluster (`ClusterTimeUnit` schema 111).
enum class ClusterTimeUnit : std::int32_t
{
    millis = 0,
    micros = 1,
    nanos = 2
};

/// `TimerService.TimerHandler`: `false` leaves the timer scheduled.
class TimerHandler
{
public:
    virtual ~TimerHandler() = default;
    [[nodiscard]] virtual bool on_timer_event(std::int64_t correlation_id) = 0;
};

/// `TimerService.TimerSnapshotTaker`: one record per scheduled timer.
class TimerSnapshotTaker
{
public:
    virtual ~TimerSnapshotTaker() = default;
    virtual void snapshot_timer(std::int64_t correlation_id, std::int64_t deadline) = 0;
};

/// `TimerService.java`.
///
/// The two implementations are selected at run time by
/// `aeron.cluster.timer.service.supplier`, so this is a virtual interface --
/// the one deliberate virtual boundary in this module and not a hot path (one
/// call per `doWork` round, the reference's own shape).
class TimerService
{
public:
    virtual ~TimerService() = default;

    /// Expire everything due at `now`, up to `POLL_LIMIT`.
    [[nodiscard]] virtual int poll(std::int64_t now, TimerHandler& handler) = 0;
    virtual void schedule_timer_for_correlation_id(std::int64_t correlation_id, std::int64_t deadline) = 0;
    [[nodiscard]] virtual bool cancel_timer_by_correlation_id(std::int64_t correlation_id) = 0;
    virtual void snapshot(TimerSnapshotTaker& taker) = 0;
    virtual void current_time(std::int64_t now) = 0;
};

}  // namespace deepmsg::cluster::common::timer
