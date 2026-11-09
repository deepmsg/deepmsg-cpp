#include "cluster/service/container_context.h"

#include <utility>

#include "core/time/clock.h"

namespace deepmsg::cluster::service
{

std::string_view lifecycle_name(LifecycleCallback callback) noexcept
{
    switch (callback)
    {
        case LifecycleCallback::none:
            return "none";
        case LifecycleCallback::on_start:
            return "onStart";
        case LifecycleCallback::on_terminate:
            return "onTerminate";
        case LifecycleCallback::on_role_change:
            return "onRoleChange";
        case LifecycleCallback::do_background_work:
            return "doBackgroundWork";
    }
    return "none";
}

ContainerContext::ContainerContext(axc::core::IdleStrategy idle, std::string_view idle_name,
                                   common::ClusterMarkFile mark_file)
    : idle_(idle), fresh_idle_(idle), mark_file_(std::move(mark_file))
{
    (void)idle_name;
}

std::expected<bool, Error> ContainerContext::check_for_clock_tick(axc::core::DeepmsgClient& client, std::int64_t now_ns)
{
    if (abort_.load(std::memory_order_acquire) || shutdown_.load(std::memory_order_acquire))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "shutdown requested"));
    }

    if (now_ns - last_slow_tick_ns_ <= CLOCK_TICK_INTERVAL_NS)
    {
        return false;
    }
    last_slow_tick_ns_ = now_ns;

    // The reference's `aeronAgentInvoker.invoke()`: the conductor runs here and
    // nowhere else in the container.
    (void)client.do_work();

    if (abort_.load(std::memory_order_acquire) || aeron_is_closed(client.handle()))
    {
        abort_.store(true, std::memory_order_release);
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "unexpected Aeron close"));
    }

    if (commit_position_.has_value())
    {
        auto* reader = client.counters_reader();
        std::int64_t registration_id = 0;
        const bool alive = nullptr != reader &&
                           aeron_counters_reader_counter_registration_id(reader, commit_position_->counter_id,
                                                                         &registration_id) >= 0 &&
                           registration_id == commit_position_->registration_id;
        if (!alive)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                         "commit-pos counter unexpectedly closed, terminating"));
        }
    }

    const std::int64_t now_ms = axc::core::EpochClock::now_ms();
    if (now_ms >= mark_file_update_deadline_ms_)
    {
        mark_file_update_deadline_ms_ = now_ms + MARK_FILE_UPDATE_INTERVAL_NS / 1'000'000;
        mark_file_.update_activity_timestamp(now_ms);
    }

    return true;
}

std::expected<void, Error> ContainerContext::idle(axc::core::DeepmsgClient& client, int work)
{
    idle_.idle(work);
    if (0 == work)
    {
        auto ticked = check_for_clock_tick(client, axc::core::NanoClock::now());
        if (!ticked)
        {
            return std::unexpected(ticked.error());
        }
    }
    return {};
}

}  // namespace deepmsg::cluster::service
