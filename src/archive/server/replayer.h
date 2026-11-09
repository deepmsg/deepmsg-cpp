#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "archive/server/replay_session.h"
#include "archive/server/session_worker.h"
#include "core/collections/many_to_one_queue.h"
#include "core/counters/counter_target.h"
#include "core/counters/duty_cycle_tracker.h"

namespace deepmsg::archive::server
{

/// `ArchiveConductor.Replayer` (`ArchiveConductor.java:2781-2821`): the replay
/// sessions plus the three read metrics they feed.
///
/// Like the recorder, the counters are published only on a pass that did work
/// (`workCount > 0`).
class Replayer final : public ReplayMetrics
{
public:
    Replayer() = default;

    Replayer(axc::core::CounterTarget* total_read_bytes, axc::core::CounterTarget* total_read_time_ns,
             axc::core::CounterTarget* max_read_time_ns) noexcept;

    /// The counters are allocated lazily during conductor startup, after this
    /// object exists.
    void set_counters(axc::core::CounterTarget* total_read_bytes, axc::core::CounterTarget* total_read_time_ns,
                      axc::core::CounterTarget* max_read_time_ns) noexcept;

    Replayer(const Replayer&) = delete;
    Replayer& operator=(const Replayer&) = delete;

    void add_session(std::unique_ptr<ReplaySession> session);

    /// DEDICATED mode (`DedicatedModeArchiveConductor.java:285-434`).
    void set_queues(axc::core::ManyToOneQueue<ReplaySession>* inbox,
                    axc::core::ManyToOneQueue<ReplaySession>* close_queue) noexcept;

    void set_duty_cycle_tracker(axc::core::DutyCycleTracker* tracker) noexcept
    {
        duty_cycle_ = tracker;
    }

    /// `DedicatedModeReplayer.onStart` (`DedicatedModeArchiveConductor.java:330-336`).
    void on_start_threaded() noexcept;

    [[nodiscard]] bool threaded() const noexcept
    {
        return nullptr != inbox_;
    }

    /// One replayer-agent pass: drain the inbox, run the sessions, hand the
    /// finished ones back.
    int do_work_threaded();

    /// Shutdown: drain the inbox, hand every remaining session back, empty the
    /// worker.
    void close_threaded();

    /// Close every session (publication + segment file) and empty the worker.
    void close();

    /// `SessionWorker.doWork` plus the metric publication condition.
    template <class OnClose>
    int do_work(const OnClose& on_close)
    {
        const int work_count = sessions_.do_work(on_close);
        if (work_count > 0)
        {
            publish();
        }
        return work_count;
    }

    // `ReplaySession` feeds these (`ReplaySession.java:433-434`).
    void bytes_read(std::int64_t bytes) noexcept override;
    void read_time_ns(std::int64_t nanos) noexcept override;

    [[nodiscard]] std::int64_t total_read_bytes() const noexcept
    {
        return total_read_bytes_;
    }

    [[nodiscard]] std::int64_t total_read_time_ns() const noexcept
    {
        return total_read_time_ns_;
    }

    [[nodiscard]] std::int64_t max_read_time_ns() const noexcept
    {
        return max_read_time_ns_;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return sessions_.size();
    }

    template <class F>
    void for_each_session(F&& function)
    {
        sessions_.for_each(std::forward<F>(function));
    }

private:
    void publish() noexcept;
    void hand_back(ReplaySession* session) noexcept;

    SessionWorker<ReplaySession> sessions_;
    axc::core::ManyToOneQueue<ReplaySession>* inbox_{nullptr};
    axc::core::ManyToOneQueue<ReplaySession>* close_queue_{nullptr};
    axc::core::DutyCycleTracker* duty_cycle_{nullptr};
    axc::core::CounterTarget* total_read_bytes_counter_{nullptr};
    axc::core::CounterTarget* total_read_time_counter_{nullptr};
    axc::core::CounterTarget* max_read_time_counter_{nullptr};

    std::int64_t total_read_bytes_{0};
    std::int64_t total_read_time_ns_{0};
    std::int64_t max_read_time_ns_{0};
};

}  // namespace deepmsg::archive::server
