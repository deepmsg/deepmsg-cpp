#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "archive/server/recording_session.h"
#include "archive/server/recording_writer.h"
#include "archive/server/session_worker.h"
#include "core/collections/many_to_one_queue.h"
#include "core/counters/counter_target.h"
#include "core/counters/duty_cycle_tracker.h"

namespace deepmsg::archive::server
{

/// `ArchiveConductor.Recorder` (`ArchiveConductor.java:2737-2774`): the live
/// recording sessions plus the three write metrics they feed.
///
/// The counters are published only on a pass that did work (`workCount > 0`),
/// exactly as the reference does (`:2788-2796`).
class Recorder final : public RecordingMetrics
{
public:
    Recorder() = default;

    Recorder(axc::core::CounterTarget* total_write_bytes, axc::core::CounterTarget* total_write_time_ns,
             axc::core::CounterTarget* max_write_time_ns) noexcept;

    /// The counters are allocated lazily during conductor startup, after this
    /// object exists.
    void set_counters(axc::core::CounterTarget* total_write_bytes, axc::core::CounterTarget* total_write_time_ns,
                      axc::core::CounterTarget* max_write_time_ns) noexcept;

    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;

    void add_session(std::unique_ptr<RecordingSession> session);

    /// DEDICATED mode (`DedicatedModeArchiveConductor.java:143-283`): the
    /// conductor hands sessions over through `inbox`, finished sessions go back
    /// through `close_queue` for the conductor to close.
    void set_queues(axc::core::ManyToOneQueue<RecordingSession>* inbox,
                    axc::core::ManyToOneQueue<RecordingSession>* close_queue) noexcept;

    void set_duty_cycle_tracker(axc::core::DutyCycleTracker* tracker) noexcept
    {
        duty_cycle_ = tracker;
    }

    /// `DedicatedModeRecorder.onStart` (`DedicatedModeArchiveConductor.java:180-186`).
    void on_start_threaded() noexcept;

    [[nodiscard]] bool threaded() const noexcept
    {
        return nullptr != inbox_;
    }

    /// One recorder-agent pass: drain the inbox, run the sessions, hand the
    /// finished ones back.
    int do_work_threaded();

    /// Shutdown: drain the inbox, hand every remaining session back (its close
    /// belongs to the conductor), and empty the worker.
    void close_threaded();

    /// Close every session (writer + position counter) and empty the worker.
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

    // `RecordingWriter` feeds these (`RecordingWriter.java:143-145`).
    void bytes_written(std::int32_t length) noexcept override;
    void write_time_ns(std::int64_t duration) noexcept override;

    [[nodiscard]] std::int64_t total_write_bytes() const noexcept
    {
        return total_write_bytes_;
    }

    [[nodiscard]] std::int64_t total_write_time_ns() const noexcept
    {
        return total_write_time_ns_;
    }

    [[nodiscard]] std::int64_t max_write_time_ns() const noexcept
    {
        return max_write_time_ns_;
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
    void hand_back(RecordingSession* session) noexcept;

    SessionWorker<RecordingSession> sessions_;
    axc::core::ManyToOneQueue<RecordingSession>* inbox_{nullptr};
    axc::core::ManyToOneQueue<RecordingSession>* close_queue_{nullptr};
    axc::core::DutyCycleTracker* duty_cycle_{nullptr};
    axc::core::CounterTarget* total_write_bytes_counter_{nullptr};
    axc::core::CounterTarget* total_write_time_counter_{nullptr};
    axc::core::CounterTarget* max_write_time_counter_{nullptr};

    std::int64_t total_write_bytes_{0};
    std::int64_t total_write_time_ns_{0};
    std::int64_t max_write_time_ns_{0};
};

}  // namespace deepmsg::archive::server
