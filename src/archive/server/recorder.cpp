#include "archive/server/recorder.h"

#include <sched.h>

#include <utility>

#include "core/time/clock.h"

namespace deepmsg::archive::server
{

void Recorder::add_session(std::unique_ptr<RecordingSession> session)
{
    sessions_.add(std::move(session));
}

void Recorder::close()
{
    sessions_ = SessionWorker<RecordingSession>{};
    total_write_bytes_.store(0, std::memory_order_relaxed);
    total_write_time_ns_.store(0, std::memory_order_relaxed);
    max_write_time_ns_.store(0, std::memory_order_relaxed);
    worked_.store(false, std::memory_order_relaxed);
}

void Recorder::set_queues(axc::core::ManyToOneQueue<RecordingSession>* inbox,
                          axc::core::ManyToOneQueue<RecordingSession>* close_queue) noexcept
{
    inbox_ = inbox;
    close_queue_ = close_queue;
}

void Recorder::hand_back(RecordingSession* session) noexcept
{
    // `DedicatedModeRecorder.closeSession` (`DedicatedModeArchiveConductor.java:220-250`):
    // the close belongs to the conductor; retry until the queue takes it (it
    // only refuses when the node allocation fails).
    while (!close_queue_->push(session))
    {
        ::sched_yield();
    }
}

void Recorder::on_start_threaded() noexcept
{
    if (nullptr != duty_cycle_)
    {
        duty_cycle_->update(axc::core::NanoClock::now());
    }
}

int Recorder::do_work_threaded()
{
    // `DedicatedModeRecorder.doWork` (`DedicatedModeArchiveConductor.java:192-213`).
    int work_count = 0;

    if (nullptr != duty_cycle_)
    {
        duty_cycle_->measure_and_update(axc::core::NanoClock::now());
    }

    while (auto* session = inbox_->pop())
    {
        sessions_.add(std::unique_ptr<RecordingSession>(session));
        ++work_count;
    }

    work_count += sessions_.do_work_releasing([this](std::unique_ptr<RecordingSession> session)
                                              { hand_back(session.release()); });

    if (work_count > 0)
    {
        // The counters are published by the conductor's `summarise` pass
        // (A6f-2); a recorder only marks that this pass did work.
        worked_.store(true, std::memory_order_relaxed);
    }
    return work_count;
}

void Recorder::close_threaded()
{
    // `preSessionsClose` drains the inbox first
    // (`DedicatedModeArchiveConductor.java:215-219`), then every remaining
    // session goes back through the close queue.
    while (auto* session = inbox_->pop())
    {
        sessions_.add(std::unique_ptr<RecordingSession>(session));
    }
    sessions_.drain([this](std::unique_ptr<RecordingSession> session) { hand_back(session.release()); });
}

void Recorder::bytes_written(std::int32_t length) noexcept
{
    // `Recorder.bytesWritten` (`ArchiveConductor.java:2775-2778`).
    total_write_bytes_.fetch_add(length, std::memory_order_relaxed);
}

void Recorder::write_time_ns(std::int64_t duration) noexcept
{
    // `Recorder.writeTimeNs` (`ArchiveConductor.java:2780-2787`).
    total_write_time_ns_.fetch_add(duration, std::memory_order_relaxed);

    std::int64_t current = max_write_time_ns_.load(std::memory_order_relaxed);
    while (duration > current &&
           !max_write_time_ns_.compare_exchange_weak(current, duration, std::memory_order_relaxed))
    {
    }
}

Recorder::MetricsSummary Recorder::summarise(Recorder* const* recorders, std::size_t count) noexcept
{
    // `Recorder.doWork` (`ArchiveConductor.java:2788-2796`) publishes only a pass
    // that did work; with N recorders the conductor is the one writer and this
    // is the pass's total. `take` semantics: the flag is consumed here.
    MetricsSummary summary;
    for (std::size_t index = 0; index < count; index++)
    {
        Recorder& recorder = *recorders[index];
        summary.bytes += recorder.total_write_bytes();
        summary.time_ns += recorder.total_write_time_ns();
        if (recorder.max_write_time_ns() > summary.max_ns)
        {
            summary.max_ns = recorder.max_write_time_ns();
        }
        if (recorder.worked_.exchange(false, std::memory_order_relaxed))
        {
            summary.worked = true;
        }
    }
    return summary;
}

}  // namespace deepmsg::archive::server
