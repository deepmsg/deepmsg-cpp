#include "archive/server/recorder.h"

#include <sched.h>

#include <utility>

#include "core/time/clock.h"

namespace deepmsg::archive::server
{

Recorder::Recorder(axc::core::CounterTarget* total_write_bytes, axc::core::CounterTarget* total_write_time_ns,
                   axc::core::CounterTarget* max_write_time_ns) noexcept
    : total_write_bytes_counter_(total_write_bytes),
      total_write_time_counter_(total_write_time_ns),
      max_write_time_counter_(max_write_time_ns)
{
}

void Recorder::set_counters(axc::core::CounterTarget* total_write_bytes, axc::core::CounterTarget* total_write_time_ns,
                            axc::core::CounterTarget* max_write_time_ns) noexcept
{
    total_write_bytes_counter_ = total_write_bytes;
    total_write_time_counter_ = total_write_time_ns;
    max_write_time_counter_ = max_write_time_ns;
}

void Recorder::add_session(std::unique_ptr<RecordingSession> session)
{
    sessions_.add(std::move(session));
}

void Recorder::close()
{
    sessions_ = SessionWorker<RecordingSession>{};
    total_write_bytes_ = 0;
    total_write_time_ns_ = 0;
    max_write_time_ns_ = 0;
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
        publish();
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
    total_write_bytes_ += length;
}

void Recorder::write_time_ns(std::int64_t duration) noexcept
{
    // `Recorder.writeTimeNs` (`ArchiveConductor.java:2780-2787`).
    total_write_time_ns_ += duration;
    if (duration > max_write_time_ns_)
    {
        max_write_time_ns_ = duration;
    }
}

void Recorder::publish() noexcept
{
    // `Recorder.doWork` (`ArchiveConductor.java:2788-2796`): only a pass that did
    // work publishes, which is what `AeronStat` readers see.
    if (nullptr != total_write_bytes_counter_)
    {
        total_write_bytes_counter_->set_value(total_write_bytes_);
    }
    if (nullptr != total_write_time_counter_)
    {
        total_write_time_counter_->set_value(total_write_time_ns_);
    }
    if (nullptr != max_write_time_counter_)
    {
        max_write_time_counter_->set_value(max_write_time_ns_);
    }
}

}  // namespace deepmsg::archive::server
