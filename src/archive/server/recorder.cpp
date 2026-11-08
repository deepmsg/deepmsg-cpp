#include "archive/server/recorder.h"

#include <utility>

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
