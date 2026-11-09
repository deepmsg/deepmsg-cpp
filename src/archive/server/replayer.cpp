#include "archive/server/replayer.h"

#include <utility>

namespace deepmsg::archive::server
{

Replayer::Replayer(axc::core::CounterTarget* total_read_bytes, axc::core::CounterTarget* total_read_time_ns,
                   axc::core::CounterTarget* max_read_time_ns) noexcept
    : total_read_bytes_counter_(total_read_bytes),
      total_read_time_counter_(total_read_time_ns),
      max_read_time_counter_(max_read_time_ns)
{
}

void Replayer::set_counters(axc::core::CounterTarget* total_read_bytes, axc::core::CounterTarget* total_read_time_ns,
                            axc::core::CounterTarget* max_read_time_ns) noexcept
{
    total_read_bytes_counter_ = total_read_bytes;
    total_read_time_counter_ = total_read_time_ns;
    max_read_time_counter_ = max_read_time_ns;
}

void Replayer::add_session(std::unique_ptr<ReplaySession> session)
{
    sessions_.add(std::move(session));
}

void Replayer::close()
{
    sessions_ = SessionWorker<ReplaySession>{};
    total_read_bytes_ = 0;
    total_read_time_ns_ = 0;
    max_read_time_ns_ = 0;
}

void Replayer::bytes_read(std::int64_t bytes) noexcept
{
    // `Replayer.bytesRead` (`ArchiveConductor.java:2795-2798`).
    total_read_bytes_ += bytes;
}

void Replayer::read_time_ns(std::int64_t nanos) noexcept
{
    // `Replayer.readTimeNs` (`ArchiveConductor.java:2800-2806`).
    total_read_time_ns_ += nanos;
    if (nanos > max_read_time_ns_)
    {
        max_read_time_ns_ = nanos;
    }
}

void Replayer::publish() noexcept
{
    // `Replayer.doWork` (`ArchiveConductor.java:2808-2817`).
    if (nullptr != total_read_bytes_counter_)
    {
        total_read_bytes_counter_->set_value(total_read_bytes_);
    }
    if (nullptr != total_read_time_counter_)
    {
        total_read_time_counter_->set_value(total_read_time_ns_);
    }
    if (nullptr != max_read_time_counter_)
    {
        max_read_time_counter_->set_value(max_read_time_ns_);
    }
}

}  // namespace deepmsg::archive::server
