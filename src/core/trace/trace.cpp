#include "core/trace/trace.h"

namespace axc::core
{

TraceRing& TraceRing::instance() noexcept
{
    static TraceRing ring;
    return ring;
}

void TraceRing::record(std::int32_t code, std::int64_t value_a, std::int64_t value_b) noexcept
{
    const std::uint64_t index = write_index_.fetch_add(1, std::memory_order_acq_rel);
    records_[static_cast<std::size_t>(index % CAPACITY)] = TraceRecord{code, value_a, value_b};
}

void TraceRing::clear() noexcept
{
    write_index_.store(0, std::memory_order_release);
    read_index_ = 0;
}

}  // namespace axc::core
