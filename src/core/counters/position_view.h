#pragma once

#include <atomic>
#include <cstdint>

#include <aeronc.h>

namespace axc::core
{

/// Read-only view of a counter owned by somebody else (recording position,
/// commit position, ...). The address is resolved once; reads afterwards are a
/// single acquire load and never scan the counters directory.
class PositionView
{
public:
    PositionView() noexcept = default;

    static PositionView at(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
    {
        PositionView view;
        view.reader_ = reader;
        view.counter_id_ = counter_id;

        if (nullptr != reader && counter_id >= 0 && counter_id <= aeron_counters_reader_max_counter_id(reader))
        {
            view.addr_ = aeron_counters_reader_addr(reader, counter_id);
        }

        return view;
    }

    [[nodiscard]] bool valid() const noexcept
    {
        return nullptr != addr_;
    }

    [[nodiscard]] std::int32_t counter_id() const noexcept
    {
        return counter_id_;
    }

    [[nodiscard]] std::int64_t get_acquire() const noexcept
    {
        return nullptr == addr_ ? 0 : std::atomic_ref<std::int64_t>(*addr_).load(std::memory_order_acquire);
    }

    /// True when this slot is allocated to `registration_id` (constant time, no
    /// directory scan).
    [[nodiscard]] bool is_allocated_by(std::int64_t registration_id) const noexcept
    {
        if (nullptr == addr_ || nullptr == reader_)
        {
            return false;
        }

        std::int64_t actual = 0;
        if (0 != aeron_counters_reader_counter_registration_id(reader_, counter_id_, &actual))
        {
            return false;
        }

        return actual == registration_id;
    }

private:
    aeron_counters_reader_t* reader_{nullptr};
    std::int32_t counter_id_{-1};
    std::int64_t* addr_{nullptr};
};

}  // namespace axc::core
