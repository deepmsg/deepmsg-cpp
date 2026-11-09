#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

#include "core/deepmsg/handles.h"

namespace axc::core
{

/// Owns a counter and writes its value. The address is resolved once; writes are
/// a single release store.
class PositionWriter
{
public:
    /// Empty writer (`valid()` false); assigned later once the counter exists.
    PositionWriter() noexcept = default;

    explicit PositionWriter(CounterPtr counter) noexcept
        : counter_(std::move(counter)), addr_(nullptr == counter_ ? nullptr : aeron_counter_addr(counter_.get()))
    {
    }

    PositionWriter(PositionWriter&&) noexcept = default;
    PositionWriter& operator=(PositionWriter&&) noexcept = default;
    PositionWriter(const PositionWriter&) = delete;
    PositionWriter& operator=(const PositionWriter&) = delete;

    [[nodiscard]] std::int32_t id() const noexcept
    {
        aeron_counter_constants_t constants{};
        return nullptr == counter_ || 0 != aeron_counter_constants(counter_.get(), &constants) ? -1
                                                                                               : constants.counter_id;
    }

    /// The driver's counter registration id (`Counter.registrationId()`).
    [[nodiscard]] std::int64_t registration_id() const noexcept
    {
        aeron_counter_constants_t constants{};
        return nullptr == counter_ || 0 != aeron_counter_constants(counter_.get(), &constants)
                   ? -1
                   : constants.registration_id;
    }

    [[nodiscard]] bool valid() const noexcept
    {
        return nullptr != addr_;
    }

    void set_release(std::int64_t value) noexcept
    {
        std::atomic_ref<std::int64_t>(*addr_).store(value, std::memory_order_release);
    }

    void set_plain(std::int64_t value) noexcept
    {
        *addr_ = value;
    }

    [[nodiscard]] std::int64_t get() const noexcept
    {
        return *addr_;
    }

private:
    CounterPtr counter_;
    std::int64_t* addr_{nullptr};
};

}  // namespace axc::core
