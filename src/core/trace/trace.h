#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace axc::core
{

struct TraceRecord
{
    std::int32_t code{0};
    std::int64_t value_a{0};
    std::int64_t value_b{0};
};

/// Fixed-size lock-free ring of binary trace events.
///
/// No formatting happens on the hot path: events are three integers; rendering
/// is done by the reader. Trace codes follow the reference event codes of each
/// area (archive/cluster) once those are transcribed.
class TraceRing
{
public:
    static constexpr std::size_t CAPACITY = 1024;

    static TraceRing& instance() noexcept;

    void record(std::int32_t code, std::int64_t value_a, std::int64_t value_b) noexcept;

    /// Drains the records not yet consumed. At most CAPACITY records survive.
    template <class Consumer>
    std::size_t drain(Consumer&& consumer) noexcept
    {
        const std::uint64_t written = write_index_.load(std::memory_order_acquire);
        std::uint64_t available = written - read_index_;
        if (available > CAPACITY)
        {
            read_index_ = written - CAPACITY;
            available = CAPACITY;
        }

        for (std::uint64_t index = read_index_; index < written; ++index)
        {
            consumer(records_[static_cast<std::size_t>(index % CAPACITY)]);
        }

        read_index_ = written;
        return static_cast<std::size_t>(available);
    }

    [[nodiscard]] std::uint64_t written() const noexcept
    {
        return write_index_.load(std::memory_order_acquire);
    }

    void clear() noexcept;

private:
    std::array<TraceRecord, CAPACITY> records_{};
    std::atomic<std::uint64_t> write_index_{0};
    std::uint64_t read_index_{0};
};

}  // namespace axc::core

#if defined(AXC_TRACING)
#define AXC_TRACE(code, value_a, value_b) ::axc::core::TraceRing::instance().record((code), (value_a), (value_b))
#else
#define AXC_TRACE(code, value_a, value_b) ((void)0)
#endif
