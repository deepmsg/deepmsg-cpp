#include "core/collections/expandable_ring_buffer.h"

#include <cassert>
#include <cstring>
#include <utility>

namespace axc::core
{

namespace
{

[[nodiscard]] std::size_t next_power_of_two_at_least_one(std::size_t value) noexcept
{
    std::size_t power = 1;
    while (power < value)
    {
        const std::size_t next = power << 1;
        if (next <= power)
        {
            return value;  // overflow: the caller's maximum check rejects it
        }
        power = next;
    }
    return power;
}

[[nodiscard]] bool is_power_of_two(std::size_t value) noexcept
{
    return 0 != value && 0 == (value & (value - 1));
}

}  // namespace

ExpandableRingBuffer::ExpandableRingBuffer(std::size_t max_capacity, std::size_t initial_capacity)
    : buffer_(initial_capacity),
      capacity_(initial_capacity),
      mask_(0 == initial_capacity ? 0 : initial_capacity - 1),
      max_capacity_(max_capacity)
{
    // Agrona's own validation: both are powers of two (0 allowed for the
    // initial capacity) and the initial capacity does not exceed the maximum.
    assert(is_power_of_two(max_capacity));
    assert(0 == initial_capacity || is_power_of_two(initial_capacity));
    assert(initial_capacity <= max_capacity);
}

bool ExpandableRingBuffer::reset(std::size_t capacity)
{
    const std::size_t new_capacity = next_power_of_two_at_least_one(capacity);
    if (new_capacity > max_capacity_)
    {
        return false;
    }

    if (new_capacity != capacity_)
    {
        capacity_ = new_capacity;
        mask_ = new_capacity - 1;
        buffer_.assign(new_capacity, 0);
    }
    head_ = 0;
    tail_ = 0;
    return true;
}

bool ExpandableRingBuffer::append(std::span<const std::uint8_t> payload)
{
    const std::size_t head_index = head_ & mask_;
    const std::size_t tail_index = tail_ & mask_;
    const std::size_t length = align(HEADER_LENGTH + payload.size());
    std::size_t remaining = capacity_ - (tail_ - head_);

    if (length > remaining)
    {
        if (!resize(length))
        {
            return false;
        }
    }
    else if (tail_index >= head_index)
    {
        // A frame that would wrap is preceded by a padding frame, unless
        // resizing is cheaper.
        const std::size_t to_end = capacity_ - tail_index;
        if (length > to_end)
        {
            if (length <= remaining - to_end)
            {
                put_u32(tail_index, static_cast<std::uint32_t>(to_end));
                put_u32(tail_index + 4, PADDING);
                tail_ += to_end;
            }
            else if (!resize(length))
            {
                return false;
            }
        }
    }

    remaining = capacity_ - (tail_ - head_);
    if (length > remaining)
    {
        return false;
    }

    write_message(payload);
    tail_ += length;
    return true;
}

bool ExpandableRingBuffer::resize(std::size_t required_capacity)
{
    if (required_capacity > MAX_CAPACITY - capacity_)
    {
        return false;
    }
    const std::size_t new_capacity = next_power_of_two_at_least_one(capacity_ + required_capacity);
    if (new_capacity < capacity_ || new_capacity > max_capacity_)
    {
        return false;
    }

    const std::size_t head_index = head_ & mask_;
    const std::size_t size = this->size();
    const std::size_t to_end = capacity_ - head_index;
    const std::size_t first = size < to_end ? size : to_end;

    std::vector<std::uint8_t> new_buffer(new_capacity, 0);
    if (0 != first)
    {
        std::memcpy(new_buffer.data(), buffer_.data() + head_index, first);
    }
    if (first < size)
    {
        std::memcpy(new_buffer.data() + first, buffer_.data(), size - first);
    }

    buffer_ = std::move(new_buffer);
    capacity_ = new_capacity;
    mask_ = new_capacity - 1;
    head_ = 0;
    tail_ = size;
    return true;
}

void ExpandableRingBuffer::write_message(std::span<const std::uint8_t> payload)
{
    const std::size_t index = tail_ & mask_;
    const std::size_t length = HEADER_LENGTH + payload.size();
    put_u32(index, static_cast<std::uint32_t>(length));
    put_u32(index + 4, DATA);
    if (!payload.empty())
    {
        std::memcpy(buffer_.data() + index + HEADER_LENGTH, payload.data(), payload.size());
    }
}

}  // namespace axc::core
