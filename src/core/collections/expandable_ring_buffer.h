#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace axc::core
{

/// Bit-exact port of Agrona 2.6.1 `org.agrona.ExpandableRingBuffer` (the
/// version Aeron 1.54.0 pins), for the pending-service-message tracker.
///
/// Layout, from `javap -p -c` against `agrona-2.6.1.jar` (the Rust track's
/// `ring.rs` read the same semantics out of the jar and pinned them with a
/// fixture): a byte ring of power-of-two capacity; every message is framed by
/// an 8-byte header -- a `u32` length (header included) and a `u32` type
/// (`0` padding, `1` data). Frames are 8-byte aligned. A frame that would wrap
/// is preceded by a padding frame that fills the end of the buffer.
///
/// What the tracker depends on:
///  * `append` returning `false` is the capacity signal; it otherwise grows
///    the buffer (`PendingServiceMessageTracker.java:99-103`).
///  * `for_each(head_offset, ...)` resumes **after** the message whose end
///    offset is `head_offset`; the consumer is handed the offset just past the
///    message it is looking at, which is what the tracker stores between polls.
///  * `consume` advances the head by the bytes it really consumed and returns
///    that number; a consumer that answers `false` leaves its message in place
///    and backs the head up.
class ExpandableRingBuffer
{
public:
    /// `ExpandableRingBuffer.MAX_CAPACITY`: 1 GiB.
    static constexpr std::size_t MAX_CAPACITY = std::size_t{1} << 30;
    /// `ExpandableRingBuffer.HEADER_LENGTH` / `HEADER_ALIGNMENT`.
    static constexpr std::size_t HEADER_LENGTH = 8;
    static constexpr std::size_t HEADER_ALIGNMENT = 8;

    ExpandableRingBuffer() = default;

    /// `new ExpandableRingBuffer(maxCapacity, initialCapacity, isDirect)`.
    /// Both capacities must be powers of two (0 is accepted for
    /// `initial_capacity`, as the reference does), and `initial_capacity` must
    /// not exceed `max_capacity`.
    explicit ExpandableRingBuffer(std::size_t max_capacity, std::size_t initial_capacity = 0);

    /// `capacity()`: the bytes allocated.
    [[nodiscard]] std::size_t capacity() const noexcept
    {
        return capacity_;
    }

    /// `size()`: `tail - head`, the bytes of framed messages stored.
    [[nodiscard]] std::size_t size() const noexcept
    {
        return tail_ - head_;
    }

    [[nodiscard]] bool empty() const noexcept
    {
        return size() == 0;
    }

    [[nodiscard]] std::size_t head() const noexcept
    {
        return head_;
    }

    [[nodiscard]] std::size_t tail() const noexcept
    {
        return tail_;
    }

    /// `reset(capacity)`: a fresh buffer of the next power of two at or above
    /// `capacity` (0 maps to 1, as Agrona's `findNextPositivePowerOfTwo` does).
    /// Returns false when that exceeds the maximum capacity.
    [[nodiscard]] bool reset(std::size_t capacity);

    /// `append(buffer, offset, length)`. `false` means the frame does not fit
    /// and the buffer could not grow.
    [[nodiscard]] bool append(std::span<const std::uint8_t> payload);

    /// `forEach(headOffset, consumer, limit)` -- walk without consuming.
    /// `consumer(std::span<const uint8_t> message, std::size_t head_offset)`
    /// answers whether to keep walking. Returns how many data messages were
    /// handed over (a consumer that answers `false` is not counted).
    template <class Consumer>
    std::size_t for_each(std::size_t head_offset, Consumer&& consumer, std::size_t limit) const
    {
        const std::size_t aligned_head = check_head_offset(head_offset);
        if (0 == capacity_ || 0 == limit)
        {
            return 0;
        }

        std::size_t offset = head_ + aligned_head;
        std::size_t count = 0;

        while (count < limit && offset < tail_)
        {
            const std::size_t index = offset & mask_;
            const std::size_t length = get_u32(index);
            const std::uint32_t message_type = get_u32(index + 4);
            if (!is_valid_frame(length, offset))
            {
                break;
            }
            const std::size_t aligned = align(length);
            offset += aligned;

            if (PADDING != message_type)
            {
                const std::uint8_t* message = buffer_.data() + index + HEADER_LENGTH;
                if (!consumer(std::span<const std::uint8_t>{message, length - HEADER_LENGTH}, offset - head_))
                {
                    break;
                }
                ++count;
            }
        }

        return count;
    }

    /// Agrona's `MessageConsumer` is handed a `MutableDirectBuffer`; the
    /// tracker writes the append position back into the stored copy (`:227`),
    /// so this is the form it needs.
    template <class Consumer>
    std::size_t for_each_mut(std::size_t head_offset, Consumer&& consumer, std::size_t limit)
    {
        const std::size_t aligned_head = check_head_offset(head_offset);
        if (0 == capacity_ || 0 == limit)
        {
            return 0;
        }

        std::size_t offset = head_ + aligned_head;
        std::size_t count = 0;

        while (count < limit && offset < tail_)
        {
            const std::size_t index = offset & mask_;
            const std::size_t length = get_u32(index);
            const std::uint32_t message_type = get_u32(index + 4);
            if (!is_valid_frame(length, offset))
            {
                break;
            }
            const std::size_t aligned = align(length);
            offset += aligned;

            if (PADDING != message_type)
            {
                std::uint8_t* message = buffer_.data() + index + HEADER_LENGTH;
                if (!consumer(std::span<std::uint8_t>{message, length - HEADER_LENGTH}, offset - head_))
                {
                    break;
                }
                ++count;
            }
        }

        return count;
    }

    /// `consume(consumer, limit)` -- walk and advance the head. Returns the
    /// bytes consumed. A consumer that answers `false` leaves its message and
    /// everything after it in place.
    template <class Consumer>
    std::size_t consume(Consumer&& consumer, std::size_t limit)
    {
        if (0 == capacity_ || 0 == limit)
        {
            return 0;
        }

        std::size_t offset = head_;
        std::size_t count = 0;

        while (count < limit && offset < tail_)
        {
            const std::size_t index = offset & mask_;
            const std::size_t length = get_u32(index);
            const std::uint32_t message_type = get_u32(index + 4);
            if (!is_valid_frame(length, offset))
            {
                break;
            }
            const std::size_t aligned = align(length);
            offset += aligned;

            if (PADDING != message_type)
            {
                const std::uint8_t* message = buffer_.data() + index + HEADER_LENGTH;
                if (!consumer(std::span<const std::uint8_t>{message, length - HEADER_LENGTH}, offset - head_))
                {
                    offset -= aligned;
                    break;
                }
                ++count;
            }
        }

        const std::size_t consumed = offset - head_;
        head_ = offset;
        return consumed;
    }

private:
    static constexpr std::uint32_t PADDING = 0;
    static constexpr std::uint32_t DATA = 1;

    static constexpr std::size_t align(std::size_t value) noexcept
    {
        return (value + HEADER_ALIGNMENT - 1) & ~(HEADER_ALIGNMENT - 1);
    }

    [[nodiscard]] std::size_t check_head_offset(std::size_t head_offset) const noexcept
    {
        // Agrona throws `IllegalArgumentException` on an out-of-range head
        // offset; the tracker's own bookkeeping keeps this invariant, so a
        // violation is a caller bug. Clamp rather than read outside the ring.
        return head_offset > size() ? size() : head_offset;
    }

    [[nodiscard]] bool is_valid_frame(std::size_t length, std::size_t offset) const noexcept
    {
        // A well-formed frame at most fills the ring; a corrupt length would
        // otherwise spin or read outside the buffer (a snapshot is untrusted).
        return length >= HEADER_LENGTH && length <= capacity_ && offset + align(length) <= tail_;
    }

    [[nodiscard]] bool resize(std::size_t required_capacity);
    void write_message(std::span<const std::uint8_t> payload);

    [[nodiscard]] std::uint32_t get_u32(std::size_t index) const noexcept
    {
        return static_cast<std::uint32_t>(buffer_[index]) | (static_cast<std::uint32_t>(buffer_[index + 1]) << 8) |
               (static_cast<std::uint32_t>(buffer_[index + 2]) << 16) |
               (static_cast<std::uint32_t>(buffer_[index + 3]) << 24);
    }

    void put_u32(std::size_t index, std::uint32_t value) noexcept
    {
        buffer_[index] = static_cast<std::uint8_t>(value);
        buffer_[index + 1] = static_cast<std::uint8_t>(value >> 8);
        buffer_[index + 2] = static_cast<std::uint8_t>(value >> 16);
        buffer_[index + 3] = static_cast<std::uint8_t>(value >> 24);
    }

    std::vector<std::uint8_t> buffer_{};
    std::size_t capacity_{0};
    std::size_t mask_{0};
    std::size_t max_capacity_{MAX_CAPACITY};
    std::size_t head_{0};
    std::size_t tail_{0};
};

}  // namespace axc::core
