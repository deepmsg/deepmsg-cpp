#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace axc::core
{

/// Bit-exact port of Agrona 2.7.0 `org.agrona.collections.Long2LongHashMap`,
/// which is the Agrona version Aeron 1.54.0 pins.
///
/// Iteration order is a byte contract: cluster snapshots serialise timers in
/// this order. Do not substitute `std::unordered_map`, do not "improve" the
/// hash, probing or resize policy.
///
/// The map allocates only when it grows (in the reference too). Pre-size it
/// with `reserve()` semantics by constructing with the final capacity where a
/// preallocated table is required (cluster timer tables do this). Because a
/// growth allocates, `put` is the one mutator that is not `noexcept`.
class Long2LongHashMap
{
public:
    static constexpr int MIN_CAPACITY = 8;
    static constexpr float DEFAULT_LOAD_FACTOR = 0.65F;

    explicit Long2LongHashMap(int initial_capacity = MIN_CAPACITY, float load_factor = DEFAULT_LOAD_FACTOR,
                              std::int64_t missing_value = -1);

    Long2LongHashMap(const Long2LongHashMap&) = default;
    Long2LongHashMap& operator=(const Long2LongHashMap&) = default;
    Long2LongHashMap(Long2LongHashMap&&) noexcept = default;
    Long2LongHashMap& operator=(Long2LongHashMap&&) noexcept = default;
    ~Long2LongHashMap() = default;

    /// Agrona `Hashing.hash(long)` -- `h = x ^ (x >>> 30); h *= 0xbf58476d1ce4e5b9;
    /// h = h ^ (h >>> 27); h *= 0x94d049bb133111eb; h ^= h >>> 31; return (int)h ^ (int)(h >>> 32)`.
    [[nodiscard]] static std::int32_t hash(std::int64_t value) noexcept;

    /// Agrona `Hashing.evenHash(long, int)`: `((hash << 1) - (hash << 8)) & mask`.
    [[nodiscard]] static std::int32_t even_hash(std::int64_t value, std::int32_t mask) noexcept;

    [[nodiscard]] std::int64_t get(std::int64_t key) const noexcept;
    [[nodiscard]] std::int64_t get(std::int64_t key, std::int64_t default_value) const noexcept;
    [[nodiscard]] bool contains_key(std::int64_t key) const noexcept;

    /// Returns the previous value, or `missing_value()` when the key was absent.
    /// A `value == missing_value()` is rejected by returning `missing_value()`
    /// without inserting (Agrona throws; hot paths must not throw).
    std::int64_t put(std::int64_t key, std::int64_t value);

    bool remove(std::int64_t key) noexcept;
    bool remove(std::int64_t key, std::int64_t value) noexcept;
    void clear() noexcept;

    [[nodiscard]] std::size_t size() const noexcept
    {
        return size_;
    }

    [[nodiscard]] bool empty() const noexcept
    {
        return 0 == size_;
    }

    /// Logical capacity (entries_ holds two slots per key).
    [[nodiscard]] int capacity() const noexcept
    {
        return static_cast<int>(entries_.size() / 2);
    }

    [[nodiscard]] int resize_threshold() const noexcept
    {
        return resize_threshold_;
    }

    [[nodiscard]] std::int64_t missing_value() const noexcept
    {
        return missing_value_;
    }

    /// Visits (key, value) in Agrona's iteration order: starting just before the
    /// first empty slot found from the top, walking backwards by 2 with wrap.
    template <class Consumer>
    void for_each(Consumer&& consumer) const
    {
        const int length = static_cast<int>(entries_.size());
        const int mask = length - 1;
        int key_index = length;

        if (missing_value_ != entries_[static_cast<std::size_t>(length) - 1])
        {
            for (int i = 1; i < length; i += 2)
            {
                if (missing_value_ == entries_[static_cast<std::size_t>(i)])
                {
                    key_index = i - 1;
                    break;
                }
            }
        }

        const int stop_counter = key_index;
        const int position_counter = key_index + length;

        for (int position = position_counter - 2; position >= stop_counter; position -= 2)
        {
            const int slot = position & mask;
            if (missing_value_ != entries_[static_cast<std::size_t>(slot) + 1])
            {
                consumer(entries_[static_cast<std::size_t>(slot)], entries_[static_cast<std::size_t>(slot) + 1]);
            }
        }
    }

private:
    void capacity(int new_capacity);
    void increase_capacity();
    void rehash(int new_capacity);
    void compact_chain(int delete_key_index);
    [[nodiscard]] int find_key(std::int64_t key) const noexcept;

    std::vector<std::int64_t> entries_;
    float load_factor_;
    int resize_threshold_{0};
    std::size_t size_{0};
    std::int64_t missing_value_{0};
};

}  // namespace axc::core
