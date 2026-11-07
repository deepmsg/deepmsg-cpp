#include "core/collections/long2long_hash_map.h"

#include <algorithm>
#include <bit>
#include <stdexcept>

namespace axc::core
{
namespace
{

int next_positive_power_of_two(int value) noexcept
{
    int result = 1;
    while (result < value)
    {
        result <<= 1;
    }
    return result;
}

bool is_power_of_two(int value) noexcept
{
    return value > 0 && 0 == (value & (value - 1));
}

}  // namespace

Long2LongHashMap::Long2LongHashMap(int initial_capacity, float load_factor, std::int64_t missing_value)
    : load_factor_(load_factor), missing_value_(missing_value)
{
    if (!(load_factor > 0.0F && load_factor < 1.0F))
    {
        throw std::invalid_argument("load factor must be in (0, 1)");
    }
    capacity(next_positive_power_of_two(std::max(MIN_CAPACITY, initial_capacity)));
}

std::int32_t Long2LongHashMap::hash(std::int64_t value) noexcept
{
    auto x = static_cast<std::uint64_t>(value);
    x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31U);
    return std::bit_cast<std::int32_t>(static_cast<std::uint32_t>(x) ^ static_cast<std::uint32_t>(x >> 32U));
}

std::int32_t Long2LongHashMap::even_hash(std::int64_t value, std::int32_t mask) noexcept
{
    const auto hash_value = static_cast<std::uint32_t>(hash(value));
    const auto even_value = (hash_value << 1U) - (hash_value << 8U);
    return std::bit_cast<std::int32_t>(even_value & static_cast<std::uint32_t>(mask));
}

void Long2LongHashMap::capacity(int new_capacity)
{
    if (!is_power_of_two(new_capacity))
    {
        throw std::invalid_argument("capacity must be a power of two");
    }

    const auto entries_length = static_cast<std::size_t>(new_capacity) * 2;
    resize_threshold_ = static_cast<int>(static_cast<float>(new_capacity) * load_factor_);
    entries_.assign(entries_length, missing_value_);
}

int Long2LongHashMap::find_key(std::int64_t key) const noexcept
{
    const int mask = static_cast<int>(entries_.size()) - 1;
    int index = even_hash(key, mask);

    while (missing_value_ != entries_[static_cast<std::size_t>(index + 1)])
    {
        if (key == entries_[static_cast<std::size_t>(index)])
        {
            return index;
        }
        index = (index + 2) & mask;
    }

    return -1;
}

std::int64_t Long2LongHashMap::get(std::int64_t key) const noexcept
{
    const int index = find_key(key);
    return index < 0 ? missing_value_ : entries_[static_cast<std::size_t>(index + 1)];
}

std::int64_t Long2LongHashMap::get(std::int64_t key, std::int64_t default_value) const noexcept
{
    const std::int64_t value = get(key);
    return missing_value_ == value ? default_value : value;
}

bool Long2LongHashMap::contains_key(std::int64_t key) const noexcept
{
    return missing_value_ != get(key);
}

std::int64_t Long2LongHashMap::put(std::int64_t key, std::int64_t value) noexcept
{
    if (missing_value_ == value)
    {
        return missing_value_;
    }

    const int mask = static_cast<int>(entries_.size()) - 1;
    int index = even_hash(key, mask);

    std::int64_t old_value = missing_value_;
    while (missing_value_ != (old_value = entries_[static_cast<std::size_t>(index + 1)]))
    {
        if (key == entries_[static_cast<std::size_t>(index)])
        {
            break;
        }
        index = (index + 2) & mask;
    }

    if (missing_value_ == old_value)
    {
        ++size_;
        entries_[static_cast<std::size_t>(index)] = key;
    }

    entries_[static_cast<std::size_t>(index + 1)] = value;

    increase_capacity();

    return old_value;
}

bool Long2LongHashMap::remove(std::int64_t key) noexcept
{
    const int index = find_key(key);
    if (index < 0)
    {
        return false;
    }

    entries_[static_cast<std::size_t>(index + 1)] = missing_value_;
    --size_;
    compact_chain(index);
    return true;
}

bool Long2LongHashMap::remove(std::int64_t key, std::int64_t value) noexcept
{
    const int index = find_key(key);
    if (index < 0 || value != entries_[static_cast<std::size_t>(index + 1)])
    {
        return false;
    }

    entries_[static_cast<std::size_t>(index + 1)] = missing_value_;
    --size_;
    compact_chain(index);
    return true;
}

void Long2LongHashMap::clear() noexcept
{
    std::fill(entries_.begin(), entries_.end(), missing_value_);
    size_ = 0;
}

void Long2LongHashMap::increase_capacity()
{
    if (size_ > static_cast<std::size_t>(resize_threshold_))
    {
        // entries_.size() == 2 * logical capacity, which is the new capacity.
        rehash(static_cast<int>(entries_.size()));
    }
}

void Long2LongHashMap::rehash(int new_capacity)
{
    const std::vector<std::int64_t> old_entries = entries_;
    capacity(new_capacity);

    const int mask = static_cast<int>(entries_.size()) - 1;

    for (std::size_t value_index = 1; value_index < old_entries.size(); value_index += 2)
    {
        if (missing_value_ == old_entries[value_index])
        {
            continue;
        }

        const std::int64_t key = old_entries[value_index - 1];
        int new_key_index = even_hash(key, mask);
        while (missing_value_ != entries_[static_cast<std::size_t>(new_key_index + 1)])
        {
            new_key_index = (new_key_index + 2) & mask;
        }

        entries_[static_cast<std::size_t>(new_key_index)] = key;
        entries_[static_cast<std::size_t>(new_key_index + 1)] = old_entries[value_index];
    }
}

void Long2LongHashMap::compact_chain(int delete_key_index)
{
    const int mask = static_cast<int>(entries_.size()) - 1;
    int key_index = delete_key_index;

    for (;;)
    {
        key_index = (key_index + 2) & mask;
        const std::int64_t value = entries_[static_cast<std::size_t>(key_index + 1)];
        if (missing_value_ == value)
        {
            break;
        }

        const std::int64_t key = entries_[static_cast<std::size_t>(key_index)];
        const int hash_position = even_hash(key, mask);

        if ((key_index < hash_position && (hash_position <= delete_key_index || delete_key_index <= key_index)) ||
            (hash_position <= delete_key_index && delete_key_index <= key_index))
        {
            entries_[static_cast<std::size_t>(delete_key_index)] = key;
            entries_[static_cast<std::size_t>(delete_key_index + 1)] = value;
            entries_[static_cast<std::size_t>(key_index + 1)] = missing_value_;
            delete_key_index = key_index;
        }
    }
}

}  // namespace axc::core
