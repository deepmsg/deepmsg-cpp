#include "core/media/dpdk/stack/neighbour.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace axc::core::media::dpdk::neighbour
{

namespace
{

/// `now - then`, where a clock that went backwards counts as no time at all.
[[nodiscard]] std::int64_t elapsed(std::int64_t now, std::int64_t then) noexcept
{
    return now > then ? now - then : 0;
}

}  // namespace

NeighbourTable::NeighbourTable(std::size_t capacity, std::int64_t timeout_ns, std::int64_t interval_ns)
    : capacity_{std::max<std::size_t>(capacity, 1)}, timeout_ns_{timeout_ns}, interval_ns_{interval_ns}
{
    entries_.reserve(capacity_);
}

void NeighbourTable::insert_fixed(std::uint32_t address, Mac hardware)
{
    if (const auto index = position(address))
    {
        entries_[*index].hardware = hardware;
        entries_[*index].resolved = true;
        entries_[*index].fixed = true;
        return;
    }
    push(Entry{address, hardware, 0, std::nullopt, true, true});
}

std::optional<Mac> NeighbourTable::resolve(std::uint32_t address, std::int64_t now) const
{
    const auto index = position(address);
    if (!index)
    {
        return std::nullopt;
    }
    const Entry& entry = entries_[*index];
    if (!entry.resolved)
    {
        return std::nullopt;
    }
    if (entry.fixed || elapsed(now, entry.refreshed_ns) < timeout_ns_)
    {
        return entry.hardware;
    }
    return std::nullopt;
}

void NeighbourTable::learn(std::uint32_t address, Mac hardware, std::int64_t now)
{
    const auto index = position(address);
    if (index)
    {
        if (!entries_[*index].fixed)
        {
            entries_[*index].hardware = hardware;
            entries_[*index].refreshed_ns = now;
            entries_[*index].resolved = true;
        }
        return;
    }
    push(Entry{address, hardware, now, std::nullopt, true, false});
}

bool NeighbourTable::ask(std::uint32_t address, std::int64_t now)
{
    const auto index = position(address);
    if (index)
    {
        const auto& asked = entries_[*index].asked_ns;
        if (asked && elapsed(now, *asked) < interval_ns_)
        {
            return false;
        }
        entries_[*index].asked_ns = now;
        return true;
    }

    push(Entry{address, Mac{}, 0, now, false, false});
    return true;
}

bool NeighbourTable::ask_due(std::uint32_t address, std::int64_t now) const
{
    const auto index = position(address);
    if (!index)
    {
        return true;
    }
    const auto& asked = entries_[*index].asked_ns;
    return !asked || elapsed(now, *asked) >= interval_ns_;
}

void NeighbourTable::forget(std::uint32_t address)
{
    std::erase_if(entries_, [address](const Entry& entry) { return entry.address == address; });
}

std::optional<std::size_t> NeighbourTable::position(std::uint32_t address) const
{
    for (std::size_t index = 0; index < entries_.size(); ++index)
    {
        if (entries_[index].address == address)
        {
            return index;
        }
    }
    return std::nullopt;
}

void NeighbourTable::push(Entry entry)
{
    if (entries_.size() < capacity_)
    {
        entries_.push_back(entry);
        return;
    }

    // Displace the least recently confirmed entry, never a fixed one: a
    // configured peer is worth more than one that was overheard.
    std::optional<std::size_t> victim;
    for (std::size_t index = 0; index < entries_.size(); ++index)
    {
        if (entries_[index].fixed)
        {
            continue;
        }
        if (!victim || entries_[index].refreshed_ns < entries_[*victim].refreshed_ns)
        {
            victim = index;
        }
    }
    if (victim)
    {
        entries_[*victim] = entry;
    }
}

bool on_the_same_link(std::uint32_t own, std::uint32_t other, std::uint8_t prefix_length) noexcept
{
    const std::uint8_t prefix = std::min<std::uint8_t>(prefix_length, 32);
    if (0 == prefix)
    {
        return true;
    }
    const std::uint32_t mask = 0xffffffffu << (32 - prefix);
    return (own & mask) == (other & mask);
}

}  // namespace axc::core::media::dpdk::neighbour
