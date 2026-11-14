#include "core/media/dpdk/stack/ports.h"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace axc::core::media::dpdk::ports
{

namespace
{

/// One past the highest port.
constexpr std::size_t PORTS = 1u << 16;

/// The address field's value for a wildcard bind; no real address collides.
constexpr std::uint32_t WILDCARD = 0xffffffffu;

/// Bind and release happen on the conductor, one lock for the two of them;
/// the receive path never takes it. An explicit spin lock rather than
/// `std::mutex`, which this repository does not use.
class SpinLock
{
public:
    void lock() noexcept
    {
        while (flag_.test_and_set(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }

    void unlock() noexcept
    {
        flag_.clear(std::memory_order_release);
    }

private:
    std::atomic_flag flag_{};
};

class SpinGuard
{
public:
    explicit SpinGuard(SpinLock& lock) noexcept : lock_{lock}
    {
        lock_.lock();
    }
    ~SpinGuard()
    {
        lock_.unlock();
    }
    SpinGuard(const SpinGuard&) = delete;
    SpinGuard& operator=(const SpinGuard&) = delete;

private:
    SpinLock& lock_;
};

struct Slot
{
    std::atomic<std::uint32_t> address{WILDCARD};
    /// The connected peer, packed as `(address << 16) | port`, or zero.
    std::atomic<std::uint64_t> peer{0};
    std::atomic<std::uint32_t> generation{0};
};

struct Bindings
{
    std::vector<SlotId> free;
    std::uint16_t next_ephemeral{EPHEMERAL_PORTS_FIRST};
};

[[nodiscard]] BindFailure failure(int errcode, Ipv4Endpoint address) noexcept
{
    return BindFailure{errcode, -1, address};
}

[[nodiscard]] std::uint64_t pack_peer(std::optional<Ipv4Endpoint> peer) noexcept
{
    if (!peer)
    {
        return 0;
    }
    return (static_cast<std::uint64_t>(peer->address) << 16) | peer->port;
}

[[nodiscard]] std::optional<Ipv4Endpoint> connected_peer(const Slot& slot) noexcept
{
    const std::uint64_t packed = slot.peer.load(std::memory_order_acquire);
    if (0 == packed)
    {
        return std::nullopt;
    }
    return Ipv4Endpoint{static_cast<std::uint32_t>(packed >> 16), static_cast<std::uint16_t>(packed & 0xffff)};
}

}  // namespace

struct PortTable::Impl
{
    std::unique_ptr<std::atomic<std::uint16_t>[]> entries;
    std::unique_ptr<Slot[]> slots;
    mutable SpinLock lock;
    Bindings bindings;
    std::uint16_t ephemeral_low{EPHEMERAL_PORTS_FIRST};
    std::uint16_t ephemeral_high{EPHEMERAL_PORTS_LAST};
    std::size_t capacity{0};

    [[nodiscard]] std::optional<std::uint16_t> allocate_ephemeral(Bindings& current) noexcept
    {
        const std::uint16_t first = current.next_ephemeral;
        std::uint16_t candidate = first;

        for (;;)
        {
            if (entries[candidate].load(std::memory_order_acquire) == 0)
            {
                current.next_ephemeral = candidate == ephemeral_high ? ephemeral_low : candidate + 1;
                return candidate;
            }
            candidate = candidate == ephemeral_high ? ephemeral_low : candidate + 1;
            if (candidate == first)
            {
                return std::nullopt;
            }
        }
    }
};

PortTable::PortTable(std::size_t capacity, std::uint16_t ephemeral_low, std::uint16_t ephemeral_high)
    : impl_{std::make_unique<Impl>()}
{
    if (0 == capacity || capacity > 0xfffeu || ephemeral_low > ephemeral_high)
    {
        // A programmer error rather than a runtime one: a table of no capacity
        // refuses every bind (ENOBUFS) instead of dereferencing nothing.
        impl_->capacity = 0;
        return;
    }

    impl_->capacity = capacity;
    impl_->ephemeral_low = ephemeral_low;
    impl_->ephemeral_high = ephemeral_high;
    impl_->entries = std::make_unique<std::atomic<std::uint16_t>[]>(PORTS);
    for (std::size_t index = 0; index < PORTS; ++index)
    {
        impl_->entries[index].store(0, std::memory_order_relaxed);
    }
    impl_->slots = std::make_unique<Slot[]>(capacity);
    impl_->bindings.free.reserve(capacity);
    for (std::size_t index = 0; index < capacity; ++index)
    {
        impl_->bindings.free.push_back(static_cast<SlotId>(capacity - 1 - index));
    }
    impl_->bindings.next_ephemeral = ephemeral_low;
}

PortTable::~PortTable() = default;
PortTable::PortTable(PortTable&& other) noexcept = default;
PortTable& PortTable::operator=(PortTable&& other) noexcept = default;

std::size_t PortTable::capacity() const noexcept
{
    return impl_->capacity;
}

std::size_t PortTable::in_use() const noexcept
{
    SpinGuard guard{impl_->lock};
    return impl_->capacity - impl_->bindings.free.size();
}

std::expected<Binding, BindFailure> PortTable::bind(std::optional<std::uint32_t> address, std::uint16_t port,
                                                    std::optional<Ipv4Endpoint> peer, Ipv4Endpoint failure_at)
{
    SpinGuard guard{impl_->lock};

    std::uint16_t allocated = port;
    if (0 == port)
    {
        const auto ephemeral = impl_->allocate_ephemeral(impl_->bindings);
        if (!ephemeral)
        {
            return std::unexpected(failure(EADDRNOTAVAIL, failure_at));
        }
        allocated = *ephemeral;
    }
    else if (impl_->entries[port].load(std::memory_order_acquire) != 0)
    {
        return std::unexpected(failure(EADDRINUSE, failure_at));
    }

    if (impl_->bindings.free.empty())
    {
        return std::unexpected(failure(ENOBUFS, failure_at));
    }

    const SlotId slot = impl_->bindings.free.back();
    impl_->bindings.free.pop_back();

    Slot& entry = impl_->slots[slot];
    entry.address.store(address.value_or(WILDCARD), std::memory_order_relaxed);
    entry.peer.store(pack_peer(peer), std::memory_order_relaxed);
    const std::uint32_t generation = entry.generation.fetch_add(1, std::memory_order_acq_rel) + 1;

    impl_->entries[allocated].store(static_cast<std::uint16_t>(slot + 1), std::memory_order_release);

    return Binding{slot, allocated, generation};
}

void PortTable::release(const Binding& binding)
{
    SpinGuard guard{impl_->lock};
    Slot& slot = impl_->slots[binding.slot];

    // Only the binding that holds the slot may give it up: a late release from
    // a transport that closed must not free the port out from under the one
    // that bound it afterwards.
    if (slot.generation.load(std::memory_order_acquire) != binding.generation)
    {
        return;
    }
    if (impl_->entries[binding.port].load(std::memory_order_acquire) != static_cast<std::uint16_t>(binding.slot + 1))
    {
        return;
    }

    impl_->entries[binding.port].store(0, std::memory_order_release);
    slot.address.store(WILDCARD, std::memory_order_relaxed);
    slot.peer.store(0, std::memory_order_relaxed);
    slot.generation.fetch_add(1, std::memory_order_acq_rel);
    impl_->bindings.free.push_back(binding.slot);
}

std::optional<Binding> PortTable::lookup(std::uint32_t own, std::uint32_t destination, std::uint16_t port,
                                         Ipv4Endpoint source) const
{
    const std::uint16_t entry = impl_->entries[port].load(std::memory_order_acquire);
    if (0 == entry)
    {
        return std::nullopt;
    }

    const SlotId slot = static_cast<SlotId>(entry - 1);
    if (slot >= impl_->capacity)
    {
        return std::nullopt;
    }
    const Slot& record = impl_->slots[slot];

    const std::uint32_t bound = record.address.load(std::memory_order_acquire);
    const std::uint32_t expected = bound == WILDCARD ? own : bound;
    if (destination != expected)
    {
        return std::nullopt;
    }

    const auto peer = connected_peer(record);
    if (peer && !(*peer == source))
    {
        return std::nullopt;
    }

    return Binding{slot, port, record.generation.load(std::memory_order_acquire)};
}

void PortTable::reconnect(const Binding& binding, Ipv4Endpoint peer)
{
    Slot& slot = impl_->slots[binding.slot];
    if (slot.generation.load(std::memory_order_acquire) != binding.generation)
    {
        return;
    }
    slot.peer.store(pack_peer(peer), std::memory_order_release);
}

}  // namespace axc::core::media::dpdk::ports
