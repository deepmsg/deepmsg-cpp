#include "core/media/dpdk/stack/fake_port.h"

#include <sched.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace axc::core::media::dpdk::fake_port
{

namespace
{

std::size_t index_of(Side side) noexcept
{
    return Side::a == side ? 0 : 1;
}

class SpinLock
{
public:
    void lock() noexcept
    {
        while (flag_.test_and_set(std::memory_order_acquire))
        {
            ::sched_yield();
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

bool every(std::uint64_t count, const std::optional<std::uint64_t>& period) noexcept
{
    return period && *period > 0 && count % *period == 0;
}

}  // namespace

struct Wire::State
{
    std::deque<std::vector<std::uint8_t>> queues[2];
    std::uint64_t carried[2]{0, 0};
    Injection injection[2];
    mutable SpinLock lock;
};

Wire::Wire() : state_{std::make_unique<State>()} {}

Wire::~Wire() = default;

void Wire::set_injection(Side side, const Injection& injection)
{
    SpinGuard guard{state_->lock};
    state_->injection[index_of(side)] = injection;
}

std::size_t Wire::queued(Side side) const
{
    SpinGuard guard{state_->lock};
    return state_->queues[index_of(side)].size();
}

std::uint64_t Wire::carried(Side side) const
{
    SpinGuard guard{state_->lock};
    return state_->carried[index_of(side)];
}

void Wire::send(Side side, std::span<const std::uint8_t> frame)
{
    enqueue(side, std::vector<std::uint8_t>(frame.begin(), frame.end()));
}

std::vector<std::vector<std::uint8_t>> Wire::take(Side side, std::size_t max)
{
    std::vector<std::vector<std::uint8_t>> taken;
    while (taken.size() < max)
    {
        auto frame = pop(side);
        if (!frame)
        {
            break;
        }
        taken.push_back(std::move(*frame));
    }
    return taken;
}

void Wire::enqueue(Side side, std::vector<std::uint8_t> frame)
{
    SpinGuard guard{state_->lock};
    const std::size_t index = index_of(side);
    std::uint64_t& carried = state_->carried[index];
    ++carried;
    const std::uint64_t count = carried;
    const Injection injection = state_->injection[index];

    if (every(count, injection.drop_every))
    {
        return;
    }

    if (every(count, injection.corrupt_checksum_every) && !frame.empty())
    {
        frame.back() ^= 0xff;
    }

    std::optional<std::vector<std::uint8_t>> copy;
    if (every(count, injection.duplicate_every))
    {
        copy = frame;
    }

    auto& queue = state_->queues[index];
    if (injection.reorder && !queue.empty())
    {
        queue.insert(queue.end() - 1, std::move(frame));
    }
    else
    {
        queue.push_back(std::move(frame));
    }

    if (copy)
    {
        queue.push_back(std::move(*copy));
    }
}

std::optional<std::vector<std::uint8_t>> Wire::pop(Side side)
{
    SpinGuard guard{state_->lock};
    auto& queue = state_->queues[index_of(other_side(side))];
    if (queue.empty())
    {
        return std::nullopt;
    }
    auto frame = std::move(queue.front());
    queue.pop_front();
    return frame;
}

std::size_t Wire::delivery_capacity(Side side) const
{
    SpinGuard guard{state_->lock};
    const auto& capacity = state_->injection[index_of(other_side(side))].capacity;
    return capacity.value_or(static_cast<std::size_t>(-1));
}

std::size_t FakePort::rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                               std::vector<std::pair<ports::Binding, packet_io::ReadyFrame>>& kept)
{
    const std::size_t limit = std::min(max, wire_->delivery_capacity(side_));

    std::size_t handed = 0;
    while (handed < limit)
    {
        auto frame = wire_->pop(side_);
        if (!frame)
        {
            break;
        }

        const packet_io::RxFrame view{*frame, frame::Checksums::software};
        const packet_io::Disposition disposition = callback(context, view);
        if (disposition.keep)
        {
            kept.emplace_back(*disposition.keep, packet_io::ReadyFrame{std::move(*frame)});
        }
        ++handed;
    }
    return handed;
}

packet_io::Offloads FakePort::offloads() const noexcept
{
    return packet_io::Offloads::none();
}

std::array<std::uint8_t, 6> FakePort::mac() const noexcept
{
    return mac_;
}

std::size_t FakePort::mtu() const noexcept
{
    return mtu_;
}

std::size_t FakePort::tx_burst(std::size_t count, packet_io::TxFill fill, void* context)
{
    std::size_t taken = 0;
    for (std::size_t index = 0; index < count; ++index)
    {
        const auto [length, offloads] = fill(context, scratch_);

        // A wire has no hardware to compute anything; a frame that asks it for
        // a checksum is a frame whose author and whose port disagree.
        assert(!offloads.compute && "the in-memory wire computes no checksums");

        if (0 == length || length > scratch_.size())
        {
            continue;
        }

        const auto* const begin = scratch_.data();
        wire_->enqueue(side_, std::vector<std::uint8_t>(begin, begin + length));
        ++taken;
    }
    return taken;
}

frame::L4Checksum FakePort::checksums() const noexcept
{
    return frame::L4Checksum::software;
}

}  // namespace axc::core::media::dpdk::fake_port
