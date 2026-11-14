// Next hop -> MAC address: the table, how it is filled, and when it ages out.
//
// The kernel is not here to answer, so the stack keeps the table a kernel
// would: what it learns from the traffic it sees, what it is told statically,
// and what it has to ask for with ARP. Transcribed from the Rust track's
// `media/userspace/neighbour.rs`.
//
// The table itself holds no lock; the stack that owns it serialises writers
// and readers (the Rust track wraps it in a mutex, which this repository does
// not use).

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace axc::core::media::dpdk::neighbour
{

using Mac = std::array<std::uint8_t, 6>;

/// How many next hops one stack remembers at once.
inline constexpr std::size_t NEIGHBOUR_CAPACITY = 256;

/// The table: addresses, MACs, and the two times that go with them.
class NeighbourTable
{
public:
    NeighbourTable(std::size_t capacity, std::int64_t timeout_ns, std::int64_t interval_ns);

    [[nodiscard]] std::size_t size() const noexcept
    {
        return entries_.size();
    }
    [[nodiscard]] bool empty() const noexcept
    {
        return entries_.empty();
    }

    /// Add an address that was configured: it never ages out.
    void insert_fixed(std::uint32_t address, Mac hardware);

    /// What to send to, for an address the stack is about to send to. An entry
    /// that has not been confirmed within the timeout is not an answer any
    /// more.
    [[nodiscard]] std::optional<Mac> resolve(std::uint32_t address, std::int64_t now) const;

    /// Record what an address's MAC is (gleaning and a reply both come to
    /// this). A fixed entry is not overwritten.
    void learn(std::uint32_t address, Mac hardware, std::int64_t now);

    /// Whether an ARP request for `address` may be sent now, recording that it
    /// was.
    [[nodiscard]] bool ask(std::uint32_t address, std::int64_t now);

    /// Whether a request for `address` is due, without recording one.
    [[nodiscard]] bool ask_due(std::uint32_t address, std::int64_t now) const;

    void forget(std::uint32_t address);

private:
    struct Entry
    {
        std::uint32_t address{0};
        Mac hardware{};
        std::int64_t refreshed_ns{0};
        std::optional<std::int64_t> asked_ns;
        bool resolved{false};
        bool fixed{false};
    };

    [[nodiscard]] std::optional<std::size_t> position(std::uint32_t address) const;
    void push(Entry entry);

    std::vector<Entry> entries_;
    std::size_t capacity_;
    std::int64_t timeout_ns_;
    std::int64_t interval_ns_;
};

/// Whether two addresses are on the same link, given the port's own address
/// and its prefix length.
[[nodiscard]] bool on_the_same_link(std::uint32_t own, std::uint32_t other, std::uint8_t prefix_length) noexcept;

}  // namespace axc::core::media::dpdk::neighbour
