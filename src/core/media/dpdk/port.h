// One DPDK port and its queues.
//
// Transcribed from the Rust track's `crates/dpdk/src/port.rs`: the shim's own
// PODs cross the boundary, no DPDK structure is visible here, and every call
// reads `rte_errno` only after the shim cleared it.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>

#include "core/error/error.h"
#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/shim/shim.h"

namespace axc::core::media::dpdk
{

using Mac = std::array<std::uint8_t, 6>;

struct PortInfo
{
    std::string driver;
    std::uint16_t max_rx_queues{0};
    std::uint16_t max_tx_queues{0};
    std::uint16_t max_mtu{0};
    std::uint16_t min_mtu{0};
    int socket_id{-1};
    std::uint64_t rx_offloads{0};
    std::uint64_t tx_offloads{0};
};

struct PortStats
{
    std::uint64_t ipackets{0};
    std::uint64_t opackets{0};
    std::uint64_t ibytes{0};
    std::uint64_t obytes{0};
    std::uint64_t imissed{0};
    std::uint64_t ierrors{0};
    std::uint64_t oerrors{0};
    std::uint64_t rx_nombuf{0};
};

struct PortLink
{
    std::uint32_t speed_mbps{0};
    bool up{false};
    bool full_duplex{false};
};

class Port
{
public:
    [[nodiscard]] static std::expected<Port, Error> open(unsigned index) noexcept;
    [[nodiscard]] static std::expected<Port, Error> open_by_name(std::string_view name) noexcept;

    Port(Port&& other) noexcept;
    Port& operator=(Port&& other) noexcept;
    ~Port();

    Port(const Port&) = delete;
    Port& operator=(const Port&) = delete;

    [[nodiscard]] unsigned index() const noexcept
    {
        return index_;
    }
    [[nodiscard]] std::string name() const;
    [[nodiscard]] std::expected<PortInfo, Error> info() const noexcept;
    [[nodiscard]] std::expected<Mac, Error> mac() const noexcept;
    [[nodiscard]] std::expected<std::uint16_t, Error> mtu() const noexcept;
    [[nodiscard]] std::expected<PortStats, Error> stats() const noexcept;
    [[nodiscard]] std::expected<PortLink, Error> link() const noexcept;

    [[nodiscard]] std::expected<void, Error> configure(std::uint16_t rx_queues, std::uint16_t tx_queues,
                                                       std::uint64_t rx_offloads, std::uint64_t tx_offloads) noexcept;
    [[nodiscard]] std::expected<void, Error> setup_rx_queue(std::uint16_t queue, std::uint16_t descriptors,
                                                            const Mempool& pool) noexcept;
    [[nodiscard]] std::expected<void, Error> setup_tx_queue(std::uint16_t queue, std::uint16_t descriptors) noexcept;
    [[nodiscard]] std::expected<void, Error> start() noexcept;
    void stop() noexcept;
    void close() noexcept;

    [[nodiscard]] int xstats_count() const noexcept;
    [[nodiscard]] int xstats_names(std::span<shim_xstat_name_t> names) const noexcept;
    [[nodiscard]] int xstats_values(std::span<std::uint64_t> values) const noexcept;

    [[nodiscard]] std::size_t rx_burst(unsigned queue, shim_rx_desc* out, unsigned max) noexcept;
    [[nodiscard]] std::size_t tx_alloc(const Mempool& pool, shim_tx_slot* out, unsigned count) noexcept;
    [[nodiscard]] std::size_t tx_burst(unsigned queue, shim_tx_slot* slots, unsigned count) noexcept;

    [[nodiscard]] static std::uint64_t rx_offload_ipv4_checksum() noexcept;
    [[nodiscard]] static std::uint64_t rx_offload_udp_checksum() noexcept;
    [[nodiscard]] static std::uint64_t tx_offload_ipv4_checksum() noexcept;
    [[nodiscard]] static std::uint64_t tx_offload_udp_checksum() noexcept;
    [[nodiscard]] static std::uint64_t mbuf_tx_ipv4() noexcept;
    [[nodiscard]] static std::uint64_t mbuf_tx_ip_checksum() noexcept;
    [[nodiscard]] static std::uint64_t mbuf_tx_udp_checksum() noexcept;

private:
    explicit Port(unsigned index) noexcept : index_{index} {}

    unsigned index_;
    bool open_{true};
};

}  // namespace axc::core::media::dpdk
