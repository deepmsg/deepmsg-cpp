#include "core/media/dpdk/port.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "core/media/dpdk/shim/shim.h"

namespace axc::core::media::dpdk
{

namespace
{

[[nodiscard]] Error rte_error(std::string_view what) noexcept
{
    std::string message{what};
    message += ": ";
    message += shim_rte_strerror(shim_rte_errno());
    return make_error(ErrorCode::internal_error, message);
}

}  // namespace

std::expected<Port, Error> Port::open(unsigned index) noexcept
{
    const unsigned count = shim_port_count();
    if (index >= count)
    {
        return std::unexpected(make_error(ErrorCode::not_found, "no such DPDK port"));
    }
    return Port{index};
}

std::expected<Port, Error> Port::open_by_name(std::string_view name) noexcept
{
    const std::string copy{name};
    const int index = shim_port_by_name(copy.c_str());
    if (index < 0)
    {
        return std::unexpected(make_error(ErrorCode::not_found, "no DPDK port with that name"));
    }
    return Port{static_cast<unsigned>(index)};
}

Port::Port(Port&& other) noexcept : index_{other.index_}, open_{other.open_}, started_{other.started_}
{
    other.open_ = false;
    other.started_ = false;
}

Port& Port::operator=(Port&& other) noexcept
{
    if (this != &other)
    {
        close();
        index_ = other.index_;
        open_ = other.open_;
        started_ = other.started_;
        other.open_ = false;
        other.started_ = false;
    }
    return *this;
}

Port::~Port()
{
    close();
}

std::string Port::name() const
{
    std::array<char, 64> buffer{};
    shim_port_name(index_, buffer.data(), static_cast<unsigned>(buffer.size()));
    return std::string{buffer.data()};
}

std::expected<PortInfo, Error> Port::info() const noexcept
{
    shim_port_info_t raw{};
    if (0 != shim_port_info(index_, &raw))
    {
        return std::unexpected(rte_error("rte_eth_dev_info_get"));
    }

    PortInfo info;
    info.driver = std::string{raw.driver};
    info.max_rx_queues = raw.max_rx_queues;
    info.max_tx_queues = raw.max_tx_queues;
    info.max_mtu = raw.max_mtu;
    info.min_mtu = raw.min_mtu;
    info.socket_id = raw.socket_id;
    info.rx_offloads = raw.rx_offloads;
    info.tx_offloads = raw.tx_offloads;
    return info;
}

std::expected<Mac, Error> Port::mac() const noexcept
{
    Mac address{};
    if (0 != shim_port_mac(index_, address.data()))
    {
        return std::unexpected(rte_error("rte_eth_macaddr_get"));
    }
    return address;
}

std::expected<std::uint16_t, Error> Port::mtu() const noexcept
{
    std::uint16_t value = 0;
    if (0 != shim_port_mtu(index_, &value))
    {
        return std::unexpected(rte_error("rte_eth_dev_get_mtu"));
    }
    return value;
}

std::expected<PortStats, Error> Port::stats() const noexcept
{
    shim_port_stats_t raw{};
    if (0 != shim_port_stats(index_, &raw))
    {
        return std::unexpected(rte_error("rte_eth_stats_get"));
    }
    return PortStats{raw.ipackets, raw.opackets, raw.ibytes,  raw.obytes,
                     raw.imissed,  raw.ierrors,  raw.oerrors, raw.rx_nombuf};
}

std::expected<PortLink, Error> Port::link() const noexcept
{
    shim_port_link_t raw{};
    if (0 != shim_port_link(index_, &raw))
    {
        return std::unexpected(rte_error("rte_eth_link_get_nowait"));
    }
    return PortLink{raw.speed_mbps, 0 != raw.up, 0 != raw.full_duplex};
}

std::expected<void, Error> Port::configure(std::uint16_t rx_queues, std::uint16_t tx_queues, std::uint64_t rx_offloads,
                                           std::uint64_t tx_offloads) noexcept
{
    if (0 != shim_port_configure(index_, rx_queues, tx_queues, rx_offloads, tx_offloads))
    {
        return std::unexpected(rte_error("rte_eth_dev_configure"));
    }
    return {};
}

std::expected<void, Error> Port::setup_rx_queue(std::uint16_t queue, std::uint16_t descriptors,
                                                const Mempool& pool) noexcept
{
    if (0 != shim_rx_queue_setup(index_, queue, descriptors, pool.handle()))
    {
        return std::unexpected(rte_error("rte_eth_rx_queue_setup"));
    }
    return {};
}

std::expected<void, Error> Port::setup_tx_queue(std::uint16_t queue, std::uint16_t descriptors) noexcept
{
    if (0 != shim_tx_queue_setup(index_, queue, descriptors))
    {
        return std::unexpected(rte_error("rte_eth_tx_queue_setup"));
    }
    return {};
}

std::expected<void, Error> Port::start() noexcept
{
    if (0 != shim_port_start(index_))
    {
        return std::unexpected(rte_error("rte_eth_dev_start"));
    }
    started_ = true;
    return {};
}

void Port::stop() noexcept
{
    if (open_ && started_)
    {
        shim_port_stop(index_);
        started_ = false;
    }
}

void Port::close() noexcept
{
    if (open_)
    {
        stop();
        shim_port_close(index_);
        open_ = false;
    }
}

int Port::xstats_count() const noexcept
{
    return shim_port_xstats_count(index_);
}

int Port::xstats_names(std::span<shim_xstat_name_t> names) const noexcept
{
    return shim_port_xstats_names(index_, names.data(), static_cast<unsigned>(names.size()));
}

int Port::xstats_values(std::span<std::uint64_t> values) const noexcept
{
    return shim_port_xstats_values(index_, values.data(), static_cast<unsigned>(values.size()));
}

std::size_t Port::rx_burst(unsigned queue, shim_rx_desc* out, unsigned max) noexcept
{
    return shim_rx_burst(index_, queue, out, max);
}

std::size_t Port::tx_alloc(const Mempool& pool, shim_tx_slot* out, unsigned count) noexcept
{
    return shim_tx_alloc(pool.handle(), out, count);
}

std::size_t Port::tx_burst(unsigned queue, shim_tx_slot* slots, unsigned count) noexcept
{
    return shim_tx_burst(index_, queue, slots, count);
}

std::uint64_t Port::rx_offload_ipv4_checksum() noexcept
{
    return shim_rx_offload_ipv4_checksum();
}

std::uint64_t Port::rx_offload_udp_checksum() noexcept
{
    return shim_rx_offload_udp_checksum();
}

std::uint64_t Port::tx_offload_ipv4_checksum() noexcept
{
    return shim_tx_offload_ipv4_checksum();
}

std::uint64_t Port::tx_offload_udp_checksum() noexcept
{
    return shim_tx_offload_udp_checksum();
}

std::uint64_t Port::mbuf_tx_ipv4() noexcept
{
    return shim_mbuf_tx_ipv4();
}

std::uint64_t Port::mbuf_tx_ip_checksum() noexcept
{
    return shim_mbuf_tx_ip_checksum();
}

std::uint64_t Port::mbuf_tx_udp_checksum() noexcept
{
    return shim_mbuf_tx_udp_checksum();
}

}  // namespace axc::core::media::dpdk
