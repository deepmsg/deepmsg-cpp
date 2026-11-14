#include "core/media/dpdk/dpdk_port.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace axc::core::media::dpdk
{

namespace
{

[[nodiscard]] packet_io::Verdict verdict_of(std::uint32_t checksum) noexcept
{
    switch (checksum)
    {
        case SHIM_CKSUM_GOOD:
            return packet_io::Verdict::passed;
        case SHIM_CKSUM_BAD:
            return packet_io::Verdict::failed;
        default:
            return packet_io::Verdict::not_judged;
    }
}

}  // namespace

DpdkPort::DpdkPort(Port& port, Mempool& pool, unsigned rx_queue, unsigned tx_queue, bool tx_checksum_offload)
    : port_{&port},
      pool_{&pool},
      rx_queue_{rx_queue},
      tx_queue_{tx_queue},
      tx_checksums_{tx_checksum_offload ? frame::L4Checksum::by_the_nic : frame::L4Checksum::software}
{
    if (const auto info = port.info(); info)
    {
        offloads_.ipv4_checksum = 0 != (info->rx_offloads & Port::rx_offload_ipv4_checksum());
        offloads_.udp_checksum = 0 != (info->rx_offloads & Port::rx_offload_udp_checksum());
    }
    if (const auto address = port.mac(); address)
    {
        mac_ = *address;
    }
    if (const auto mtu = port.mtu(); mtu)
    {
        mtu_ = *mtu;
    }
}

std::size_t DpdkPort::rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                               std::vector<std::pair<ports::Binding, packet_io::ReadyFrame>>& kept)
{
    const auto burst = static_cast<unsigned>(std::min<std::size_t>(max, SHIM_BURST_MAX));
    const std::size_t received = port_->rx_burst(rx_queue_, rx_scratch_.data(), burst);

    for (std::size_t index = 0; index < received; ++index)
    {
        const shim_rx_desc& descriptor = rx_scratch_[index];
        const packet_io::RxFrame view{std::span<const std::uint8_t>{descriptor.data, descriptor.len},
                                      offloads_.checksums(verdict_of(descriptor.checksum))};
        const packet_io::Disposition disposition = callback(context, view);
        if (disposition.keep)
        {
            kept.emplace_back(*disposition.keep,
                              packet_io::ReadyFrame{std::vector<std::uint8_t>(view.bytes.begin(), view.bytes.end())});
        }
        void* mbuf = descriptor.mbuf;
        Mempool::free_bulk(&mbuf, 1);
    }
    return received;
}

packet_io::Offloads DpdkPort::offloads() const noexcept
{
    return offloads_;
}

std::array<std::uint8_t, 6> DpdkPort::mac() const noexcept
{
    return mac_;
}

std::size_t DpdkPort::mtu() const noexcept
{
    return mtu_;
}

std::size_t DpdkPort::tx_burst(std::size_t count, packet_io::TxFill fill, void* context)
{
    const auto burst = static_cast<unsigned>(std::min<std::size_t>(count, SHIM_BURST_MAX));
    const std::size_t allocated = port_->tx_alloc(*pool_, tx_scratch_.data(), burst);

    std::size_t written = 0;
    for (std::size_t index = 0; index < allocated; ++index)
    {
        shim_tx_slot& slot = tx_scratch_[index];
        const auto [length, offloads] = fill(context, std::span<std::uint8_t>{slot.data, slot.room});
        if (0 == length || length > slot.room)
        {
            void* mbuf = slot.mbuf;
            Mempool::free_bulk(&mbuf, 1);
            continue;
        }

        slot.len = static_cast<std::uint16_t>(length);
        if (offloads.compute)
        {
            slot.ol_flags = Port::mbuf_tx_ipv4() | Port::mbuf_tx_ip_checksum() | Port::mbuf_tx_udp_checksum();
            slot.l2_len = offloads.l2_length;
            slot.l3_len = offloads.l3_length;
        }
        tx_written_[written++] = slot;
    }

    if (0 == written)
    {
        return 0;
    }
    return port_->tx_burst(tx_queue_, tx_written_.data(), static_cast<unsigned>(written));
}

frame::L4Checksum DpdkPort::checksums() const noexcept
{
    return tx_checksums_;
}

}  // namespace axc::core::media::dpdk
