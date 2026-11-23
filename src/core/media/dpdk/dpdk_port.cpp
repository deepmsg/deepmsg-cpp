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

DpdkPort::DpdkPort(Port& port, Mempool& pool, std::vector<unsigned> rx_queues, unsigned tx_queue,
                   bool tx_checksum_offload)
    : port_{&port},
      pool_{&pool},
      rx_queues_{std::move(rx_queues)},
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

namespace
{

/// What a parked frame does with the mbuf when the transport is done with it.
void release_mbuf(void* mbuf) noexcept
{
    Mempool::free_bulk(&mbuf, 1);
}

}  // namespace

std::size_t DpdkPort::rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                               std::vector<packet_io::KeptFrame>& kept)
{
    // One burst per queue, starting where the last call left off: the budget
    // stays the caller's, the queues take turns, and a queue that is empty
    // costs one `rte_eth_rx_burst` and no more.
    std::size_t total = 0;
    const std::size_t queues = rx_queues_.size();
    for (std::size_t turn = 0; turn < queues && total < max; ++turn)
    {
        const unsigned queue = rx_queues_[rx_cursor_];
        rx_cursor_ = (rx_cursor_ + 1) % queues;
        const auto burst = static_cast<unsigned>(std::min<std::size_t>(max - total, SHIM_BURST_MAX));
        const std::size_t received = port_->rx_burst(queue, rx_scratch_.data(), burst);

        for (std::size_t index = 0; index < received; ++index)
        {
            const shim_rx_desc& descriptor = rx_scratch_[index];
            const packet_io::RxFrame view{std::span<const std::uint8_t>{descriptor.data, descriptor.len},
                                          offloads_.checksums(verdict_of(descriptor.checksum))};
            const packet_io::Disposition disposition = callback(context, view);
            if (disposition.keep)
            {
                // The frame stays in the pool until the transport has copied it
                // out; nothing is copied here and nothing is allocated.
                kept.push_back(packet_io::KeptFrame{
                    *disposition.keep, disposition.source, disposition.payload_offset,
                    packet_io::ReadyFrame::parked(descriptor.mbuf, descriptor.data, descriptor.len, &release_mbuf)});
                continue;
            }
            void* mbuf = descriptor.mbuf;
            Mempool::free_bulk(&mbuf, 1);
        }
        total += received;
    }
    return total;
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

packet_io::TxResult DpdkPort::tx_burst(std::size_t count, packet_io::TxFill fill, void* context)
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

    // An empty pool is counter 77 and a ring that will not take the frame is
    // counter 78; the caller cannot tell them apart from `taken` alone.
    const auto not_allocated = static_cast<std::size_t>(burst - allocated);
    if (0 == written)
    {
        return packet_io::TxResult{0, not_allocated};
    }
    return packet_io::TxResult{port_->tx_burst(tx_queue_, tx_written_.data(), static_cast<unsigned>(written)),
                               not_allocated};
}

frame::L4Checksum DpdkPort::checksums() const noexcept
{
    return tx_checksums_;
}

}  // namespace axc::core::media::dpdk
