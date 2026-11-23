// The userspace stack over a DPDK port: the bridge between the shim's frame
// descriptors and the stack's `RxPort`/`TxPort`.
//
// The frames the shim hands over are mbufs; the stack's `ReadyFrame` owns
// bytes. P4 keeps the copy (the zero-copy parked-mbuf arm is a later tuning
// step), and frees the mbuf before the burst returns.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/port.h"
#include "core/media/dpdk/shim/shim.h"
#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/packet_io.h"

namespace axc::core::media::dpdk
{

class DpdkPort : public packet_io::RxPort, public packet_io::TxPort
{
public:
    /// `rx_queues` is every receive queue this adapter polls, in the order it
    /// will take them; the one receive thread owns them, so the round-robin
    /// cursor needs no synchronisation. A transmit-only adapter passes none.
    ///
    /// Why more than one: virtio announces `RTE_MAX(nb_rx, nb_tx)` queue pairs
    /// at device start (`virtio_ethdev.c:2383-2385`), and the backend hashes
    /// inbound flows across all of them. A port opened with one receive queue
    /// and two transmit queues leaves half its inbound flows on a queue nobody
    /// polls (2026-11-23: DEDICATED over an Aliyun virtio ENI lost every burst
    /// whose tuple hashed to pair 1, while SHARED -- one pair -- took all).
    DpdkPort(Port& port, Mempool& pool, std::vector<unsigned> rx_queues, unsigned tx_queue, bool tx_checksum_offload);

    std::size_t rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                         std::vector<packet_io::KeptFrame>& kept) override;

    [[nodiscard]] packet_io::Offloads offloads() const noexcept override;
    [[nodiscard]] std::array<std::uint8_t, 6> mac() const noexcept override;
    [[nodiscard]] std::size_t mtu() const noexcept override;

    packet_io::TxResult tx_burst(std::size_t count, packet_io::TxFill fill, void* context) override;
    [[nodiscard]] frame::L4Checksum checksums() const noexcept override;

private:
    Port* port_;
    Mempool* pool_;
    std::vector<unsigned> rx_queues_;
    std::size_t rx_cursor_{0};
    unsigned tx_queue_;
    packet_io::Offloads offloads_;
    frame::L4Checksum tx_checksums_;
    std::array<std::uint8_t, 6> mac_{};
    std::size_t mtu_{1500};
    std::array<shim_rx_desc, SHIM_BURST_MAX> rx_scratch_{};
    std::array<shim_tx_slot, SHIM_BURST_MAX> tx_scratch_{};
    std::array<shim_tx_slot, SHIM_BURST_MAX> tx_written_{};
};

}  // namespace axc::core::media::dpdk
