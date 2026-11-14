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
    DpdkPort(Port& port, Mempool& pool, unsigned rx_queue, unsigned tx_queue, bool tx_checksum_offload);

    std::size_t rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                         std::vector<std::pair<ports::Binding, packet_io::ReadyFrame>>& kept) override;

    [[nodiscard]] packet_io::Offloads offloads() const noexcept override;
    [[nodiscard]] std::array<std::uint8_t, 6> mac() const noexcept override;
    [[nodiscard]] std::size_t mtu() const noexcept override;

    std::size_t tx_burst(std::size_t count, packet_io::TxFill fill, void* context) override;
    [[nodiscard]] frame::L4Checksum checksums() const noexcept override;

private:
    Port* port_;
    Mempool* pool_;
    unsigned rx_queue_;
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
