// What the transports of one machine share: the port table, the neighbour
// table, the three ports and the rings a transport's frames wait in.
//
// A stack is built by whoever owns the ports — a test over a FakePort wire, the
// DPDK path over a queue pair — and handed to the driver as part of the
// bindings. Transcribed from the Rust track's `media/userspace/stack.rs`; the
// Rust mutexes become explicit spin locks (this repository does not use
// the standard mutex type), and the receive path itself takes no lock.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "core/error/error.h"
#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/neighbour.h"
#include "core/media/dpdk/stack/packet_io.h"
#include "core/media/dpdk/stack/ports.h"
#include "core/media/dpdk/stack/spin_lock.h"
#include "core/media/dpdk/stack/stack_params.h"

namespace axc::core::media::dpdk
{

namespace demux
{
struct Deferred;
}

namespace stack
{

/// The counters a stack keeps, named rather than numbered. The numbers are the
/// reference's (`aeron-client/src/main/c/aeron_counters.h:155-173`).
struct StackCounters
{
    std::atomic<std::uint64_t> arp_miss{0};                   // 85
    std::atomic<std::uint64_t> rx_sender_discard{0};          // 86
    std::atomic<std::uint64_t> poller{0};                     // 87
    std::atomic<std::uint64_t> queue_drop{0};                 // 88
    std::atomic<std::uint64_t> checksum_failure{0};           // 89
    std::atomic<std::uint64_t> fragmented{0};                 // 90
    std::atomic<std::uint64_t> unsupported_ethernet_type{0};  // 93
    std::atomic<std::uint64_t> unsupported_protocol{0};       // 94
    std::atomic<std::uint64_t> rx_receiver_discard{0};        // 95
    std::atomic<std::uint64_t> tx_eagain{0};                  // 78
    std::atomic<std::uint64_t> nobufs{0};                     // 77
    std::atomic<std::uint64_t> errors{0};                     // 79
};

class Stack
{
public:
    Stack(StackParams params, std::unique_ptr<packet_io::RxPort> rx, std::unique_ptr<packet_io::TxPort> sender_tx,
          std::unique_ptr<packet_io::TxPort> receiver_tx);
    ~Stack();

    Stack(const Stack&) = delete;
    Stack& operator=(const Stack&) = delete;
    Stack(Stack&&) = delete;
    Stack& operator=(Stack&&) = delete;

    [[nodiscard]] const StackParams& params() const noexcept
    {
        return params_;
    }
    [[nodiscard]] StackCounters& counters() noexcept
    {
        return counters_;
    }

    /// How many ring buffers the pool has been committed to (0 with an
    /// unbounded budget).
    [[nodiscard]] std::size_t ring_mbufs() const noexcept
    {
        return ring_mbufs_;
    }

    [[nodiscard]] std::expected<ports::Binding, ports::BindFailure> bind(std::optional<std::uint32_t> address,
                                                                         std::uint16_t port,
                                                                         std::optional<ports::Ipv4Endpoint> peer,
                                                                         ports::Ipv4Endpoint failure_at);
    void release(const ports::Binding& binding);
    void reconnect(const ports::Binding& binding, ports::Ipv4Endpoint peer);

    [[nodiscard]] std::optional<ports::Binding> lookup(std::uint32_t destination, std::uint16_t port,
                                                       ports::Ipv4Endpoint source) const;

    /// The next hop for a destination: the destination itself on this link, or
    /// the configured gateway.
    [[nodiscard]] std::optional<std::uint32_t> next_hop(std::uint32_t destination) const;

    /// The MAC for a next hop, if it is known and fresh.
    [[nodiscard]] std::optional<Mac> neighbour(std::uint32_t address) const;

    /// Remember a source address's MAC if it is on this link (gleaning).
    void glean(std::uint32_t address, Mac hardware);
    void learn(std::uint32_t address, Mac hardware);
    void insert_neighbour(std::uint32_t address, Mac hardware);

    /// One pass: take up to `params().rx_burst` frames off the receive port,
    /// hand each to the demultiplexer, then send whatever that produced.
    std::size_t pump();

    /// One datagram to `destination` from `binding`. Answers the payload bytes
    /// taken, zero for back pressure (no MAC yet, or the port refused it), or
    /// an error when the datagram cannot travel at all -- it is larger than
    /// this port's MTU allows (counter 79).
    [[nodiscard]] std::expected<std::size_t, Error> send(const ports::Binding& binding, ports::Ipv4Endpoint destination,
                                                         std::span<const std::uint8_t> payload);

    /// The same, for a datagram the caller holds in pieces (the driver's
    /// `send_func` hands us iovecs): the frame is written once, straight into
    /// the port's buffer, so no one copies the payload to put it together.
    [[nodiscard]] std::expected<std::size_t, Error> send(const ports::Binding& binding, ports::Ipv4Endpoint destination,
                                                         std::span<const std::span<const std::uint8_t>> parts);

    /// What [`send_datagrams`] did: how many of the datagrams the port took, in
    /// order, and how many bytes those were.
    struct DatagramsSent
    {
        std::size_t datagrams{0};
        std::int64_t bytes{0};
    };

    /// Hand each element to the port as its own datagram, in order, and answer
    /// how many went. This is the reference's `send_func` contract exactly: one
    /// iov is one datagram, and the return is the number of them the port took
    /// (`media/aeron_udp_channel_transport.c:728-773`, `sendv`). A datagram the
    /// port cannot take stops the run where it is -- what left before it stays
    /// counted, which is what lets the sender's position advance by those and
    /// no further (`aeron_network_publication.c:495-580`).
    [[nodiscard]] std::expected<DatagramsSent, Error> send_datagrams(
        const ports::Binding& binding, ports::Ipv4Endpoint destination,
        std::span<const std::span<const std::uint8_t>> datagrams);

    /// Copy the next datagram for `binding` into `buffer`; answers its length,
    /// or zero when none is waiting.
    std::size_t receive(const ports::Binding& binding, std::span<std::uint8_t> buffer,
                        ports::Ipv4Endpoint* source = nullptr);

    [[nodiscard]] std::uint16_t next_identification() noexcept;
    [[nodiscard]] std::int64_t now_ns() const noexcept;
    void set_now_for_test(std::int64_t now_ns) noexcept;

    // Used by the demultiplexer.
    void count_a_discard(frame::Discard discard) noexcept;
    void count_unclaimed(std::span<const std::uint8_t> payload) noexcept;
    bool deliver(const ports::Binding& binding, ports::Ipv4Endpoint source, std::span<const std::uint8_t> payload);

    [[nodiscard]] packet_io::TxPort& sender_tx() noexcept
    {
        return *sender_tx_;
    }
    [[nodiscard]] packet_io::TxPort& receiver_tx() noexcept
    {
        return *receiver_tx_;
    }

private:
    struct Datagram
    {
        ports::Ipv4Endpoint source{};
        std::size_t payload_offset{0};
        packet_io::ReadyFrame frame;
    };

    /// A transport's ready list: a fixed array (sized once, at bind) with a
    /// read index and a count, so a delivered frame costs no allocation. Slots
    /// are `optional` because a parked frame has no empty value to fall back
    /// on -- an empty slot has to be distinguishable from a parked one.
    struct Ring
    {
        std::vector<std::optional<Datagram>> frames;
        std::size_t head{0};
        std::size_t size{0};
        std::uint32_t generation{0};
        std::size_t capacity{0};
        SpinLock lock;
    };

    void reset_ring(const ports::Binding& binding);
    void clear_ring(std::uint16_t slot);
    bool deliver_frame(const ports::Binding& binding, ports::Ipv4Endpoint source, std::size_t payload_offset,
                       packet_io::ReadyFrame&& frame);
    std::size_t send_frame(std::span<const std::uint8_t> frame, packet_io::TxPort& port);
    void note_tx_result(const packet_io::TxResult& result) noexcept;

    StackParams params_;
    ports::PortTable ports_;
    mutable SpinLock neighbours_lock_;
    neighbour::NeighbourTable neighbours_;
    std::unique_ptr<Ring[]> rings_;
    StackCounters counters_;
    std::size_t ring_mbufs_{0};
    std::unique_ptr<packet_io::RxPort> rx_;
    std::unique_ptr<packet_io::TxPort> sender_tx_;

    // A pass's scratch, owned by the stack and reused: `pump` runs on every
    // poller pass, so three fresh vectors per pass would put the allocator on
    // the receive path. `deferred_` is the answers a pass has to send once the
    // receive callback has returned ([`pump`] cannot send from inside it);
    // `kept_frames_` is what a port's `rx_burst` hands back for transports
    // whose frames must wait — the DPDK path delivers in the callback and
    // leaves it empty; `answer_buffer_` is one frame's worth of bytes for an
    // ARP or ICMP answer. A delivered datagram still copies its payload into
    // its transport's ring; zero-copy is not in this slice.
    std::vector<demux::Deferred> deferred_;
    std::vector<packet_io::KeptFrame> kept_frames_;
    std::vector<std::uint8_t> answer_buffer_;
    std::unique_ptr<packet_io::TxPort> receiver_tx_;
    std::atomic<std::uint16_t> identification_{0};
    std::atomic<std::int64_t> manual_now_{0};
    bool manual_clock_{false};
};

}  // namespace stack

}  // namespace axc::core::media::dpdk
