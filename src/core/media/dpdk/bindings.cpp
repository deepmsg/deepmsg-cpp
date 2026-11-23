#include "core/media/dpdk/bindings.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>

extern "C"
{
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "media/aeron_udp_transport_poller.h"
#include "util/aeron_error.h"
}

#include "core/media/dpdk/counters.h"
#include "core/media/dpdk/eal.h"
#include "core/media/dpdk/media.h"
#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/port.h"
#include "core/media/dpdk/stack/stack.h"

namespace axc::core::media::dpdk::bindings
{

namespace
{

const aeron_udp_channel_transport_bindings_t* g_default = nullptr;
stack::Stack* g_stack = nullptr;
Port* g_port = nullptr;
Mempool* g_pool = nullptr;

struct CounterAddresses
{
    std::int64_t* port_info{nullptr};
    std::int64_t* nobufs{nullptr};
    std::int64_t* tx_eagain{nullptr};
    std::int64_t* error{nullptr};
    std::int64_t* pkts{nullptr};
    std::int64_t* bytes{nullptr};
    std::int64_t* missed{nullptr};
    std::int64_t* arp_miss{nullptr};
    std::int64_t* rx_sender_discard{nullptr};
    std::int64_t* poller{nullptr};
    std::int64_t* queue_drop{nullptr};
    std::int64_t* checksum_failure{nullptr};
    std::int64_t* fragmented{nullptr};
    std::int64_t* mempool_available{nullptr};
    std::int64_t* extended_stats{nullptr};
    std::int64_t* unsupported_ethernet_type{nullptr};
    std::int64_t* unsupported_protocol{nullptr};
    std::int64_t* rx_receiver_discard{nullptr};
};

CounterAddresses g_counters{};
bool g_counters_ready = false;
std::int64_t g_next_stats_ns = 0;

struct TransportState
{
    std::optional<ports::Binding> binding;
    std::int64_t* counter{nullptr};
    // The agent that owns this transport: the reference initialises one
    // transport per affinity, and the sender and receiver agents must burst on
    // their own DPDK queues (a tx queue is not thread-safe).
    aeron_udp_channel_transport_affinity_t affinity{AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER};
};

[[nodiscard]] std::int64_t* allocate_counter(aeron_driver_context_t* context, std::int32_t type_id, const void* key,
                                             std::size_t key_length) noexcept
{
    auto* manager = context->counters_manager;
    if (nullptr == manager)
    {
        return nullptr;
    }
    const std::string_view label = counters::label(type_id);
    const std::int32_t id = aeron_counters_manager_allocate(manager, type_id, static_cast<const std::uint8_t*>(key),
                                                            key_length, label.data(), label.size());
    return id < 0 ? nullptr : aeron_counters_manager_addr(manager, id);
}

struct PortCounterKey
{
    std::uint16_t index;
    std::uint16_t type;
};

void allocate_one(aeron_driver_context_t* context, std::int32_t type_id, std::int64_t*& slot)
{
    const PortCounterKey key{0, static_cast<std::uint16_t>(type_id)};
    slot = allocate_counter(context, type_id, &key, sizeof(key));
}

void allocate_port_counters(aeron_driver_context_t* context) noexcept
{
    if (g_counters_ready || nullptr == context->counters_manager)
    {
        return;
    }
    allocate_one(context, counters::PORT_INFO, g_counters.port_info);
    allocate_one(context, counters::NOBUFS, g_counters.nobufs);
    allocate_one(context, counters::TX_EAGAIN, g_counters.tx_eagain);
    allocate_one(context, counters::ERROR, g_counters.error);
    allocate_one(context, counters::PKTS, g_counters.pkts);
    allocate_one(context, counters::BYTES, g_counters.bytes);
    allocate_one(context, counters::MISSED_PACKETS, g_counters.missed);
    allocate_one(context, counters::ARP_MISS, g_counters.arp_miss);
    allocate_one(context, counters::RX_SENDER_DISCARD, g_counters.rx_sender_discard);
    allocate_one(context, counters::POLLER, g_counters.poller);
    allocate_one(context, counters::QUEUE_DROP_COUNT, g_counters.queue_drop);
    allocate_one(context, counters::CHECKSUM_FAILURE, g_counters.checksum_failure);
    allocate_one(context, counters::FRAGMENTED_PACKETS, g_counters.fragmented);
    allocate_one(context, counters::MEMPOOL_AVAILABLE, g_counters.mempool_available);
    allocate_one(context, counters::EXTENDED_STATS, g_counters.extended_stats);
    allocate_one(context, counters::RX_UNSUPPORTED_ETHERNET_TYPE, g_counters.unsupported_ethernet_type);
    allocate_one(context, counters::RX_UNSUPPORTED_PROTOCOL, g_counters.unsupported_protocol);
    allocate_one(context, counters::RX_RECEIVER_DISCARD, g_counters.rx_receiver_discard);
    g_counters_ready = true;
}

void set_counter(std::int64_t* address, std::uint64_t value) noexcept
{
    if (nullptr != address)
    {
        aeron_counter_set_release(address, static_cast<std::int64_t>(value));
    }
}

void mirror_stack_counters() noexcept
{
    if (nullptr == g_stack)
    {
        return;
    }
    auto& source = g_stack->counters();
    set_counter(g_counters.arp_miss, source.arp_miss.load(std::memory_order_relaxed));
    set_counter(g_counters.rx_sender_discard, source.rx_sender_discard.load(std::memory_order_relaxed));
    set_counter(g_counters.queue_drop, source.queue_drop.load(std::memory_order_relaxed));
    set_counter(g_counters.checksum_failure, source.checksum_failure.load(std::memory_order_relaxed));
    set_counter(g_counters.fragmented, source.fragmented.load(std::memory_order_relaxed));
    set_counter(g_counters.unsupported_ethernet_type, source.unsupported_ethernet_type.load(std::memory_order_relaxed));
    set_counter(g_counters.unsupported_protocol, source.unsupported_protocol.load(std::memory_order_relaxed));
    set_counter(g_counters.rx_receiver_discard, source.rx_receiver_discard.load(std::memory_order_relaxed));
    set_counter(g_counters.tx_eagain, source.tx_eagain.load(std::memory_order_relaxed));
    set_counter(g_counters.nobufs, source.nobufs.load(std::memory_order_relaxed));
    set_counter(g_counters.error, source.errors.load(std::memory_order_relaxed));
}

/// One second's worth of the counters, and nothing more.
///
/// The stack's counters are the driver's bookkeeping, not the data path: the
/// Rust media this one transcribes publishes them from the conductor's timer
/// (`deepmsg:crates/driver/src/conductor.rs:1132,1427`, the 1 s
/// `timer_interval_ns` branch), and a pass that mirrors them instead is what
/// made this media slower than that one — eleven release stores and a clock
/// read on every poll, at ~5-6 million polls a second, was ~62% of the
/// receiver's cycles in the kernel and a fifth of the rest in the mirror
/// (`analysis/dpdk/notes-2026-11-18-why-the-cpp-dpdk-is-slower.md`). The
/// stack's half moves here so both halves change together.
void update_port_counters() noexcept
{
    const std::int64_t now =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    if (now < g_next_stats_ns)
    {
        return;
    }
    g_next_stats_ns = now + 1000LL * 1000 * 1000;

    mirror_stack_counters();

    if (nullptr != g_port)
    {
        if (const auto stats = g_port->stats(); stats)
        {
            set_counter(g_counters.pkts, stats->ipackets);
            set_counter(g_counters.bytes, stats->ibytes);
            set_counter(g_counters.missed, stats->imissed);
        }
        set_counter(g_counters.extended_stats, static_cast<std::uint64_t>(g_port->xstats_count()));
    }
    if (nullptr != g_pool)
    {
        set_counter(g_counters.mempool_available, g_pool->available());
    }
}

struct PollerState
{
    aeron_udp_transport_poller_t inner{};
    bool inner_ready{false};
    bool receiver{false};
    std::vector<aeron_udp_channel_transport_t*> transports;
    /// The transports in this poller whose frames do **not** come off the
    /// stack. They are the only reason the reference poller below still has
    /// anything to do; with none of them its `recvmmsg` would be a syscall on a
    /// socket that never has a datagram (see `dpdk_poller_poll`).
    std::vector<aeron_udp_channel_transport_t*> kernel_transports;
    /// A count of passes, so the clock read that paces the counters happens
    /// once every thousand and not once a pass.
    std::uint64_t passes{0};
    std::vector<std::uint8_t> buffer;
};

void register_thread_once() noexcept
{
    static thread_local bool registered = false;
    if (!registered)
    {
        std::ignore = Eal::register_thread();
        registered = true;
    }
}

[[nodiscard]] std::uint32_t address_of(const struct sockaddr_storage* storage) noexcept
{
    if (nullptr == storage || AF_INET != storage->ss_family)
    {
        return 0;
    }
    return ntohl(reinterpret_cast<const struct sockaddr_in*>(storage)->sin_addr.s_addr);
}

[[nodiscard]] std::uint16_t port_of(const struct sockaddr_storage* storage) noexcept
{
    if (nullptr == storage || AF_INET != storage->ss_family)
    {
        return 0;
    }
    return ntohs(reinterpret_cast<const struct sockaddr_in*>(storage)->sin_port);
}

[[nodiscard]] bool is_loopback(std::uint32_t address) noexcept
{
    return (address & 0xff000000u) == 0x7f000000u;
}

[[nodiscard]] bool is_multicast(std::uint32_t address) noexcept
{
    return (address & 0xf0000000u) == 0xe0000000u;
}

[[nodiscard]] bool is_broadcast(std::uint32_t address) noexcept
{
    return 0xffffffffu == address;
}

[[nodiscard]] std::optional<std::uint16_t> local_port(aeron_udp_channel_transport_t* transport) noexcept
{
    char buffer[128] = {};
    // The reference's `bind_addr_and_port` answers the number of characters it
    // wrote, not 0 for success; a negative answer is the failure.
    if (0 > g_default->bind_addr_and_port_func(transport, buffer, sizeof(buffer)))
    {
        return std::nullopt;
    }
    const std::string_view text{buffer};
    const std::size_t colon = text.rfind(':');
    if (std::string_view::npos == colon)
    {
        return std::nullopt;
    }
    std::uint16_t port = 0;
    const auto [end, error] = std::from_chars(text.data() + colon + 1, text.data() + text.size(), port);
    if (std::errc{} != error || end != text.data() + text.size())
    {
        return std::nullopt;
    }
    return port;
}

extern "C" int dpdk_init(aeron_udp_channel_transport_t* transport, struct sockaddr_storage* bind_addr,
                         struct sockaddr_storage* multicast_if_addr, struct sockaddr_storage* connect_addr,
                         aeron_udp_channel_transport_params_t* params, aeron_driver_context_t* context,
                         aeron_udp_channel_transport_affinity_t affinity)
{
    if (nullptr == g_default || nullptr == g_stack)
    {
        return -1;
    }

    // The kernel cannot own an address this media owns: `bind(10.10.0.1)` is
    // EADDRNOTAVAIL there, and the default bindings would fail the whole
    // transport. The socket keeps the port on the wildcard instead, and the
    // stack below takes the address.
    struct sockaddr_storage kernel_bind{};
    struct sockaddr_storage* kernel_bind_addr = bind_addr;
    if (nullptr != bind_addr && AF_INET == bind_addr->ss_family && address_of(bind_addr) == g_stack->params().address)
    {
        kernel_bind = *bind_addr;
        reinterpret_cast<struct sockaddr_in*>(&kernel_bind)->sin_addr.s_addr = htonl(INADDR_ANY);
        kernel_bind_addr = &kernel_bind;
    }

    if (0 !=
        g_default->init_func(transport, kernel_bind_addr, multicast_if_addr, connect_addr, params, context, affinity))
    {
        return -1;
    }

    auto state = std::make_unique<TransportState>();
    state->affinity = affinity;
    bool capable = false;
    std::optional<std::uint32_t> address;
    std::optional<ports::Ipv4Endpoint> peer;

    if (nullptr != bind_addr && AF_INET == bind_addr->ss_family)
    {
        const std::uint32_t local = address_of(bind_addr);
        if (0 == local || local == g_stack->params().address)
        {
            capable = true;
            if (0 != local)
            {
                address = local;
            }
        }
    }

    if (capable && nullptr != connect_addr && AF_INET == connect_addr->ss_family)
    {
        const std::uint32_t remote = address_of(connect_addr);
        if (is_loopback(remote) || is_multicast(remote) || is_broadcast(remote))
        {
            capable = false;
        }
        else
        {
            peer = ports::Ipv4Endpoint{remote, port_of(connect_addr)};
        }
    }

    if (capable)
    {
        if (const auto port = local_port(transport); port && 0 != *port)
        {
            const ports::Ipv4Endpoint failure{g_stack->params().address, *port};
            if (auto binding = g_stack->bind(address, *port, peer, failure); binding)
            {
                state->binding = *binding;
            }
            else
            {
                // A channel that named this media's own address has nowhere
                // else to go: the kernel cannot bind it (`EADDRNOTAVAIL`), and
                // the socket the default bindings made sits on the wildcard
                // where nothing for this address arrives. Returning 0 here
                // (2026-11-23, an `ENOBUFS` that printed one line) left the
                // transport in `kernel_transports`, and it silently received
                // nothing for the life of the process. Fail instead, and let
                // the driver report it (the reference treats a refused bind as
                // a failed transport, `aeron_receive_channel_endpoint.c:1038`).
                const bool owns_the_address = address.has_value() && *address == g_stack->params().address;
                if (owns_the_address)
                {
                    const int errcode = binding.error().errcode;
                    char name[INET_ADDRSTRLEN]{};
                    struct in_addr own{};
                    own.s_addr = htonl(g_stack->params().address);
                    const char* text = inet_ntop(AF_INET, &own, name, sizeof(name));
                    (void)g_default->close_func(transport);
                    AERON_SET_ERR(errcode,
                                  "dpdk media: channel %s:%u could not be bound on the media (rings use %zu of %zu "
                                  "buffers)",
                                  nullptr != text ? text : "?", static_cast<unsigned>(*port), g_stack->ring_mbufs(),
                                  g_stack->params().ring_mbuf_budget);
                    return -1;
                }
                if (ENOBUFS == binding.error().errcode)
                {
                    std::fprintf(stdout,
                                 "dpdk media: a channel was refused: the mbuf pool has no room for another ring "
                                 "(rings use %zu of %zu buffers)\n",
                                 g_stack->ring_mbufs(), g_stack->params().ring_mbuf_budget);
                }
            }
        }
    }

    if (context->counters_manager)
    {
        struct TransportKey
        {
            std::uint32_t address;
            std::uint16_t port;
            std::uint16_t type;
        };
        const TransportKey key{address.value_or(g_stack->params().address),
                               static_cast<std::uint16_t>(local_port(transport).value_or(0)),
                               static_cast<std::uint16_t>(counters::TRANSPORT)};
        state->counter = allocate_counter(context, counters::TRANSPORT, &key, sizeof(key));
    }

    // The datagram is the driver's to size (`aeron.mtu.length`, 1408 by
    // default) and the frame bound is the port's. Copying only up to the port's
    // MTU sent anything larger down the kernel fallback in silence, where the
    // media's own address cannot go; the stack is the one that decides, and it
    // refuses what it cannot frame (counter 79, `EMSGSIZE`).
    transport->bindings_clientd = state.release();
    return 0;
}

extern "C" int dpdk_reconnect(aeron_udp_channel_transport_t* transport, struct sockaddr_storage* connect_addr)
{
    const int result = g_default->reconnect_func(transport, connect_addr);
    auto* state = static_cast<TransportState*>(transport->bindings_clientd);
    if (nullptr != state && state->binding && nullptr != connect_addr && AF_INET == connect_addr->ss_family)
    {
        g_stack->reconnect(*state->binding, ports::Ipv4Endpoint{address_of(connect_addr), port_of(connect_addr)});
    }
    return result;
}

extern "C" int dpdk_close(aeron_udp_channel_transport_t* transport)
{
    auto* state = static_cast<TransportState*>(transport->bindings_clientd);
    if (nullptr != state)
    {
        if (state->binding)
        {
            g_stack->release(*state->binding);
        }
        delete state;
        transport->bindings_clientd = nullptr;
    }
    return g_default->close_func(transport);
}

extern "C" int dpdk_recvmmsg(aeron_udp_channel_transport_t* transport, struct mmsghdr* msgvec, size_t vlen,
                             int64_t* bytes_rcved, aeron_udp_transport_recv_func_t recv_func, void* clientd)
{
    return g_default->recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
}

extern "C" int dpdk_send(aeron_udp_channel_data_paths_t* data_paths, aeron_udp_channel_transport_t* transport,
                         struct sockaddr_storage* address, struct iovec* iov, size_t iov_length, int64_t* bytes_sent)
{
    register_thread_once();

    auto* state = static_cast<TransportState*>(transport->bindings_clientd);
    if (nullptr == state || !state->binding || nullptr == address || AF_INET != address->ss_family)
    {
        return g_default->send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    // The name resolver's transport is the conductor's (DNS to a resolver); it
    // has a kernel socket of its own and no DPDK queue, and it is the one
    // affinity that is not one of the two sending agents.
    if (AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR == state->affinity)
    {
        return g_default->send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    const std::uint32_t destination = address_of(address);
    const std::uint16_t port = port_of(address);
    if (is_loopback(destination) || is_multicast(destination) || is_broadcast(destination) ||
        !g_stack->next_hop(destination))
    {
        return g_default->send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    // The reference's `send_func` contract: **one iov is one datagram**, and the
    // return is how many of them went, in order (`media/aeron_udp_channel_
    // transport.c:728-773`, `sendv`). It is what the sender's accounting stands
    // on -- `snd-pos` advances only when the whole batch was taken
    // (`aeron_network_publication.c:495-580`) -- so flattening a batch into one
    // datagram would both change the wire and read back as a short send.
    constexpr std::size_t MAX_DATAGRAMS = 16;  // AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND
    std::array<std::span<const std::uint8_t>, MAX_DATAGRAMS> datagrams{};
    std::size_t count = 0;
    for (std::size_t index = 0; index < iov_length && count < datagrams.size(); ++index)
    {
        datagrams[count++] =
            std::span<const std::uint8_t>{static_cast<const std::uint8_t*>(iov[index].iov_base), iov[index].iov_len};
    }
    if (0 == count)
    {
        return g_default->send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    // The affinity names the sending agent, and the agent picks the queue. In
    // DEDICATED they are two threads and two queues; in SHARED,
    // SHARED_NETWORK and INVOKER both agents run on one thread and the media
    // opened one transmit queue for it, so the two roles name the same queue
    // and the choice costs nothing.
    const auto role = AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER == state->affinity ? stack::Stack::TxRole::receiver
                                                                                       : stack::Stack::TxRole::sender;
    const auto sent =
        g_stack->send_datagrams(*state->binding, ports::Ipv4Endpoint{destination, port},
                                std::span<const std::span<const std::uint8_t>>{datagrams.data(), count}, role);
    if (!sent)
    {
        // The stack's 79 already counted it; the driver logs the sender error
        // this return becomes (`aeron_driver_sender.c:433-437`), so a datagram
        // the port cannot carry is loud instead of an eternal back pressure.
        AERON_SET_ERR(EMSGSIZE, "%s", std::string{sent.error().message.view()}.c_str());
        return -1;
    }
    *bytes_sent += sent->bytes;
    return static_cast<int>(sent->datagrams);
}

extern "C" int dpdk_get_so_rcvbuf(aeron_udp_channel_transport_t* transport, size_t* so_rcvbuf)
{
    return g_default->get_so_rcvbuf_func(transport, so_rcvbuf);
}

extern "C" int dpdk_bind_addr_and_port(aeron_udp_channel_transport_t* transport, char* buffer, size_t length)
{
    return g_default->bind_addr_and_port_func(transport, buffer, length);
}

extern "C" int dpdk_poller_init(aeron_udp_transport_poller_t* poller, aeron_driver_context_t* context,
                                aeron_udp_channel_transport_affinity_t affinity)
{
    auto state = std::make_unique<PollerState>();
    state->receiver = AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER == affinity;
    if (0 != g_default->poller_init_func(&state->inner, context, affinity))
    {
        return -1;
    }
    state->inner_ready = true;
    allocate_port_counters(context);
    state->buffer.resize(g_stack->params().mtu);
    poller->bindings_clientd = state.release();
    return 0;
}

extern "C" int dpdk_poller_close(aeron_udp_transport_poller_t* poller)
{
    auto* state = static_cast<PollerState*>(poller->bindings_clientd);
    if (nullptr == state)
    {
        return 0;
    }
    int result = 0;
    if (state->inner_ready)
    {
        result = g_default->poller_close_func(&state->inner);
    }
    delete state;
    poller->bindings_clientd = nullptr;
    return result;
}

extern "C" int dpdk_poller_add(aeron_udp_transport_poller_t* poller, aeron_udp_channel_transport_t* transport)
{
    auto* state = static_cast<PollerState*>(poller->bindings_clientd);
    const int result = g_default->poller_add_func(&state->inner, transport);
    auto* transport_state = static_cast<TransportState*>(transport->bindings_clientd);
    if (nullptr != transport_state && transport_state->binding)
    {
        state->transports.push_back(transport);
    }
    else
    {
        // A transport with no stack binding is the kernel's: the default
        // poller is the only thing that reads it, so it is what decides
        // whether the poller poll calls the default at all.
        state->kernel_transports.push_back(transport);
    }
    return result;
}

extern "C" int dpdk_poller_remove(aeron_udp_transport_poller_t* poller, aeron_udp_channel_transport_t* transport)
{
    auto* state = static_cast<PollerState*>(poller->bindings_clientd);
    const int result = g_default->poller_remove_func(&state->inner, transport);
    std::erase(state->transports, transport);
    // By the transport and not by the binding: the transport state may have
    // been closed by the time the reference takes it out of the poller, and a
    // classification read again would have forgotten which list it went in.
    std::erase(state->kernel_transports, transport);
    return result;
}

extern "C" int dpdk_poller_poll(aeron_udp_transport_poller_t* poller, struct mmsghdr* msgvec, size_t vlen,
                                int64_t* bytes_rcved, aeron_udp_transport_recv_func_t recv_func,
                                aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func, void* clientd)
{
    register_thread_once();

    auto* state = static_cast<PollerState*>(poller->bindings_clientd);
    std::size_t work = 0;

    if (state->receiver)
    {
        work += g_stack->pump();
        // The counters are paced by the clock, and the clock is read once
        // every thousand passes rather than once a pass: 0x3FF is ~180 us at
        // the measured ~5.6 million passes a second, and
        // `update_port_counters` keeps its own one-second boundary on top.
        if (0 == (++state->passes & 0x3FF))
        {
            update_port_counters();
        }
    }
    if (nullptr != g_counters.poller)
    {
        aeron_counter_increment(g_counters.poller);
    }

    for (auto* transport : state->transports)
    {
        auto* transport_state = static_cast<TransportState*>(transport->bindings_clientd);
        if (nullptr == transport_state || !transport_state->binding)
        {
            continue;
        }
        for (;;)
        {
            ports::Ipv4Endpoint source{};
            const std::size_t length = g_stack->receive(*transport_state->binding, state->buffer, &source);
            if (0 == length)
            {
                break;
            }
            struct sockaddr_storage address{};
            auto* sin = reinterpret_cast<struct sockaddr_in*>(&address);
            sin->sin_family = AF_INET;
            sin->sin_port = htons(source.port);
            sin->sin_addr.s_addr = htonl(source.address);
            recv_func(transport->data_paths, transport, clientd, transport->dispatch_clientd,
                      transport->destination_clientd, state->buffer.data(), length, &address, nullptr);
            // Every datagram this transport takes off the media, which is what
            // 76 counts. Without this the counter is allocated and never moves.
            if (nullptr != transport_state->counter)
            {
                aeron_counter_increment(transport_state->counter);
            }
            ++work;
        }
    }

    // The reference poller is called only when something in it is not on the
    // stack. Its `poll` is a `recvmmsg` per transport per pass
    // (`media/aeron_udp_transport_poller.c:187-215`), and a DPDK transport's
    // composite kernel socket never has a datagram: calling it anyway is a
    // syscall paid by every pass of a loop that runs millions of times a
    // second (measured in `analysis/dpdk/notes-2026-11-18-why-the-cpp-dpdk-is-
    // slower.md`: 62.7% of the receiver's cycles in the kernel).
    if (!state->kernel_transports.empty())
    {
        work += static_cast<std::size_t>(
            g_default->poller_poll_func(&state->inner, msgvec, vlen, bytes_rcved, recv_func, recvmmsg_func, clientd));
    }
    return static_cast<int>(work);
}

aeron_udp_channel_transport_bindings_t g_bindings = {
    &dpdk_init,
    &dpdk_reconnect,
    &dpdk_close,
    &dpdk_recvmmsg,
    &dpdk_send,
    &dpdk_get_so_rcvbuf,
    &dpdk_bind_addr_and_port,
    &dpdk_poller_init,
    &dpdk_poller_close,
    &dpdk_poller_add,
    &dpdk_poller_remove,
    &dpdk_poller_poll,
    {"dpdk", "media", nullptr},
};

}  // namespace

std::expected<void, Error> install(aeron_driver_context_t* context, stack::Stack& target, Port* port,
                                   Mempool* pool) noexcept
{
    if (nullptr == context)
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "null driver context"));
    }
    // Every threading mode is served. Which thread calls which slot is the
    // driver's business (`aeron_driver.c:1043-1170`); the media's only
    // mode-dependent choice is how many transmit queues the port was opened
    // with, and `open_media` made it from this same context before the driver
    // started. In DEDICATED the sender and receiver agents each send on their
    // own queue; in the other three modes both send from one thread, so both
    // affinities map to that one queue (the port reports one transmit queue
    // and `Media`'s single-queue layout is what `dpdk_send` below finds).

    // Nobody else drains the port: an idle strategy that sleeps leaves the
    // frames in the NIC ring (counter 84) with no hint of why. A warning, not a
    // refusal -- the reference lets an operator pick (design §4.12); stdout,
    // because the reference's harnesses read stderr as a failure channel.
    // The strategy's name is not printed: `aeron_driver_context_init` loads the
    // function from `AERON_*_IDLE_STRATEGY` but leaves the name at `backoff`,
    // so the name would read "backoff" for a strategy that sleeps.
    //
    // The agent that drains the ring is the one whose strategy is read: the
    // receiver and the sender in DEDICATED (two runners); one agent for both in
    // SHARED and INVOKER (`aeron_driver_shared_do_work`, `aeron_driver.c:753-
    // 761`); the shared network agent in SHARED_NETWORK (`:776-782`). The
    // strategies of agents that do not exist in the mode are not consulted.
    const auto warn_if_idle_starves_the_ring = [](const char* role, aeron_idle_strategy_func_t strategy)
    {
        if (idle_strategy_can_starve_the_ring(strategy))
        {
            std::fprintf(stdout,
                         "dpdk media warning: the %s idle strategy is not a busy spin; frames wait in the NIC "
                         "ring while that thread idles (counter 84 counts what the ring drops)\n",
                         role);
        }
    };
    switch (context->threading_mode)
    {
        case AERON_THREADING_MODE_SHARED:
        case AERON_THREADING_MODE_INVOKER:
            warn_if_idle_starves_the_ring("shared", context->shared_idle_strategy_func);
            break;
        case AERON_THREADING_MODE_SHARED_NETWORK:
            warn_if_idle_starves_the_ring("shared network", context->shared_network_idle_strategy_func);
            break;
        case AERON_THREADING_MODE_DEDICATED:
        default:
            warn_if_idle_starves_the_ring("receiver", context->receiver_idle_strategy_func);
            warn_if_idle_starves_the_ring("sender", context->sender_idle_strategy_func);
            break;
    }
    if (nullptr != g_stack && g_stack != &target)
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "the DPDK bindings are already installed over another stack"));
    }

    const auto* defaults = aeron_driver_context_get_udp_channel_transport_bindings(context);
    if (nullptr == defaults)
    {
        return std::unexpected(
            make_error(ErrorCode::internal_error, "the driver's default transport bindings are missing"));
    }

    g_default = defaults;
    g_stack = &target;
    g_port = port;
    g_pool = pool;
    (void)aeron_driver_context_set_udp_channel_transport_bindings(context, &g_bindings);
    return {};
}

}  // namespace axc::core::media::dpdk::bindings
