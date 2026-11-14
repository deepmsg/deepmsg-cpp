// The transport the driver sees: one bound `(address, port)` on one stack.
//
// Everything the endpoints, publications and images ask of a transport is
// asked here, and the answers are the kernel's answers (G1–G12). Transcribed
// from the Rust track's `media/userspace/transport.rs`.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include "core/media/dpdk/stack/ports.h"
#include "core/media/dpdk/stack/stack.h"

namespace axc::core::media::dpdk::transport
{

class Transport
{
public:
    /// The stack must outlive the transport.
    Transport(stack::Stack& target, const ports::Binding& binding,
              std::optional<ports::Ipv4Endpoint> peer = std::nullopt);
    ~Transport();

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;
    Transport(Transport&&) = delete;
    Transport& operator=(Transport&&) = delete;

    /// Send to the connected peer. Zero is back pressure.
    std::size_t send(std::span<const std::uint8_t> payload);

    /// Send to an explicit destination (an unconnected transport).
    std::size_t send_to(ports::Ipv4Endpoint destination, std::span<const std::uint8_t> payload);

    /// Copy the next datagram into `buffer`; zero when none is waiting.
    std::size_t receive(std::span<std::uint8_t> buffer);

    [[nodiscard]] std::optional<ports::Ipv4Endpoint> peer() const noexcept
    {
        return peer_;
    }
    void reconnect(ports::Ipv4Endpoint peer) noexcept;

    [[nodiscard]] std::uint16_t local_port() const noexcept
    {
        return binding_.port;
    }
    [[nodiscard]] std::uint32_t local_address() const noexcept;
    [[nodiscard]] const ports::Binding& binding() const noexcept
    {
        return binding_;
    }

    /// Give the port back. Idempotent; the destructor calls it.
    void close() noexcept;

private:
    stack::Stack* stack_;
    ports::Binding binding_;
    std::optional<ports::Ipv4Endpoint> peer_;
    bool closed_{false};
};

}  // namespace axc::core::media::dpdk::transport
