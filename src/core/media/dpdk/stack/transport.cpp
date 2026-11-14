#include "core/media/dpdk/stack/transport.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace axc::core::media::dpdk::transport
{

Transport::Transport(stack::Stack& target, const ports::Binding& binding, std::optional<ports::Ipv4Endpoint> peer)
    : stack_{&target}, binding_{binding}, peer_{peer}
{
}

Transport::~Transport()
{
    close();
}

std::size_t Transport::send(std::span<const std::uint8_t> payload)
{
    if (!peer_)
    {
        return 0;
    }
    return stack_->send(binding_, *peer_, payload);
}

std::size_t Transport::send_to(ports::Ipv4Endpoint destination, std::span<const std::uint8_t> payload)
{
    return stack_->send(binding_, destination, payload);
}

std::size_t Transport::receive(std::span<std::uint8_t> buffer)
{
    return stack_->receive(binding_, buffer);
}

void Transport::reconnect(ports::Ipv4Endpoint peer) noexcept
{
    peer_ = peer;
    stack_->reconnect(binding_, peer);
}

std::uint32_t Transport::local_address() const noexcept
{
    return stack_->params().address;
}

void Transport::close() noexcept
{
    if (!closed_)
    {
        stack_->release(binding_);
        closed_ = true;
    }
}

}  // namespace axc::core::media::dpdk::transport
