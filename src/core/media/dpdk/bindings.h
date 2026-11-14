// The C driver's bindings vtable, over the userspace stack.
//
// The 12 slots are the reference's seam
// (`media/aeron_udp_channel_transport_bindings.h:113-134`). Every transport
// keeps its kernel socket: the default bindings are delegated to first, and the
// stack is an additional path for the frames the DPDK port carries. Sends are
// routed per destination, and the poller is a hybrid of the kernel's own and
// the stack's rings.

#pragma once

#include <expected>

#include "core/error/error.h"

extern "C"
{
#include "aeronmd.h"
}

namespace axc::core::media::dpdk
{

namespace stack
{
class Stack;
}

class Mempool;
class Port;

namespace bindings
{

/// Install the composite vtable into `context`, over `target`.
///
/// Only DEDICATED drivers: the receiver thread pumps the port and pushes into
/// the per-slot rings, and the sender thread drains its own transports' rings.
[[nodiscard]] std::expected<void, Error> install(aeron_driver_context_t* context, stack::Stack& target, Port* port,
                                                 Mempool* pool) noexcept;

}  // namespace bindings

}  // namespace axc::core::media::dpdk
