#pragma once

#include <cstdint>
#include <expected>
#include <optional>

#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"
#include "core/time/clock.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// Drive the client until an async add completes, or fail at the deadline.
///
/// This is the reference's synchronous `aeron.addPublication(...)` shape: the
/// caller is the thread that also runs `aeronAgentInvoker`, so the conductor
/// makes progress only while we poll. Used only on startup/egress paths, never
/// inside a log callback.
template <class H>
[[nodiscard]] std::expected<H, Error> await_async(axc::core::DeepmsgClient& client, axc::core::AsyncAdd<H>& add,
                                                  std::int64_t timeout_ns)
{
    const std::int64_t deadline = axc::core::NanoClock::now() + timeout_ns;

    while (true)
    {
        (void)client.do_work();

        auto polled = add.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            return std::move(**polled);
        }

        if (axc::core::NanoClock::now() >= deadline)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity, "async add timed out"));
        }
    }
}

}  // namespace deepmsg::cluster::service
