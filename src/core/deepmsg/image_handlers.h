#pragma once

#include <aeronc.h>

namespace axc::core
{

/// Callbacks registered with a subscription at add time (the C API takes them
/// on `aeron_async_add_subscription`, not on the subscription afterwards).
struct ImageHandlers
{
    aeron_on_available_image_t on_available{nullptr};
    void* on_available_clientd{nullptr};
    aeron_on_unavailable_image_t on_unavailable{nullptr};
    void* on_unavailable_clientd{nullptr};
};

}  // namespace axc::core
