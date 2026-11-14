#include "core/media/dpdk/ring.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "core/media/dpdk/shim/shim.h"

namespace axc::core::media::dpdk
{

std::expected<FrameRing, Error> FrameRing::create(std::string_view name, std::uint32_t capacity) noexcept
{
    const std::string copy{name};
    void* ring = shim_ring_create(copy.c_str(), capacity);
    if (nullptr == ring)
    {
        std::string message{"rte_ring_create: "};
        message += shim_rte_strerror(shim_rte_errno());
        return std::unexpected(make_error(ErrorCode::internal_error, message));
    }
    return FrameRing{ring};
}

FrameRing::FrameRing(FrameRing&& other) noexcept : handle_{other.handle_}
{
    other.handle_ = nullptr;
}

FrameRing& FrameRing::operator=(FrameRing&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != handle_)
        {
            shim_ring_free(handle_);
        }
        handle_ = other.handle_;
        other.handle_ = nullptr;
    }
    return *this;
}

FrameRing::~FrameRing()
{
    if (nullptr != handle_)
    {
        shim_ring_free(handle_);
    }
}

std::size_t FrameRing::enqueue(void* const* mbufs, std::uint32_t count) noexcept
{
    return shim_ring_enqueue_burst(handle_, mbufs, count);
}

std::size_t FrameRing::dequeue(void** mbufs, std::uint32_t count) noexcept
{
    return shim_ring_dequeue_burst(handle_, mbufs, count);
}

}  // namespace axc::core::media::dpdk
