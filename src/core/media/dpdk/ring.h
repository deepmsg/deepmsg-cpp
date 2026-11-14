// A ring of frame handles, for the frames that cross threads.

#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core::media::dpdk
{

class FrameRing
{
public:
    /// A single-producer, single-consumer ring; `capacity` must be a power of
    /// two.
    [[nodiscard]] static std::expected<FrameRing, Error> create(std::string_view name, std::uint32_t capacity) noexcept;

    FrameRing(FrameRing&& other) noexcept;
    FrameRing& operator=(FrameRing&& other) noexcept;
    ~FrameRing();

    FrameRing(const FrameRing&) = delete;
    FrameRing& operator=(const FrameRing&) = delete;

    [[nodiscard]] std::size_t enqueue(void* const* mbufs, std::uint32_t count) noexcept;
    [[nodiscard]] std::size_t dequeue(void** mbufs, std::uint32_t count) noexcept;

private:
    explicit FrameRing(void* handle) noexcept : handle_{handle} {}

    void* handle_{nullptr};
};

}  // namespace axc::core::media::dpdk
