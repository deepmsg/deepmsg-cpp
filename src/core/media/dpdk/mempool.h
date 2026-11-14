// The mempool the frames come from.
//
// Transcribed from the Rust track's `crates/dpdk/src/mempool.rs`: one pool per
// process, created on the port's NUMA socket, and the handles it hands out are
// the shim's own PODs rather than DPDK's.

#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core::media::dpdk
{

class Mempool
{
public:
    /// `data_room` of zero takes DPDK's default; `socket_id` `-1` means any.
    [[nodiscard]] static std::expected<Mempool, Error> create(std::string_view name, std::uint32_t count,
                                                              std::uint32_t cache, std::uint32_t data_room,
                                                              int socket_id) noexcept;

    Mempool(Mempool&& other) noexcept;
    Mempool& operator=(Mempool&& other) noexcept;
    ~Mempool();

    Mempool(const Mempool&) = delete;
    Mempool& operator=(const Mempool&) = delete;

    [[nodiscard]] std::uint32_t available() const noexcept;
    [[nodiscard]] void* handle() const noexcept
    {
        return handle_;
    }

    /// Give frames back to their pool.
    static void free_bulk(void** mbufs, std::uint32_t count) noexcept;

private:
    explicit Mempool(void* handle) noexcept : handle_{handle} {}

    void* handle_{nullptr};
};

}  // namespace axc::core::media::dpdk
