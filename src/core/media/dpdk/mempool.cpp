#include "core/media/dpdk/mempool.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "core/media/dpdk/shim/shim.h"

namespace axc::core::media::dpdk
{

std::expected<Mempool, Error> Mempool::create(std::string_view name, std::uint32_t count, std::uint32_t cache,
                                              std::uint32_t data_room, int socket_id) noexcept
{
    const std::string copy{name};
    void* pool = shim_pool_create(copy.c_str(), count, cache, data_room, socket_id);
    if (nullptr == pool)
    {
        std::string message{"rte_pktmbuf_pool_create: "};
        message += shim_rte_strerror(shim_rte_errno());
        return std::unexpected(make_error(ErrorCode::internal_error, message));
    }
    return Mempool{pool};
}

Mempool::Mempool(Mempool&& other) noexcept : handle_{other.handle_}
{
    other.handle_ = nullptr;
}

Mempool& Mempool::operator=(Mempool&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != handle_)
        {
            shim_pool_free(handle_);
        }
        handle_ = other.handle_;
        other.handle_ = nullptr;
    }
    return *this;
}

Mempool::~Mempool()
{
    if (nullptr != handle_)
    {
        shim_pool_free(handle_);
    }
}

std::uint32_t Mempool::available() const noexcept
{
    return shim_pool_avail(handle_);
}

void Mempool::free_bulk(void** mbufs, std::uint32_t count) noexcept
{
    shim_mbuf_free_bulk(mbufs, count);
}

}  // namespace axc::core::media::dpdk
