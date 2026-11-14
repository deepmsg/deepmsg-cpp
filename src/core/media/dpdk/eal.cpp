#include "core/media/dpdk/eal.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "core/media/dpdk/shim/shim.h"

namespace axc::core::media::dpdk
{

namespace
{

std::atomic<bool> g_initialised{false};

[[nodiscard]] Error rte_error(std::string_view what) noexcept
{
    std::string message{what};
    message += ": ";
    message += shim_rte_strerror(shim_rte_errno());
    return make_error(ErrorCode::config_error, message);
}

[[nodiscard]] Error pthread_error(std::string_view what) noexcept
{
    std::string message{what};
    message += ": ";
    message += std::strerror(errno);
    return make_error(ErrorCode::internal_error, message);
}

}  // namespace

std::expected<Eal, Error> Eal::init(std::span<const std::string> args) noexcept
{
    if (g_initialised.load(std::memory_order_acquire))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "the EAL is already initialised in this process"));
    }
    if (args.empty())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "the EAL needs argv[0] as its first argument"));
    }

    std::vector<std::vector<char>> storage;
    storage.reserve(args.size());
    for (const auto& argument : args)
    {
        storage.emplace_back(argument.begin(), argument.end());
        storage.back().push_back('\0');
    }

    std::vector<char*> argv;
    argv.reserve(storage.size());
    for (auto& argument : storage)
    {
        argv.push_back(argument.data());
    }

    if (0 > shim_eal_init(static_cast<int>(argv.size()), argv.data()))
    {
        return std::unexpected(rte_error("rte_eal_init"));
    }

    g_initialised.store(true, std::memory_order_release);
    return Eal{};
}

bool Eal::initialised() noexcept
{
    return g_initialised.load(std::memory_order_acquire);
}

std::expected<void, Error> Eal::cleanup() noexcept
{
    if (!g_initialised.load(std::memory_order_acquire))
    {
        return {};
    }
    if (0 != shim_eal_cleanup())
    {
        return std::unexpected(rte_error("rte_eal_cleanup"));
    }
    g_initialised.store(false, std::memory_order_release);
    return {};
}

std::expected<void, Error> Eal::save_affinity() noexcept
{
    if (0 != shim_save_affinity())
    {
        return std::unexpected(pthread_error("pthread_getaffinity_np"));
    }
    return {};
}

std::expected<void, Error> Eal::restore_affinity() noexcept
{
    if (0 != shim_restore_affinity())
    {
        return std::unexpected(pthread_error("pthread_setaffinity_np"));
    }
    return {};
}

std::expected<void, Error> Eal::register_thread() noexcept
{
    if (0 != shim_thread_register())
    {
        return std::unexpected(rte_error("rte_thread_register"));
    }
    return {};
}

void Eal::unregister_thread() noexcept
{
    shim_thread_unregister();
}

unsigned Eal::lcore_id() noexcept
{
    return shim_lcore_id();
}

unsigned Eal::lcore_count() noexcept
{
    return shim_lcore_count();
}

Eal::Eal(Eal&& other) noexcept = default;
Eal& Eal::operator=(Eal&& other) noexcept = default;

}  // namespace axc::core::media::dpdk
