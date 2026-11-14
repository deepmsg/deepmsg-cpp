// The process's DPDK EAL, once.
//
// Transcribed from the Rust track's `crates/dpdk/src/lib.rs` (`Eal`) and
// `media/userspace/eal.rs`: the shim's signatures are the boundary, and the
// rules are the shim's own — one `rte_eal_init` per process, and a
// `rte_eal_cleanup` that does not undo it.

#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>

#include "core/error/error.h"

namespace axc::core::media::dpdk
{

class Eal
{
public:
    /// `args` is the whole command line, `argv[0]` first, as DPDK expects it.
    [[nodiscard]] static std::expected<Eal, Error> init(std::span<const std::string> args) noexcept;

    /// The process's EAL, if it is up.
    [[nodiscard]] static bool initialised() noexcept;

    /// `rte_eal_cleanup`; the process is expected to exit afterwards.
    [[nodiscard]] static std::expected<void, Error> cleanup() noexcept;

    [[nodiscard]] static std::expected<void, Error> save_affinity() noexcept;
    [[nodiscard]] static std::expected<void, Error> restore_affinity() noexcept;

    /// Register the calling thread with EAL so its mempool cache is used.
    [[nodiscard]] static std::expected<void, Error> register_thread() noexcept;
    static void unregister_thread() noexcept;
    [[nodiscard]] static unsigned lcore_id() noexcept;
    [[nodiscard]] static unsigned lcore_count() noexcept;

    Eal(Eal&& other) noexcept;
    Eal& operator=(Eal&& other) noexcept;
    ~Eal() = default;

    Eal(const Eal&) = delete;
    Eal& operator=(const Eal&) = delete;

private:
    Eal() = default;
};

}  // namespace axc::core::media::dpdk
