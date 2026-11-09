#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include "core/agent/cpu_affinity.h"
#include "core/agent/idle_strategy.h"
#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `ArchiveThreadingMode` (`ArchiveThreadingMode.java`); default `DEDICATED`
/// (`Archive.java:793-795`).
enum class ArchiveThreadingMode
{
    invoker,
    shared,
    dedicated
};

/// `aeron.thread.naming` (`CommonContext.java:538-591`): `classic` (default) or
/// `new`; anything else throws in the reference.
enum class ThreadNaming
{
    classic,
    new_names
};

/// The four named agent roles (`Archive.java:126-134`).
enum class AgentRole
{
    conductor,
    recorder,
    replayer,
    shared
};

[[nodiscard]] std::expected<ArchiveThreadingMode, axc::core::Error> parse_threading_mode(
    std::string_view value) noexcept;

[[nodiscard]] std::expected<ThreadNaming, axc::core::Error> parse_thread_naming(std::string_view value) noexcept;

/// `CommonContext.threadName(newName, classicName)` (`CommonContext.java:582-591`).
[[nodiscard]] std::string_view thread_name(ThreadNaming naming, AgentRole role) noexcept;

/// The three idle strategies, three affinities and three cycle thresholds
/// (`Archive.java:404-449`, `:571-601`, `:1489-1545`).
struct ThreadingSettings
{
    ArchiveThreadingMode mode{ArchiveThreadingMode::dedicated};
    ThreadNaming naming{ThreadNaming::classic};

    axc::core::IdleStrategy idle;
    /// Unset ⇒ falls back to `idle` (`Archive.java:1489-1507`).
    axc::core::IdleStrategy recorder_idle;
    axc::core::IdleStrategy replayer_idle;

    axc::core::CpuAffinity conductor_affinity;
    axc::core::CpuAffinity recorder_affinity;
    axc::core::CpuAffinity replayer_affinity;

    std::int64_t conductor_cycle_threshold_ns{100'000'000};
    std::int64_t recorder_cycle_threshold_ns{100'000'000};
    std::int64_t replayer_cycle_threshold_ns{100'000'000};
};

/// Reads `aeron.archive.threading.mode`, the three idle names, the three
/// affinities, the three thresholds and `aeron.thread.naming`; the reference
/// throws on unknown values and so do we (`config_error`).
[[nodiscard]] std::expected<ThreadingSettings, axc::core::Error> parse_threading_settings(
    const axc::core::PropertySource& source) noexcept;

}  // namespace deepmsg::archive::server
