#include "archive/server/threading.h"

#include <string>
#include <utility>

#include "archive/server/archive_config.h"

namespace deepmsg::archive::server
{
namespace
{

/// `Archive.java:449`: the default idle strategy name.
constexpr std::string_view DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

[[nodiscard]] std::expected<std::int64_t, axc::core::Error> duration_or(const axc::core::PropertySource& source,
                                                                        std::string_view name,
                                                                        std::int64_t fallback) noexcept
{
    const auto value = source.get(name);
    if (!value.has_value())
    {
        return fallback;
    }

    auto parsed = parse_duration_ns(*value);
    if (!parsed)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }
    return *parsed;
}

/// Parses `name` when present, otherwise the reference default name; a bad name
/// is a configuration error carrying the property name.
[[nodiscard]] std::expected<axc::core::IdleStrategy, axc::core::Error> idle_or(
    const axc::core::PropertySource& source, std::string_view name,
    std::string_view default_name = DEFAULT_IDLE_STRATEGY) noexcept
{
    const auto value = source.get(name);
    const auto id = value.has_value() ? std::string{*value} : std::string{default_name};

    auto parsed = axc::core::IdleStrategy::by_agrona_name(id);
    if (!parsed)
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error,
            "invalid " + std::string{name} + ": " + id + " (" + std::string{parsed.error().message.view()} + ")"));
    }
    return *parsed;
}

}  // namespace

std::expected<ArchiveThreadingMode, axc::core::Error> parse_threading_mode(std::string_view value) noexcept
{
    if (value == "INVOKER")
    {
        return ArchiveThreadingMode::invoker;
    }
    if (value == "SHARED")
    {
        return ArchiveThreadingMode::shared;
    }
    if (value == "DEDICATED")
    {
        return ArchiveThreadingMode::dedicated;
    }
    return std::unexpected(axc::core::make_error(
        axc::core::ErrorCode::config_error,
        "invalid aeron.archive.threading.mode: " + std::string{value} + " (INVOKER, SHARED, DEDICATED)"));
}

std::expected<ThreadNaming, axc::core::Error> parse_thread_naming(std::string_view value) noexcept
{
    if (value == "classic")
    {
        return ThreadNaming::classic;
    }
    if (value == "new")
    {
        return ThreadNaming::new_names;
    }
    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                 std::string{"Unknown thread naming mode: "} + std::string{value}));
}

std::string_view threading_mode_name(ArchiveThreadingMode mode) noexcept
{
    switch (mode)
    {
        case ArchiveThreadingMode::invoker:
            return "INVOKER";
        case ArchiveThreadingMode::shared:
            return "SHARED";
        case ArchiveThreadingMode::dedicated:
            return "DEDICATED";
    }
    return "SHARED";
}

std::string_view thread_name(ThreadNaming naming, AgentRole role) noexcept
{
    if (ThreadNaming::new_names == naming)
    {
        switch (role)
        {
            case AgentRole::conductor:
                return "aeron-ar-cnd";
            case AgentRole::recorder:
                return "aeron-ar-rec";
            case AgentRole::replayer:
                return "aeron-ar-rep";
            case AgentRole::shared:
                return "aeron-ar-shd";
        }
    }

    switch (role)
    {
        case AgentRole::recorder:
            return "archive-recorder";
        case AgentRole::replayer:
            return "archive-replayer";
        case AgentRole::conductor:
        case AgentRole::shared:
            // `Archive.java:132` / `:134`: classic SHARED and conductor share a name.
            return "archive-conductor";
    }

    return "archive-conductor";
}

std::expected<ThreadingSettings, axc::core::Error> parse_threading_settings(
    const axc::core::PropertySource& source) noexcept
{
    ThreadingSettings settings;

    if (const auto value = source.get("aeron.archive.threading.mode"); value.has_value())
    {
        auto mode = parse_threading_mode(*value);
        if (!mode)
        {
            return std::unexpected(mode.error());
        }
        settings.mode = *mode;
    }

    if (const auto value = source.get("aeron.thread.naming"); value.has_value())
    {
        auto naming = parse_thread_naming(*value);
        if (!naming)
        {
            return std::unexpected(naming.error());
        }
        settings.naming = *naming;
    }

    auto archive_idle = idle_or(source, "aeron.archive.idle.strategy");
    if (!archive_idle)
    {
        return std::unexpected(archive_idle.error());
    }
    settings.idle = *archive_idle;

    // `Archive.java:1489-1507`: an unset recorder/replayer strategy falls back
    // to the archive's own.
    settings.recorder_idle = settings.idle;
    if (source.contains("aeron.archive.recorder.idle.strategy"))
    {
        auto recorder_idle = idle_or(source, "aeron.archive.recorder.idle.strategy");
        if (!recorder_idle)
        {
            return std::unexpected(recorder_idle.error());
        }
        settings.recorder_idle = *recorder_idle;
    }
    settings.replayer_idle = settings.idle;
    if (source.contains("aeron.archive.replayer.idle.strategy"))
    {
        auto replayer_idle = idle_or(source, "aeron.archive.replayer.idle.strategy");
        if (!replayer_idle)
        {
            return std::unexpected(replayer_idle.error());
        }
        settings.replayer_idle = *replayer_idle;
    }

    for (const auto& [name, target] : {std::pair<std::string_view, axc::core::CpuAffinity* const>{
                                           "aeron.archive.conductor.cpu.affinity", &settings.conductor_affinity},
                                       std::pair<std::string_view, axc::core::CpuAffinity* const>{
                                           "aeron.archive.recorder.cpu.affinity", &settings.recorder_affinity},
                                       std::pair<std::string_view, axc::core::CpuAffinity* const>{
                                           "aeron.archive.replayer.cpu.affinity", &settings.replayer_affinity}})
    {
        const auto value = source.get(name);
        if (!value.has_value())
        {
            continue;
        }
        auto parsed = axc::core::CpuAffinity::parse(*value);
        if (!parsed)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                         "invalid " + std::string{name} + ": " + std::string{*value}));
        }
        *target = *parsed;
    }

    auto conductor_threshold =
        duration_or(source, "aeron.archive.conductor.cycle.threshold", settings.conductor_cycle_threshold_ns);
    if (!conductor_threshold)
    {
        return std::unexpected(conductor_threshold.error());
    }
    settings.conductor_cycle_threshold_ns = *conductor_threshold;

    auto recorder_threshold =
        duration_or(source, "aeron.archive.recorder.cycle.threshold", settings.recorder_cycle_threshold_ns);
    if (!recorder_threshold)
    {
        return std::unexpected(recorder_threshold.error());
    }
    settings.recorder_cycle_threshold_ns = *recorder_threshold;

    auto replayer_threshold =
        duration_or(source, "aeron.archive.replayer.cycle.threshold", settings.replayer_cycle_threshold_ns);
    if (!replayer_threshold)
    {
        return std::unexpected(replayer_threshold.error());
    }
    settings.replayer_cycle_threshold_ns = *replayer_threshold;

    return settings;
}

}  // namespace deepmsg::archive::server
