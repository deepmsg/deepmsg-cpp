#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// `aeron.thread.naming` (`CommonContext.java:534`).
enum class ThreadNaming
{
    classic,
    new_naming
};

[[nodiscard]] std::optional<ThreadNaming> parse_thread_naming(std::string_view value) noexcept;
[[nodiscard]] std::string container_thread_name(std::int32_t cluster_id, std::int32_t service_id, ThreadNaming naming);

struct ServiceContainerConfig
{
    static constexpr std::int32_t MAX_SERVICE_COUNT = 10;

    std::int32_t cluster_id{0};
    std::int32_t service_id{0};
    std::string service_name{};
    std::optional<std::string> service{};
    ThreadNaming thread_naming{ThreadNaming::classic};
    std::string aeron_dir{};
    std::string cluster_dir{"aeron-cluster"};
    std::optional<std::string> mark_file_dir{};
    std::string control_channel{"aeron:ipc?term-length=128k"};
    std::int32_t service_stream_id{104};
    std::int32_t consensus_module_stream_id{105};
    std::size_t log_fragment_limit{50};
    bool responding_service{true};
    std::int32_t error_buffer_length{1024 * 1024};
    std::string idle_strategy{"org.agrona.concurrent.BackoffIdleStrategy"};
    std::int32_t app_version{1};
    std::string replay_channel{"aeron:ipc"};
    std::int32_t replay_stream_id{103};
    std::string snapshot_channel{"aeron:ipc?alias=snapshot"};
    std::int32_t snapshot_stream_id{106};
    std::string archive_local_control_channel{"aeron:ipc?term-length=64k"};
    std::int32_t archive_local_control_stream_id{10};
    std::int32_t archive_control_response_stream_id{0};
    std::int64_t snapshot_duration_threshold_ns{1'000'000'000};
    std::int64_t cycle_threshold_ns{100'000'000};

    /// Where this container's own mark file lives
    /// (`ClusterMarkFile.markFilenameForService`).
    [[nodiscard]] std::string mark_file_path() const;
    [[nodiscard]] std::string mark_file_link_name() const;
};

/// Reads the names the container honours, with the reference's defaults.
[[nodiscard]] std::expected<ServiceContainerConfig, Error> container_config_from_properties(
    const axc::core::PropertySource& properties);

}  // namespace deepmsg::cluster::service
