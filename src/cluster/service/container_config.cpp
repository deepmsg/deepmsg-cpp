#include "cluster/service/container_config.h"

#include <cctype>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string>

namespace deepmsg::cluster::service
{
namespace
{

constexpr std::string_view CLUSTER_ID_PROP_NAME = "aeron.cluster.id";
constexpr std::string_view SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";
constexpr std::string_view SERVICE_NAME_PROP_NAME = "aeron.cluster.service.name";
constexpr std::string_view SERVICE_REGISTRY_PROP_NAME = "deepmsg.cluster.service";
constexpr std::string_view THREAD_NAMING_PROP_NAME = "aeron.thread.naming";
constexpr std::string_view SERVICE_CLASS_NAME_PROP_NAME = "aeron.cluster.service.class.name";
constexpr std::string_view CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.control.channel";
constexpr std::string_view SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";
constexpr std::string_view CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";
constexpr std::string_view REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";
constexpr std::string_view REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";
constexpr std::string_view SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";
constexpr std::string_view SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";
constexpr std::string_view CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";
constexpr std::string_view MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";
constexpr std::string_view ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.service.error.buffer.length";
constexpr std::string_view RESPONDER_SERVICE_PROP_NAME = "aeron.cluster.service.responder";
constexpr std::string_view LOG_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.log.fragment.limit";
constexpr std::string_view IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";
constexpr std::string_view AERON_DIR_PROP_NAME = "aeron.dir";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME = "aeron.archive.local.control.channel";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.local.control.stream.id";
constexpr std::string_view ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";
constexpr std::string_view CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.service.cycle.threshold";
constexpr std::string_view SNAPSHOT_DURATION_THRESHOLD_PROP_NAME = "aeron.cluster.service.snapshot.threshold";

using axc::core::Error;
using axc::core::ErrorCode;
using axc::core::make_error;

[[nodiscard]] std::expected<std::int32_t, Error> parse_int(const axc::core::PropertySource& properties,
                                                           std::string_view name, std::int32_t default_value)
{
    const auto text = properties.get(name);
    if (!text.has_value())
    {
        return default_value;
    }

    std::int32_t value = 0;
    const auto result = std::from_chars(text->data(), text->data() + text->size(), value);
    if (std::errc{} != result.ec || result.ptr != text->data() + text->size())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, std::string{name} + "=" + std::string{*text} + " is not a value"));
    }
    return value;
}

[[nodiscard]] std::int64_t parse_duration_ns(std::string_view value)
{
    std::string lower{value};
    for (char& c : lower)
    {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    struct Unit
    {
        std::string_view suffix;
        std::int64_t scale;
    };
    constexpr Unit units[] = {{"ns", 1}, {"us", 1'000}, {"ms", 1'000'000}, {"s", 1'000'000'000}};

    for (const Unit& unit : units)
    {
        if (lower.size() >= unit.suffix.size() &&
            lower.compare(lower.size() - unit.suffix.size(), unit.suffix.size(), unit.suffix) == 0)
        {
            const std::string digits = lower.substr(0, lower.size() - unit.suffix.size());
            std::int64_t parsed = 0;
            const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), parsed);
            if (std::errc{} == result.ec && result.ptr == digits.data() + digits.size())
            {
                return parsed * unit.scale;
            }
            break;
        }
    }

    std::int64_t parsed = 0;
    const auto result = std::from_chars(lower.data(), lower.data() + lower.size(), parsed);
    return std::errc{} == result.ec && result.ptr == lower.data() + lower.size() ? parsed : -1;
}

[[nodiscard]] bool parse_bool(const axc::core::PropertySource& properties, std::string_view name, bool default_value)
{
    const auto text = properties.get(name);
    if (!text.has_value())
    {
        return default_value;
    }
    std::string lower{*text};
    for (char& c : lower)
    {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return "true" == lower;
}

}  // namespace

std::optional<ThreadNaming> parse_thread_naming(std::string_view value) noexcept
{
    if ("classic" == value)
    {
        return ThreadNaming::classic;
    }
    if ("new" == value)
    {
        return ThreadNaming::new_naming;
    }
    return std::nullopt;
}

std::string container_thread_name(std::int32_t cluster_id, std::int32_t service_id, ThreadNaming naming)
{
    if (ThreadNaming::new_naming == naming)
    {
        return "aeron-cl-cs-" + std::to_string(service_id);
    }
    return "clustered-service-" + std::to_string(cluster_id) + "-" + std::to_string(service_id);
}

std::string ServiceContainerConfig::mark_file_path() const
{
    const std::string directory = mark_file_dir.has_value() ? *mark_file_dir : cluster_dir;
    return directory + "/cluster-mark-service-" + std::to_string(service_id) + ".dat";
}

std::string ServiceContainerConfig::mark_file_link_name() const
{
    return "cluster-mark-service-" + std::to_string(service_id) + ".lnk";
}

std::expected<ServiceContainerConfig, Error> container_config_from_properties(
    const axc::core::PropertySource& properties)
{
    if (properties.contains(SERVICE_CLASS_NAME_PROP_NAME))
    {
        return std::unexpected(
            make_error(ErrorCode::config_error,
                       std::string{SERVICE_CLASS_NAME_PROP_NAME} +
                           " names a Java class; this build selects its service from a compiled-in registry"));
    }

    ServiceContainerConfig config;

    auto get_int = [&properties](std::string_view name,
                                 std::int32_t default_value) -> std::expected<std::int32_t, Error>
    { return parse_int(properties, name, default_value); };

    auto service_id = get_int(SERVICE_ID_PROP_NAME, 0);
    if (!service_id)
    {
        return std::unexpected(service_id.error());
    }
    if (*service_id < 0 || *service_id >= ServiceContainerConfig::MAX_SERVICE_COUNT)
    {
        return std::unexpected(make_error(ErrorCode::config_error,
                                          "service id outside allowed range [0,9]: " + std::to_string(*service_id)));
    }
    config.service_id = *service_id;

    auto cluster_id = get_int(CLUSTER_ID_PROP_NAME, 0);
    if (!cluster_id)
    {
        return std::unexpected(cluster_id.error());
    }
    config.cluster_id = *cluster_id;

    if (const auto naming = properties.get(THREAD_NAMING_PROP_NAME); naming.has_value())
    {
        const auto parsed = parse_thread_naming(*naming);
        if (!parsed.has_value())
        {
            return std::unexpected(make_error(ErrorCode::config_error, std::string{THREAD_NAMING_PROP_NAME} + "=" +
                                                                           std::string{*naming} + " is not a value"));
        }
        config.thread_naming = *parsed;
    }

    config.service_name = container_thread_name(config.cluster_id, config.service_id, config.thread_naming);
    if (const auto name = properties.get(SERVICE_NAME_PROP_NAME); name.has_value() && !name->empty())
    {
        config.service_name = std::string{*name};
    }

    if (const auto service = properties.get(SERVICE_REGISTRY_PROP_NAME); service.has_value())
    {
        config.service = std::string{*service};
    }

    auto response_stream_id = get_int(ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME, 20);
    if (!response_stream_id)
    {
        return std::unexpected(response_stream_id.error());
    }
    config.archive_control_response_stream_id =
        config.cluster_id * 100 + 100 + *response_stream_id + (config.service_id + 1);

    auto fragment_limit = get_int(LOG_FRAGMENT_LIMIT_PROP_NAME, 50);
    if (!fragment_limit)
    {
        return std::unexpected(fragment_limit.error());
    }
    if (*fragment_limit < 0)
    {
        return std::unexpected(make_error(ErrorCode::config_error, "negative log fragment limit"));
    }
    config.log_fragment_limit = static_cast<std::size_t>(*fragment_limit);

    config.aeron_dir = std::string{properties.get_or(AERON_DIR_PROP_NAME, "")};
    config.cluster_dir = std::string{properties.get_or(CLUSTER_DIR_PROP_NAME, "aeron-cluster")};
    if (const auto mark_dir = properties.get(MARK_FILE_DIR_PROP_NAME); mark_dir.has_value())
    {
        config.mark_file_dir = std::string{*mark_dir};
    }
    config.control_channel = std::string{properties.get_or(CONTROL_CHANNEL_PROP_NAME, config.control_channel)};

    auto service_stream_id = get_int(SERVICE_STREAM_ID_PROP_NAME, config.service_stream_id);
    if (!service_stream_id)
    {
        return std::unexpected(service_stream_id.error());
    }
    config.service_stream_id = *service_stream_id;

    auto consensus_stream_id = get_int(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, config.consensus_module_stream_id);
    if (!consensus_stream_id)
    {
        return std::unexpected(consensus_stream_id.error());
    }
    config.consensus_module_stream_id = *consensus_stream_id;

    config.responding_service = parse_bool(properties, RESPONDER_SERVICE_PROP_NAME, config.responding_service);

    auto error_buffer_length = get_int(ERROR_BUFFER_LENGTH_PROP_NAME, config.error_buffer_length);
    if (!error_buffer_length)
    {
        return std::unexpected(error_buffer_length.error());
    }
    config.error_buffer_length = *error_buffer_length;

    config.idle_strategy = std::string{properties.get_or(IDLE_STRATEGY_PROP_NAME, config.idle_strategy)};
    config.replay_channel = std::string{properties.get_or(REPLAY_CHANNEL_PROP_NAME, config.replay_channel)};

    auto replay_stream_id = get_int(REPLAY_STREAM_ID_PROP_NAME, config.replay_stream_id);
    if (!replay_stream_id)
    {
        return std::unexpected(replay_stream_id.error());
    }
    config.replay_stream_id = *replay_stream_id;

    config.snapshot_channel = std::string{properties.get_or(SNAPSHOT_CHANNEL_PROP_NAME, config.snapshot_channel)};

    auto snapshot_stream_id = get_int(SNAPSHOT_STREAM_ID_PROP_NAME, config.snapshot_stream_id);
    if (!snapshot_stream_id)
    {
        return std::unexpected(snapshot_stream_id.error());
    }
    config.snapshot_stream_id = *snapshot_stream_id;

    config.archive_local_control_channel =
        std::string{properties.get_or(ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME, config.archive_local_control_channel)};

    auto archive_stream_id = get_int(ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME, config.archive_local_control_stream_id);
    if (!archive_stream_id)
    {
        return std::unexpected(archive_stream_id.error());
    }
    config.archive_local_control_stream_id = *archive_stream_id;

    if (const auto threshold = properties.get(CYCLE_THRESHOLD_PROP_NAME); threshold.has_value())
    {
        const std::int64_t parsed = parse_duration_ns(*threshold);
        if (parsed < 0)
        {
            return std::unexpected(
                make_error(ErrorCode::config_error, std::string{CYCLE_THRESHOLD_PROP_NAME} + " is not a duration"));
        }
        config.cycle_threshold_ns = parsed;
    }
    if (const auto threshold = properties.get(SNAPSHOT_DURATION_THRESHOLD_PROP_NAME); threshold.has_value())
    {
        const std::int64_t parsed = parse_duration_ns(*threshold);
        if (parsed < 0)
        {
            return std::unexpected(make_error(
                ErrorCode::config_error, std::string{SNAPSHOT_DURATION_THRESHOLD_PROP_NAME} + " is not a duration"));
        }
        config.snapshot_duration_threshold_ns = parsed;
    }

    return config;
}

}  // namespace deepmsg::cluster::service
