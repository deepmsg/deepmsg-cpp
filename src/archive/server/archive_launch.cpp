#include "archive/server/archive_launch.h"

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

#include "core/io/fs.h"

namespace deepmsg::archive::server
{
namespace
{

constexpr std::string_view SAMPLE_AUTHENTICATOR_SUPPLIER = "io.aeron.samples.archive.SampleAuthenticatorSupplier";
constexpr std::int64_t MARK_FILE_PAGE_SIZE = 4096;

[[nodiscard]] std::optional<std::string_view> value_of(const axc::core::PropertySource& source,
                                                       std::string_view name) noexcept
{
    return source.get(name);
}

[[nodiscard]] std::string string_or(const axc::core::PropertySource& source, std::string_view name,
                                    std::string fallback)
{
    const auto value = value_of(source, name);
    return value.has_value() ? std::string{*value} : std::move(fallback);
}

[[nodiscard]] std::expected<std::int64_t, axc::core::Error> int64_or(const axc::core::PropertySource& source,
                                                                     std::string_view name,
                                                                     std::int64_t fallback) noexcept
{
    const auto value = value_of(source, name);
    if (!value.has_value())
    {
        return fallback;
    }

    std::int64_t parsed = 0;
    const char* begin = value->data();
    const char* end = begin + value->size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }
    return parsed;
}

[[nodiscard]] std::expected<std::int32_t, axc::core::Error> int32_or(const axc::core::PropertySource& source,
                                                                     std::string_view name,
                                                                     std::int32_t fallback) noexcept
{
    auto parsed = int64_or(source, name, fallback);
    if (!parsed)
    {
        return std::unexpected(parsed.error());
    }
    return static_cast<std::int32_t>(*parsed);
}

[[nodiscard]] std::expected<std::int64_t, axc::core::Error> size_or(const axc::core::PropertySource& source,
                                                                    std::string_view name,
                                                                    std::int64_t fallback) noexcept
{
    const auto value = value_of(source, name);
    if (!value.has_value())
    {
        return fallback;
    }

    auto parsed = parse_size(*value);
    if (!parsed)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }
    return *parsed;
}

/// `aeron.archive.connect.timeout` and friends are durations in the reference
/// (`getDurationInNanos`); a bare number is read as milliseconds here.
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> duration_ms_or(const axc::core::PropertySource& source,
                                                                           std::string_view name,
                                                                           std::int64_t fallback) noexcept
{
    const auto value = value_of(source, name);
    if (!value.has_value())
    {
        return fallback;
    }

    std::string_view text = *value;
    std::int64_t divisor = 1;
    if (text.size() > 2 && text.substr(text.size() - 2) == "ns")
    {
        divisor = 1000000;
        text.remove_suffix(2);
    }
    else if (text.size() > 2 && text.substr(text.size() - 2) == "us")
    {
        divisor = 1000;
        text.remove_suffix(2);
    }
    else if (text.size() > 2 && text.substr(text.size() - 2) == "ms")
    {
        text.remove_suffix(2);
    }
    else if (!text.empty())
    {
        switch (text.back())
        {
            case 's':
                divisor = -1000;
                text.remove_suffix(1);
                break;
            case 'm':
                divisor = -60000;
                text.remove_suffix(1);
                break;
            case 'h':
                divisor = -3600000;
                text.remove_suffix(1);
                break;
            default:
                break;
        }
    }

    std::int64_t magnitude = 0;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto result = std::from_chars(begin, end, magnitude);
    if (result.ec != std::errc{} || result.ptr != end || text.empty())
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }

    if (divisor < 0)
    {
        return magnitude * (-divisor);
    }
    return magnitude / divisor;
}

[[nodiscard]] bool bool_or(const axc::core::PropertySource& source, std::string_view name, bool fallback) noexcept
{
    const auto value = value_of(source, name);
    return value.has_value() ? "true" == *value : fallback;
}

}  // namespace

std::expected<ArchiveLaunchConfig, axc::core::Error> parse_launch_config(
    const axc::core::PropertySource& source) noexcept
{
    ArchiveLaunchConfig config;

    config.aeron_dir = string_or(source, "aeron.dir", {});
    config.archive_dir = string_or(source, "aeron.archive.dir", {});
    if (config.archive_dir.empty())
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error, "Archive.Context.archiveDir must be set"));
    }
    config.mark_file_dir = string_or(source, "aeron.archive.mark.file.dir", config.archive_dir);

    config.control_channel = string_or(source, "aeron.archive.control.channel", config.control_channel);
    config.local_control_channel =
        string_or(source, "aeron.archive.local.control.channel", config.local_control_channel);

    auto control_stream_id = int32_or(source, "aeron.archive.control.stream.id", config.control_stream_id);
    if (!control_stream_id)
    {
        return std::unexpected(control_stream_id.error());
    }
    config.control_stream_id = *control_stream_id;

    auto local_control_stream_id =
        int32_or(source, "aeron.archive.local.control.stream.id", config.local_control_stream_id);
    if (!local_control_stream_id)
    {
        return std::unexpected(local_control_stream_id.error());
    }
    config.local_control_stream_id = *local_control_stream_id;

    auto archive_id = int64_or(source, "aeron.archive.id", config.archive_id);
    if (!archive_id)
    {
        return std::unexpected(archive_id.error());
    }
    config.archive_id = *archive_id;

    config.delete_archive_on_start = bool_or(source, "aeron.archive.dir.delete.on.start", false);

    auto connect_timeout = duration_ms_or(source, "aeron.archive.connect.timeout", config.connect_timeout_ms);
    if (!connect_timeout)
    {
        return std::unexpected(connect_timeout.error());
    }
    config.connect_timeout_ms = *connect_timeout;

    auto liveness = duration_ms_or(source, "aeron.archive.session.liveness.check.interval",
                                   config.session_liveness_check_interval_ms);
    if (!liveness)
    {
        return std::unexpected(liveness.error());
    }
    config.session_liveness_check_interval_ms = *liveness;

    auto control_term_length =
        size_or(source, "aeron.archive.control.term.buffer.length", config.control_term_buffer_length);
    if (!control_term_length)
    {
        return std::unexpected(control_term_length.error());
    }
    config.control_term_buffer_length = static_cast<std::int32_t>(*control_term_length);

    auto control_mtu = size_or(source, "aeron.archive.control.mtu.length", config.control_mtu_length);
    if (!control_mtu)
    {
        return std::unexpected(control_mtu.error());
    }
    config.control_mtu_length = static_cast<std::int32_t>(*control_mtu);

    config.control_term_buffer_sparse = bool_or(source, "aeron.archive.control.term.buffer.sparse", true);

    auto error_buffer_length = size_or(source, "aeron.archive.error.buffer.length", config.error_buffer_length);
    if (!error_buffer_length)
    {
        return std::unexpected(error_buffer_length.error());
    }
    config.error_buffer_length = static_cast<std::int32_t>(*error_buffer_length);

    auto catalog_capacity = size_or(source, "aeron.archive.catalog.capacity", config.catalog_capacity);
    if (!catalog_capacity)
    {
        return std::unexpected(catalog_capacity.error());
    }
    config.catalog_capacity = *catalog_capacity;

    auto recording = parse_recording_settings(source);
    if (!recording)
    {
        return std::unexpected(recording.error());
    }
    config.recording = std::move(*recording);

    auto replay = parse_replay_settings(source);
    if (!replay)
    {
        return std::unexpected(replay.error());
    }
    config.replay = std::move(*replay);

    auto catalog_file_sync_level =
        int32_or(source, "aeron.archive.catalog.file.sync.level", config.catalog_file_sync_level);
    if (!catalog_file_sync_level)
    {
        return std::unexpected(catalog_file_sync_level.error());
    }
    config.catalog_file_sync_level = *catalog_file_sync_level;

    // `Archive.java:1268-1272`.
    if (config.catalog_file_sync_level < config.recording.file_sync_level)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  "catalogFileSyncLevel " + std::to_string(config.catalog_file_sync_level) +
                                      " < fileSyncLevel " + std::to_string(config.recording.file_sync_level)));
    }

    config.authenticator_supplier = string_or(source, "aeron.archive.authenticator.supplier", {});
    if (!config.authenticator_supplier.empty() && config.authenticator_supplier != SAMPLE_AUTHENTICATOR_SUPPLIER)
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error,
            "unsupported aeron.archive.authenticator.supplier: " + config.authenticator_supplier));
    }

    return config;
}

std::expected<void, axc::core::Error> prepare_archive_directories(const ArchiveLaunchConfig& config)
{
    std::error_code error;
    if (config.delete_archive_on_start)
    {
        std::filesystem::remove_all(config.archive_dir, error);
        if (error)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "delete archive dir failed: " + error.message()));
        }
    }

    if (auto created = axc::core::create_directories(config.archive_dir); !created)
    {
        return std::unexpected(created.error());
    }
    if (config.mark_file_dir != config.archive_dir)
    {
        if (auto created = axc::core::create_directories(config.mark_file_dir); !created)
        {
            return std::unexpected(created.error());
        }
    }
    return {};
}

ArchiveServer::ArchiveServer(const ArchiveLaunchConfig& config) : config_(config) {}

ArchiveServer::~ArchiveServer()
{
    close();
}

std::expected<std::unique_ptr<ArchiveServer>, axc::core::Error> ArchiveServer::create(const ArchiveLaunchConfig& config)
{
    auto server = std::unique_ptr<ArchiveServer>(new ArchiveServer(config));
    if (auto initialized = server->init(); !initialized)
    {
        return std::unexpected(initialized.error());
    }
    return server;
}

std::expected<void, axc::core::Error> ArchiveServer::init()
{
    const std::int64_t now_ms = epoch_clock_.time_ms();

    // `ArchiveMarkFile.alignedTotalFileLength` (`ArchiveMarkFile.java:462-484`).
    const auto header_and_error_buffer = ArchiveMarkFile::HEADER_LENGTH + config_.error_buffer_length;
    const auto aligned_length =
        ((header_and_error_buffer + MARK_FILE_PAGE_SIZE - 1) / MARK_FILE_PAGE_SIZE) * MARK_FILE_PAGE_SIZE;

    ArchiveMarkFile::Options mark_file_options;
    mark_file_options.directory = config_.mark_file_dir;
    mark_file_options.total_file_length = static_cast<std::int32_t>(aligned_length);
    mark_file_options.error_buffer_length = config_.error_buffer_length;
    mark_file_options.epoch_clock = &epoch_clock_;

    ArchiveMarkFile::HeaderValues mark_file_values;
    mark_file_values.start_timestamp = now_ms;
    mark_file_values.control_stream_id = config_.control_stream_id;
    mark_file_values.local_control_stream_id = config_.local_control_stream_id;
    mark_file_values.events_stream_id = config_.recording.recording_events_stream_id;
    mark_file_values.archive_id = config_.archive_id;
    mark_file_values.control_channel = config_.control_channel;
    mark_file_values.local_control_channel = config_.local_control_channel;
    mark_file_values.events_channel = config_.recording.recording_events_channel;
    mark_file_values.aeron_directory = config_.aeron_dir;

    auto mark_file = ArchiveMarkFile::create_or_open(mark_file_options, mark_file_values);
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }
    mark_file_ = std::move(*mark_file);

    // The catalog is created by the conductor (`Archive.Context.catalog`).
    ArchiveConductorContext conductor_context;
    conductor_context.archive_dir = config_.archive_dir;
    conductor_context.control_channel = config_.control_channel;
    conductor_context.control_stream_id = config_.control_stream_id;
    conductor_context.aeron_dir = config_.aeron_dir;
    conductor_context.connect_timeout_ms = config_.connect_timeout_ms;
    conductor_context.session_liveness_check_interval_ms = config_.session_liveness_check_interval_ms;
    conductor_context.archive_id = config_.archive_id;
    conductor_context.control_term_buffer_length = config_.control_term_buffer_length;
    conductor_context.control_mtu_length = config_.control_mtu_length;
    conductor_context.control_term_buffer_sparse = config_.control_term_buffer_sparse;
    conductor_context.recording = config_.recording;
    conductor_context.replay = config_.replay;

    if (!config_.replay.replay_checksum_name.empty())
    {
        auto checksum = checksum_by_name(config_.replay.replay_checksum_name);
        if (!checksum)
        {
            close();
            return std::unexpected(checksum.error());
        }
        conductor_context.replay_checksum = *checksum;
    }
    conductor_context.catalog_capacity = config_.catalog_capacity;
    conductor_context.catalog_file_sync_level = config_.catalog_file_sync_level;
    conductor_context.authenticator = config_.authenticator_supplier.empty() ? nullptr : &sample_authenticator_;

    auto conductor = ArchiveConductor::create(conductor_context);
    if (!conductor)
    {
        close();
        return std::unexpected(conductor.error());
    }
    conductor_ = std::move(*conductor);

    // `ArchiveMarkFile.signalReady` (`ArchiveMarkFile.java:342-350`).
    mark_file_.signal_ready(now_ms);
    started_ = true;
    return {};
}

int ArchiveServer::do_work()
{
    int work_count = conductor_->do_work();
    mark_file_.update_activity_timestamp(epoch_clock_.time_ms());
    return work_count;
}

void ArchiveServer::close()
{
    if (conductor_)
    {
        conductor_.reset();
    }
    if (started_)
    {
        mark_file_.signal_terminated();
        mark_file_.close();
        started_ = false;
    }
}

}  // namespace deepmsg::archive::server
