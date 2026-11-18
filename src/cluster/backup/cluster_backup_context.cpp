#include "cluster/backup/cluster_backup_context.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <filesystem>
#include <string>
#include <utility>

#include "cluster/common/cluster_member.h"
#include "core/io/fs.h"

namespace deepmsg::cluster::backup
{
namespace
{

constexpr std::string_view CLUSTER_ID_PROP_NAME = "aeron.cluster.id";
constexpr std::string_view MEMBER_ID_PROP_NAME = "aeron.cluster.member.id";
constexpr std::string_view MEMBERS_PROP_NAME = "aeron.cluster.members";
constexpr std::string_view MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";
constexpr std::string_view CONSENSUS_CHANNEL_PROP_NAME = "aeron.cluster.consensus.channel";
constexpr std::string_view CONSENSUS_ENDPOINTS_PROP_NAME = "aeron.cluster.consensus.endpoints";
constexpr std::string_view CONSENSUS_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.stream.id";
constexpr std::string_view SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";
constexpr std::string_view LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";
constexpr std::string_view CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";
constexpr std::string_view MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";
constexpr std::string_view SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";
constexpr std::string_view CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";
constexpr std::string_view INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";
constexpr std::string_view ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.error.buffer.length";
constexpr std::string_view IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";
constexpr std::string_view AERON_DIR_PROP_NAME = "aeron.dir";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME = "aeron.archive.local.control.channel";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.local.control.stream.id";
constexpr std::string_view ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";
constexpr std::string_view ARCHIVE_CONTROL_CHANNEL_PROP_NAME = "aeron.archive.control.channel";
constexpr std::string_view ARCHIVE_CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.control.stream.id";
constexpr std::string_view ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP_NAME = "aeron.archive.control.response.channel";
constexpr std::string_view REPLICATION_PROGRESS_TIMEOUT_PROP_NAME = "aeron.cluster.replication.progress.timeout";
constexpr std::string_view REPLICATION_PROGRESS_INTERVAL_PROP_NAME = "aeron.cluster.replication.progress.interval";
constexpr std::string_view THREAD_NAMING_PROP_NAME = "aeron.thread.naming";

constexpr std::string_view CATCHUP_ENDPOINT_PROP_NAME = "aeron.cluster.backup.catchup.endpoint";
constexpr std::string_view CATCHUP_CHANNEL_PROP_NAME = "aeron.cluster.backup.catchup.channel";
constexpr std::string_view BACKUP_INTERVAL_PROP_NAME = "aeron.cluster.backup.interval";
constexpr std::string_view RESPONSE_TIMEOUT_PROP_NAME = "aeron.cluster.backup.response.timeout";
constexpr std::string_view PROGRESS_TIMEOUT_PROP_NAME = "aeron.cluster.backup.progress.timeout";
constexpr std::string_view COOL_DOWN_INTERVAL_PROP_NAME = "aeron.cluster.backup.cool.down.interval";
constexpr std::string_view SOURCE_TYPE_PROP_NAME = "aeron.cluster.backup.source.type";
/// The C++ shell's own names (not in the reference): the credentials a backup
/// sends on the connect request and the fixed answer it gives to a challenge.
/// The Java tests use them with the fork's `ChallengeAuthenticatorSupplier`.
constexpr std::string_view CREDENTIALS_PROP_NAME = "deepmsg.cluster.backup.credentials";
constexpr std::string_view CHALLENGE_ANSWER_PROP_NAME = "deepmsg.cluster.backup.challenge.response";

/// Deliberately without the `aeron.` prefix in the reference
/// (`ClusterBackup.java:561`).
constexpr std::string_view INITIAL_REPLAY_START_PROP_NAME = "cluster.backup.initial.replay.start";

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
            return std::stoll(lower.substr(0, lower.size() - unit.suffix.size())) * unit.scale;
        }
    }
    return std::stoll(lower);
}

[[nodiscard]] std::expected<std::int64_t, Error> parse_duration(const axc::core::PropertySource& properties,
                                                                std::string_view name, std::int64_t default_value)
{
    const auto text = properties.get(name);
    if (!text.has_value())
    {
        return default_value;
    }
    try
    {
        return parse_duration_ns(*text);
    }
    catch (const std::exception&)
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, std::string{name} + "=" + std::string{*text} + " is not a duration"));
    }
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

[[nodiscard]] std::string get_string(const axc::core::PropertySource& properties, std::string_view name,
                                     std::string default_value)
{
    const auto text = properties.get(name);
    return text.has_value() ? std::string{*text} : std::move(default_value);
}

[[nodiscard]] std::expected<void, Error> ensure_directory(std::string_view path, std::string_view description)
{
    auto created = axc::core::create_directories(path);
    if (!created)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "unable to create " + std::string{description} +
                                                                   " directory " + std::string{path} + ": " +
                                                                   std::string{created.error().message.view()}));
    }
    return {};
}

/// `Configuration.catchupEndpoint` / `Configuration.consensusChannel`
/// (`ClusterBackup.java:411-480`): derive this node's endpoint from the member
/// entry when the property does not name one.
[[nodiscard]] std::expected<common::ClusterMember, Error> this_member(const ClusterBackupConfig& config)
{
    auto members = common::parse_cluster_members(config.members);
    if (!members)
    {
        return std::unexpected(members.error());
    }
    return common::determine_member(*members, config.member_id, config.member_endpoints);
}

}  // namespace

std::string_view backup_state_name(ClusterBackupState state) noexcept
{
    switch (state)
    {
        case ClusterBackupState::backup_query:
            return "BACKUP_QUERY";
        case ClusterBackupState::snapshot_retrieve:
            return "SNAPSHOT_RETRIEVE";
        case ClusterBackupState::live_log_record:
            return "LIVE_LOG_RECORD";
        case ClusterBackupState::live_log_replay:
            return "LIVE_LOG_REPLAY";
        case ClusterBackupState::update_recording_log:
            return "UPDATE_RECORDING_LOG";
        case ClusterBackupState::backing_up:
            return "BACKING_UP";
        case ClusterBackupState::reset_backup:
            return "RESET_BACKUP";
        case ClusterBackupState::closed:
            return "CLOSED";
    }
    return "UNKNOWN";
}

std::expected<BackupSourceType, Error> parse_backup_source_type(std::string_view value)
{
    std::string upper{value};
    for (char& c : upper)
    {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    if ("ANY" == upper)
    {
        return BackupSourceType::any;
    }
    if ("LEADER" == upper)
    {
        return BackupSourceType::leader;
    }
    if ("FOLLOWER" == upper)
    {
        return BackupSourceType::follower;
    }
    return std::unexpected(make_error(ErrorCode::config_error, "unknown backup source type: " + std::string{value}));
}

std::string_view backup_source_type_name(BackupSourceType type) noexcept
{
    switch (type)
    {
        case BackupSourceType::any:
            return "ANY";
        case BackupSourceType::leader:
            return "LEADER";
        case BackupSourceType::follower:
            return "FOLLOWER";
    }
    return "UNKNOWN";
}

std::string ClusterBackupConfig::mark_file_path() const
{
    const std::string directory = mark_file_dir.has_value() ? *mark_file_dir : cluster_dir;
    return directory + "/cluster-mark.dat";
}

std::string_view backup_thread_name(const ClusterBackupConfig& /*config*/)
{
    return "aeron-cl-bu";
}

std::expected<void, Error> ClusterBackupConfig::conclude()
{
    if (concluded_)
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "ClusterBackup.Context already concluded"));
    }
    concluded_ = true;

    if (delete_dir_on_start)
    {
        std::error_code error;
        std::filesystem::remove_all(cluster_dir, error);
    }

    if (auto created = ensure_directory(cluster_dir, "cluster"); !created)
    {
        return created;
    }

    const std::string mark_dir = mark_file_dir.has_value() ? *mark_file_dir : cluster_dir;
    if (auto created = ensure_directory(mark_dir, "mark file"); !created)
    {
        return created;
    }

    if (!members.empty() || !member_endpoints.empty())
    {
        auto member = this_member(*this);
        if (!member)
        {
            return std::unexpected(member.error());
        }
        if (catchup_endpoint.empty())
        {
            catchup_endpoint = member->catchup_endpoint;
        }
        if (consensus_endpoints.empty())
        {
            consensus_endpoints = member->consensus_endpoint;
        }
    }

    // `ClusterBackup.java:717-720`: the catchup endpoint must be resolvable.
    if (catchup_endpoint.empty())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "ClusterBackup.Context.catchupEndpoint must be set"));
    }

    // `ClusterBackup.java:860-870`: the local archive control must be IPC.
    if (0 != archive_local_control_channel.find("aeron:ipc"))
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "local archive control must be IPC: " + archive_local_control_channel));
    }

    if (replication_progress_interval_ns <= 0)
    {
        replication_progress_interval_ns = std::max<std::int64_t>(replication_progress_timeout_ns / 10, 1);
    }

    return {};
}

std::expected<ClusterBackupConfig, Error> cluster_backup_config_from_properties(
    const axc::core::PropertySource& properties)
{
    ClusterBackupConfig config;

    auto cluster_id = parse_int(properties, CLUSTER_ID_PROP_NAME, config.cluster_id);
    if (!cluster_id)
    {
        return std::unexpected(cluster_id.error());
    }
    config.cluster_id = *cluster_id;

    auto member_id = parse_int(properties, MEMBER_ID_PROP_NAME, config.member_id);
    if (!member_id)
    {
        return std::unexpected(member_id.error());
    }
    config.member_id = *member_id;

    config.members = get_string(properties, MEMBERS_PROP_NAME, {});
    config.member_endpoints = get_string(properties, MEMBER_ENDPOINTS_PROP_NAME, {});
    config.consensus_channel = get_string(properties, CONSENSUS_CHANNEL_PROP_NAME, config.consensus_channel);
    config.consensus_endpoints = get_string(properties, CONSENSUS_ENDPOINTS_PROP_NAME, {});
    config.catchup_endpoint = get_string(properties, CATCHUP_ENDPOINT_PROP_NAME, {});
    config.catchup_channel = get_string(properties, CATCHUP_CHANNEL_PROP_NAME, config.catchup_channel);
    config.aeron_dir = get_string(properties, AERON_DIR_PROP_NAME, {});
    config.cluster_dir = get_string(properties, CLUSTER_DIR_PROP_NAME, config.cluster_dir);
    config.idle_strategy = get_string(properties, IDLE_STRATEGY_PROP_NAME, config.idle_strategy);
    config.archive_local_control_channel =
        get_string(properties, ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME, config.archive_local_control_channel);
    config.cluster_archive_control_channel =
        get_string(properties, ARCHIVE_CONTROL_CHANNEL_PROP_NAME, config.cluster_archive_control_channel);
    config.cluster_archive_control_response_channel =
        get_string(properties, ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP_NAME, {});

    const auto mark_dir = properties.get(MARK_FILE_DIR_PROP_NAME);
    if (mark_dir.has_value() && !mark_dir->empty())
    {
        config.mark_file_dir = std::string{*mark_dir};
    }

    config.delete_dir_on_start =
        parse_bool(properties, "aeron.cluster.dir.delete.on.start", config.delete_dir_on_start);

    auto consensus_stream_id = parse_int(properties, CONSENSUS_STREAM_ID_PROP_NAME, config.consensus_stream_id);
    if (!consensus_stream_id)
    {
        return std::unexpected(consensus_stream_id.error());
    }
    config.consensus_stream_id = *consensus_stream_id;

    auto log_stream_id = parse_int(properties, LOG_STREAM_ID_PROP_NAME, config.log_stream_id);
    if (!log_stream_id)
    {
        return std::unexpected(log_stream_id.error());
    }
    config.log_stream_id = *log_stream_id;

    auto snapshot_stream_id = parse_int(properties, SNAPSHOT_STREAM_ID_PROP_NAME, config.service_snapshot_stream_id);
    if (!snapshot_stream_id)
    {
        return std::unexpected(snapshot_stream_id.error());
    }
    config.service_snapshot_stream_id = *snapshot_stream_id;
    config.consensus_module_snapshot_stream_id = *snapshot_stream_id;

    auto service_stream_id = parse_int(properties, SERVICE_STREAM_ID_PROP_NAME, config.service_stream_id);
    if (!service_stream_id)
    {
        return std::unexpected(service_stream_id.error());
    }
    config.service_stream_id = *service_stream_id;

    auto consensus_module_stream_id =
        parse_int(properties, CONSENSUS_MODULE_STREAM_ID_PROP_NAME, config.consensus_module_stream_id);
    if (!consensus_module_stream_id)
    {
        return std::unexpected(consensus_module_stream_id.error());
    }
    config.consensus_module_stream_id = *consensus_module_stream_id;

    auto ingress_stream_id = parse_int(properties, INGRESS_STREAM_ID_PROP_NAME, config.ingress_stream_id);
    if (!ingress_stream_id)
    {
        return std::unexpected(ingress_stream_id.error());
    }
    config.ingress_stream_id = *ingress_stream_id;

    auto error_buffer_length = parse_int(properties, ERROR_BUFFER_LENGTH_PROP_NAME, config.error_buffer_length);
    if (!error_buffer_length)
    {
        return std::unexpected(error_buffer_length.error());
    }
    config.error_buffer_length = *error_buffer_length;

    auto archive_local_control_stream_id =
        parse_int(properties, ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME, config.archive_local_control_stream_id);
    if (!archive_local_control_stream_id)
    {
        return std::unexpected(archive_local_control_stream_id.error());
    }
    config.archive_local_control_stream_id = *archive_local_control_stream_id;

    auto archive_response_stream_id =
        parse_int(properties, ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME, config.archive_control_response_stream_id);
    if (!archive_response_stream_id)
    {
        return std::unexpected(archive_response_stream_id.error());
    }
    config.archive_control_response_stream_id = *archive_response_stream_id;

    auto cluster_archive_stream_id =
        parse_int(properties, ARCHIVE_CONTROL_STREAM_ID_PROP_NAME, config.cluster_archive_control_stream_id);
    if (!cluster_archive_stream_id)
    {
        return std::unexpected(cluster_archive_stream_id.error());
    }
    config.cluster_archive_control_stream_id = *cluster_archive_stream_id;

    auto interval = parse_duration(properties, BACKUP_INTERVAL_PROP_NAME, config.backup_interval_ns);
    if (!interval)
    {
        return std::unexpected(interval.error());
    }
    config.backup_interval_ns = *interval;

    auto response_timeout = parse_duration(properties, RESPONSE_TIMEOUT_PROP_NAME, config.response_timeout_ns);
    if (!response_timeout)
    {
        return std::unexpected(response_timeout.error());
    }
    config.response_timeout_ns = *response_timeout;

    auto progress_timeout = parse_duration(properties, PROGRESS_TIMEOUT_PROP_NAME, config.progress_timeout_ns);
    if (!progress_timeout)
    {
        return std::unexpected(progress_timeout.error());
    }
    config.progress_timeout_ns = *progress_timeout;

    auto cooldown = parse_duration(properties, COOL_DOWN_INTERVAL_PROP_NAME, config.cooldown_interval_ns);
    if (!cooldown)
    {
        return std::unexpected(cooldown.error());
    }
    config.cooldown_interval_ns = *cooldown;

    auto replication_timeout =
        parse_duration(properties, REPLICATION_PROGRESS_TIMEOUT_PROP_NAME, config.replication_progress_timeout_ns);
    if (!replication_timeout)
    {
        return std::unexpected(replication_timeout.error());
    }
    config.replication_progress_timeout_ns = *replication_timeout;

    auto replication_interval =
        parse_duration(properties, REPLICATION_PROGRESS_INTERVAL_PROP_NAME, config.replication_progress_interval_ns);
    if (!replication_interval)
    {
        return std::unexpected(replication_interval.error());
    }
    config.replication_progress_interval_ns = *replication_interval;

    if (const auto source_type = properties.get(SOURCE_TYPE_PROP_NAME); source_type.has_value())
    {
        auto parsed = parse_backup_source_type(*source_type);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        config.source_type = *parsed;
    }

    if (const auto replay_start = properties.get(INITIAL_REPLAY_START_PROP_NAME); replay_start.has_value())
    {
        std::string upper{*replay_start};
        for (char& c : upper)
        {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        if ("BEGINNING" == upper)
        {
            config.initial_replay_start = BackupReplayStart::beginning;
        }
        else if ("LATEST_SNAPSHOT" == upper)
        {
            config.initial_replay_start = BackupReplayStart::latest_snapshot;
        }
        else
        {
            return std::unexpected(make_error(
                ErrorCode::config_error, "unknown cluster.backup.initial.replay.start: " + std::string{*replay_start}));
        }
    }

    if (const auto credentials = properties.get(CREDENTIALS_PROP_NAME); credentials.has_value())
    {
        std::string value{*credentials};
        config.encoded_credentials = [value]() { return std::vector<std::uint8_t>(value.begin(), value.end()); };
    }
    if (const auto answer = properties.get(CHALLENGE_ANSWER_PROP_NAME); answer.has_value())
    {
        std::string value{*answer};
        config.on_challenge = [value](std::span<const std::uint8_t>)
        { return std::vector<std::uint8_t>(value.begin(), value.end()); };
    }

    (void)THREAD_NAMING_PROP_NAME;
    return config;
}

}  // namespace deepmsg::cluster::backup
