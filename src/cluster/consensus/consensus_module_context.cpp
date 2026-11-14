#include "cluster/consensus/consensus_module_context.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "cluster/common/cluster_member.h"
#include "core/io/fs.h"

namespace deepmsg::cluster::consensus
{
namespace
{

constexpr std::string_view CLUSTER_ID_PROP_NAME = "aeron.cluster.id";
constexpr std::string_view MEMBER_ID_PROP_NAME = "aeron.cluster.member.id";
constexpr std::string_view APPOINTED_LEADER_ID_PROP_NAME = "aeron.cluster.appointed.leader.id";
constexpr std::string_view MEMBERS_PROP_NAME = "aeron.cluster.members";
constexpr std::string_view MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";
constexpr std::string_view CONSENSUS_ENDPOINTS_PROP_NAME = "aeron.cluster.consensus.endpoints";
constexpr std::string_view INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";
constexpr std::string_view INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";
constexpr std::string_view INGRESS_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.ingress.fragment.limit";
constexpr std::string_view INGRESS_IPC_ALLOWED_PROP_NAME = "aeron.cluster.ingress.ipc.allowed";
constexpr std::string_view EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";
constexpr std::string_view EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";
constexpr std::string_view LOG_CHANNEL_PROP_NAME = "aeron.cluster.log.channel";
constexpr std::string_view LOG_CONTROL_ENABLE_PROP_NAME = "aeron.cluster.log.control.enable";
constexpr std::string_view CONSENSUS_CONTROL_ENABLE_PROP_NAME = "aeron.cluster.consensus.control.enable";
constexpr std::string_view LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";
constexpr std::string_view LOG_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.log.fragment.limit";
constexpr std::string_view CONSENSUS_CHANNEL_PROP_NAME = "aeron.cluster.consensus.channel";
constexpr std::string_view CONSENSUS_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.stream.id";
constexpr std::string_view REPLICATION_CHANNEL_PROP_NAME = "aeron.cluster.replication.channel";
constexpr std::string_view FOLLOWER_CATCHUP_CHANNEL_PROP_NAME = "aeron.cluster.follower.catchup.channel";
constexpr std::string_view LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.leader.archive.control.channel";
constexpr std::string_view CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.control.channel";
constexpr std::string_view SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";
constexpr std::string_view CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";
constexpr std::string_view SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";
constexpr std::string_view SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";
constexpr std::string_view REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";
constexpr std::string_view REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";
constexpr std::string_view CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";
constexpr std::string_view SERVICES_DIR_PROP_NAME = "aeron.cluster.services.dir";
constexpr std::string_view MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";
constexpr std::string_view SERVICE_COUNT_PROP_NAME = "aeron.cluster.service.count";
constexpr std::string_view MAX_SESSIONS_PROP_NAME = "aeron.cluster.max.sessions";
constexpr std::string_view SESSION_TIMEOUT_PROP_NAME = "aeron.cluster.session.timeout";
constexpr std::string_view LEADER_HEARTBEAT_TIMEOUT_PROP_NAME = "aeron.cluster.leader.heartbeat.timeout";
constexpr std::string_view LEADER_HEARTBEAT_INTERVAL_PROP_NAME = "aeron.cluster.leader.heartbeat.interval";
constexpr std::string_view STARTUP_CANVASS_TIMEOUT_PROP_NAME = "aeron.cluster.startup.canvass.timeout";
constexpr std::string_view ELECTION_TIMEOUT_PROP_NAME = "aeron.cluster.election.timeout";
constexpr std::string_view ELECTION_STATUS_INTERVAL_PROP_NAME = "aeron.cluster.election.status.interval";
constexpr std::string_view TERMINATION_TIMEOUT_PROP_NAME = "aeron.cluster.termination.timeout";
constexpr std::string_view CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.cycle.threshold";
constexpr std::string_view TOTAL_SNAPSHOT_THRESHOLD_PROP_NAME = "aeron.cluster.total.snapshot.threshold";
constexpr std::string_view FILE_SYNC_LEVEL_PROP_NAME = "aeron.cluster.file.sync.level";
constexpr std::string_view ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.error.buffer.length";
constexpr std::string_view WHEEL_TICK_RESOLUTION_PROP_NAME = "aeron.cluster.wheel.tick.resolution";
constexpr std::string_view TICKS_PER_WHEEL_PROP_NAME = "aeron.cluster.ticks.per.wheel";
constexpr std::string_view TIMER_SERVICE_SUPPLIER_PROP_NAME = "aeron.cluster.timer.service.supplier";
constexpr std::string_view IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";
constexpr std::string_view AGENT_ROLE_NAME_PROP_NAME = "aeron.cluster.consensus.module.agent.role.name";
constexpr std::string_view AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME = "aeron.cluster.authorisation.service.supplier";
constexpr std::string_view THREAD_NAMING_PROP_NAME = "aeron.thread.naming";
constexpr std::string_view AERON_DIR_PROP_NAME = "aeron.dir";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME = "aeron.archive.local.control.channel";
constexpr std::string_view ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.local.control.stream.id";
constexpr std::string_view ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

/// The C++ shell's own switch (Q6). Deliberately not in the reference.
constexpr std::string_view STANDBY_ON_STARTUP_PROP_NAME = "aeron.cluster.cpp.standby.snapshot.on.startup";

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

[[nodiscard]] std::expected<std::int64_t, Error> parse_duration_property(const axc::core::PropertySource& properties,
                                                                         std::string_view name,
                                                                         std::int64_t default_value)
{
    const auto text = properties.get(name);
    if (!text.has_value())
    {
        return default_value;
    }

    const std::int64_t value = parse_duration_ns(*text);
    if (value < 0)
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, std::string{name} + "=" + std::string{*text} + " is not a duration"));
    }
    return value;
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

/// `ChannelUri.containsKey`: parameter names live after the first `?` and are
/// separated by `|`.
[[nodiscard]] bool channel_has_param(std::string_view channel, std::string_view key) noexcept
{
    const std::size_t query = channel.find('?');
    if (std::string_view::npos == query)
    {
        return false;
    }

    std::string_view rest = channel.substr(query + 1);
    while (!rest.empty())
    {
        const std::size_t bar = rest.find('|');
        const std::string_view parameter = rest.substr(0, bar);
        const std::size_t equals = parameter.find('=');
        if (parameter.substr(0, equals) == key)
        {
            return true;
        }
        if (std::string_view::npos == bar)
        {
            break;
        }
        rest.remove_prefix(bar + 1);
    }
    return false;
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

}  // namespace

std::string_view state_name(ConsensusModuleState state) noexcept
{
    switch (state)
    {
        case ConsensusModuleState::init:
            return "INIT";
        case ConsensusModuleState::active:
            return "ACTIVE";
        case ConsensusModuleState::suspended:
            return "SUSPENDED";
        case ConsensusModuleState::snapshot:
            return "SNAPSHOT";
        case ConsensusModuleState::quitting:
            return "QUITTING";
        case ConsensusModuleState::terminating:
            return "TERMINATING";
        case ConsensusModuleState::closed:
            return "CLOSED";
    }
    return "UNKNOWN";
}

std::string ConsensusModuleConfig::mark_file_path() const
{
    const std::string directory = mark_file_dir.has_value() ? *mark_file_dir : cluster_dir;
    return directory + "/cluster-mark.dat";
}

std::string ConsensusModuleConfig::mark_file_link_name() const
{
    return "cluster-mark.lnk";
}

std::expected<ConsensusModuleConfig, Error> consensus_module_config_from_properties(
    const axc::core::PropertySource& properties)
{
    ConsensusModuleConfig config;

    auto get_int = [&properties](std::string_view name,
                                 std::int32_t default_value) -> std::expected<std::int32_t, Error>
    { return parse_int(properties, name, default_value); };
    auto get_duration = [&properties](std::string_view name,
                                      std::int64_t default_value) -> std::expected<std::int64_t, Error>
    { return parse_duration_property(properties, name, default_value); };

    const struct
    {
        std::string_view name;
        std::int32_t* field;
    } ints[] = {{CLUSTER_ID_PROP_NAME, &config.cluster_id},
                {MEMBER_ID_PROP_NAME, &config.member_id},
                {APPOINTED_LEADER_ID_PROP_NAME, &config.appointed_leader_id},
                {INGRESS_STREAM_ID_PROP_NAME, &config.ingress_stream_id},
                {INGRESS_FRAGMENT_LIMIT_PROP_NAME, &config.ingress_fragment_limit},
                {EGRESS_STREAM_ID_PROP_NAME, &config.egress_stream_id},
                {LOG_STREAM_ID_PROP_NAME, &config.log_stream_id},
                {LOG_FRAGMENT_LIMIT_PROP_NAME, &config.log_fragment_limit},
                {CONSENSUS_STREAM_ID_PROP_NAME, &config.consensus_stream_id},
                {SERVICE_STREAM_ID_PROP_NAME, &config.service_stream_id},
                {CONSENSUS_MODULE_STREAM_ID_PROP_NAME, &config.consensus_module_stream_id},
                {SNAPSHOT_STREAM_ID_PROP_NAME, &config.snapshot_stream_id},
                {REPLAY_STREAM_ID_PROP_NAME, &config.replay_stream_id},
                {SERVICE_COUNT_PROP_NAME, &config.service_count},
                {MAX_SESSIONS_PROP_NAME, &config.max_concurrent_sessions},
                {FILE_SYNC_LEVEL_PROP_NAME, &config.file_sync_level},
                {ERROR_BUFFER_LENGTH_PROP_NAME, &config.error_buffer_length},
                {TICKS_PER_WHEEL_PROP_NAME, &config.ticks_per_wheel},
                {ARCHIVE_LOCAL_CONTROL_STREAM_ID_PROP_NAME, &config.archive_local_control_stream_id},
                {ARCHIVE_CONTROL_RESPONSE_STREAM_ID_PROP_NAME, &config.archive_control_response_stream_id}};

    for (const auto& entry : ints)
    {
        auto parsed = get_int(entry.name, *entry.field);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        *entry.field = *parsed;
    }

    const struct
    {
        std::string_view name;
        std::int64_t* field;
    } durations[] = {{SESSION_TIMEOUT_PROP_NAME, &config.session_timeout_ns},
                     {LEADER_HEARTBEAT_TIMEOUT_PROP_NAME, &config.leader_heartbeat_timeout_ns},
                     {LEADER_HEARTBEAT_INTERVAL_PROP_NAME, &config.leader_heartbeat_interval_ns},
                     {STARTUP_CANVASS_TIMEOUT_PROP_NAME, &config.startup_canvass_timeout_ns},
                     {ELECTION_TIMEOUT_PROP_NAME, &config.election_timeout_ns},
                     {ELECTION_STATUS_INTERVAL_PROP_NAME, &config.election_status_interval_ns},
                     {TERMINATION_TIMEOUT_PROP_NAME, &config.termination_timeout_ns},
                     {CYCLE_THRESHOLD_PROP_NAME, &config.cycle_threshold_ns},
                     {TOTAL_SNAPSHOT_THRESHOLD_PROP_NAME, &config.total_snapshot_duration_threshold_ns},
                     {WHEEL_TICK_RESOLUTION_PROP_NAME, &config.wheel_tick_resolution_ns}};

    for (const auto& entry : durations)
    {
        auto parsed = get_duration(entry.name, *entry.field);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        *entry.field = *parsed;
    }

    config.members = get_string(properties, MEMBERS_PROP_NAME, {});
    config.member_endpoints = get_string(properties, MEMBER_ENDPOINTS_PROP_NAME, {});
    config.consensus_endpoints = get_string(properties, CONSENSUS_ENDPOINTS_PROP_NAME, {});
    config.ingress_channel = get_string(properties, INGRESS_CHANNEL_PROP_NAME, {});
    config.egress_channel = get_string(properties, EGRESS_CHANNEL_PROP_NAME, {});
    config.log_channel = get_string(properties, LOG_CHANNEL_PROP_NAME, config.log_channel);
    config.consensus_channel = get_string(properties, CONSENSUS_CHANNEL_PROP_NAME, config.consensus_channel);
    config.log_control_enabled = parse_bool(properties, LOG_CONTROL_ENABLE_PROP_NAME, config.log_control_enabled);
    config.consensus_control_enabled =
        parse_bool(properties, CONSENSUS_CONTROL_ENABLE_PROP_NAME, config.consensus_control_enabled);
    config.replication_channel = get_string(properties, REPLICATION_CHANNEL_PROP_NAME, {});
    config.follower_catchup_channel =
        get_string(properties, FOLLOWER_CATCHUP_CHANNEL_PROP_NAME, config.follower_catchup_channel);
    config.leader_archive_control_channel =
        get_string(properties, LEADER_ARCHIVE_CONTROL_CHANNEL_PROP_NAME, config.leader_archive_control_channel);
    config.control_channel = get_string(properties, CONTROL_CHANNEL_PROP_NAME, config.control_channel);
    config.snapshot_channel = get_string(properties, SNAPSHOT_CHANNEL_PROP_NAME, config.snapshot_channel);
    config.replay_channel = get_string(properties, REPLAY_CHANNEL_PROP_NAME, config.replay_channel);
    config.cluster_dir = get_string(properties, CLUSTER_DIR_PROP_NAME, config.cluster_dir);
    config.services_dir = get_string(properties, SERVICES_DIR_PROP_NAME, {});
    if (const auto mark_file_dir = properties.get(MARK_FILE_DIR_PROP_NAME); mark_file_dir.has_value())
    {
        config.mark_file_dir = std::string{*mark_file_dir};
    }
    config.aeron_dir = get_string(properties, AERON_DIR_PROP_NAME, {});
    config.timer_service_supplier =
        get_string(properties, TIMER_SERVICE_SUPPLIER_PROP_NAME, config.timer_service_supplier);
    config.idle_strategy = get_string(properties, IDLE_STRATEGY_PROP_NAME, config.idle_strategy);
    config.agent_role_name = get_string(properties, AGENT_ROLE_NAME_PROP_NAME, {});
    config.authorisation_service_supplier =
        get_string(properties, AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, config.authorisation_service_supplier);
    config.archive_local_control_channel =
        get_string(properties, ARCHIVE_LOCAL_CONTROL_CHANNEL_PROP_NAME, config.archive_local_control_channel);

    config.ingress_ipc_allowed = parse_bool(properties, INGRESS_IPC_ALLOWED_PROP_NAME, config.ingress_ipc_allowed);

    // Names owned by later cards: `aeron.cluster.clock` and the supplier names
    // are C4b/C4f, `cpu.affinity` and the control toggles are C4e/C4f,
    // `replication.progress.*` and the standby notification delay are C4d.
    // `common::cluster_name_table()` carries their disposition, so they are
    // never silent even though this struct does not read them yet.

    if (const auto mode = properties.get(STANDBY_ON_STARTUP_PROP_NAME); mode.has_value())
    {
        if ("warn" == *mode)
        {
            config.standby_snapshot_on_startup = StandbySnapshotStartupMode::warn;
        }
        else if ("refuse" == *mode)
        {
            config.standby_snapshot_on_startup = StandbySnapshotStartupMode::refuse;
        }
        else
        {
            return std::unexpected(make_error(
                ErrorCode::config_error,
                std::string{STANDBY_ON_STARTUP_PROP_NAME} + "=" + std::string{*mode} + " is not one of warn|refuse"));
        }
    }

    // `CommonContext.threadName`: the node's own name wins over the scheme.
    if (const auto naming = properties.get(THREAD_NAMING_PROP_NAME); naming.has_value())
    {
        if ("classic" == *naming)
        {
            // The default scheme; `conclude()` fills the classic name.
        }
        else if ("new" == *naming)
        {
            if (config.agent_role_name.empty())
            {
                config.agent_role_name = "aeron-cl-cm";
            }
        }
        else
        {
            return std::unexpected(
                make_error(ErrorCode::config_error, "Unknown thread naming mode: " + std::string{*naming}));
        }
    }

    auto concluded = config.conclude();
    if (!concluded)
    {
        return std::unexpected(concluded.error());
    }
    return config;
}

std::expected<void, Error> ConsensusModuleConfig::conclude()
{
    if (concluded_)
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "ConsensusModule.Context already concluded (ConcurrentConclude)"));
    }
    concluded_ = true;

    if (channel_has_param(log_channel, "initial-term-id"))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "logChannel must not contain: initial-term-id"));
    }
    if (channel_has_param(log_channel, "term-id"))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "logChannel must not contain: term-id"));
    }
    if (channel_has_param(log_channel, "term-offset"))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "logChannel must not contain: term-offset"));
    }

    if (service_count < 0 || service_count > MAX_SERVICE_COUNT)
    {
        return std::unexpected(make_error(
            ErrorCode::config_error,
            "service count of range [0, " + std::to_string(MAX_SERVICE_COUNT) + "]: " + std::to_string(service_count)));
    }
    if (0 == service_count)
    {
        return std::unexpected(make_error(ErrorCode::config_error,
                                          "zero services are only supported when ConsensusModuleExtension is enabled"));
    }

    // `Configuration.authorisationServiceSupplier()` (`:1350-1365`): the empty
    // value is the default service; the two tokens are exact; a class name is
    // not loadable here, so it is refused rather than silently ignored.
    if (authorisation_service_supplier.empty())
    {
        authorisation_service_kind = AuthorisationServiceKind::allow_backup_and_standby;
    }
    else if ("ALLOW_ALL" == authorisation_service_supplier)
    {
        authorisation_service_kind = AuthorisationServiceKind::allow_all;
    }
    else if ("DENY_ALL" == authorisation_service_supplier)
    {
        authorisation_service_kind = AuthorisationServiceKind::deny_all;
    }
    else
    {
        return std::unexpected(make_error(
            ErrorCode::config_error,
            "aeron.cluster.authorisation.service.supplier=" + authorisation_service_supplier +
                " is not one of the supported values (empty, ALLOW_ALL, DENY_ALL); a custom class cannot be loaded"));
    }

    // `ConsensusModule.Context.clusterMembers must be set` (`:1772-1775`).
    if (members.empty())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "ConsensusModule.Context.clusterMembers must be set"));
    }

    auto parsed_members = common::parse_cluster_members(members);
    if (!parsed_members)
    {
        return std::unexpected(parsed_members.error());
    }
    if (nullptr == common::find_member(*parsed_members, member_id))
    {
        return std::unexpected(
            make_error(ErrorCode::config_error,
                       "could not find memberId=" + std::to_string(member_id) + " in clusterMembers=" + members));
    }

    // `IoUtil.ensureDirectoryExists` (`:1777-1785`).
    std::error_code error;
    cluster_dir = std::filesystem::weakly_canonical(cluster_dir, error).string();
    if (error)
    {
        return std::unexpected(
            make_error(ErrorCode::io_error, "unable to resolve cluster directory: " + error.message()));
    }
    const std::string mark_dir = mark_file_dir.value_or(cluster_dir);
    mark_file_dir = std::filesystem::weakly_canonical(mark_dir, error).string();
    if (error)
    {
        return std::unexpected(
            make_error(ErrorCode::io_error, "unable to resolve mark file directory: " + error.message()));
    }
    if (services_dir.empty())
    {
        services_dir = cluster_dir;
    }
    else
    {
        services_dir = std::filesystem::weakly_canonical(services_dir, error).string();
        if (error)
        {
            return std::unexpected(
                make_error(ErrorCode::io_error, "unable to resolve services directory: " + error.message()));
        }
    }

    auto cluster_created = ensure_directory(cluster_dir, "cluster");
    if (!cluster_created)
    {
        return std::unexpected(cluster_created.error());
    }
    auto mark_created = ensure_directory(*mark_file_dir, "mark file");
    if (!mark_created)
    {
        return std::unexpected(mark_created.error());
    }

    if (startup_canvass_timeout_ns / leader_heartbeat_timeout_ns < 2)
    {
        return std::unexpected(make_error(
            ErrorCode::config_error,
            "startupCanvassTimeoutNs=" + std::to_string(startup_canvass_timeout_ns) +
                " must be a multiple of leaderHeartbeatTimeoutNs=" + std::to_string(leader_heartbeat_timeout_ns)));
    }

    if (ingress_channel.empty())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "ingressChannel must be specified"));
    }
    if (replication_channel.empty())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "replicationChannel must be set"));
    }

    // The reference checks the request and the response channel; in C++ both
    // are `archive_local_control_channel` (`ConsensusModule.java:2118-2130`).
    if (0 != archive_local_control_channel.rfind("aeron:ipc", 0))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "local archive control must be IPC"));
    }

    // `isLogMdc = channelUri.isUdp() && null == get(ENDPOINT)`
    // (`ConsensusModule.java:2180`).
    log_is_mdc = 0 == log_channel.rfind("aeron:udp", 0) && !channel_has_param(log_channel, "endpoint");

    // `clusterId * 100 + 100 + AeronArchive.Configuration.controlResponseStreamId()`
    // (`ConsensusModule.java:2105-2111`).
    archive_control_response_stream_id = cluster_id * 100 + 100 + archive_control_response_stream_id;

    if (agent_role_name.empty())
    {
        agent_role_name = consensus_module_thread_name(*this);
    }

    return {};
}

std::string consensus_module_thread_name(const ConsensusModuleConfig& config)
{
    // `CommonContext.threadName` (`CommonContext.java:582-592`), classic
    // scheme. The new scheme (`aeron-cl-cm`) is chosen while reading
    // `aeron.thread.naming`, before `conclude()`.
    return "consensus-module-" + std::to_string(config.cluster_id) + "-" + std::to_string(config.member_id);
}

}  // namespace deepmsg::cluster::consensus
