// T2: `io.aeron.cluster.ClusterTool` and `ClusterToolOperator`, transcribed.
//
// Every action is the reference method named beside it; the output text is the
// contract (the interop test runs the reference tool on the same directory and
// compares, normalising only the wall clock, the pid and the temporary paths).
// The Rust track's `deepmsg/crates/cluster/tool/` is the second reference used
// where this transcription needed a second reading.

#include "tool.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <aeronc.h>
#include <client/aeron_archive_client.h>

#include "cluster/common/cluster_codecs.h"
#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/recording_log.h"
#include "cluster/consensus/cluster_control.h"
#include "cluster/consensus/cm_snapshot.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/consensus/node_control.h"
#include "cluster/service/consensus_module_proxy.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/io/distinct_error_log.h"
#include "core/io/fs.h"
#include "core/io/mapped_file.h"
#include "format.h"
#include "io_aeron_cluster_codecs/ClusterMembersExtendedResponse.h"
#include "io_aeron_cluster_codecs/ClusterMembersResponse.h"
#include "io_aeron_cluster_codecs/ConsensusModule.h"
#include "io_aeron_cluster_codecs/SnapshotMarker.h"

namespace deepmsg::tools
{

namespace
{

namespace codecs = ::io::aeron::cluster::codecs;
namespace common = cluster::common;
namespace consensus = cluster::consensus;
namespace service = cluster::service;

using axc::core::DeepmsgClient;
using axc::core::Error;
using axc::core::ErrorCode;
using axc::core::make_error;

constexpr int SUCCESS = TOOL_SUCCESS;
constexpr int FAILURE = TOOL_FAILURE;
constexpr std::int64_t WAIT_STEP_MS = 1;
constexpr std::int64_t COMMAND_TIMEOUT_NS = 5'000'000'000;
constexpr std::int64_t SNAPSHOT_REPLAY_TIMEOUT_MS = 30'000;

constexpr std::string_view HELP_PREFIX = "Usage: <cluster-dir> <command> [options]";
constexpr std::string_view TOOL_CLIENT_NAME = "cluster-tool";

struct CommandSpec
{
    std::string_view name;
    std::string_view description;
};

/// `ClusterTool.COMMANDS` (`ClusterTool.java:128-227`), in insertion order.
constexpr std::array<CommandSpec, 18> COMMANDS{{
    {"describe", "prints out all descriptors in the mark file."},
    {"pid", "prints PID of cluster component."},
    {"recovery-plan", "[service count] prints recovery plan of cluster component."},
    {"recording-log", "prints recording log of cluster component."},
    {"sort-recording-log", "reorders entries in the recording log to match the order in memory."},
    {"seed-recording-log-from-snapshot", "creates a new recording log based on the latest valid snapshot."},
    {"errors", "prints Aeron and cluster component error logs."},
    {"list-members", "prints leader memberId and active members."},
    {"backup-query", "[delay] get, or set, time of next backup query."},
    {"invalidate-latest-snapshot", "marks the latest snapshot as a invalid so the previous is loaded."},
    {"is-leader", "returns zero if the cluster node is leader, non-zero if not."},
    {"snapshot", "triggers a snapshot on the leader."},
    {"suspend", "suspends appending to the log."},
    {"resume", "resumes appending to the log."},
    {"shutdown", "initiates an orderly stop of the cluster with a snapshot."},
    {"abort", "stops the cluster without a snapshot."},
    {"describe-latest-cm-snapshot", "prints the contents of the latest valid consensus module snapshot."},
    {"validate-recording-log", "triggers the cluster node to validate its recording log against the archive."},
}};

/// `ClusterToolCommand.printHelp` (`ClusterToolCommand.java:67-81`): names
/// right-aligned to the widest one.
std::string usage()
{
    std::size_t width = 0;
    for (const auto& command : COMMANDS)
    {
        width = std::max(width, command.name.size());
    }

    std::string out{HELP_PREFIX};
    out += "\n";
    for (const auto& command : COMMANDS)
    {
        out.append(width - command.name.size(), ' ');
        out += command.name;
        out += ": ";
        out += command.description;
        out += "\n";
    }
    return out;
}

bool command_exists(std::string_view name)
{
    return std::any_of(COMMANDS.begin(), COMMANDS.end(), [name](const CommandSpec& spec) { return spec.name == name; });
}

std::string join_path(std::string_view directory, std::string_view name)
{
    std::string path{directory};
    if (!path.empty() && '/' != path.back())
    {
        path += '/';
    }
    path += name;
    return path;
}

std::string trim(std::string value)
{
    while (!value.empty() &&
           (' ' == value.back() || '\n' == value.back() || '\r' == value.back() || '\t' == value.back()))
    {
        value.pop_back();
    }
    std::size_t start = 0;
    while (start < value.size() && (' ' == value[start] || '\t' == value[start]))
    {
        ++start;
    }
    return value.substr(start);
}

/// `SystemUtil.getDurationInNanos`'s suffix forms, as the tool reads them.
std::optional<std::int64_t> parse_duration_ns(std::string_view text)
{
    const std::string trimmed = trim(std::string{text});
    if (trimmed.empty())
    {
        return std::nullopt;
    }

    std::size_t digits = 0;
    while (digits < trimmed.size() && (trimmed[digits] >= '0' && trimmed[digits] <= '9'))
    {
        ++digits;
    }
    if (0 == digits)
    {
        return std::nullopt;
    }

    std::int64_t value = 0;
    try
    {
        value = std::stoll(trimmed.substr(0, digits));
    }
    catch (const std::exception&)
    {
        return std::nullopt;
    }
    const std::string suffix = trimmed.substr(digits);
    std::int64_t multiplier = 1;
    if (suffix.empty() || "ns" == suffix)
    {
        multiplier = 1;
    }
    else if ("us" == suffix)
    {
        multiplier = 1'000;
    }
    else if ("ms" == suffix)
    {
        multiplier = 1'000'000;
    }
    else if ("s" == suffix)
    {
        multiplier = 1'000'000'000;
    }
    else if ("m" == suffix || "min" == suffix)
    {
        multiplier = 60LL * 1'000'000'000;
    }
    else if ("h" == suffix)
    {
        multiplier = 3'600LL * 1'000'000'000;
    }
    else if ("d" == suffix)
    {
        multiplier = 86'400LL * 1'000'000'000;
    }
    else
    {
        return std::nullopt;
    }
    return value * multiplier;
}

struct ToolProperties
{
    std::int64_t timeout_ms{0};
    std::int64_t delay_ms{0};
    std::string replay_channel{"aeron:ipc"};
    std::int32_t replay_stream_id{103};
    std::string aeron_dir{};
};

struct PropertySet
{
    std::vector<std::pair<std::string, std::string>> values;

    [[nodiscard]] std::optional<std::string> get(std::string_view name) const
    {
        for (const auto& [key, value] : values)
        {
            if (key == name)
            {
                return value;
            }
        }
        return std::nullopt;
    }
};

std::expected<ToolProperties, std::string> resolve_tool_properties(const PropertySet& properties)
{
    ToolProperties resolved;

    if (auto text = properties.get("aeron.cluster.tool.timeout"))
    {
        const auto ns = parse_duration_ns(*text);
        if (!ns)
        {
            return std::unexpected("aeron.cluster.tool.timeout: " + *text + " is not a duration");
        }
        resolved.timeout_ms = *ns / 1'000'000;
    }
    if (auto text = properties.get("aeron.cluster.tool.delay"))
    {
        const auto ns = parse_duration_ns(*text);
        if (!ns)
        {
            return std::unexpected("aeron.cluster.tool.delay: " + *text + " is not a duration");
        }
        resolved.delay_ms = *ns / 1'000'000;
    }
    if (auto channel = properties.get("aeron.cluster.tool.replay.channel"))
    {
        resolved.replay_channel = *channel;
    }
    if (auto text = properties.get("aeron.cluster.tool.replay.stream.id"))
    {
        try
        {
            resolved.replay_stream_id = static_cast<std::int32_t>(std::stol(*text));
        }
        catch (const std::exception&)
        {
            return std::unexpected("aeron.cluster.tool.replay.stream.id=" + *text + " is not an integer");
        }
    }
    if (auto directory = properties.get("aeron.dir"))
    {
        resolved.aeron_dir = *directory;
    }
    return resolved;
}

// -- mark files -------------------------------------------------------------

bool mark_file_exists(const std::string& cluster_dir)
{
    const std::string mark_dir = common::ClusterMarkFile::resolve_mark_file_dir(cluster_dir);
    return axc::core::file_exists(join_path(mark_dir, common::ClusterMarkFile::FILE_NAME));
}

/// `openMarkFile` (`ClusterToolOperator.java:1185-1188`): the reference's
/// `ClusterMarkFile` waits for a version when a timeout asks it to.
std::optional<common::ClusterMarkFile> open_mark_file(const std::string& cluster_dir, std::int64_t timeout_ms,
                                                      std::string& error)
{
    const std::string mark_dir = common::ClusterMarkFile::resolve_mark_file_dir(cluster_dir);
    const std::string path = join_path(mark_dir, common::ClusterMarkFile::FILE_NAME);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms));

    while (true)
    {
        if (axc::core::file_exists(path))
        {
            auto file = common::ClusterMarkFile::open_existing(path, now_ms());
            if (file)
            {
                if (0 != file->semantic_version() || file->previous_startup_failed())
                {
                    return std::optional<common::ClusterMarkFile>{std::move(*file)};
                }
            }
            else if (std::chrono::steady_clock::now() >= deadline)
            {
                error = file.error().message.view();
                return std::nullopt;
            }
        }

        if (std::chrono::steady_clock::now() >= deadline)
        {
            error = path + " does not exist";
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
    }
}

std::string services_directory(const std::string& cluster_dir, const common::ClusterMarkFile& mark_file)
{
    const std::string directory = mark_file.services_cluster_dir();
    return directory.empty() ? cluster_dir : directory;
}

/// `printTypeAndActivityTimestamp` (`ClusterToolOperator.java:1256-1276`).
void print_type_and_activity(const common::ClusterMarkFile& mark_file, std::string& out)
{
    out += "Type: ";
    out += common::cluster_component_type_name(mark_file.component_type());
    out += " ";
    out += time_of_day(now_ms());
    out += " (start: ";
    out += date_time(mark_file.start_timestamp());
    out += ", activity: ";
    out += date_time(mark_file.activity_timestamp_volatile());
    out += ")\n";
}

/// `printErrors(out, markFile::errorBuffer, "Cluster")` (`:1279-1283`).
void print_mark_file_errors(const common::ClusterMarkFile& mark_file, std::string& out)
{
    out += "Cluster component error log:\n";
    out += axc::core::render_distinct_error_log(mark_file.error_buffer()).text;
}

/// `printDriverErrors` (`:1285-1310`): the media driver's error log from
/// `<aeronDirectory>/cnc.dat`.
bool print_driver_errors(const std::string& aeron_directory, std::string& out, std::string& error)
{
    out += "Aeron driver error log (directory: ";
    out += aeron_directory;
    out += "):\n";

    const std::string cnc_path = join_path(aeron_directory, "cnc.dat");
    auto cnc = axc::core::MappedFile::open(cnc_path, false);
    if (!cnc)
    {
        error = cnc.error().message.view();
        return false;
    }

    auto region = axc::core::cnc_error_log_region(cnc->mapping());
    if (!region)
    {
        error = region.error().message.view();
        return false;
    }

    out += axc::core::render_distinct_error_log(*region).text;
    return true;
}

/// `openServiceMarkFiles` (`:1190-1254`): `.lnk` first, then plain `.dat`
/// whose base name no link claimed; unreadable files are logged and skipped.
std::vector<common::ClusterMarkFile> open_service_mark_files(const std::string& directory, std::int64_t timeout_ms,
                                                             std::string& out)
{
    std::vector<std::string> names;
    std::error_code list_error;
    for (const auto& entry : std::filesystem::directory_iterator(directory, list_error))
    {
        const std::string name = entry.path().filename().string();
        const std::string extension{common::ClusterMarkFile::FILE_EXTENSION};
        const std::string link_extension{common::ClusterMarkFile::LINK_FILE_EXTENSION};
        const bool is_mark = name.size() > extension.size() &&
                             name.compare(name.size() - extension.size(), extension.size(), extension) == 0;
        const bool is_link =
            name.size() > link_extension.size() &&
            name.compare(name.size() - link_extension.size(), link_extension.size(), link_extension) == 0;
        if (0 == name.rfind(std::string{common::ClusterMarkFile::SERVICE_FILE_NAME_PREFIX}, 0) && (is_mark || is_link))
        {
            names.push_back(name);
        }
    }
    if (list_error)
    {
        return {};
    }
    std::sort(names.begin(), names.end());

    std::vector<std::string> paths;
    std::vector<std::string> services;

    for (const auto& name : names)
    {
        const std::string link_extension{common::ClusterMarkFile::LINK_FILE_EXTENSION};
        if (name.size() <= link_extension.size() ||
            name.compare(name.size() - link_extension.size(), link_extension.size(), link_extension) != 0)
        {
            continue;
        }
        const std::string stem = name.substr(0, name.size() - link_extension.size());
        const std::string link_path = join_path(directory, name);
        std::string target_directory;
        try
        {
            std::ifstream link{link_path};
            std::getline(link, target_directory);
        }
        catch (const std::exception& exception)
        {
            out += exception.what();
            out += "\n";
            continue;
        }
        if (target_directory.empty())
        {
            continue;
        }
        std::filesystem::path target{target_directory};
        if (target.is_relative())
        {
            target = std::filesystem::path{directory} / target;
        }
        paths.push_back(join_path(target.string(), stem + std::string{common::ClusterMarkFile::FILE_EXTENSION}));
        services.push_back(stem);
    }

    for (const auto& name : names)
    {
        const std::string extension{common::ClusterMarkFile::FILE_EXTENSION};
        if (name.size() <= extension.size() ||
            name.compare(name.size() - extension.size(), extension.size(), extension) != 0)
        {
            continue;
        }
        const std::string stem = name.substr(0, name.size() - extension.size());
        if (std::find(services.begin(), services.end(), stem) != services.end())
        {
            continue;
        }
        paths.push_back(join_path(directory, name));
        services.push_back(stem);
    }

    std::vector<common::ClusterMarkFile> files;
    for (const auto& path : paths)
    {
        auto file = common::ClusterMarkFile::open_existing(path, now_ms());
        if (!file)
        {
            out += file.error().message.view();
            out += "\n";
            continue;
        }
        // Agrona's `MarkFile` logger, which `openServiceMarkFiles` passes as
        // `out::println` (`ClusterToolOperator.java:1202`).
        out += "INFO: mark file exists: ";
        out += path;
        out += "\n";
        files.push_back(std::move(*file));
    }
    return files;
}

// -- counters and the control channel ---------------------------------------

struct ControlProperties
{
    std::string aeron_dir;
    std::string control_channel;
    std::int32_t service_stream_id{0};
    std::int32_t consensus_module_stream_id{0};
};

std::optional<ControlProperties> control_properties(const common::ClusterMarkFile& mark_file)
{
    const auto properties = mark_file.load_control_properties();
    if (properties.aeron_directory.empty() || properties.control_channel.empty())
    {
        return std::nullopt;
    }
    return ControlProperties{properties.aeron_directory, properties.control_channel, properties.service_stream_id,
                             properties.consensus_module_stream_id};
}

std::optional<DeepmsgClient> connect_client(const std::string& aeron_dir, std::string& error)
{
    auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir});
    if (!client)
    {
        error = client.error().message.view();
        return std::nullopt;
    }
    return std::optional<DeepmsgClient>{std::move(*client)};
}

std::int32_t find_counter(aeron_counters_reader_t* reader, std::int32_t type_id, std::int32_t cluster_id)
{
    return common::find_cluster_counter(reader, type_id, cluster_id);
}

std::optional<std::int64_t> counter_value(aeron_counters_reader_t* reader, std::int32_t counter_id)
{
    if (AERON_NULL_COUNTER_ID == counter_id)
    {
        return std::nullopt;
    }
    std::int64_t* address = aeron_counters_reader_addr(reader, counter_id);
    if (nullptr == address)
    {
        return std::nullopt;
    }
    return __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

// -- membership -------------------------------------------------------------

struct MembershipMember
{
    std::int64_t leadership_term_id{0};
    std::int64_t log_position{0};
    std::int64_t time_of_last_append_ns{0};
    std::int32_t member_id{0};
    std::string ingress_endpoint{};
    std::string consensus_endpoint{};
    std::string log_endpoint{};
    std::string catchup_endpoint{};
    std::string archive_endpoint{};
};

struct Membership
{
    std::int64_t current_time_ns{0};
    std::int32_t leader_member_id{-1};
    std::int32_t member_id{-1};
    std::optional<std::vector<MembershipMember>> active_members{};

    [[nodiscard]] std::string active_members_text() const
    {
        if (!active_members.has_value())
        {
            return "null";
        }
        std::string text = "[";
        for (std::size_t i = 0; i < active_members->size(); ++i)
        {
            if (0 != i)
            {
                text += ", ";
            }
            text += member_text((*active_members)[i], leader_member_id);
        }
        text += "]";
        return text;
    }

    /// `ClusterMember.toString` (`ClusterMember.java:1309-1330`).
    static std::string member_text(const MembershipMember& member, std::int32_t leader_member_id)
    {
        const std::string endpoints = member.ingress_endpoint + "," + member.consensus_endpoint + "," +
                                      member.log_endpoint + "," + member.catchup_endpoint + "," +
                                      member.archive_endpoint;
        return "ClusterMember{id=" + std::to_string(member.member_id) +
               ", isBallotSent=false, isLeader=" + (member.member_id == leader_member_id ? "true" : "false") +
               ", leadershipTermId=" + std::to_string(member.leadership_term_id) +
               ", logPosition=" + std::to_string(member.log_position) +
               ", candidateTermId=-1, catchupReplaySessionId=-1, correlationId=-1, timeOfLastAppendPositionNs=" +
               std::to_string(member.time_of_last_append_ns) + ", ingressEndpoint='" + member.ingress_endpoint +
               "', consensusEndpoint='" + member.consensus_endpoint + "', logEndpoint='" + member.log_endpoint +
               "', catchupEndpoint='" + member.catchup_endpoint + "', archiveEndpoint='" + member.archive_endpoint +
               "', endpoints='" + endpoints + "', publication=null, vote=null}";
    }
};

struct MembershipDecode
{
    std::int64_t correlation_id{0};
    Membership membership{};
    bool found{false};

    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t*)
    {
        auto* decode = static_cast<MembershipDecode*>(clientd);
        if (decode->found)
        {
            return AERON_ACTION_BREAK;
        }

        const std::span<const std::uint8_t> message{buffer, length};
        auto header = common::raw_message_header(message);
        if (!header)
        {
            return AERON_ACTION_CONTINUE;
        }

        if (codecs::ClusterMembersExtendedResponse::SBE_TEMPLATE_ID == header->templateId())
        {
            auto decoder = common::wrap_body<codecs::ClusterMembersExtendedResponse>(message, *header);
            if (decoder.correlationId() != decode->correlation_id)
            {
                return AERON_ACTION_CONTINUE;
            }

            Membership membership;
            membership.current_time_ns = decoder.currentTimeNs();
            membership.leader_member_id = decoder.leaderMemberId();
            membership.member_id = decoder.memberId();

            std::vector<MembershipMember> members;
            auto& group = decoder.activeMembers();
            while (group.hasNext())
            {
                group.next();
                MembershipMember member;
                member.leadership_term_id = group.leadershipTermId();
                member.log_position = group.logPosition();
                member.time_of_last_append_ns = group.timeOfLastAppendNs();
                member.member_id = group.memberId();
                // SBE var-data decoding advances the position: read in wire
                // order, one local at a time (`C9`'s record, item 4).
                member.ingress_endpoint = std::string{group.getIngressEndpointAsStringView()};
                member.consensus_endpoint = std::string{group.getConsensusEndpointAsStringView()};
                member.log_endpoint = std::string{group.getLogEndpointAsStringView()};
                member.catchup_endpoint = std::string{group.getCatchupEndpointAsStringView()};
                member.archive_endpoint = std::string{group.getArchiveEndpointAsStringView()};
                members.push_back(std::move(member));
            }
            membership.active_members = std::move(members);
            decode->membership = std::move(membership);
            decode->found = true;
            return AERON_ACTION_BREAK;
        }

        if (codecs::ClusterMembersResponse::SBE_TEMPLATE_ID == header->templateId())
        {
            auto decoder = common::wrap_body<codecs::ClusterMembersResponse>(message, *header);
            if (decoder.correlationId() != decode->correlation_id)
            {
                return AERON_ACTION_CONTINUE;
            }
            Membership membership;
            membership.current_time_ns = 0;
            membership.leader_member_id = decoder.leaderMemberId();
            membership.member_id = -1;
            membership.active_members = std::nullopt;
            decode->membership = std::move(membership);
            decode->found = true;
            return AERON_ACTION_BREAK;
        }

        return AERON_ACTION_CONTINUE;
    }
};

template <class H>
std::expected<H, Error> await_handle(DeepmsgClient& client, axc::core::AsyncAdd<H>& add, std::int64_t timeout_ns,
                                     const char* what)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::nanoseconds(timeout_ns);
    while (true)
    {
        (void)client.do_work();
        auto polled = add.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            return std::move(**polled);
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return std::unexpected(make_error(ErrorCode::no_capacity, std::string{what} + " timed out"));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
    }
}

/// `queryClusterMembers` (`ClusterToolOperator.java:548-631`): a publication
/// on the consensus module's stream, a subscription on the services' stream,
/// one `ClusterMembersQuery`, one response.
std::optional<Membership> query_cluster_members(const ControlProperties& control, std::int64_t timeout_ms)
{
    std::string error;
    auto client = connect_client(control.aeron_dir, error);
    if (!client)
    {
        return std::nullopt;
    }

    auto sub_add = client->add_subscription(control.control_channel, control.service_stream_id);
    auto sub = await_handle(*client, sub_add, COMMAND_TIMEOUT_NS, "addSubscription");
    if (!sub)
    {
        return std::nullopt;
    }

    auto pub_add = client->add_publication(control.control_channel, control.consensus_module_stream_id);
    auto pub = await_handle(*client, pub_add, COMMAND_TIMEOUT_NS, "addPublication");
    if (!pub)
    {
        return std::nullopt;
    }

    service::ConsensusModuleProxy proxy(0);
    proxy.set_publication(pub->get());

    const std::int64_t correlation_id = client->next_correlation_id();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms));

    const auto connected_deadline = deadline;
    while (!aeron_subscription_is_connected(sub->get()))
    {
        (void)client->do_work();
        if (std::chrono::steady_clock::now() >= connected_deadline)
        {
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
    }

    while (true)
    {
        auto sent = proxy.cluster_members_query(correlation_id);
        if (!sent)
        {
            return std::nullopt;
        }
        if (*sent)
        {
            break;
        }
        (void)client->do_work();
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return std::nullopt;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
    }

    MembershipDecode decode;
    decode.correlation_id = correlation_id;

    aeron_controlled_fragment_assembler_t* assembler = nullptr;
    if (0 != aeron_controlled_fragment_assembler_create(&assembler, &MembershipDecode::on_fragment, &decode))
    {
        return std::nullopt;
    }

    while (!decode.found)
    {
        (void)client->do_work();
        const int polled =
            aeron_subscription_controlled_poll(sub->get(), aeron_controlled_fragment_assembler_handler, assembler, 10);
        if (polled < 0)
        {
            (void)aeron_controlled_fragment_assembler_delete(assembler);
            return std::nullopt;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            (void)aeron_controlled_fragment_assembler_delete(assembler);
            return std::nullopt;
        }
        if (0 == polled)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
        }
    }

    (void)aeron_controlled_fragment_assembler_delete(assembler);
    return std::optional<Membership>{std::move(decode.membership)};
}

// -- toggles ----------------------------------------------------------------

enum class ToggleKind
{
    cluster_control,
    node_control
};

struct ToggleCommand
{
    std::string_view name;  // the target's enum name
    ToggleKind kind;
    std::int32_t target_code;
    consensus::ConsensusModuleState expected_state;
    bool leader_required;
    bool wait_for_completion;
    std::int64_t default_timeout_ms;
};

std::optional<ToggleCommand> named_toggle(std::string_view name)
{
    if ("snapshot" == name)
    {
        return ToggleCommand{"SNAPSHOT",
                             ToggleKind::cluster_control,
                             consensus::cluster_control_code(consensus::ClusterControlState::snapshot),
                             consensus::ConsensusModuleState::active,
                             true,
                             true,
                             30'000};
    }
    if ("suspend" == name)
    {
        return ToggleCommand{"SUSPEND",
                             ToggleKind::cluster_control,
                             consensus::cluster_control_code(consensus::ClusterControlState::suspend),
                             consensus::ConsensusModuleState::active,
                             true,
                             false,
                             1'000};
    }
    if ("resume" == name)
    {
        return ToggleCommand{"RESUME",
                             ToggleKind::cluster_control,
                             consensus::cluster_control_code(consensus::ClusterControlState::resume),
                             consensus::ConsensusModuleState::suspended,
                             true,
                             true,
                             1'000};
    }
    if ("shutdown" == name)
    {
        return ToggleCommand{"SHUTDOWN",
                             ToggleKind::cluster_control,
                             consensus::cluster_control_code(consensus::ClusterControlState::shutdown),
                             consensus::ConsensusModuleState::active,
                             true,
                             false,
                             1'000};
    }
    if ("abort" == name)
    {
        return ToggleCommand{"ABORT",
                             ToggleKind::cluster_control,
                             consensus::cluster_control_code(consensus::ClusterControlState::abort),
                             consensus::ConsensusModuleState::active,
                             true,
                             false,
                             1'000};
    }
    if ("validate-recording-log" == name)
    {
        return ToggleCommand{"VALIDATE_RECORDING_LOG",
                             ToggleKind::node_control,
                             consensus::node_control_code(consensus::NodeControlState::validate_recording_log),
                             consensus::ConsensusModuleState::active,
                             false,
                             true,
                             30'000};
    }
    return std::nullopt;
}

std::string toggle_name_at(ToggleKind kind, std::int64_t value)
{
    if (ToggleKind::cluster_control == kind)
    {
        auto state = consensus::cluster_control_state(value);
        if (state)
        {
            switch (*state)
            {
                case consensus::ClusterControlState::inactive:
                    return "INACTIVE";
                case consensus::ClusterControlState::neutral:
                    return "NEUTRAL";
                case consensus::ClusterControlState::suspend:
                    return "SUSPEND";
                case consensus::ClusterControlState::resume:
                    return "RESUME";
                case consensus::ClusterControlState::snapshot:
                    return "SNAPSHOT";
                case consensus::ClusterControlState::shutdown:
                    return "SHUTDOWN";
                case consensus::ClusterControlState::abort:
                    return "ABORT";
                case consensus::ClusterControlState::standby_snapshot:
                    return "STANDBY_SNAPSHOT";
            }
        }
        return std::to_string(value);
    }

    auto state = consensus::node_control_state(value);
    if (state)
    {
        switch (*state)
        {
            case consensus::NodeControlState::inactive:
                return "INACTIVE";
            case consensus::NodeControlState::neutral:
                return "NEUTRAL";
            case consensus::NodeControlState::replicate_standby_snapshot:
                return "REPLICATE_STANDBY_SNAPSHOT";
            case consensus::NodeControlState::validate_recording_log:
                return "VALIDATE_RECORDING_LOG";
        }
    }
    return std::to_string(value);
}

// -- snapshot printer -------------------------------------------------------

class SnapshotPrinter final : public consensus::SnapshotLoadHandler, public consensus::SnapshotRecordObserver
{
public:
    explicit SnapshotPrinter(std::string& out) : out_(&out) {}

    void on_snapshot_record(std::span<const std::uint8_t> record) override
    {
        auto header = common::raw_message_header(record);
        if (!header)
        {
            return;
        }
        if (codecs::SnapshotMarker::SBE_TEMPLATE_ID == header->templateId())
        {
            pending_hex_ = hex_dump(record);
            pending_length_ = record.size();
        }
        else if (codecs::ConsensusModule::SBE_TEMPLATE_ID == header->templateId())
        {
            pending_hex_ = hex_dump(record);
        }
    }

    std::expected<void, Error> on_load_begin_snapshot(std::int32_t app_version, service::TimeUnit time_unit) override
    {
        *out_ += "Snapshot: appVersion=" + std::to_string(app_version) +
                 " timeUnit=" + std::string{time_unit_name(time_unit)} + "\n";
        return {};
    }

    std::expected<void, Error> on_load_end_snapshot() override
    {
        *out_ += "End Snapshot, offset=0, length=" + std::to_string(pending_length_) + "\n";
        *out_ += pending_hex_;
        *out_ += "\n";
        return {};
    }

    std::expected<void, Error> on_load_cluster_session(std::int64_t cluster_session_id, std::int64_t correlation_id,
                                                       std::int64_t opened_log_position,
                                                       std::int64_t time_of_last_activity,
                                                       codecs::CloseReason::Value close_reason,
                                                       std::int32_t response_stream_id,
                                                       std::string_view response_channel) override
    {
        *out_ += "Cluster Session: clusterSessionId=" + std::to_string(cluster_session_id) +
                 " correlationId=" + std::to_string(correlation_id) +
                 " openedLogPosition=" + std::to_string(opened_log_position) +
                 " timeOfLastActivity=" + std::to_string(time_of_last_activity) +
                 " closeReason=" + codecs::CloseReason::c_str(close_reason) +
                 " responseStreamId=" + std::to_string(response_stream_id) +
                 " responseChannel=" + std::string{response_channel} + "\n";
        return {};
    }

    std::expected<void, Error> on_load_consensus_module_state(std::int64_t next_session_id,
                                                              std::int64_t next_service_session_id,
                                                              std::int64_t log_service_session_id,
                                                              std::int32_t pending_message_capacity) override
    {
        *out_ += "Consensus Module State: nextSessionId=" + std::to_string(next_session_id) +
                 " nextServiceSessionId=" + std::to_string(next_service_session_id) +
                 " logServiceSessionId=" + std::to_string(log_service_session_id) +
                 " pendingMessageCapacity=" + std::to_string(pending_message_capacity) + "\n";
        *out_ += pending_hex_;
        *out_ += "\n";
        return {};
    }

    std::expected<void, Error> on_load_pending_message_tracker(std::int64_t next_service_session_id,
                                                               std::int64_t log_service_session_id,
                                                               std::int32_t pending_message_capacity,
                                                               std::int32_t service_id) override
    {
        *out_ += "Pending Message Tracker: nextServiceSessionId=" + std::to_string(next_service_session_id) +
                 " logServiceSessionId=" + std::to_string(log_service_session_id) +
                 " pendingMessageCapacity=" + std::to_string(pending_message_capacity) +
                 " serviceId=" + std::to_string(service_id) + "\n";
        return {};
    }

    std::expected<void, Error> on_load_pending_message(std::int64_t cluster_session_id,
                                                       std::span<const std::uint8_t> message) override
    {
        *out_ += "Pending Message: length=" + std::to_string(message.size()) +
                 " clusterSessionId=" + std::to_string(cluster_session_id) + "\n";
        return {};
    }

    std::expected<void, Error> on_load_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        *out_ +=
            "Timer: correlationId=" + std::to_string(correlation_id) + " deadline=" + std::to_string(deadline) + "\n";
        return {};
    }

private:
    static std::string_view time_unit_name(service::TimeUnit time_unit)
    {
        switch (time_unit)
        {
            case service::TimeUnit::milliseconds:
                return "MILLISECONDS";
            case service::TimeUnit::microseconds:
                return "MICROSECONDS";
            case service::TimeUnit::nanoseconds:
                return "NANOSECONDS";
        }
        return "MILLISECONDS";
    }

    std::string* out_;
    std::string pending_hex_{};
    std::size_t pending_length_{0};
};

// -- the operator -----------------------------------------------------------

class Operator
{
public:
    explicit Operator(ToolProperties properties) : properties_(std::move(properties)) {}

    /// `describeClusterMarkFile` (`:1132-1157`), wrapped in `ignoreFailures`.
    int describe(const std::string& cluster_dir, std::string& out)
    {
        (void)describe_inner(cluster_dir, out);
        return SUCCESS;
    }

    /// `pid` (`:117-131`).
    int pid(const std::string& cluster_dir, std::string& out)
    {
        if (!mark_file_exists(cluster_dir) && properties_.timeout_ms <= 0)
        {
            return FAILURE;
        }
        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            return FAILURE;
        }
        out += std::to_string(mark_file->pid());
        out += "\n";
        return SUCCESS;
    }

    /// `recordingLog` (`:158-165`).
    int recording_log(const std::string& cluster_dir, std::string& out)
    {
        auto log = common::RecordingLog::open(cluster_dir, false);
        if (!log)
        {
            return FAILURE;
        }
        out += recording_log_text(*log);
        out += "\n";
        return SUCCESS;
    }

    /// `recoveryPlan` (`:141-149`).
    int recovery_plan(const std::string& cluster_dir, std::string& out, std::int32_t service_count)
    {
        auto log = common::RecordingLog::open(cluster_dir, false);
        if (!log)
        {
            return FAILURE;
        }

        std::string aeron_dir = properties_.aeron_dir;
        std::string mark_error;
        if (auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, mark_error))
        {
            aeron_dir = mark_file->aeron_directory();
        }
        if (aeron_dir.empty())
        {
            out += "Aeron directory not found\n";
            return FAILURE;
        }

        std::string error;
        auto client = connect_client(aeron_dir, error);
        if (!client)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            out += "aeron_archive_context_init failed\n";
            return FAILURE;
        }
        (void)aeron_archive_context_set_aeron(context, client->handle());
        (void)aeron_archive_context_set_owns_aeron_client(context, false);
        (void)aeron_archive_context_set_control_request_channel(context, "aeron:ipc");
        (void)aeron_archive_context_set_control_response_channel(context, "aeron:ipc");
        (void)aeron_archive_context_set_client_name(context, std::string{TOOL_CLIENT_NAME}.c_str());

        aeron_archive_t* archive = nullptr;
        const int connected = aeron_archive_connect(&archive, context);
        if (0 != connected)
        {
            out += aeron_errmsg();
            out += "\n";
            (void)aeron_archive_context_close(context);
            return FAILURE;
        }

        auto source = [archive](std::int64_t recording_id) -> std::optional<common::RecordingLog::RecordingExtent>
        {
            struct Capture
            {
                bool found{false};
                common::RecordingLog::RecordingExtent extent{};
            } capture;

            auto consumer = [](aeron_archive_recording_descriptor_t* descriptor, void* clientd)
            {
                auto* captured = static_cast<Capture*>(clientd);
                captured->found = true;
                captured->extent.start_position = descriptor->start_position;
                captured->extent.stop_position = descriptor->stop_position;
                captured->extent.initial_term_id = descriptor->initial_term_id;
                captured->extent.term_buffer_length = descriptor->term_buffer_length;
                captured->extent.mtu_length = descriptor->mtu_length;
                captured->extent.session_id = descriptor->session_id;
            };

            std::int32_t count = 0;
            if (0 != aeron_archive_list_recording(&count, archive, recording_id, consumer, &capture) || !capture.found)
            {
                return std::nullopt;
            }
            return capture.extent;
        };

        auto plan = log->create_recovery_plan(source, service_count, common::RecordingLog::NULL_VALUE);
        (void)aeron_archive_close(archive);
        (void)aeron_archive_context_close(context);

        if (!plan)
        {
            out += plan.error().message.view();
            out += "\n";
            return FAILURE;
        }

        out += recovery_plan_text(*plan);
        out += "\n";
        return SUCCESS;
    }

    /// `sortRecordingLog` (`:173-192`): a no-op is `FAILURE`.
    int sort_recording_log(const std::string& cluster_dir)
    {
        const std::string path = join_path(cluster_dir, common::RecordingLog::FILE_NAME);
        if (!axc::core::file_exists(path))
        {
            return FAILURE;
        }

        auto log = common::RecordingLog::open(cluster_dir, false);
        if (!log)
        {
            return FAILURE;
        }

        const auto& entries = log->entries();
        bool sorted = true;
        for (std::size_t i = entries.size(); i-- > 0;)
        {
            if (entries[i].entry_index != static_cast<std::int32_t>(i))
            {
                sorted = false;
                break;
            }
        }
        if (sorted)
        {
            return FAILURE;
        }

        std::vector<common::RecordingLog::Entry> copy{entries.begin(), entries.end()};
        auto rewritten = common::RecordingLog::rewrite(cluster_dir, copy);
        return rewritten ? SUCCESS : FAILURE;
    }

    /// `seedRecordingLogFromSnapshot` (`:201-269`).
    int seed_recording_log_from_snapshot(const std::string& cluster_dir, std::string& out)
    {
        const std::string path = join_path(cluster_dir, common::RecordingLog::FILE_NAME);
        auto log = common::RecordingLog::open(cluster_dir, false);
        if (!log)
        {
            return FAILURE;
        }

        const std::vector<common::RecordingLog::Entry> entries{log->entries().begin(), log->entries().end()};
        std::optional<std::size_t> snapshot_index;
        for (std::size_t i = entries.size(); i-- > 0;)
        {
            const auto& entry = entries[i];
            if (entry.is_valid && entry.is_snapshot() && common::RecordingLog::SERVICE_ID == entry.service_id)
            {
                snapshot_index = i;
                break;
            }
        }

        const std::string backup_path = path + ".bak";
        std::error_code copy_error;
        std::filesystem::copy_file(path, backup_path, std::filesystem::copy_options::overwrite_existing, copy_error);
        if (copy_error)
        {
            out += copy_error.message();
            out += "\n";
            return FAILURE;
        }
        std::error_code time_error;
        const auto modified = std::filesystem::last_write_time(path, time_error);
        if (!time_error)
        {
            std::filesystem::last_write_time(backup_path, modified, time_error);
        }

        std::vector<common::RecordingLog::Entry> truncated;
        if (snapshot_index.has_value())
        {
            std::int32_t service_id = common::RecordingLog::SERVICE_ID;
            for (std::size_t i = *snapshot_index + 1; i-- > 0;)
            {
                const auto& entry = entries[i];
                if (entry.is_valid && entry.is_snapshot() && entry.service_id == service_id)
                {
                    auto seeded = entry;
                    seeded.term_base_log_position = 0;
                    seeded.log_position = 0;
                    seeded.entry_index = 0;
                    seeded.archive_endpoint.clear();
                    truncated.push_back(std::move(seeded));
                    ++service_id;
                }
                else
                {
                    break;
                }
            }
            std::reverse(truncated.begin(), truncated.end());
        }

        auto rewritten = common::RecordingLog::rewrite(cluster_dir, truncated);
        if (!rewritten)
        {
            out += rewritten.error().message.view();
            out += "\n";
            return FAILURE;
        }
        return SUCCESS;
    }

    /// `errors` (`:278-308`).
    int errors(const std::string& cluster_dir, std::string& out)
    {
        std::string service_directory = cluster_dir;

        if (mark_file_exists(cluster_dir) || properties_.timeout_ms > 0)
        {
            std::string error;
            auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
            if (!mark_file)
            {
                out += error;
                out += "\n";
                return FAILURE;
            }

            print_type_and_activity(*mark_file, out);
            print_mark_file_errors(*mark_file, out);

            const std::string aeron_directory = mark_file->aeron_directory();
            out += "\n";
            std::string driver_error;
            if (!print_driver_errors(aeron_directory, out, driver_error))
            {
                out += driver_error;
                out += "\n";
                return FAILURE;
            }

            service_directory = services_directory(cluster_dir, *mark_file);
        }
        else
        {
            out += "cluster-mark.dat does not exist.\n";
        }

        auto service_files = open_service_mark_files(service_directory, properties_.timeout_ms, out);
        for (const auto& service_file : service_files)
        {
            print_type_and_activity(service_file, out);
            print_mark_file_errors(service_file, out);
        }
        return SUCCESS;
    }

    /// `listMembers` (`:317-347`).
    int list_members(const std::string& cluster_dir, std::string& out)
    {
        if (!mark_file_exists(cluster_dir) && properties_.timeout_ms <= 0)
        {
            out += "cluster-mark.dat does not exist.\n";
            return FAILURE;
        }

        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        const auto control = control_properties(*mark_file);
        if (!control)
        {
            out += "cluster-mark.dat does not describe a driver\n";
            return FAILURE;
        }

        const std::int64_t query_timeout_ms = std::max<std::int64_t>(1'000, properties_.timeout_ms);
        auto membership = query_cluster_members(*control, query_timeout_ms);
        if (!membership)
        {
            out += "timeout waiting for response from node\n";
            return FAILURE;
        }

        out += "currentTimeNs=" + std::to_string(membership->current_time_ns) +
               ", leaderMemberId=" + std::to_string(membership->leader_member_id) +
               ", memberId=" + std::to_string(membership->member_id) +
               ", activeMembers=" + membership->active_members_text() + "\n";
        return SUCCESS;
    }

    /// `isLeader` (`:444-488`).
    int is_leader(const std::string& cluster_dir, std::string& out)
    {
        if (!mark_file_exists(cluster_dir) && properties_.timeout_ms <= 0)
        {
            out += "cluster-mark.dat does not exist.\n";
            return FAILURE;
        }

        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        const auto control = control_properties(*mark_file);
        if (!control)
        {
            return FAILURE;
        }

        auto client = connect_client(control->aeron_dir, error);
        if (!client)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        const std::int32_t cluster_id = mark_file->cluster_id();
        auto* reader = client->counters_reader();
        const auto role = counter_value(reader, find_counter(reader, common::CLUSTER_NODE_ROLE_TYPE_ID, cluster_id));
        const auto election =
            counter_value(reader, find_counter(reader, common::CLUSTER_ELECTION_STATE_TYPE_ID, cluster_id));

        // `Cluster.Role.LEADER` = 2; `ElectionState.CLOSED` = 17.
        const bool leader = role.has_value() && 2 == *role && election.has_value() && 17 == *election;
        return leader ? 0 : 1;
    }

    /// `backup-query` (`:356-418`).
    int backup_query(const std::string& cluster_dir, std::string& out, std::optional<std::int64_t> delay_ms)
    {
        if (!mark_file_exists(cluster_dir) && properties_.timeout_ms <= 0)
        {
            out += "cluster-mark.dat does not exist.\n";
            return FAILURE;
        }

        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        if (common::ClusterComponentType::backup != mark_file->component_type())
        {
            out += "not a cluster backup node\n";
            return FAILURE;
        }

        const auto control = control_properties(*mark_file);
        if (!control)
        {
            out += "cluster-mark.dat does not describe a driver\n";
            return FAILURE;
        }

        auto client = connect_client(control->aeron_dir, error);
        if (!client)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        auto* reader = client->counters_reader();
        const std::int32_t cluster_id = mark_file->cluster_id();
        const std::int32_t counter_id = find_counter(reader, common::CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID, cluster_id);

        const std::int64_t now = now_ms();
        const bool setting = delay_ms.has_value();
        if (setting)
        {
            if (std::int64_t* address =
                    AERON_NULL_COUNTER_ID == counter_id ? nullptr : aeron_counters_reader_addr(reader, counter_id);
                nullptr != address)
            {
                __atomic_store_n(address, now + *delay_ms, __ATOMIC_RELEASE);
            }
        }

        const auto deadline = counter_value(reader, counter_id).value_or(-1);
        out += date_only(deadline);
        out += " ";
        out += time_of_day(now);
        out += setting ? " setting next: " : " next: ";
        out += date_only(deadline);
        out += " ";
        out += time_of_day(deadline);
        out += "\n";
        return SUCCESS;
    }

    /// `invalidateLatestSnapshot` (`:738-746`).
    int invalidate_latest_snapshot(const std::string& cluster_dir, std::string& out)
    {
        auto log = common::RecordingLog::open(cluster_dir, false);
        if (!log)
        {
            return FAILURE;
        }
        auto result = log->invalidate_latest_snapshot();
        if (!result)
        {
            return FAILURE;
        }
        out += " invalidate latest snapshot: ";
        out += *result ? "true" : "false";
        out += "\n";
        return *result ? SUCCESS : FAILURE;
    }

    /// `describeLatestConsensusModuleSnapshot` (`:773-843`).
    int describe_latest_cm_snapshot(const std::string& cluster_dir, std::string& out)
    {
        auto log = common::RecordingLog::open(cluster_dir, false);
        const common::RecordingLog::Entry* entry =
            log ? log->get_latest_snapshot(common::RecordingLog::SERVICE_ID) : nullptr;
        if (nullptr == entry)
        {
            out += "Snapshot not found\n";
            return FAILURE;
        }

        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            out += error;
            out += "\n";
            return FAILURE;
        }

        const std::string aeron_dir = mark_file->aeron_directory();
        const std::int32_t member_id = mark_file->member_id();
        const std::int64_t recording_id = entry->recording_id;

        auto length = replay_snapshot(aeron_dir, recording_id, out);
        if (!length)
        {
            return FAILURE;
        }

        out += "Consensus Module Snapshot End: memberId=" + std::to_string(member_id) +
               " recordingId=" + std::to_string(recording_id) + " length=" + std::to_string(*length) + "\n";
        return SUCCESS;
    }

    /// The six switches and `validate-recording-log` (`:845-967`, `:995-1115`).
    int toggle(const std::string& cluster_dir, std::string& out, std::string_view name)
    {
        const auto command = named_toggle(name);
        if (!command)
        {
            out += "Unknown command: ";
            out += name;
            out += "\n";
            return FAILURE;
        }
        return toggle_state(cluster_dir, out, *command) ? SUCCESS : FAILURE;
    }

private:
    bool describe_inner(const std::string& cluster_dir, std::string& out)
    {
        std::string service_directory = cluster_dir;

        if (mark_file_exists(cluster_dir) || properties_.timeout_ms > 0)
        {
            std::string error;
            auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
            if (!mark_file)
            {
                return false;
            }
            print_type_and_activity(*mark_file, out);
            out += mark_header_line(*mark_file);
            out += "\n";
            service_directory = services_directory(cluster_dir, *mark_file);
        }
        else
        {
            out += "cluster-mark.dat does not exist.\n";
            return true;
        }

        auto service_files = open_service_mark_files(service_directory, properties_.timeout_ms, out);
        for (const auto& service_file : service_files)
        {
            print_type_and_activity(service_file, out);
            out += mark_header_line(service_file);
            out += "\n";
        }
        return true;
    }

    bool toggle_state(const std::string& cluster_dir, std::string& out, const ToggleCommand& command)
    {
        if (!mark_file_exists(cluster_dir) && properties_.timeout_ms <= 0)
        {
            out += "cluster-mark.dat does not exist.\n";
            return false;
        }

        std::string error;
        auto mark_file = open_mark_file(cluster_dir, properties_.timeout_ms, error);
        if (!mark_file)
        {
            out += error;
            out += "\n";
            return false;
        }

        const std::int32_t cluster_id = mark_file->cluster_id();
        const auto control = control_properties(*mark_file);
        if (!control)
        {
            out += "cluster-mark.dat does not describe a driver\n";
            return false;
        }

        const std::int64_t query_timeout_ms = std::max<std::int64_t>(1'000, properties_.timeout_ms);
        auto membership = query_cluster_members(*control, query_timeout_ms);
        if (!membership)
        {
            out += "Timed out querying cluster.\n";
            return false;
        }

        const std::string prefix = "Member [" + std::to_string(membership->member_id) + "]: ";

        if (command.leader_required && membership->leader_member_id != membership->member_id)
        {
            out += prefix +
                   "Current node is not the leader (leaderMemberId = " + std::to_string(membership->leader_member_id) +
                   "), unable to " + std::string{command.name} + "\n";
            return false;
        }

        const std::string cnc_path = join_path(control->aeron_dir, "cnc.dat");
        if (!axc::core::file_exists(cnc_path))
        {
            out += prefix + "Unable to locate media driver. C`n`C file [" +
                   std::filesystem::absolute(cnc_path).string() + "] does not exist.\n";
            return false;
        }

        auto client = connect_client(control->aeron_dir, error);
        if (!client)
        {
            out += prefix + "Unable to locate media driver.\n";
            return false;
        }

        auto* reader = client->counters_reader();
        const auto state_value =
            counter_value(reader, find_counter(reader, common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, cluster_id));
        std::optional<consensus::ConsensusModuleState> state;
        if (state_value.has_value() && *state_value >= 0 && *state_value <= 6)
        {
            state = static_cast<consensus::ConsensusModuleState>(*state_value);
        }
        if (!state.has_value())
        {
            out += prefix + "Unable to resolve state of consensus module.\n";
            return false;
        }

        if (*state != command.expected_state)
        {
            out += prefix + "Unable to " + std::string{command.name} + " as the state of the consensus module is " +
                   std::string{consensus::state_name(*state)} + ", but needs to be " +
                   std::string{consensus::state_name(command.expected_state)} + "\n";
            return false;
        }

        const std::int32_t toggle_type_id = ToggleKind::cluster_control == command.kind
                                                ? common::CLUSTER_CONTROL_TOGGLE_TYPE_ID
                                                : common::NODE_CONTROL_TOGGLE_TYPE_ID;
        const std::int32_t toggle_id = find_counter(reader, toggle_type_id, cluster_id);
        if (AERON_NULL_COUNTER_ID == toggle_id)
        {
            out += prefix + "Failed to find control toggle\n";
            return false;
        }

        std::int64_t* address = aeron_counters_reader_addr(reader, toggle_id);
        bool toggled = false;
        if (nullptr != address)
        {
            if (ToggleKind::cluster_control == command.kind)
            {
                toggled = consensus::cluster_control_toggle(
                    address, static_cast<consensus::ClusterControlState>(command.target_code));
            }
            else
            {
                toggled = consensus::node_control_toggle(address,
                                                         static_cast<consensus::NodeControlState>(command.target_code));
            }
        }
        if (!toggled)
        {
            out += prefix + "Failed to apply " + std::string{command.name} + ", current toggle value = " +
                   toggle_name_at(command.kind, counter_value(reader, toggle_id).value_or(0)) + "\n";
            return false;
        }

        if (command.wait_for_completion)
        {
            const std::int64_t timeout_ms = std::max(command.default_timeout_ms, properties_.timeout_ms);
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max<std::int64_t>(0, timeout_ms));
            bool neutral = false;
            while (true)
            {
                neutral = counter_value(reader, toggle_id).value_or(0) == 1;
                if (neutral || std::chrono::steady_clock::now() > deadline)
                {
                    break;
                }
                (void)client->do_work();
                std::this_thread::yield();
            }
            if (!neutral)
            {
                out += prefix + "Timed out after " + std::to_string(timeout_ms) + "ms waiting for " +
                       std::string{command.name} + " to complete.\n";
                return false;
            }
        }

        out += prefix + std::string{command.name} + " applied successfully\n";
        return true;
    }

    std::optional<std::int64_t> replay_snapshot(const std::string& aeron_dir, std::int64_t recording_id,
                                                std::string& out)
    {
        std::string error;
        auto client = connect_client(aeron_dir, error);
        if (!client)
        {
            out += error;
            out += "\n";
            return std::nullopt;
        }

        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            out += "aeron_archive_context_init failed\n";
            return std::nullopt;
        }
        (void)aeron_archive_context_set_aeron(context, client->handle());
        (void)aeron_archive_context_set_owns_aeron_client(context, false);
        // `ClusterToolOperator.java:787-789`: both channels are `aeron:ipc`.
        (void)aeron_archive_context_set_control_request_channel(context, "aeron:ipc");
        (void)aeron_archive_context_set_control_response_channel(context, "aeron:ipc");
        (void)aeron_archive_context_set_client_name(context, std::string{TOOL_CLIENT_NAME}.c_str());

        aeron_archive_t* archive = nullptr;
        if (0 != aeron_archive_connect(&archive, context))
        {
            out += aeron_errmsg();
            out += "\n";
            (void)aeron_archive_context_close(context);
            return std::nullopt;
        }

        aeron_archive_replay_params_t params{};
        if (0 != aeron_archive_replay_params_init(&params))
        {
            out += "aeron_archive_replay_params_init failed\n";
            (void)aeron_archive_close(archive);
            (void)aeron_archive_context_close(context);
            return std::nullopt;
        }
        params.position = 0;
        params.length = -1;

        std::int64_t replay_session_id = -1;
        if (0 != aeron_archive_start_replay(&replay_session_id, archive, recording_id,
                                            properties_.replay_channel.c_str(), properties_.replay_stream_id, &params))
        {
            out += aeron_errmsg();
            out += "\n";
            (void)aeron_archive_close(archive);
            (void)aeron_archive_context_close(context);
            return std::nullopt;
        }

        auto sub_add = client->add_subscription(properties_.replay_channel, properties_.replay_stream_id);
        auto subscription = await_handle(*client, sub_add, COMMAND_TIMEOUT_NS, "addSubscription");
        if (!subscription)
        {
            out += subscription.error().message.view();
            out += "\n";
            (void)aeron_archive_stop_replay(archive, replay_session_id);
            (void)aeron_archive_close(archive);
            (void)aeron_archive_context_close(context);
            return std::nullopt;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(SNAPSHOT_REPLAY_TIMEOUT_MS);
        aeron_image_t* image = nullptr;
        while (nullptr == image)
        {
            (void)client->do_work();
            image = aeron_subscription_image_by_session_id(subscription->get(),
                                                           static_cast<std::int32_t>(replay_session_id));
            if (nullptr == image)
            {
                (void)aeron_archive_check_for_error_response(archive);
                if (std::chrono::steady_clock::now() > deadline)
                {
                    out += "snapshot replay never started\n";
                    (void)aeron_archive_stop_replay(archive, replay_session_id);
                    (void)aeron_archive_close(archive);
                    (void)aeron_archive_context_close(context);
                    return std::nullopt;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
            }
        }

        SnapshotPrinter printer{out};
        consensus::ConsensusModuleSnapshotAdapter adapter;
        adapter.record_observer(&printer);

        bool failed = false;
        while (!adapter.is_done())
        {
            auto polled = adapter.poll(image, printer);
            if (!polled)
            {
                out += polled.error().message.view();
                out += "\n";
                failed = true;
                break;
            }
            (void)client->do_work();
            if (aeron_image_is_closed(image) && !adapter.is_done())
            {
                out += "snapshot ended unexpectedly\n";
                failed = true;
                break;
            }
            if (std::chrono::steady_clock::now() > deadline)
            {
                out += "snapshot ended unexpectedly\n";
                failed = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_STEP_MS));
        }

        const std::int64_t length = failed ? 0 : aeron_image_position(image);
        (void)aeron_image_release(image);
        (void)aeron_archive_stop_replay(archive, replay_session_id);
        (void)aeron_archive_close(archive);
        (void)aeron_archive_context_close(context);

        if (failed)
        {
            return std::nullopt;
        }
        return length;
    }

    ToolProperties properties_;
};

}  // namespace

int run_cluster_tool(const std::vector<std::string>& args, std::string& out, std::string& err)
{
    PropertySet properties;
    std::vector<std::string> positional;

    for (const auto& argument : args)
    {
        if (0 == argument.rfind("-D", 0))
        {
            const std::string assignment = argument.substr(2);
            const std::size_t equals = assignment.find('=');
            if (std::string::npos == equals)
            {
                err += "invalid system property: ";
                err += argument;
                err += "\n";
                return FAILURE;
            }
            properties.values.emplace_back(assignment.substr(0, equals), assignment.substr(equals + 1));
        }
        else
        {
            positional.push_back(argument);
        }
    }

    if (positional.size() < 2)
    {
        out += usage();
        return FAILURE;
    }

    const std::string cluster_dir = positional[0];
    std::error_code exists_error;
    if (!std::filesystem::exists(cluster_dir, exists_error))
    {
        err +=
            "ERR: cluster directory not found: " + std::filesystem::absolute(cluster_dir, exists_error).string() + "\n";
        out += usage();
        return FAILURE;
    }

    const std::string command = positional[1];
    if (!command_exists(command))
    {
        out += "Unknown command: " + command + "\n";
        out += usage();
        return FAILURE;
    }

    auto resolved = resolve_tool_properties(properties);
    if (!resolved)
    {
        err += resolved.error();
        err += "\n";
        return FAILURE;
    }

    Operator tool{*resolved};
    const std::vector<std::string> remaining(positional.begin() + 2, positional.end());

    if ("describe" == command)
    {
        return tool.describe(cluster_dir, out);
    }
    if ("pid" == command)
    {
        return tool.pid(cluster_dir, out);
    }
    if ("recovery-plan" == command)
    {
        if (remaining.empty())
        {
            out += usage();
            return FAILURE;
        }
        try
        {
            return tool.recovery_plan(cluster_dir, out, static_cast<std::int32_t>(std::stol(remaining[0])));
        }
        catch (const std::exception&)
        {
            err += "recovery-plan: [" + remaining[0] + "] is not a service count\n";
            return FAILURE;
        }
    }
    if ("recording-log" == command)
    {
        return tool.recording_log(cluster_dir, out);
    }
    if ("sort-recording-log" == command)
    {
        return tool.sort_recording_log(cluster_dir);
    }
    if ("seed-recording-log-from-snapshot" == command)
    {
        return tool.seed_recording_log_from_snapshot(cluster_dir, out);
    }
    if ("errors" == command)
    {
        return tool.errors(cluster_dir, out);
    }
    if ("list-members" == command)
    {
        return tool.list_members(cluster_dir, out);
    }
    if ("backup-query" == command)
    {
        std::optional<std::int64_t> delay;
        if (!remaining.empty())
        {
            const auto ns = parse_duration_ns(remaining[0]);
            if (!ns)
            {
                err += "backup-query: [" + remaining[0] + "] is not a duration\n";
                return FAILURE;
            }
            delay = *ns / 1'000'000;
        }
        return tool.backup_query(cluster_dir, out, delay);
    }
    if ("invalidate-latest-snapshot" == command)
    {
        return tool.invalidate_latest_snapshot(cluster_dir, out);
    }
    if ("is-leader" == command)
    {
        return tool.is_leader(cluster_dir, out);
    }
    if ("describe-latest-cm-snapshot" == command)
    {
        return tool.describe_latest_cm_snapshot(cluster_dir, out);
    }
    return tool.toggle(cluster_dir, out, command);
}

int run_cluster_tool(int argc, char* argv[])
{
    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i)
    {
        args.emplace_back(argv[i]);
    }

    std::string out;
    std::string err;
    const int status = run_cluster_tool(args, out, err);

    if (!out.empty())
    {
        std::fwrite(out.data(), 1, out.size(), stdout);
        std::fflush(stdout);
    }
    if (!err.empty())
    {
        std::fwrite(err.data(), 1, err.size(), stderr);
        std::fflush(stderr);
    }
    return SUCCESS == status ? 0 : 255;
}

}  // namespace deepmsg::tools
