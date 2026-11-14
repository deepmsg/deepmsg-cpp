#include "cluster/consensus/consensus_module_agent.h"

#include <cstdio>
#include <cstdlib>
#include <map>

#include <string>
#include "aeron_counters.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include <aeronc.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_member.h"
#include "cluster/common/termination_hook.h"
#include "cluster/consensus/cluster_control.h"
#include "cluster/consensus/node_control.h"
#include "cluster/consensus/standby_startup_check.h"
#include "core/io/fs.h"
#include "core/time/clock.h"
#include "io_aeron_cluster_codecs/AdminRequest.h"
#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/AdminResponseCode.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;

constexpr std::string_view VERSION = "1.54.0";
constexpr std::string_view GIT_SHA = "454da1e4e6";

/// `aeron_archive_client_map_archive_to_client_error_code(ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING)`
/// (`aeron_archive_client.c:2701-2709`): archive code 5 becomes 205, and
/// `AERON_SET_ERR` stores the negated value.
constexpr std::int32_t ARCHIVE_CLIENT_ERROR_UNKNOWN_RECORDING = -205;
constexpr std::int64_t ASYNC_ADD_TIMEOUT_NS = 5'000'000'000;

/// `SystemUtil.formatDuration`: whole seconds, milliseconds, microseconds,
/// then nanoseconds.
[[nodiscard]] std::string format_duration_ns(std::int64_t duration_ns)
{
    for (const auto& [unit, scale] :
         {std::pair<std::string_view, std::int64_t>{"s", 1'000'000'000}, {"ms", 1'000'000}, {"us", 1'000}})
    {
        if (duration_ns >= scale && 0 == duration_ns % scale)
        {
            return std::to_string(duration_ns / scale) + std::string{unit};
        }
    }
    return std::to_string(duration_ns) + "ns";
}

/// The reference's synchronous `aeron.addCounter` on the startup path.
template <class H>
[[nodiscard]] std::expected<H, Error> await_handle(axc::core::DeepmsgClient& client, axc::core::AsyncAdd<H>& add,
                                                   std::int64_t timeout_ns)
{
    const std::int64_t deadline = axc::core::NanoClock::now() + timeout_ns;
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
        if (axc::core::NanoClock::now() >= deadline)
        {
            return std::unexpected(make_error(ErrorCode::no_capacity, "async counter add timed out"));
        }
    }
}

[[nodiscard]] std::int64_t counter_value(const axc::core::CounterPtr& counter) noexcept
{
    if (!counter)
    {
        return 0;
    }
    std::int64_t* address = aeron_counter_addr(counter.get());
    return nullptr == address ? 0 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

void counter_set(const axc::core::CounterPtr& counter, std::int64_t value) noexcept
{
    if (!counter)
    {
        return;
    }
    if (std::int64_t* address = aeron_counter_addr(counter.get()); nullptr != address)
    {
        __atomic_store_n(address, value, __ATOMIC_RELEASE);
    }
}

void counter_increment(const axc::core::CounterPtr& counter) noexcept
{
    if (!counter)
    {
        return;
    }
    if (std::int64_t* address = aeron_counter_addr(counter.get()); nullptr != address)
    {
        (void)__atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
    }
}

/// `AdminRequestType.toString()` for the two values the decoder can produce;
/// the SBE decoder rejects anything else (`AdminRequestType.get` throws).
[[nodiscard]] std::string admin_request_type_name(std::int32_t request_type)
{
    switch (request_type)
    {
        case common::codecs::AdminRequestType::SNAPSHOT:
            return "SNAPSHOT";
        case common::codecs::AdminRequestType::NULL_VALUE:
            return "NULL_VAL";
        default:
            return std::to_string(request_type);
    }
}

void counter_propose_max(const axc::core::CounterPtr& counter, std::int64_t value) noexcept
{
    if (!counter)
    {
        return;
    }
    if (std::int64_t* address = aeron_counter_addr(counter.get()); nullptr != address)
    {
        const std::int64_t current = __atomic_load_n(address, __ATOMIC_ACQUIRE);
        if (value > current)
        {
            __atomic_store_n(address, value, __ATOMIC_RELEASE);
        }
    }
}

/// `ClusterMarkFile` link name handling, as the container's `create_mark_file`.
[[nodiscard]] std::expected<void, Error> write_mark_file_link(const ConsensusModuleConfig& config)
{
    std::error_code error;
    const std::filesystem::path cluster_canonical = std::filesystem::weakly_canonical(config.cluster_dir, error);
    const std::filesystem::path mark_canonical = std::filesystem::weakly_canonical(
        config.mark_file_dir.has_value() ? *config.mark_file_dir : config.cluster_dir, error);
    if (error)
    {
        return std::unexpected(
            make_error(ErrorCode::io_error, "unable to resolve the mark file link: " + error.message()));
    }

    const std::filesystem::path link = std::filesystem::path{config.cluster_dir} / config.mark_file_link_name();
    if (cluster_canonical == mark_canonical)
    {
        std::filesystem::remove(link, error);
        return {};
    }

    std::ofstream out{link};
    if (!out)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "could not write the consensus module mark file link"));
    }
    out << mark_canonical.string();
    return {};
}

/// `Configuration.authorisationServiceSupplier().get()` (`CM:1350-1365`): the
/// raw property was resolved in `conclude()`, so this only builds the object.
[[nodiscard]] std::unique_ptr<AuthorisationService> make_authorisation_service(AuthorisationServiceKind kind)
{
    switch (kind)
    {
        case AuthorisationServiceKind::allow_all:
            return std::make_unique<AllowAllAuthorisationService>();
        case AuthorisationServiceKind::deny_all:
            return std::make_unique<DenyAllAuthorisationService>();
        case AuthorisationServiceKind::allow_backup_and_standby:
            break;
    }
    return std::make_unique<AllowBackupAndStandbyAuthorisationService>();
}

}  // namespace

ConsensusModuleAgent::ConsensusModuleAgent(ConsensusModuleConfig config)
    : config_(std::move(config)), authorisation_service_(make_authorisation_service(config_.authorisation_service_kind))
{
}

ConsensusModuleAgent::~ConsensusModuleAgent() noexcept
{
    close_resources();
}

std::string_view ConsensusModuleAgent::role_name() const noexcept
{
    return config_.agent_role_name;
}

std::int64_t ConsensusModuleAgent::now_ns() const noexcept
{
    return config_.nano_clock ? config_.nano_clock() : axc::core::NanoClock::now();
}

std::int64_t ConsensusModuleAgent::now_ms() const noexcept
{
    return config_.epoch_clock_ms ? config_.epoch_clock_ms() : axc::core::EpochClock::now_ms();
}

std::int64_t ConsensusModuleAgent::commit_position() const noexcept
{
    return counter_value(commit_position_counter_);
}

void ConsensusModuleAgent::on_error(const Error& error) noexcept
{
    report(error);
}

std::expected<void, Error> ConsensusModuleAgent::on_start()
{
    if (started_)
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "consensus module already started"));
    }

    auto mark_file = open_mark_file();
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }

    axc::core::ClientConfig client_config;
    client_config.dir = config_.aeron_dir;
    client_config.use_conductor_agent_invoker = true;
    // `ConsensusModule.Context.conclude` gives the agent its role name; the
    // reference's `shouldSetClientName` reads it from the heartbeat counter.
    client_config.client_name = config_.agent_role_name;
    // `ConsensusModuleAgent.onStart:395`: a service client that disappears is
    // noticed through the counter handler, not by polling.
    client_config.on_unavailable_counter = &ConsensusModuleAgent::unavailable_counter_trampoline;
    client_config.on_unavailable_counter_clientd = this;

    auto client = axc::core::DeepmsgClient::connect(client_config);
    if (!client)
    {
        (void)mark_file_.signal_failed_start();
        return std::unexpected(client.error());
    }
    client_ = std::move(*client);
    (void)client_.do_work();

    if (AERON_NULL_COUNTER_ID != common::find_cluster_counter(client_.counters_reader(),
                                                              common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
                                                              config_.cluster_id))
    {
        (void)mark_file_.signal_failed_start();
        return std::unexpected(make_error(ErrorCode::config_error, "existing consensus module detected for clusterId=" +
                                                                       std::to_string(config_.cluster_id)));
    }

    auto buffer = mark_file_.error_buffer();
    if (!buffer.empty())
    {
        if (0 == aeron_distinct_error_log_init(&error_log_, buffer.data(), buffer.size(),
                                               []() -> std::int64_t { return axc::core::EpochClock::now_ms(); }))
        {
            error_log_initialised_ = true;
        }
    }

    auto counters = allocate_counters();
    if (!counters)
    {
        (void)mark_file_.signal_failed_start();
        return std::unexpected(counters.error());
    }

    auto node_files = open_node_files();
    if (!node_files)
    {
        (void)mark_file_.signal_failed_start();
        return std::unexpected(node_files.error());
    }

    auto archive = connect_archive();
    if (!archive)
    {
        (void)mark_file_.signal_failed_start();
        return std::unexpected(archive.error());
    }

    // `ConsensusModuleAgent.onStart:344-348`: a previous term's recording, if
    // any, is stopped. With C4a's empty recording.log this is a no-op.
    const std::int64_t last_term_recording_id = recording_log_.find_last_term_recording_id();
    if (NULL_VALUE != last_term_recording_id)
    {
        bool stopped = false;
        if (0 != aeron_archive_try_stop_recording_by_identity(&stopped, archive_, last_term_recording_id))
        {
            report(make_error(ErrorCode::internal_error,
                              std::string{"archive stop recording failed: "} + std::string{aeron_errmsg()}));
        }
    }

    // Q6 replaces `replicateStandbySnapshotsForStartup`.
    auto standby =
        check_standby_snapshots_on_startup(recording_log_, config_.service_count, config_.standby_snapshot_on_startup,
                                           [this](const Error& error) { report(error); });
    if (!standby)
    {
        report(standby.error());
        (void)mark_file_.signal_failed_start();
        return std::unexpected(standby.error());
    }

    auto session_layer = open_session_layer();
    if (!session_layer)
    {
        report(session_layer.error());
        (void)mark_file_.signal_failed_start();
        return std::unexpected(session_layer.error());
    }

    auto service_control = open_service_control_layer();
    if (!service_control)
    {
        report(service_control.error());
        (void)mark_file_.signal_failed_start();
        return std::unexpected(service_control.error());
    }

    auto recovered = recover_from_snapshot_and_log();
    if (!recovered)
    {
        report(recovered.error());
        (void)mark_file_.signal_failed_start();
        return std::unexpected(recovered.error());
    }

    log_adapter_ = std::make_unique<LogAdapter>(*this, config_.log_fragment_limit);

    consensus_publisher_ =
        std::make_unique<ConsensusPublisher>(client_, config_.consensus_channel, config_.consensus_stream_id);
    for (const common::ClusterMember& member : active_members_)
    {
        auto added = consensus_publisher_->add_publication(member);
        if (!added)
        {
            report(added.error());
            (void)mark_file_.signal_failed_start();
            return std::unexpected(added.error());
        }
    }

    // The consensus channel's endpoint is this member's
    // (`ConsensusModuleAgent:251-258`).
    std::string consensus_channel = config_.consensus_channel;
    if (!this_member_.consensus_endpoint.empty())
    {
        consensus_channel.push_back(std::string::npos == consensus_channel.find('?') ? '?' : '|');
        consensus_channel.append("endpoint=");
        consensus_channel.append(this_member_.consensus_endpoint);
    }
    {
        auto add = client_.add_subscription(consensus_channel, config_.consensus_stream_id);
        auto subscription = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
        if (!subscription)
        {
            report(subscription.error());
            (void)mark_file_.signal_failed_start();
            return std::unexpected(subscription.error());
        }
        consensus_subscription_ = std::move(*subscription);
    }
    consensus_adapter_ = std::make_unique<ConsensusAdapter>(CONSENSUS_FRAGMENT_LIMIT);

    election::ElectionContext election_context;
    election_context.node_state_file = &node_state_;
    election_context.recording_log = &recording_log_;
    election_context.now_ns = [this]() { return now_ns(); };
    election_context.epoch_ms = [this]() { return now_ms(); };
    election_context.time_unit = service::TimeUnit::milliseconds;
    election_context.file_sync_level = config_.file_sync_level;
    election_context.appointed_leader_id = config_.appointed_leader_id;
    election_context.startup_canvass_timeout_ns = config_.startup_canvass_timeout_ns;
    election_context.leader_heartbeat_timeout_ns = config_.leader_heartbeat_timeout_ns;
    election_context.leader_heartbeat_interval_ns = config_.leader_heartbeat_interval_ns;
    election_context.election_timeout_ns = config_.election_timeout_ns;
    election_context.election_status_interval_ns = config_.election_status_interval_ns;
    election_context.consensus_stream_id = config_.consensus_stream_id;
    election_context.log_channel = config_.log_channel;
    election_context.log_stream_id = config_.log_stream_id;
    election_context.commit_position = [this]() { return commit_position(); };
    election_context.random = [this]() { return std::uniform_real_distribution<double>{0.0, 1.0}(random_); };
    election_context.set_election_state = [this](election::ElectionState new_state)
    { counter_set(election_state_counter_, static_cast<std::int64_t>(new_state)); };
    election_context.increment_election_counter = [this]() { counter_increment(election_count_counter_); };

    election_ = std::make_unique<election::Election>(true, NULL_VALUE, recovery_plan_.last_leadership_term_id,
                                                     recovery_plan_.last_term_base_log_position, commit_position(),
                                                     recovery_plan_.appended_log_position, active_members_,
                                                     this_member_, *consensus_publisher_, *this, election_context);
    auto elected = election_->do_work(now_ns());
    if (!elected)
    {
        report(elected.error());
    }

    state(ConsensusModuleState::active, "started");
    time_of_last_update_ns_ = now_ns();

    started_ = true;
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::open_session_layer()
{
    auto members = common::parse_cluster_members(config_.members);
    if (!members)
    {
        return std::unexpected(members.error());
    }
    active_members_ = std::move(*members);
    ingress_endpoints_ = common::ingress_endpoints(active_members_);

    auto this_member = common::determine_member(active_members_, config_.member_id, config_.member_endpoints);
    if (!this_member)
    {
        return std::unexpected(this_member.error());
    }
    this_member_ = *this_member;
    leader_member_ = this_member_;
    // The reference's `thisMember` is the array element itself (so updating it
    // updates the quorum input); C++ keeps a copy, so remember where it lives.
    for (std::size_t i = 0; i < active_members_.size(); i++)
    {
        if (active_members_[i].id == this_member_.id)
        {
            this_member_index_ = i;
            break;
        }
    }
    ranked_positions_.assign(common::quorum_threshold(static_cast<std::int32_t>(active_members_.size())), 0);

    log_publisher_ = std::make_unique<LogPublisher>(config_.log_channel);
    auto log_publication_add = client_.add_exclusive_publication(config_.log_channel, config_.log_stream_id);
    auto log_publication = await_handle(client_, log_publication_add, ASYNC_ADD_TIMEOUT_NS);
    if (!log_publication)
    {
        return std::unexpected(log_publication.error());
    }
    log_publisher_->publication(std::move(*log_publication));

    // `ConsensusModuleAgent.connectIngress:3422/3428`: both ingress
    // subscriptions carry the unavailable-image handler.
    axc::core::ImageHandlers ingress_handlers{};
    ingress_handlers.on_unavailable = &ConsensusModuleAgent::unavailable_image_trampoline;
    ingress_handlers.on_unavailable_clientd = this;

    auto ingress_add = client_.add_subscription(config_.ingress_channel, config_.ingress_stream_id, ingress_handlers);
    auto ingress = await_handle(client_, ingress_add, ASYNC_ADD_TIMEOUT_NS);
    if (!ingress)
    {
        return std::unexpected(ingress.error());
    }
    ingress_subscription_ = std::move(*ingress);

    if (config_.ingress_ipc_allowed)
    {
        auto ipc_add = client_.add_subscription("aeron:ipc", config_.ingress_stream_id, ingress_handlers);
        auto ipc = await_handle(client_, ipc_add, ASYNC_ADD_TIMEOUT_NS);
        if (!ipc)
        {
            return std::unexpected(ipc.error());
        }
        ipc_ingress_subscription_ = std::move(*ipc);
    }

    egress_publisher_ = std::make_unique<EgressPublisher>(config_.leader_heartbeat_timeout_ns);
    ingress_adapter_ = std::make_unique<IngressAdapter>(*this, config_.ingress_fragment_limit);
    ingress_adapter_->connect(std::move(ingress_subscription_), std::move(ipc_ingress_subscription_));

    SessionManagerConfig session_config;
    session_config.member_id = config_.member_id;
    session_config.cluster_id = config_.cluster_id;
    session_config.session_timeout_ns = config_.session_timeout_ns;
    session_config.max_concurrent_sessions = config_.max_concurrent_sessions;
    session_config.service_count = config_.service_count;

    session_manager_ = std::make_unique<SessionManager>(
        session_config, client_, *log_publisher_, *egress_publisher_, authenticator_, *authorisation_service_,
        [this]() { return now_ns(); }, [this]() { return now_ms(); },
        [this]() { counter_increment(timed_out_client_counter_); }, [this](const Error& error) { report(error); });
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::open_service_control_layer()
{
    // `ConsensusModuleAgent` constructor (`:263-275`): the control-channel
    // publication (service stream) and subscription (consensus-module stream),
    // then one tracker per service.
    auto publication_add = client_.add_publication(config_.control_channel, config_.service_stream_id);
    auto publication = await_handle(client_, publication_add, ASYNC_ADD_TIMEOUT_NS);
    if (!publication)
    {
        return std::unexpected(publication.error());
    }
    control_publication_ = std::move(*publication);
    service_send_ = std::make_unique<PublicationSend>(control_publication_.get());
    service_proxy_ = std::make_unique<ServiceProxy>(*service_send_, *this);

    auto subscription_add = client_.add_subscription(config_.control_channel, config_.consensus_module_stream_id);
    auto subscription = await_handle(client_, subscription_add, ASYNC_ADD_TIMEOUT_NS);
    if (!subscription)
    {
        return std::unexpected(subscription.error());
    }
    control_adapter_ = std::make_unique<ConsensusModuleAdapter>(*this, FRAGMENT_LIMIT);
    control_adapter_->connect(std::move(*subscription));

    const auto service_count = static_cast<std::size_t>(config_.service_count);
    service_ack_queues_ = new_array_of_queues(service_count);
    service_client_ids_.assign(service_count, NULL_VALUE);

    pending_service_message_trackers_.clear();
    pending_service_message_trackers_.reserve(service_count);
    for (std::size_t service_id = 0; service_id < service_count; service_id++)
    {
        pending_service_message_trackers_.emplace_back(static_cast<std::int32_t>(service_id), *log_publisher_,
                                                       [this]() { return now_ms(); });
    }

    // `ConsensusModule.Context.getTimerServiceSupplierFromSystemProperty`
    // (`ConsensusModule.java:4653-4673`): the default wheel starts at 0 and
    // rounds the nanosecond resolution up to a power of two in cluster units.
    auto kind = common::timer::timer_service_kind(config_.timer_service_supplier);
    if (!kind)
    {
        return std::unexpected(kind.error());
    }
    timer_service_ = common::timer::make_timer_service(
        *kind, common::timer::ClusterTimeUnit::millis, 0,
        common::timer::wheel_tick_resolution(config_.wheel_tick_resolution_ns, common::timer::ClusterTimeUnit::millis),
        config_.ticks_per_wheel);
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::open_mark_file()
{
    common::ClusterMarkFile::Options options;
    options.path = config_.mark_file_path();
    options.component_type = common::ClusterComponentType::consensus_module;
    options.error_buffer_length = config_.error_buffer_length;
    options.timeout_ms = LIVENESS_TIMEOUT_MS;
    options.file_page_size = 4 * 1024;
    options.now_ms = now_ms();

    auto mark_file = common::ClusterMarkFile::create_or_open(options);
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }
    mark_file_ = std::move(*mark_file);
    mark_file_open_ = true;

    mark_file_.set_archive_stream_id(config_.archive_local_control_stream_id);
    mark_file_.set_service_stream_id(config_.service_stream_id);
    mark_file_.set_consensus_module_stream_id(config_.consensus_module_stream_id);
    mark_file_.set_ingress_stream_id(config_.ingress_stream_id);
    mark_file_.set_member_id(config_.member_id);
    mark_file_.set_service_id(-1);
    mark_file_.set_cluster_id(config_.cluster_id);
    mark_file_.set_channels(config_.aeron_dir, config_.control_channel, config_.ingress_channel, {}, {},
                            config_.services_dir);

    auto linked = write_mark_file_link(config_);
    if (!linked)
    {
        return std::unexpected(linked.error());
    }

    auto ready = mark_file_.signal_ready(options.now_ms);
    if (!ready)
    {
        return std::unexpected(ready.error());
    }
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::open_node_files()
{
    auto node_state = common::NodeStateFile::open(config_.cluster_dir, true, config_.file_sync_level);
    if (!node_state)
    {
        return std::unexpected(node_state.error());
    }
    node_state_ = std::move(*node_state);

    // `ConsensusModule.Context.conclude:1862-1868`: a first run inherits the
    // mark file's candidate term id before touching the node state file.
    if (NULL_VALUE == node_state_.candidate_term().candidate_term_id && NULL_VALUE != mark_file_.candidate_term_id())
    {
        auto updated = node_state_.update_candidate_term_id(mark_file_.candidate_term_id(), NULL_VALUE, now_ms());
        if (!updated)
        {
            return std::unexpected(updated.error());
        }
    }

    auto recording_log = common::RecordingLog::open(config_.cluster_dir, true);
    if (!recording_log)
    {
        return std::unexpected(recording_log.error());
    }
    recording_log_ = std::move(*recording_log);
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::allocate_counters()
{
    {
        auto add =
            common::allocate_versioned(client_, "Cluster Errors", common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
                                       config_.cluster_id, VERSION, GIT_SHA);
        auto counter = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
        if (!counter)
        {
            return std::unexpected(counter.error());
        }
        error_counter_ = std::move(*counter);
    }

    const auto await_label = [this](std::string&& name,
                                    std::int32_t type_id) -> std::expected<axc::core::CounterPtr, Error>
    {
        auto add = common::allocate(client_, name, type_id, config_.cluster_id);
        return await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
    };

    struct Counter
    {
        axc::core::CounterPtr* field;
        const char* name;
        std::int32_t type_id;
    };

    const Counter counters[] = {
        {&module_state_counter_, "Consensus Module state", common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID},
        {&election_state_counter_, "Cluster election state", common::CLUSTER_ELECTION_STATE_TYPE_ID},
        {&election_count_counter_, "Cluster election count", common::CLUSTER_ELECTION_COUNT_TYPE_ID},
        {&leadership_term_counter_, "Cluster leadership term id", common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID},
        {&role_counter_, "Cluster node role", common::CLUSTER_NODE_ROLE_TYPE_ID},
        {&commit_position_counter_, "Cluster commit-pos:", common::CLUSTER_COMMIT_POSITION_TYPE_ID},
        {&control_toggle_counter_, "Cluster control toggle", common::CLUSTER_CONTROL_TOGGLE_TYPE_ID},
        {&node_control_toggle_counter_, "Node control toggle", common::NODE_CONTROL_TOGGLE_TYPE_ID},
        {&snapshot_counter_, "Cluster snapshot count", common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID},
        {&timed_out_client_counter_, "Cluster timed out client count", common::CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID},
        {&max_cycle_time_counter_, "Cluster max cycle time in ns", common::CLUSTER_MAX_CYCLE_TIME_TYPE_ID}};

    for (const Counter& counter : counters)
    {
        auto allocated = await_label(counter.name, counter.type_id);
        if (!allocated)
        {
            return std::unexpected(allocated.error());
        }
        *counter.field = std::move(*allocated);
    }

    auto exceeded = await_label(
        "Cluster work cycle time exceeded count: threshold=" + format_duration_ns(config_.cycle_threshold_ns),
        common::CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID);
    if (!exceeded)
    {
        return std::unexpected(exceeded.error());
    }
    cycle_threshold_exceeded_counter_ = std::move(*exceeded);

    // `RecoveryState.allocate` (`service/RecoveryState.java`): the container's
    // second handshake. A first start has no snapshot, so the key is the term,
    // a zero log position and timestamp, the cluster id and zero snapshot
    // recording ids (28 bytes). C4d rewrites it from the recovery plan.
    {
        std::array<std::uint8_t, 28> key{};
        const auto put_i64 = [&key](std::size_t offset, std::int64_t value)
        {
            const auto bits = static_cast<std::uint64_t>(value);
            for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
            {
                key[offset + i] = static_cast<std::uint8_t>(bits >> (8 * i));
            }
        };
        put_i64(0, NULL_VALUE);
        put_i64(8, 0);
        put_i64(16, 0);
        const auto cluster_bits = static_cast<std::uint32_t>(config_.cluster_id);
        key[24] = static_cast<std::uint8_t>(cluster_bits);
        key[25] = static_cast<std::uint8_t>(cluster_bits >> 8);
        key[26] = static_cast<std::uint8_t>(cluster_bits >> 16);
        key[27] = static_cast<std::uint8_t>(cluster_bits >> 24);

        auto add = client_.add_counter(common::CLUSTER_RECOVERY_STATE_TYPE_ID, key,
                                       "Cluster recovery: leadershipTermId=" + std::to_string(NULL_VALUE) +
                                           " logPosition=0 clusterId=" + std::to_string(config_.cluster_id));
        auto recovery_state = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
        if (!recovery_state)
        {
            return std::unexpected(recovery_state.error());
        }
        recovery_state_counter_ = std::move(*recovery_state);
    }

    // The state counter starts at INIT (0), as `AtomicCounter` allocation does.
    counter_set(module_state_counter_, state_code(ConsensusModuleState::init));
    counter_set(role_counter_, role_code(ClusterRole::follower));
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::connect_archive()
{
    if (0 != aeron_archive_context_init(&archive_context_))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "aeron_archive_context_init failed"));
    }

    (void)aeron_archive_context_set_aeron(archive_context_, client_.handle());
    (void)aeron_archive_context_set_owns_aeron_client(archive_context_, false);
    (void)aeron_archive_context_set_control_request_channel(archive_context_,
                                                            config_.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_request_stream_id(archive_context_,
                                                              config_.archive_local_control_stream_id);
    (void)aeron_archive_context_set_control_response_channel(archive_context_,
                                                             config_.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_response_stream_id(archive_context_,
                                                               config_.archive_control_response_stream_id);
    (void)aeron_archive_context_set_message_timeout_ns(archive_context_, 5'000'000'000ULL);

    if (0 != aeron_archive_connect(&archive_, archive_context_))
    {
        const std::string message = aeron_errmsg();
        (void)aeron_archive_context_close(archive_context_);
        archive_context_ = nullptr;
        return std::unexpected(make_error(ErrorCode::protocol_error, "archive connection failed: " + message));
    }

    archive_id_ = aeron_archive_get_archive_id(archive_);
    return {};
}

int ConsensusModuleAgent::do_work() noexcept
{
    // `ConsensusModuleAgent.doWork` leaves through `ClusterTerminationException`
    // once the module is CLOSED; the runner then calls `onClose`. Ours has no
    // exception to throw, so a CLOSED module does no work and the owner calls
    // `on_close` (the interop fixtures and `ConsensusModule` do).
    if (ConsensusModuleState::closed == state_)
    {
        return 0;
    }

    const std::int64_t timestamp_ms = now_ms();
    const std::int64_t now = now_ns();
    int work_count = 0;

    measure_duty_cycle(now);

    if (now >= slow_tick_deadline_ns_)
    {
        const int slow_work = slow_tick_work(now);
        work_count += slow_work;
        slow_tick_deadline_ns_ = slow_work > 0 ? now + 1 : now + SLOW_TICK_INTERVAL_NS;
    }

    const int client_work = client_.do_work();
    if (client_work < 0)
    {
        report(make_error(ErrorCode::internal_error, std::string{"aeron client do_work failed: "} + aeron_errmsg()));
    }
    else
    {
        work_count += client_work;
    }

    // `ConsensusModuleAgent.doWork:426-437`.
    if (nullptr != consensus_adapter_ && nullptr != consensus_subscription_)
    {
        auto polled = consensus_adapter_->poll(consensus_subscription_.get(), *this);
        if (!polled)
        {
            report(polled.error());
        }
        else
        {
            work_count += *polled;
        }
    }

    if (nullptr != election_)
    {
        auto elected = election_->do_work(now);
        if (!elected)
        {
            report(elected.error());
            auto handled = election_->handle_error(now, elected.error());
            if (!handled)
            {
                report(handled.error());
            }
        }
        else
        {
            work_count += *elected;
        }
        if (election_reset_pending_)
        {
            election_reset_pending_ = false;
            election_.reset();
        }
    }
    else
    {
        work_count += consensus_work(timestamp_ms, now);
    }

    return work_count;
}

int ConsensusModuleAgent::slow_tick_work(std::int64_t now_ns) noexcept
{
    int work_count = 0;

    if (now_ns >= mark_file_update_deadline_ns_)
    {
        mark_file_update_deadline_ns_ = now_ns + MARK_FILE_UPDATE_INTERVAL_NS;
        if (mark_file_open_)
        {
            mark_file_.update_activity_timestamp(now_ms());
        }
    }

    work_count += poll_archive_error_response();

    // `ConsensusModuleAgent.slowTickWork:2417-2421`: the toggle is read
    // whenever this member is the leader with no election running -- not only
    // while ACTIVE, or a SUSPENDED module could never see the RESUME.
    if (nullptr == election_ && ClusterRole::leader == role_)
    {
        work_count += check_cluster_control_toggle(now_ns);
    }

    // `ConsensusModuleAgent.slowTickWork:2431-2460`: the leader runs redirects,
    // rejections and the pending-session checks before the consensus work.
    if (nullptr != session_manager_ && ClusterRole::leader == role_ && state_ == ConsensusModuleState::active)
    {
        work_count += session_manager_->send_redirects(leadership_term_id_, config_.member_id, now_ns);
        work_count += session_manager_->send_rejections(leadership_term_id_, config_.member_id, now_ns);
        work_count += session_manager_->process_all_pending_sessions(now_ns, config_.member_id, leadership_term_id_);
        work_count +=
            session_manager_->check_sessions(now_ns, leadership_term_id_, config_.member_id, ingress_endpoints_);
    }

    // `ConsensusModuleAgent.slowTickWork:2473-2476`: the node toggle is read
    // for both roles once ACTIVE and no election is running.
    if (nullptr == election_ && ConsensusModuleState::active == state_)
    {
        work_count += check_node_control_toggle();
    }

    // `ConsensusModuleAgent.slowTickWork:2493-2505`.
    if (nullptr != recording_log_validator_)
    {
        work_count += recording_log_validator_->poll();
        if (recording_log_validator_->is_complete())
        {
            recording_log_validator_.reset();
            close_validator_archive();
        }
    }

    return work_count;
}

int ConsensusModuleAgent::poll_archive_error_response() noexcept
{
    if (nullptr == archive_)
    {
        return 0;
    }

    char buffer[1024]{};
    const int result = aeron_archive_poll_for_error_response(archive_, buffer, sizeof(buffer));
    if (result < 0)
    {
        report(make_error(ErrorCode::protocol_error, std::string{"archive error poll failed: "} + aeron_errmsg()));
        return 0;
    }
    if ('\0' != buffer[0])
    {
        // `ConsensusModuleAgent.pollArchiveEvents:2077-2140` reports control
        // errors through the counted error handler; Election owns the
        // catchup/replay correlation bookkeeping from C4d.
        report(make_error(ErrorCode::protocol_error, std::string{"archive control error: "} + buffer));
        return 1;
    }
    return 0;
}

int ConsensusModuleAgent::consensus_work(std::int64_t timestamp, std::int64_t now_ns) noexcept
{
    // `ConsensusModuleAgent.consensusWork:2499-2540`: the leader's active
    // branch polls timers, the pending service messages and ingress, then
    // advances the commit position; the service-control stream is polled on
    // every round (`:2553`). The follower log branch is C4d.
    int work_count = 0;

    if (ClusterRole::leader == role_)
    {
        if (ConsensusModuleState::snapshot == state_ && 0 == config_.service_count && nullptr != log_publisher_ &&
            log_publisher_->position() <= commit_position())
        {
            // 1.54 `37db10bebd`: the zero-service snapshot is taken in
            // `consensusWork`, not on the slow tick.
            auto taken = snapshot_on_service_ack(commit_position(), timestamp, {});
            if (!taken)
            {
                report(taken.error());
            }
            work_count += 1;
        }
        else if (state_ == ConsensusModuleState::active)
        {
            if (nullptr != timer_service_)
            {
                work_count += timer_service_->poll(timestamp, *this);
            }
            for (PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
            {
                work_count += tracker.poll();
            }
            if (nullptr != ingress_adapter_)
            {
                auto polled = ingress_adapter_->poll();
                if (!polled)
                {
                    report(polled.error());
                }
                else
                {
                    work_count += *polled;
                }
            }
        }

        work_count += update_leader_position(now_ns);
    }

    if (nullptr != control_adapter_)
    {
        auto polled = control_adapter_->poll();
        if (!polled)
        {
            report(polled.error());
        }
        else
        {
            work_count += *polled;
        }
    }

    return work_count;
}

int ConsensusModuleAgent::update_leader_position(std::int64_t now_ns) noexcept
{
    // `ConsensusModuleAgent.quorumPositionBoundedByLeaderLog:2915-2928` and
    // `updateLeaderPosition:2903-2940`. C4c has no archive recording of the
    // log, so the publication's position is the leader's append position;
    // C4d replaces it with the `RecordingPos` counter.
    if (nullptr == log_publisher_ || active_members_.empty())
    {
        return 0;
    }

    const std::int64_t leader_append_position = recording_position();
    this_member_.log_position = leader_append_position;
    this_member_.time_of_last_append_position_ns = now_ns;
    if (this_member_index_ < active_members_.size())
    {
        active_members_[this_member_index_].log_position = leader_append_position;
        active_members_[this_member_index_].time_of_last_append_position_ns = now_ns;
    }

    const std::int64_t quorum_position =
        common::quorum_position(active_members_, ranked_positions_, now_ns, config_.leader_heartbeat_timeout_ns);
    const std::int64_t commit = std::min(quorum_position, leader_append_position);

    const std::int64_t leader_commit_position = commit_position();
    if (commit > leader_commit_position ||
        now_ns >= (time_of_last_log_update_ns_ + config_.leader_heartbeat_interval_ns))
    {
        if (commit < leader_commit_position && leader_commit_position > last_quorum_backtrack_commit_position_)
        {
            last_quorum_backtrack_commit_position_ = leader_commit_position;
            report(make_error(ErrorCode::internal_error, "quorum position went backwards: leaderCommitPosition=" +
                                                             std::to_string(leader_commit_position) +
                                                             " quorumPosition=" + std::to_string(commit)));
        }

        counter_propose_max(commit_position_counter_, commit);
        time_of_last_log_update_ns_ = now_ns;
        sweep_uncommitted_entries_to(commit);
        return 1;
    }

    return 0;
}

void ConsensusModuleAgent::sweep_uncommitted_entries_to(std::int64_t commit_position) noexcept
{
    // `ConsensusModuleAgent.sweepUncommittedEntriesTo:3016-3028`. The session
    // half (`SessionManager.sweepUncommittedSessions`) is C4d.
    for (PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
    {
        tracker.sweep_leader_messages(commit_position);
    }

    while (!uncommitted_timers_.empty() && uncommitted_timers_.front().first <= commit_position)
    {
        uncommitted_timers_.pop_front();
    }

    if (nullptr != session_manager_)
    {
        session_manager_->sweep_uncommitted_sessions(commit_position);
    }

    while (!uncommitted_previous_state_.empty() && uncommitted_previous_state_.front().first <= commit_position)
    {
        uncommitted_previous_state_.pop_front();
    }
}

void ConsensusModuleAgent::restore_uncommitted_entries(std::int64_t commit_position)
{
    if (nullptr != timer_service_)
    {
        for (auto it = uncommitted_timers_.begin(); it != uncommitted_timers_.end(); ++it)
        {
            if (it->first > commit_position)
            {
                timer_service_->schedule_timer_for_correlation_id(it->second, 0);
            }
        }
    }
    uncommitted_timers_.clear();

    for (PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
    {
        tracker.restore_uncommitted_messages(commit_position);
    }

    if (nullptr != session_manager_)
    {
        session_manager_->restore_uncommitted_sessions(commit_position);
    }

    while (!uncommitted_previous_state_.empty() && uncommitted_previous_state_.front().first <= commit_position)
    {
        uncommitted_previous_state_.pop_front();
    }
    if (!uncommitted_previous_state_.empty())
    {
        uncommitted_previous_state_.pop_front();
        if (!uncommitted_previous_state_.empty())
        {
            const ConsensusModuleState previous = uncommitted_previous_state_.front().second;
            uncommitted_previous_state_.pop_front();
            if (ConsensusModuleState::closed != state_)
            {
                state(previous, "rollback");
            }
        }
    }
    uncommitted_previous_state_.clear();
}

void ConsensusModuleAgent::measure_duty_cycle(std::int64_t now_ns) noexcept
{
    if (0 == time_of_last_update_ns_)
    {
        time_of_last_update_ns_ = now_ns;
        return;
    }

    const std::int64_t cycle_time_ns = now_ns - time_of_last_update_ns_;
    time_of_last_update_ns_ = now_ns;

    counter_propose_max(max_cycle_time_counter_, cycle_time_ns);
    if (cycle_time_ns > config_.cycle_threshold_ns)
    {
        counter_increment(cycle_threshold_exceeded_counter_);
    }
}

void ConsensusModuleAgent::report(const Error& error) noexcept
{
    counter_increment(error_counter_);

    if (error_log_initialised_)
    {
        const std::string description{error.message.view()};
        (void)aeron_distinct_error_log_record(&error_log_, static_cast<std::int32_t>(error.code), description.c_str());
    }
}

void ConsensusModuleAgent::state(ConsensusModuleState new_state, const char* /*reason*/) noexcept
{
    if (new_state != state_)
    {
        state_ = new_state;
        counter_set(module_state_counter_, state_code(new_state));
    }
}

void ConsensusModuleAgent::role(ClusterRole new_role) noexcept
{
    if (new_role != role_)
    {
        role_ = new_role;
        counter_set(role_counter_, role_code(new_role));
    }
}

bool ConsensusModuleAgent::on_ingress_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                              const std::uint8_t* payload, std::size_t payload_length)
{
    if (nullptr != session_manager_ && leadership_term_id == leadership_term_id_ && ClusterRole::leader == role_)
    {
        return session_manager_->on_session_message(leadership_term_id, cluster_session_id, payload, payload_length);
    }
    return true;
}

void ConsensusModuleAgent::on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id,
                                              std::int32_t version, std::string_view response_channel,
                                              std::span<const std::uint8_t> encoded_credentials,
                                              std::string_view client_info, const IngressImageInfo& image_info)
{
    if (nullptr == session_manager_)
    {
        return;
    }
    session_manager_->on_session_connect(correlation_id, response_stream_id, version, std::string{response_channel},
                                         encoded_credentials, client_info, image_info.correlation_id,
                                         image_info.source_identity, image_info.session_id, role_, ingress_endpoints_);
}

void ConsensusModuleAgent::on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id)
{
    if (nullptr != session_manager_ && leadership_term_id == leadership_term_id_ && ClusterRole::leader == role_)
    {
        session_manager_->on_session_close(leadership_term_id, cluster_session_id);
    }
}

void ConsensusModuleAgent::on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                                 std::int64_t image_correlation_id)
{
    if (nullptr != session_manager_ && leadership_term_id == leadership_term_id_ && ClusterRole::leader == role_)
    {
        session_manager_->on_session_keep_alive(leadership_term_id, cluster_session_id, image_correlation_id);
    }
}

void ConsensusModuleAgent::on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                                 std::span<const std::uint8_t> encoded_credentials)
{
    if (nullptr != session_manager_ && ClusterRole::leader == role_)
    {
        session_manager_->on_challenge_response(correlation_id, cluster_session_id, encoded_credentials);
    }
    else
    {
        // The non-leader path forwards to the leader over the consensus
        // publication (`ConsensusPublisher.challengeResponse`), which C6 owns.
        report(make_error(ErrorCode::internal_error, "challenge response for a follower is not transcribed yet (C6)"));
    }
}

void ConsensusModuleAgent::on_admin_request(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                            std::int64_t correlation_id, std::int32_t request_type,
                                            std::span<const std::uint8_t> /*payload*/)
{
    // `ConsensusModuleAgent.onAdminRequest:780-822`, in the reference's order.
    if (ClusterRole::leader != role_ || leadership_term_id != leadership_term_id_)
    {
        return;
    }

    ClusterSession* session =
        nullptr == session_manager_ ? nullptr : session_manager_->find_by_session_id(cluster_session_id);
    if (nullptr == session || !session->is_open() || nullptr == egress_publisher_ || nullptr == authorisation_service_)
    {
        return;
    }

    const auto request = static_cast<common::codecs::AdminRequestType::Value>(request_type);
    if (!authorisation_service_->is_authorised(common::SBE_SCHEMA_ID, common::codecs::AdminRequest::SBE_TEMPLATE_ID,
                                               request_type, session->encoded_principal()))
    {
        const std::string message =
            "Execution of the " + admin_request_type_name(request_type) + " request was not authorised";
        (void)egress_publisher_->send_admin_response(*session, correlation_id, request,
                                                     common::codecs::AdminResponseCode::UNAUTHORISED_ACCESS, message);
        return;
    }

    if (common::codecs::AdminRequestType::SNAPSHOT == request_type)
    {
        if (cluster_control_toggle(control_toggle_counter_, ClusterControlState::snapshot))
        {
            (void)egress_publisher_->send_admin_response(*session, correlation_id, request,
                                                         common::codecs::AdminResponseCode::OK, "");
        }
        else
        {
            (void)egress_publisher_->send_admin_response(
                *session, correlation_id, request, common::codecs::AdminResponseCode::ERROR,
                "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state");
        }
        return;
    }

    (void)egress_publisher_->send_admin_response(*session, correlation_id, request,
                                                 common::codecs::AdminResponseCode::ERROR,
                                                 "Unknown request type: " + admin_request_type_name(request_type));
}

void ConsensusModuleAgent::on_extension_message(std::int32_t schema_id, std::int32_t template_id,
                                                std::span<const std::uint8_t> /*message*/)
{
    report(make_error(ErrorCode::protocol_error, "expected schemaId=" + std::to_string(common::SBE_SCHEMA_ID) +
                                                     ", actual=" + std::to_string(schema_id) +
                                                     " (templateId=" + std::to_string(template_id) + ")"));
}

void ConsensusModuleAgent::leadership_term_id(std::int64_t leadership_term_id)
{
    leadership_term_id_ = leadership_term_id;
    counter_set(leadership_term_counter_, leadership_term_id);
    for (PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
    {
        tracker.leadership_term_id(leadership_term_id);
    }
}

std::int64_t ConsensusModuleAgent::append_position() const noexcept
{
    return nullptr == log_publisher_ ? 0 : log_publisher_->position();
}

std::expected<void, Error> ConsensusModuleAgent::join_log_as_leader(std::int64_t term, std::int64_t log_position,
                                                                    std::int32_t log_session_id, bool is_startup)
{
    if (nullptr == service_proxy_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "the service control layer is not open"));
    }

    leadership_term_id(term);

    // `ConsensusModuleAgent.electionComplete:1921-1931`, leader branch: the
    // timer wheel starts at the cluster's current time and the commit keepalive
    // is due immediately. C4d moves this into the Election.
    if (nullptr != timer_service_)
    {
        timer_service_->current_time(now_ms());
    }
    time_of_last_log_update_ns_ = now_ns() - config_.leader_heartbeat_interval_ns;

    // `joinLogAsLeader:1704-1731`: record the log, wait for the
    // `RecordingPos` append counter, then block until every service acks.
    std::string channel = config_.log_channel;
    channel.push_back(std::string::npos == channel.find('?') ? '?' : '|');
    channel.append("tags=")
        .append(log_publication_tag_)
        .append("|session-id=")
        .append(std::to_string(log_session_id))
        .append("|alias=log");

    auto recording = start_log_recording(channel, config_.log_stream_id);
    if (!recording)
    {
        return recording;
    }

    // `joinLogAsLeader:1718-1720`: the recording takes the raw log channel,
    // the services are told the spy channel over UDP.
    {
        const bool is_ipc = 0 == channel.rfind("aeron:ipc", 0);
        // The container adds its own `alias` when it joins (`CSA:855-857`), so
        // the channel it is handed carries none.
        std::string service_channel = channel;
        const std::size_t alias = service_channel.find("|alias=");
        if (std::string::npos != alias)
        {
            const std::size_t end = service_channel.find('|', alias + 1);
            service_channel.erase(alias, std::string::npos == end ? std::string::npos : end - alias);
        }
        local_log_channel_ = is_ipc ? service_channel : "aeron-spy:" + service_channel;
    }
    while (true)
    {
        auto created = try_create_append_position(log_session_id);
        if (!created)
        {
            return std::unexpected(created.error());
        }
        if (*created)
        {
            break;
        }
        (void)client_.do_work();
    }

    auto ready = await_services_ready(local_log_channel_, config_.log_stream_id, log_session_id, log_position,
                                      std::numeric_limits<std::int64_t>::max(), is_startup, service::Role::leader);
    return ready;
}

std::expected<bool, Error> ConsensusModuleAgent::service_acks_reached()
{
    auto reached = has_reached(expected_ack_position_, service_ack_id_, service_ack_queues_);
    if (!reached)
    {
        return reached;
    }
    if (*reached)
    {
        remove_head(service_ack_queues_);
        ++service_ack_id_;
    }
    return reached;
}

std::expected<void, Error> ConsensusModuleAgent::capture_service_ack(std::int64_t log_position, std::int64_t ack_id,
                                                                     std::int64_t relevant_id, std::int32_t service_id)
{
    if (0 == ack_id && NULL_VALUE != service_client_ids_[static_cast<std::size_t>(service_id)])
    {
        return std::unexpected(make_error(
            ErrorCode::protocol_error,
            "initial ack already received from service: possible duplicate serviceId=" + std::to_string(service_id)));
    }

    service_ack_queues_[static_cast<std::size_t>(service_id)].push_back(ServiceAck{ack_id, log_position, relevant_id});
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_service_message(std::int64_t cluster_session_id,
                                                                    std::span<const std::uint8_t> message)
{
    // 1.54.0 `8554648566` (`ConsensusModuleAgent.java:1443-1456`): an unknown
    // service id is reported and dropped, never indexed.
    const std::int32_t service_id = PendingServiceMessageTracker::service_id_from_service_message(cluster_session_id);
    if (service_id < 0 || service_id >= static_cast<std::int32_t>(pending_service_message_trackers_.size()))
    {
        report(make_error(ErrorCode::protocol_error,
                          "service message dropped, unknown serviceId=" + std::to_string(service_id) +
                              " clusterSessionId=" + std::to_string(cluster_session_id) +
                              " serviceCount=" + std::to_string(pending_service_message_trackers_.size())));
        return {};
    }

    return pending_service_message_trackers_[static_cast<std::size_t>(service_id)].enqueue_message(message);
}

std::expected<void, Error> ConsensusModuleAgent::on_service_close_session(std::int64_t cluster_session_id)
{
    if (nullptr != session_manager_)
    {
        session_manager_->on_service_close_session(
            cluster_session_id, ClusterRole::leader == role_ && ConsensusModuleState::active == state_,
            leadership_term_id_);
    }
    return {};
}

void ConsensusModuleAgent::on_schedule_timer(std::int64_t correlation_id, std::int64_t deadline)
{
    // `ConsensusModuleAgent.onScheduleTimer:1457-1468`: a timer that already
    // expired during replay is counted down instead of scheduled again.
    const std::int64_t expired = expired_timer_count_by_correlation_id_.get(correlation_id, 0);
    if (0 == expired)
    {
        if (nullptr != timer_service_)
        {
            timer_service_->schedule_timer_for_correlation_id(correlation_id, deadline);
        }
        return;
    }

    if (1 == expired)
    {
        (void)expired_timer_count_by_correlation_id_.remove(correlation_id);
    }
    else
    {
        (void)expired_timer_count_by_correlation_id_.put(correlation_id, expired - 1);
    }
}

void ConsensusModuleAgent::on_cancel_timer(std::int64_t correlation_id)
{
    if (nullptr != timer_service_)
    {
        (void)timer_service_->cancel_timer_by_correlation_id(correlation_id);
    }
}

std::expected<void, Error> ConsensusModuleAgent::on_service_ack(std::int64_t log_position, std::int64_t timestamp,
                                                                std::int64_t ack_id, std::int64_t relevant_id,
                                                                std::int32_t service_id)
{
    // 1.54.0 `8554648566` (`ConsensusModuleAgent.java:1474-1485`).
    if (service_id < 0 || service_id >= static_cast<std::int32_t>(service_ack_queues_.size()))
    {
        report(make_error(ErrorCode::protocol_error,
                          "service ack dropped, unknown serviceId=" + std::to_string(service_id) + " ackId=" +
                              std::to_string(ack_id) + " serviceCount=" + std::to_string(service_ack_queues_.size())));
        return {};
    }

    auto captured = capture_service_ack(log_position, ack_id, relevant_id, service_id);
    if (!captured)
    {
        return captured;
    }

    auto reached = has_reached(log_position, service_ack_id_, service_ack_queues_);
    if (!reached)
    {
        return std::unexpected(reached.error());
    }

    if (!*reached)
    {
        return {};
    }

    switch (state_)
    {
        case ConsensusModuleState::snapshot:
        {
            ++service_ack_id_;
            auto acks = poll_service_acks(log_position, service_id, service_ack_queues_);
            if (!acks)
            {
                return std::unexpected(acks.error());
            }
            return snapshot_on_service_ack(log_position, timestamp, *acks);
        }
        case ConsensusModuleState::quitting:
            close_and_terminate();
            break;
        case ConsensusModuleState::terminating:
            terminate_on_service_ack(log_position);
            break;
        default:
            break;
    }

    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_cluster_members_query(std::int64_t correlation_id,
                                                                          bool is_extended_request)
{
    if (nullptr == service_proxy_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "the service control layer is not open"));
    }

    if (is_extended_request)
    {
        service_proxy_->cluster_members_extended_response(correlation_id, now_ms(), leader_member_.id,
                                                          config_.member_id, active_members_);
    }
    else
    {
        service_proxy_->cluster_members_response(correlation_id, leader_member_.id,
                                                 common::encode_cluster_members(active_members_));
    }
    return {};
}

bool ConsensusModuleAgent::on_timer_event(std::int64_t correlation_id)
{
    // `ConsensusModuleAgent.onTimerEvent:889-900`: a `TimerEvent` record in
    // the log, remembered until the commit position passes it.
    const long append_position = nullptr == log_publisher_
                                     ? AERON_PUBLICATION_NOT_CONNECTED
                                     : log_publisher_->append_timer(correlation_id, leadership_term_id_, now_ms());
    if (append_position > 0)
    {
        uncommitted_timers_.emplace_back(append_position, correlation_id);
        return true;
    }

    return false;
}

void ConsensusModuleAgent::on_close() noexcept
{
    if (!started_ || closed_)
    {
        return;
    }

    closed_ = true;
    // `ConsensusModuleAgent.onClose:286-292`: services are told the final
    // commit position before the resources go away.
    if (config_.service_count > 0 && nullptr != service_proxy_)
    {
        service_proxy_->termination_position(commit_position(), false);
    }
    if (nullptr != session_manager_)
    {
        // `SessionManager.closeSessions` (`SessionManager.java:1063-1068`).
        session_manager_->close_sessions();
    }
    state(ConsensusModuleState::closed, "closed");
    // `ConsensusModuleAgent.onClose:300-302`: stop the log recording before the
    // archive connection goes away. Without it the recording session is still
    // active when the next run extends the same recording.
    try_stop_log_recording();
    close_resources();

    if (mark_file_open_)
    {
        (void)mark_file_.signal_terminated();
    }
    client_.close();
}

void ConsensusModuleAgent::close_resources() noexcept
{
    // Session manager first: it refers to both publishers.
    session_manager_.reset();
    ingress_adapter_.reset();
    egress_publisher_.reset();

    // C4c: the control adapter owns the subscription; the proxy refers to
    // `service_send_`, which refers to the publication.
    timer_service_.reset();
    pending_service_message_trackers_.clear();
    control_adapter_.reset();
    service_proxy_.reset();
    service_send_.reset();
    control_publication_.reset();

    // C4d: the election (through its log replay), the log adapter, the
    // consensus channel and the member publications hold subscriptions and
    // publications as well. They go before `client_.close()` like every other
    // handle, or their closers dereference the conductor of a closed client.
    election_.reset();
    log_adapter_.reset();
    consensus_adapter_.reset();
    consensus_subscription_.reset();
    consensus_publisher_.reset();

    log_publisher_.reset();

    recording_log_validator_.reset();
    close_validator_archive();

    if (nullptr != archive_)
    {
        (void)aeron_archive_close(archive_);
        archive_ = nullptr;
    }
    if (nullptr != archive_context_)
    {
        (void)aeron_archive_context_close(archive_context_);
        archive_context_ = nullptr;
    }
    if (error_log_initialised_)
    {
        aeron_distinct_error_log_close(&error_log_);
        error_log_initialised_ = false;
    }

    // Every handle closes through the client's conductor, so all of them must
    // be released while the client is still open. `on_close` calls this before
    // `client_.close()`; without this block the counter destructors ran after
    // the client was gone and `aeron_counter_close` dereferenced a stale
    // conductor (one crash in ~40 starts of the CM interop test).
    recovery_state_counter_.reset();
    cycle_threshold_exceeded_counter_.reset();
    max_cycle_time_counter_.reset();
    timed_out_client_counter_.reset();
    snapshot_counter_.reset();
    leadership_term_counter_.reset();
    election_count_counter_.reset();
    election_state_counter_.reset();
    node_control_toggle_counter_.reset();
    control_toggle_counter_.reset();
    commit_position_counter_.reset();
    role_counter_.reset();
    module_state_counter_.reset();
    error_counter_.reset();
}

// ==== C4d: ElectionHost, replay, snapshot load and termination ==============

namespace
{

[[nodiscard]] service::Role to_service_role(ClusterRole role) noexcept
{
    switch (role)
    {
        case ClusterRole::follower:
            return service::Role::follower;
        case ClusterRole::candidate:
            return service::Role::candidate;
        case ClusterRole::leader:
            return service::Role::leader;
    }
    return service::Role::follower;
}

[[nodiscard]] ClusterRole from_service_role(service::Role role) noexcept
{
    switch (role)
    {
        case service::Role::follower:
            return ClusterRole::follower;
        case service::Role::candidate:
            return ClusterRole::candidate;
        case service::Role::leader:
            return ClusterRole::leader;
    }
    return ClusterRole::follower;
}

[[nodiscard]] election::ModuleState to_module_state(ConsensusModuleState state) noexcept
{
    switch (state)
    {
        case ConsensusModuleState::init:
            return election::ModuleState::init;
        case ConsensusModuleState::active:
            return election::ModuleState::active;
        case ConsensusModuleState::suspended:
            return election::ModuleState::suspended;
        case ConsensusModuleState::snapshot:
            return election::ModuleState::snapshot;
        case ConsensusModuleState::quitting:
            return election::ModuleState::quitting;
        case ConsensusModuleState::terminating:
            return election::ModuleState::terminating;
        case ConsensusModuleState::closed:
            return election::ModuleState::closed;
    }
    return election::ModuleState::init;
}

[[nodiscard]] constexpr std::int64_t time_to_nanos_ms(std::int64_t timestamp) noexcept
{
    return timestamp * 1'000'000;
}

constexpr std::int32_t CLUSTER_ACTION_SUSPEND = 0;
constexpr std::int32_t CLUSTER_ACTION_RESUME = 1;
constexpr std::int32_t CLUSTER_ACTION_SNAPSHOT = 2;
constexpr std::int32_t CLUSTER_ACTION_FLAGS_DEFAULT = 0;
constexpr std::int32_t CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT = 1;

[[nodiscard]] Error not_transcribed(std::string_view what, std::string_view card)
{
    return make_error(ErrorCode::internal_error,
                      std::string{what} + " is not transcribed yet (" + std::string{card} + ")");
}

struct ExtentContext
{
    std::int64_t recording_id;
    std::optional<common::RecordingLog::RecordingExtent> extent{};
};

void on_recording_descriptor(aeron_archive_recording_descriptor_t* descriptor, void* clientd)
{
    auto* context = static_cast<ExtentContext*>(clientd);
    if (nullptr != descriptor && descriptor->recording_id == context->recording_id)
    {
        common::RecordingLog::RecordingExtent extent;
        extent.start_position = descriptor->start_position;
        extent.stop_position = descriptor->stop_position;
        extent.initial_term_id = descriptor->initial_term_id;
        extent.term_buffer_length = descriptor->term_buffer_length;
        extent.mtu_length = descriptor->mtu_length;
        extent.session_id = descriptor->session_id;
        context->extent = extent;
    }
}

}  // namespace

void ConsensusModuleAgent::set_role(service::Role new_role)
{
    role(from_service_role(new_role));
}

service::Role ConsensusModuleAgent::role() const
{
    return to_service_role(cluster_role());
}

election::ModuleState ConsensusModuleAgent::state() const
{
    return to_module_state(state_);
}

std::int64_t ConsensusModuleAgent::quorum_position_bounded_by_leader_log(std::int64_t leader_append_position,
                                                                         std::int64_t now_ns)
{
    // The election writes `this_member_` directly; the quorum input is the
    // member table, so the copy must be folded back (C4c's pitfall).
    if (this_member_index_ < active_members_.size())
    {
        active_members_[this_member_index_] = this_member_;
    }
    const std::int64_t quorum =
        common::quorum_position(active_members_, ranked_positions_, now_ns, config_.leader_heartbeat_timeout_ns);
    return std::min(quorum, leader_append_position);
}

void ConsensusModuleAgent::update_member_log_position(common::ClusterMember& member, std::int64_t leadership_term_id,
                                                      std::int64_t log_position)
{
    member.leadership_term_id = leadership_term_id;
    member.log_position = log_position;
    member.time_of_last_append_position_ns = now_ns();
}

std::int64_t ConsensusModuleAgent::read_counter_value(std::int32_t counter_id) noexcept
{
    if (AERON_NULL_COUNTER_ID == counter_id)
    {
        return 0;
    }
    const std::int64_t* address = aeron_counters_reader_addr(client_.counters_reader(), counter_id);
    return nullptr == address ? 0 : *address;
}

std::int64_t ConsensusModuleAgent::recording_position()
{
    if (AERON_NULL_COUNTER_ID != append_position_counter_id_)
    {
        return read_counter_value(append_position_counter_id_);
    }
    return nullptr != log_publisher_ ? log_publisher_->position() : 0;
}

std::int64_t ConsensusModuleAgent::get_last_appended_position()
{
    return recording_position();
}

void ConsensusModuleAgent::update_member_details(const common::ClusterMember& leader)
{
    leader_member_ = leader;
    for (common::ClusterMember& member : active_members_)
    {
        member.is_leader = member.id == leader_member_.id;
    }
    ingress_endpoints_ = common::ingress_endpoints(active_members_);
}

std::int32_t ConsensusModuleAgent::find_member_index(std::int32_t member_id) const noexcept
{
    for (std::size_t i = 0; i < active_members_.size(); i++)
    {
        if (active_members_[i].id == member_id)
        {
            return static_cast<std::int32_t>(i);
        }
    }
    return -1;
}

std::string ConsensusModuleAgent::member_consensus_channel(const common::ClusterMember& member) const
{
    std::string channel = config_.consensus_channel;
    if (!member.consensus_endpoint.empty())
    {
        channel.push_back(std::string::npos == channel.find('?') ? '?' : '|');
        channel.append("endpoint=");
        channel.append(member.consensus_endpoint);
    }
    return channel;
}

// ---- recovery -------------------------------------------------------------

std::expected<void, Error> ConsensusModuleAgent::recover_from_snapshot_and_log()
{
    auto extent_source = [this](std::int64_t recording_id) -> std::optional<common::RecordingLog::RecordingExtent>
    {
        if (nullptr == archive_)
        {
            return std::nullopt;
        }
        ExtentContext context{recording_id};
        std::int32_t count = 0;
        if (0 != aeron_archive_list_recording(&count, archive_, recording_id, &on_recording_descriptor, &context))
        {
            return std::nullopt;
        }
        return context.extent;
    };

    auto plan = recording_log_.create_recovery_plan(extent_source, config_.service_count, log_recording_id_);
    if (!plan)
    {
        return std::unexpected(plan.error());
    }
    recovery_plan_ = std::move(*plan);
    if (recovery_plan_.log.has_value())
    {
        log_recording_id_ = recovery_plan_.log->recording_id;
        log_recording_stop_position_ = recovery_plan_.log->stop_position;
    }

    auto counter = add_recovery_state_counter(recovery_plan_);
    if (!counter)
    {
        return std::unexpected(counter.error());
    }
    recovery_state_counter_ = std::move(*counter);

    if (!recovery_plan_.snapshots.empty())
    {
        auto loaded = load_snapshot(recovery_plan_.snapshots.front());
        if (!loaded)
        {
            recovery_state_counter_.reset();
            return std::unexpected(loaded.error());
        }
    }

    // `recoverFromSnapshotAndLog:3534-3548`: wait for every service's ack at
    // the snapshot position.
    while (true)
    {
        auto reached = has_reached(expected_ack_position_, service_ack_id_, service_ack_queues_);
        if (!reached)
        {
            recovery_state_counter_.reset();
            return std::unexpected(reached.error());
        }
        if (*reached)
        {
            break;
        }
        if (nullptr != control_adapter_)
        {
            (void)control_adapter_->poll();
        }
        (void)client_.do_work();
    }

    auto acks = poll_service_acks(service_ack_queues_);
    if (!acks)
    {
        recovery_state_counter_.reset();
        return std::unexpected(acks.error());
    }
    if (!are_all_relevant_ids_non_null("failed to start clustered service", *acks,
                                       [this](const Error& error) { report(error); }))
    {
        recovery_state_counter_.reset();
        return std::unexpected(make_error(ErrorCode::internal_error, "failed to start clustered service(s)"));
    }
    for (std::size_t i = 0; i < acks->size() && i < service_client_ids_.size(); i++)
    {
        service_client_ids_[i] = (*acks)[i].relevant_id;
    }
    ++service_ack_id_;
    recovery_state_counter_.reset();
    return {};
}

std::expected<axc::core::CounterPtr, Error> ConsensusModuleAgent::add_recovery_state_counter(
    const common::RecordingLog::RecoveryPlan& plan)
{
    auto add = common::RecoveryState::allocate(client_, plan, config_.cluster_id, leadership_term_id_);
    if (!add)
    {
        return std::unexpected(add.error());
    }

    auto counter = await_handle(client_, *add, ASYNC_ADD_TIMEOUT_NS);
    if (!counter)
    {
        return std::unexpected(counter.error());
    }

    // The counter lives only for the recovery (`try (Counter ignore = ...)` at
    // `ConsensusModuleAgent:3523`); copy the key for the C4d M4 assertion.
    last_recovery_state_key_.clear();
    aeron_counter_constants_t counter_constants{};
    if (0 == aeron_counter_constants(counter->get(), &counter_constants))
    {
        std::uint8_t* key = nullptr;
        if (0 == aeron_counters_reader_metadata_key(client_.counters_reader(), counter_constants.counter_id, &key) &&
            nullptr != key)
        {
            const std::size_t service_count = plan.snapshots.empty() ? 0 : config_.service_count;
            const std::size_t length = common::RecoveryState::SNAPSHOT_RECORDING_IDS_OFFSET +
                                       (static_cast<std::size_t>(service_count) * sizeof(std::int64_t));
            last_recovery_state_key_.assign(key, key + length);
        }
    }

    return counter;
}

std::expected<void, Error> ConsensusModuleAgent::load_snapshot(const common::RecordingLog::Snapshot& snapshot)
{
    auto add = client_.add_subscription(config_.replay_channel, config_.replay_stream_id);
    auto subscription = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
    if (!subscription)
    {
        return std::unexpected(subscription.error());
    }

    aeron_subscription_constants_t constants{};
    if (0 != aeron_subscription_constants(subscription->get(), &constants))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "replay subscription constants failed"));
    }

    aeron_archive_replay_params_t params{};
    (void)aeron_archive_replay_params_init(&params);
    params.position = 0;
    params.length = -1;
    params.subscription_registration_id = constants.registration_id;

    std::int64_t replay_session_id = NULL_VALUE;
    if (0 != aeron_archive_start_replay(&replay_session_id, archive_, snapshot.recording_id,
                                        config_.replay_channel.c_str(), config_.replay_stream_id, &params))
    {
        return std::unexpected(
            make_error(ErrorCode::internal_error, std::string{"snapshot replay failed: "} + aeron_errmsg()));
    }

    aeron_image_t* image = nullptr;
    while (nullptr == image)
    {
        (void)client_.do_work();
        image =
            aeron_subscription_image_by_session_id(subscription->get(), static_cast<std::int32_t>(replay_session_id));
        if (nullptr == image)
        {
            auto polled = poll_archive_error_response();
            (void)polled;
        }
    }

    ConsensusModuleSnapshotAdapter adapter;
    while (!adapter.is_done())
    {
        auto polled = adapter.poll(image, *this);
        if (!polled)
        {
            (void)aeron_archive_stop_replay(archive_, replay_session_id);
            return std::unexpected(polled.error());
        }
        (void)client_.do_work();
        if (aeron_image_is_closed(image) && !adapter.is_done())
        {
            (void)aeron_archive_stop_replay(archive_, replay_session_id);
            return std::unexpected(make_error(ErrorCode::protocol_error, "snapshot ended unexpectedly"));
        }
    }
    (void)aeron_archive_stop_replay(archive_, replay_session_id);

    for (PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
    {
        auto verified = tracker.verify();
        if (!verified)
        {
            return std::unexpected(verified.error());
        }
        tracker.reset();
    }

    if (nullptr != timer_service_)
    {
        timer_service_->current_time(now_ms());
    }
    counter_set(commit_position_counter_, snapshot.log_position);
    leadership_term_id(snapshot.leadership_term_id);
    expected_ack_position_ = snapshot.log_position;
    return {};
}

// ---- snapshot load callbacks ---------------------------------------------

std::expected<void, Error> ConsensusModuleAgent::on_load_begin_snapshot(std::int32_t app_version,
                                                                        service::TimeUnit time_unit)
{
    (void)app_version;
    if (service::TimeUnit::milliseconds != time_unit)
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "incompatible time unit in snapshot"));
    }
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_end_snapshot()
{
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_cluster_session(
    std::int64_t cluster_session_id, std::int64_t correlation_id, std::int64_t opened_log_position,
    std::int64_t time_of_last_activity, codecs::CloseReason::Value close_reason, std::int32_t response_stream_id,
    std::string_view response_channel)
{
    session_manager_->on_load_cluster_session(cluster_session_id, correlation_id, opened_log_position,
                                              time_of_last_activity, close_reason, response_stream_id,
                                              std::string{response_channel});
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_consensus_module_state(std::int64_t next_session_id,
                                                                                std::int64_t next_service_session_id,
                                                                                std::int64_t log_service_session_id,
                                                                                std::int32_t pending_message_capacity)
{
    session_manager_->load_next_session_id(next_session_id);
    if (!pending_service_message_trackers_.empty())
    {
        (void)pending_service_message_trackers_[0].load_state(next_service_session_id, log_service_session_id,
                                                              static_cast<std::size_t>(pending_message_capacity));
    }
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_pending_message_tracker(std::int64_t next_service_session_id,
                                                                                 std::int64_t log_service_session_id,
                                                                                 std::int32_t pending_message_capacity,
                                                                                 std::int32_t service_id)
{
    if (service_id < 0 || static_cast<std::size_t>(service_id) >= pending_service_message_trackers_.size())
    {
        return std::unexpected(make_error(ErrorCode::protocol_error,
                                          "serviceId=" + std::to_string(service_id) + " invalid for serviceCount=" +
                                              std::to_string(pending_service_message_trackers_.size())));
    }
    (void)pending_service_message_trackers_[static_cast<std::size_t>(service_id)].load_state(
        next_service_session_id, log_service_session_id, static_cast<std::size_t>(pending_message_capacity));
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_pending_message(std::int64_t cluster_session_id,
                                                                         std::span<const std::uint8_t> message)
{
    const std::int32_t service_id = PendingServiceMessageTracker::service_id_from_log_message(cluster_session_id);
    if (service_id < 0 || static_cast<std::size_t>(service_id) >= pending_service_message_trackers_.size())
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "pending message for unknown service"));
    }
    (void)pending_service_message_trackers_[static_cast<std::size_t>(service_id)].append_message(message);
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_load_timer(std::int64_t correlation_id, std::int64_t deadline)
{
    on_schedule_timer(correlation_id, deadline);
    return {};
}

// ---- the six replay handlers ---------------------------------------------

void ConsensusModuleAgent::on_replay_session_message(std::int64_t cluster_session_id, std::int64_t timestamp)
{
    ClusterSession* session = session_manager_->find_by_session_id(cluster_session_id);
    if (nullptr != session)
    {
        session->time_of_last_activity_ns(time_to_nanos_ms(timestamp));
    }
    else if (cluster_session_id < 0)
    {
        const std::int32_t service_id = PendingServiceMessageTracker::service_id_from_log_message(cluster_session_id);
        if (service_id >= 0 && static_cast<std::size_t>(service_id) < pending_service_message_trackers_.size())
        {
            pending_service_message_trackers_[static_cast<std::size_t>(service_id)].sweep_follower_messages(
                cluster_session_id);
        }
    }
}

void ConsensusModuleAgent::on_replay_timer_event(std::int64_t correlation_id)
{
    if (nullptr == timer_service_ || !timer_service_->cancel_timer_by_correlation_id(correlation_id))
    {
        expired_timer_count_by_correlation_id_.put(correlation_id,
                                                   expired_timer_count_by_correlation_id_.get(correlation_id, 0) + 1);
    }
}

void ConsensusModuleAgent::on_replay_session_open(std::int64_t log_position, std::int64_t correlation_id,
                                                  std::int64_t cluster_session_id, std::int64_t timestamp,
                                                  std::int32_t response_stream_id, std::string_view response_channel)
{
    session_manager_->on_replay_session_open(log_position, correlation_id, cluster_session_id, timestamp,
                                             response_stream_id, std::string{response_channel});
}

void ConsensusModuleAgent::on_replay_session_close(std::int64_t cluster_session_id,
                                                   codecs::CloseReason::Value close_reason)
{
    session_manager_->on_replay_session_close(cluster_session_id, close_reason);
}

std::expected<void, Error> ConsensusModuleAgent::on_replay_cluster_action(std::int64_t leadership_term_id,
                                                                          std::int64_t log_position,
                                                                          std::int64_t timestamp, std::int32_t action,
                                                                          std::int32_t flags)
{
    if (leadership_term_id != leadership_term_id_)
    {
        return {};
    }

    if (CLUSTER_ACTION_SUSPEND == action)
    {
        state(ConsensusModuleState::suspended, "ReplayClusterAction.SUSPENDED");
    }
    else if (CLUSTER_ACTION_RESUME == action)
    {
        state(ConsensusModuleState::active, "ReplayClusterAction.ACTIVE");
    }
    else if (CLUSTER_ACTION_SNAPSHOT == action && CLUSTER_ACTION_FLAGS_DEFAULT == flags)
    {
        state(ConsensusModuleState::snapshot, "ReplayClusterAction.SNAPSHOT");
        if (0 == config_.service_count)
        {
            auto taken = snapshot_on_service_ack(log_position, timestamp, {});
            if (!taken)
            {
                return taken;
            }
        }
    }
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::on_replay_new_leadership_term_event(
    std::int64_t leadership_term_id, std::int64_t log_position, std::int64_t timestamp,
    std::int64_t term_base_log_position, std::int32_t time_unit_code, std::int32_t app_version)
{
    (void)time_unit_code;
    (void)app_version;
    this->leadership_term_id(leadership_term_id);
    if (nullptr != election_)
    {
        return election_->on_replay_new_leadership_term_event(leadership_term_id, log_position, timestamp,
                                                              term_base_log_position);
    }
    return {};
}

// ---- LogReplayHost --------------------------------------------------------

std::expected<void, Error> ConsensusModuleAgent::await_services_ready(
    std::string_view log_channel, std::int32_t stream_id, std::int32_t log_session_id, std::int64_t log_position,
    std::int64_t max_log_position, bool is_startup, service::Role service_role)
{
    if (0 < config_.service_count && nullptr != service_proxy_)
    {
        auto joined =
            service_proxy_->join_log(log_position, max_log_position, config_.member_id, log_session_id, stream_id,
                                     is_startup, from_service_role(service_role), log_channel, false);
        if (!joined)
        {
            return joined;
        }
        expected_ack_position_ = log_position;

        while (true)
        {
            auto reached = has_reached(log_position, service_ack_id_, service_ack_queues_);
            if (!reached)
            {
                return std::unexpected(reached.error());
            }
            if (*reached)
            {
                break;
            }
            auto polled = control_adapter_->poll();
            if (!polled)
            {
            }
            (void)client_.do_work();
            if (ConsensusModuleState::closed == state_)
            {
                return std::unexpected(make_error(ErrorCode::internal_error, "State.CLOSED == state"));
            }
        }
        (void)remove_head(service_ack_queues_);
        ++service_ack_id_;
    }
    return {};
}

std::expected<int, Error> ConsensusModuleAgent::replay_log_poll(LogAdapter& log_adapter, std::int64_t stop_position)
{
    int work_count = 0;
    if (ConsensusModuleState::active == state_ || ConsensusModuleState::suspended == state_)
    {
        auto polled = log_adapter.poll(stop_position);
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        work_count += *polled;
        const std::int64_t position = log_adapter.position();
        counter_propose_max(commit_position_counter_, position);
        if (position > commit_position() - 1)
        {
            work_count++;
        }
        else if (log_adapter.is_image_closed() && position < stop_position)
        {
            return std::unexpected(make_error(ErrorCode::internal_error, "unexpected image close when replaying log"));
        }
    }
    if (nullptr != control_adapter_)
    {
        auto polled = control_adapter_->poll();
        if (polled)
        {
            work_count += *polled;
        }
    }
    return work_count;
}

void ConsensusModuleAgent::on_log_replay_error(const Error& error)
{
    report(error);
}

// ---- ElectionHost: the steady state ---------------------------------------

std::expected<int, Error> ConsensusModuleAgent::update_leader_position(std::int64_t now_ns,
                                                                       std::int64_t append_position,
                                                                       std::int64_t quorum_position)
{
    this_member_.log_position = append_position;
    this_member_.time_of_last_append_position_ns = now_ns;
    if (this_member_index_ < active_members_.size())
    {
        active_members_[this_member_index_].log_position = append_position;
        active_members_[this_member_index_].time_of_last_append_position_ns = now_ns;
    }

    const std::int64_t leader_commit_position = commit_position();
    if (quorum_position > leader_commit_position ||
        now_ns >= (time_of_last_log_update_ns_ + config_.leader_heartbeat_interval_ns))
    {
        if (quorum_position < leader_commit_position && leader_commit_position > last_quorum_backtrack_commit_position_)
        {
            last_quorum_backtrack_commit_position_ = leader_commit_position;
            report(make_error(ErrorCode::internal_error, "quorum position went backwards: leaderCommitPosition=" +
                                                             std::to_string(leader_commit_position) +
                                                             " quorumPosition=" + std::to_string(quorum_position)));
        }

        auto published = publish_commit_position(quorum_position, leadership_term_id_);
        if (!published)
        {
            return std::unexpected(published.error());
        }
        counter_propose_max(commit_position_counter_, quorum_position);
        time_of_last_log_update_ns_ = now_ns;
        sweep_uncommitted_entries_to(quorum_position);
        return 1;
    }
    return 0;
}

std::expected<void, Error> ConsensusModuleAgent::publish_commit_position(std::int64_t commit_position,
                                                                         std::int64_t leadership_term_id)
{
    if (nullptr != consensus_publisher_)
    {
        for (const common::ClusterMember& member : active_members_)
        {
            (void)consensus_publisher_->commit_position(member.id, leadership_term_id, commit_position,
                                                        config_.member_id);
        }
    }
    return {};
}

std::expected<bool, Error> ConsensusModuleAgent::try_join_log_as_follower(aeron_image_t* image, bool is_leader_startup,
                                                                          std::int64_t now_ns)
{
    (void)image;
    (void)is_leader_startup;
    (void)now_ns;
    return std::unexpected(not_transcribed("tryJoinLogAsFollower", "C6"));
}

std::expected<std::int32_t, Error> ConsensusModuleAgent::add_log_publication(std::int64_t append_position)
{
    // `addLogPublication:1655-1702`: the publication carries a tag the log
    // recording channel repeats, so the archive's recording subscription can
    // find it (`tags=<tag>`), and `alias=log`.
    log_publication_tag_ = std::to_string(client_.next_correlation_id());
    std::string channel = config_.log_channel;
    channel.push_back(std::string::npos == channel.find('?') ? '?' : '|');
    channel.append("alias=log|tags=").append(log_publication_tag_);
    // `addLogPublication:1672`: a single member's publication counts its spies
    // as connected, or the spy link the services join through never sees it.
    if (1 == active_members_.size() && 0 != config_.log_channel.rfind("aeron:ipc", 0))
    {
        channel.append("|ssc=true");
    }

    // `:1675-1680`: continue the recovered log at its append position, with
    // the recorded log's term geometry. Without this a restart's publication
    // starts at position 0 and neither extends the recording nor matches the
    // JoinLog position.
    if (recovery_plan_.log.has_value())
    {
        const common::RecordingLog::Log& log = *recovery_plan_.log;
        // The C driver's position continuation is `init-term-id` + `term-id` +
        // `term-offset` (+ `term-length`), the set `ChannelUri.initialPosition`
        // expands to (`aeron_driver_uri.c:316-360`).
        const std::int32_t term_length = log.term_buffer_length;
        const std::int32_t bits_to_shift = term_length > 0 ? __builtin_ctz(static_cast<unsigned>(term_length)) : 0;
        const std::int32_t term_id = log.initial_term_id + static_cast<std::int32_t>(append_position >> bits_to_shift);
        const std::int32_t term_offset =
            term_length > 0 ? static_cast<std::int32_t>(append_position & (term_length - 1)) : 0;
        channel.append("|init-term-id=").append(std::to_string(log.initial_term_id));
        channel.append("|term-id=").append(std::to_string(term_id));
        channel.append("|term-offset=").append(std::to_string(term_offset));
        if (term_length > 0)
        {
            channel.append("|term-length=").append(std::to_string(term_length));
        }
    }

    auto add = client_.add_exclusive_publication(channel, config_.log_stream_id);
    auto publication = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
    if (!publication)
    {
        return std::unexpected(publication.error());
    }
    std::int32_t session_id = 0;
    aeron_publication_constants_t constants{};
    if (0 == aeron_exclusive_publication_constants(publication->get(), &constants))
    {
        session_id = constants.session_id;
    }
    log_publisher_->publication(std::move(*publication));
    return session_id;
}

std::expected<std::unique_ptr<election::LogReplay>, Error> ConsensusModuleAgent::new_log_replay(
    std::int64_t log_position, std::int64_t append_position)
{
    if (nullptr == log_adapter_ || nullptr == archive_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "log replay without an adapter or archive"));
    }
    auto replay = std::make_unique<LogReplay>(client_, archive_, log_recording_id_, log_position, append_position,
                                              *log_adapter_, config_.replay_channel, config_.replay_stream_id, *this);
    return replay;
}

std::expected<std::unique_ptr<election::RecordingReplication>, Error> ConsensusModuleAgent::new_log_replication(
    std::string_view leader_archive_endpoint, std::optional<std::string> response_archive_endpoint,
    std::int64_t leader_recording_id, std::int64_t stop_position, std::int64_t now_ns)
{
    (void)leader_archive_endpoint;
    (void)response_archive_endpoint;
    (void)leader_recording_id;
    (void)stop_position;
    (void)now_ns;
    return std::unexpected(not_transcribed("newLogReplication", "C6"));
}

std::expected<void, Error> ConsensusModuleAgent::truncate_log_entry(std::int64_t leadership_term_id,
                                                                    std::int64_t log_position)
{
    (void)leadership_term_id;
    (void)log_position;
    return std::unexpected(not_transcribed("truncateLogEntry", "C6"));
}

std::expected<bool, Error> ConsensusModuleAgent::append_new_leadership_term_event(std::int64_t now_ns)
{
    if (nullptr == log_publisher_ || nullptr == election_)
    {
        return false;
    }
    return log_publisher_->append_new_leadership_term_event(leadership_term_id_, now_ms(), election_->log_position(),
                                                            config_.member_id, log_publisher_->session_id(), 0, 0);
}

std::expected<std::int64_t, Error> ConsensusModuleAgent::prepare_for_new_leadership(std::int64_t log_position,
                                                                                    std::int64_t now_ns)
{
    (void)now_ns;
    role(ClusterRole::follower);
    // `ConsensusModuleAgent.prepareForNewLeadership:1382`: no toggle action is
    // accepted until the election completes.
    cluster_control_deactivate(control_toggle_counter_);
    try_stop_log_recording();
    if (NULL_VALUE != log_recording_id_)
    {
        last_append_position_ = get_last_appended_position();
        session_manager_->clear_sessions_after(log_position, leadership_term_id_);
        session_manager_->disconnect_sessions();
        counter_set(commit_position_counter_, log_position);
        restore_uncommitted_entries(log_position);
    }
    return last_append_position_;
}

std::expected<void, Error> ConsensusModuleAgent::catchup_initiated(std::int64_t now_ns)
{
    (void)now_ns;
    return std::unexpected(not_transcribed("catchupInitiated", "C6"));
}

std::expected<int, Error> ConsensusModuleAgent::catchup_poll(std::int64_t limit_position, std::int64_t now_ns)
{
    (void)limit_position;
    (void)now_ns;
    return std::unexpected(not_transcribed("catchupPoll", "C6"));
}

bool ConsensusModuleAgent::is_catchup_near_live(std::int64_t position)
{
    (void)position;
    return false;
}

std::expected<void, Error> ConsensusModuleAgent::track_catchup_completion(common::ClusterMember& member,
                                                                          std::int64_t leadership_term_id,
                                                                          std::int16_t append_position_flags)
{
    (void)member;
    (void)leadership_term_id;
    (void)append_position_flags;
    return std::unexpected(not_transcribed("trackCatchupCompletion", "C6"));
}

std::expected<void, Error> ConsensusModuleAgent::stop_all_catchups()
{
    // `Election.init` calls this unconditionally; a single member has no
    // catchup replays to stop, so the reference's loop body is empty here.
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::set_live_log_destination(std::string_view destination)
{
    live_log_destination_ = std::string{destination};
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::set_catchup_log_destination(std::string_view destination)
{
    catchup_log_destination_ = std::string{destination};
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::await_local_sockets_closed(std::int64_t registration_id)
{
    (void)registration_id;
    return std::unexpected(not_transcribed("awaitLocalSocketsClosed", "C6"));
}

std::expected<void, Error> ConsensusModuleAgent::election_complete(std::int64_t leadership_term_id,
                                                                   std::int32_t leader_member_id,
                                                                   std::int64_t log_position, std::int64_t now_ns)
{
    this->leadership_term_id(leadership_term_id);

    if (ClusterRole::leader == cluster_role())
    {
        time_of_last_log_update_ns_ = now_ns - config_.leader_heartbeat_interval_ns;
        if (nullptr != timer_service_)
        {
            timer_service_->current_time(now_ms());
        }
        cluster_control_activate(control_toggle_counter_);
        session_manager_->prepare_sessions_for_new_term(election_ != nullptr && election_->is_leader_startup());
    }
    else
    {
        time_of_last_log_update_ns_ = now_ns;
        last_append_position_send_ns_ = now_ns;
        local_log_channel_.clear();
    }
    // `ConsensusModuleAgent.electionComplete:1939`: the node toggle is
    // activated for both roles (1.54.0; C4d only did the leader's ClusterControl).
    node_control_activate(node_control_toggle_counter_);

    auto plan =
        recording_log_.create_recovery_plan([](std::int64_t) -> std::optional<common::RecordingLog::RecordingExtent>
                                            { return std::nullopt; }, config_.service_count, log_recording_id_);
    if (plan)
    {
        recovery_plan_ = std::move(*plan);
    }

    counter_propose_max(commit_position_counter_, log_position);
    notified_commit_position_ = std::max(notified_commit_position_, log_position);

    common::ClusterMember* leader = nullptr;
    if (nullptr != election_)
    {
        leader = election_->leader();
    }
    if (nullptr != leader)
    {
        update_member_details(*leader);
    }
    else
    {
        const std::int32_t index = find_member_index(leader_member_id);
        if (index >= 0)
        {
            update_member_details(active_members_[static_cast<std::size_t>(index)]);
        }
    }

    connect_ingress();
    // The reference nulls its `election` field here, but the object lives on
    // in the caller's stack frame. Destroying it inside `do_work` would free
    // the object that is still running (`Election::leader_ready` transitions
    // to CLOSED right after this call), so the reset is deferred to `do_work`.
    election_reset_pending_ = true;
    return {};
}

std::expected<int, Error> ConsensusModuleAgent::poll_archive_events()
{
    if (nullptr == archive_)
    {
        return 0;
    }
    return poll_archive_error_response();
}

bool ConsensusModuleAgent::member_has_publication(std::int32_t member_id) const
{
    return nullptr != consensus_publisher_ && consensus_publisher_->has_publication(member_id);
}

std::expected<void, Error> ConsensusModuleAgent::try_add_member_publication(std::int32_t member_id)
{
    if (nullptr == consensus_publisher_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "no consensus publisher"));
    }
    const std::int32_t index = find_member_index(member_id);
    if (index < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "unknown member id"));
    }
    return consensus_publisher_->add_publication(active_members_[static_cast<std::size_t>(index)]);
}

std::expected<std::int64_t, Error> ConsensusModuleAgent::add_log_subscription(std::int32_t log_session_id)
{
    (void)log_session_id;
    return std::unexpected(not_transcribed("addLogSubscription", "C6"));
}

std::expected<void, Error> ConsensusModuleAgent::remove_log_subscription(std::int64_t registration_id)
{
    (void)registration_id;
    return std::unexpected(not_transcribed("removeLogSubscription", "C6"));
}

std::expected<void, Error> ConsensusModuleAgent::add_log_destination(std::int64_t registration_id,
                                                                     std::string_view destination)
{
    (void)registration_id;
    (void)destination;
    return std::unexpected(not_transcribed("addLogDestination", "C6"));
}

std::optional<std::string> ConsensusModuleAgent::log_subscription_resolved_endpoint(std::int64_t registration_id) const
{
    (void)registration_id;
    return std::nullopt;
}

bool ConsensusModuleAgent::log_subscription_channel_errored(std::int64_t registration_id) const
{
    (void)registration_id;
    return false;
}

std::string_view ConsensusModuleAgent::log_subscription_channel(std::int64_t registration_id) const
{
    (void)registration_id;
    return {};
}

std::optional<election::LogImage> ConsensusModuleAgent::log_image_by_session(std::int64_t registration_id,
                                                                             std::int32_t session_id)
{
    (void)registration_id;
    (void)session_id;
    return std::nullopt;
}

// ---- control toggles and termination -------------------------------------

std::expected<bool, Error> ConsensusModuleAgent::append_action(std::int32_t action, std::int64_t timestamp,
                                                               std::int32_t flags)
{
    if (nullptr == log_publisher_)
    {
        return false;
    }
    return log_publisher_->append_cluster_action(leadership_term_id_, timestamp, action, flags);
}

void ConsensusModuleAgent::offer_position_and_previous_state(std::int64_t position, ConsensusModuleState previous)
{
    uncommitted_previous_state_.emplace_back(position, previous);
}

int ConsensusModuleAgent::check_cluster_control_toggle(std::int64_t now_ns)
{
    (void)now_ns;
    const auto toggled_state = cluster_control_state(cluster_control_value(control_toggle_counter_));
    if (!toggled_state)
    {
        report(toggled_state.error());
        return 0;
    }
    const auto appended_ok = [](const std::expected<bool, Error>& result) { return result.has_value() && *result; };

    if (ConsensusModuleState::active == state_)
    {
        switch (*toggled_state)
        {
            case ClusterControlState::suspend:
            {
                if (appended_ok(append_action(CLUSTER_ACTION_SUSPEND, now_ms(), CLUSTER_ACTION_FLAGS_DEFAULT)))
                {
                    offer_position_and_previous_state(log_publisher_->position(), state_);
                    state(ConsensusModuleState::suspended, "ClusterControl.SUSPEND");
                }
                return 1;
            }
            case ClusterControlState::snapshot:
            {
                const bool appended =
                    appended_ok(append_action(CLUSTER_ACTION_SNAPSHOT, now_ms(), CLUSTER_ACTION_FLAGS_DEFAULT));
                if (appended)
                {
                    offer_position_and_previous_state(log_publisher_->position(), state_);
                    state(ConsensusModuleState::snapshot, "ClusterControl.SNAPSHOT");
                }
                return 1;
            }
            case ClusterControlState::standby_snapshot:
            {
                if (appended_ok(
                        append_action(CLUSTER_ACTION_SNAPSHOT, now_ms(), CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT)))
                {
                    cluster_control_reset(control_toggle_counter_);
                }
                return 1;
            }
            case ClusterControlState::shutdown:
            {
                if (appended_ok(append_action(CLUSTER_ACTION_SNAPSHOT, now_ms(), CLUSTER_ACTION_FLAGS_DEFAULT)))
                {
                    const std::int64_t position = log_publisher_->position();
                    cluster_termination_ =
                        std::make_unique<ClusterTermination>(now_ns + config_.termination_timeout_ns);
                    termination_position_ = position;
                    termination_leadership_term_id_ = leadership_term_id_;
                    cluster_termination_->termination_position(*consensus_publisher_, active_members_, this_member_,
                                                               leadership_term_id_, position);
                    offer_position_and_previous_state(position, state_);
                    state(ConsensusModuleState::snapshot, "ClusterControl.SHUTDOWN");
                }
                return 1;
            }
            case ClusterControlState::abort:
            {
                const std::int64_t position = log_publisher_->position();
                cluster_termination_ = std::make_unique<ClusterTermination>(now_ns + config_.termination_timeout_ns);
                termination_position_ = position;
                termination_leadership_term_id_ = leadership_term_id_;
                cluster_termination_->termination_position(*consensus_publisher_, active_members_, this_member_,
                                                           leadership_term_id_, position);
                if (0 < config_.service_count && nullptr != service_proxy_)
                {
                    service_proxy_->termination_position(termination_position_, true);
                }
                else
                {
                    cluster_termination_->on_services_terminated();
                }
                state(ConsensusModuleState::terminating, "ClusterControl.ABORT");
                return 1;
            }
            default:
                return 0;
        }
    }

    if (ConsensusModuleState::suspended == state_ && ClusterControlState::resume == *toggled_state)
    {
        if (appended_ok(append_action(CLUSTER_ACTION_RESUME, now_ms(), CLUSTER_ACTION_FLAGS_DEFAULT)))
        {
            offer_position_and_previous_state(log_publisher_->position(), state_);
            state(ConsensusModuleState::active, "ClusterControl.RESUME");
            cluster_control_reset(control_toggle_counter_);
        }
        return 1;
    }
    return 0;
}

int ConsensusModuleAgent::check_node_control_toggle() noexcept
{
    const auto toggled_state = node_control_state(node_control_value(node_control_toggle_counter_));
    if (!toggled_state)
    {
        report(toggled_state.error());
        return 0;
    }

    switch (*toggled_state)
    {
        case NodeControlState::replicate_standby_snapshot:
        {
            // Q6 keeps the `StandbySnapshotReplicator` shell: the card's DoD is
            // reset plus a counted error, never a silent no-op.
            node_control_reset(node_control_toggle_counter_);
            report(make_error(ErrorCode::internal_error, "StandbySnapshotReplicator is not transcribed yet (Q6)"));
            return 1;
        }
        case NodeControlState::validate_recording_log:
            return start_recording_log_validation();
        default:
            return 0;
    }
}

int ConsensusModuleAgent::start_recording_log_validation() noexcept
{
    // `checkNodeControlToggle:2710-2722`: create the validator lazily, reset
    // the toggle immediately, and let the slow tick poll it to completion.
    if (nullptr == recording_log_validator_)
    {
        auto connected = connect_validator_archive();
        if (!connected)
        {
            report(connected.error());
            node_control_reset(node_control_toggle_counter_);
            return 1;
        }

        recording_log_validator_ = std::make_unique<RecordingLogValidator>(
            config_.member_id, recording_log_, [this](std::int64_t recording_id, Error& error)
            { return lookup_recording(recording_id, error); }, [this](const Error& error) { report(error); });
    }

    node_control_reset(node_control_toggle_counter_);
    return 1;
}

std::expected<void, Error> ConsensusModuleAgent::connect_validator_archive()
{
    if (nullptr != validator_archive_)
    {
        return {};
    }

    if (0 != aeron_archive_context_init(&validator_archive_context_))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "aeron_archive_context_init failed"));
    }

    // `RecordingLogValidator.newInstance` clones the CM's archive context
    // (`:47-55`): same Aeron client, its own control session.
    (void)aeron_archive_context_set_aeron(validator_archive_context_, client_.handle());
    (void)aeron_archive_context_set_owns_aeron_client(validator_archive_context_, false);
    (void)aeron_archive_context_set_control_request_channel(validator_archive_context_,
                                                            config_.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_request_stream_id(validator_archive_context_,
                                                              config_.archive_local_control_stream_id);
    (void)aeron_archive_context_set_control_response_channel(validator_archive_context_,
                                                             config_.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_response_stream_id(validator_archive_context_,
                                                               config_.archive_control_response_stream_id);
    (void)aeron_archive_context_set_message_timeout_ns(validator_archive_context_, 5'000'000'000ULL);

    if (0 != aeron_archive_connect(&validator_archive_, validator_archive_context_))
    {
        const std::string message = aeron_errmsg();
        (void)aeron_archive_context_close(validator_archive_context_);
        validator_archive_context_ = nullptr;
        return std::unexpected(
            make_error(ErrorCode::protocol_error, "validator archive connection failed: " + message));
    }

    return {};
}

void ConsensusModuleAgent::close_validator_archive() noexcept
{
    if (nullptr != validator_archive_)
    {
        (void)aeron_archive_close(validator_archive_);
        validator_archive_ = nullptr;
    }
    if (nullptr != validator_archive_context_)
    {
        (void)aeron_archive_context_close(validator_archive_context_);
        validator_archive_context_ = nullptr;
    }
}

RecordingLogValidator::Lookup ConsensusModuleAgent::lookup_recording(std::int64_t recording_id, Error& error) noexcept
{
    if (nullptr == validator_archive_)
    {
        error = make_error(ErrorCode::internal_error, "validator archive is not connected");
        return RecordingLogValidator::Lookup::error;
    }

    std::int64_t start_position = NULL_POSITION;
    if (0 == aeron_archive_get_start_position(&start_position, validator_archive_, recording_id))
    {
        return RecordingLogValidator::Lookup::known;
    }

    if (ARCHIVE_CLIENT_ERROR_UNKNOWN_RECORDING == aeron_errcode())
    {
        return RecordingLogValidator::Lookup::unknown;
    }

    error = make_error(ErrorCode::io_error,
                       "get start position for recordingId=" + std::to_string(recording_id) +
                           " failed: " + std::string{aeron_errmsg()},
                       aeron_errcode());
    return RecordingLogValidator::Lookup::error;
}

void ConsensusModuleAgent::unavailable_counter_trampoline(void* clientd, aeron_counters_reader_t* reader,
                                                          std::int64_t registration_id,
                                                          std::int32_t counter_id) noexcept
{
    (void)reader;
    static_cast<ConsensusModuleAgent*>(clientd)->on_unavailable_counter(registration_id, counter_id);
}

void ConsensusModuleAgent::on_unavailable_counter(std::int64_t registration_id, std::int32_t counter_id) noexcept
{
    // `ConsensusModuleAgent.onUnavailableCounter:3284-3300`.
    if (ConsensusModuleState::terminating == state_ || ConsensusModuleState::quitting == state_)
    {
        return;
    }

    for (std::size_t i = 0; i < service_client_ids_.size(); i++)
    {
        if (registration_id == service_client_ids_[i])
        {
            const std::string message = "Aeron client in service closed unexpectedly: serviceId=" + std::to_string(i);
            report(make_error(ErrorCode::internal_error, message));
            state(ConsensusModuleState::closed, message.c_str());
            return;
        }
    }

    // The reference compares the append-position counter's registration id;
    // this implementation remembers the counter id (C4d), so that is what it
    // can compare here.
    if (AERON_NULL_COUNTER_ID != append_position_counter_id_ && counter_id == append_position_counter_id_)
    {
        append_position_counter_id_ = AERON_NULL_COUNTER_ID;
    }
}

void ConsensusModuleAgent::unavailable_image_trampoline(void* clientd, aeron_subscription_t* subscription,
                                                        aeron_image_t* image) noexcept
{
    static_cast<ConsensusModuleAgent*>(clientd)->on_unavailable_ingress_image(subscription, image);
}

void ConsensusModuleAgent::on_unavailable_ingress_image(aeron_subscription_t* subscription,
                                                        aeron_image_t* image) noexcept
{
    // `ConsensusModuleAgent.onUnavailableIngressImage:3273-3282`.
    aeron_image_constants_t image_constants{};
    if (0 != aeron_image_constants(image, &image_constants))
    {
        report(make_error(ErrorCode::internal_error,
                          std::string{"image constants failed: "} + std::string{aeron_errmsg()}));
        return;
    }

    if (ClusterRole::leader == role_ && ConsensusModuleState::active == state_ && nullptr != session_manager_)
    {
        session_manager_->timeout_on_unavailable_image(image_constants.correlation_id);
    }

    bool is_ipc = false;
    aeron_subscription_constants_t subscription_constants{};
    if (0 == aeron_subscription_constants(subscription, &subscription_constants) &&
        nullptr != subscription_constants.channel)
    {
        is_ipc = 0 == std::string_view{subscription_constants.channel}.rfind("aeron:ipc", 0);
    }

    if (nullptr != ingress_adapter_)
    {
        ingress_adapter_->free_session_buffer(image_constants.session_id, is_ipc);
    }
}

void ConsensusModuleAgent::run_termination_hooks(bool expected, const Error& error) noexcept
{
    if (termination_hooks_run_)
    {
        return;
    }
    termination_hooks_run_ = true;

    // `ConsensusModuleAgent.runTerminationHooks:3457-3470`: the extended hook
    // gets the cause, the termination hook takes none; each failure is counted.
    const common::TerminationCause cause{expected, error};
    ::deepmsg::cluster::common::run_termination_hooks(config_.extended_termination_hook, config_.termination_hook,
                                                      cause, [this](const Error& hook_error) { report(hook_error); });
}

void ConsensusModuleAgent::enter_election(bool is_log_end_of_stream, std::string_view reason)
{
    (void)is_log_end_of_stream;
    report(make_error(ErrorCode::internal_error, std::string{"enter election: "} + std::string{reason}));
}

void ConsensusModuleAgent::do_termination(std::int64_t log_position)
{
    if (nullptr == cluster_termination_)
    {
        if (termination_leadership_term_id_ == leadership_term_id_ && nullptr != consensus_publisher_)
        {
            (void)consensus_publisher_->termination_ack(leader_member_.id, leadership_term_id_, log_position,
                                                        config_.member_id);
        }
        (void)recording_log_.commit_log_position(leadership_term_id_, log_position);
        close_and_terminate();
        return;
    }

    if (cluster_termination_->can_terminate(active_members_, now_ns()))
    {
        (void)recording_log_.commit_log_position(leadership_term_id_, log_position);
        close_and_terminate();
    }
}

void ConsensusModuleAgent::close_and_terminate()
{
    try_stop_log_recording();
    state(ConsensusModuleState::closed, "expected termination");
    // `closeAndTerminate:3308-3322` throws `ClusterTerminationException(true)`;
    // `doWork`'s catch runs the hooks before the exception leaves the agent.
    run_termination_hooks(true, Error{});
    // The reference throws `ClusterTerminationException(true)` and the runner
    // then calls `onClose` (`ConsensusModuleAgent.java:3318-3322`), which sends
    // the CLOSED events and signals the mark file. Leave `closed_` for
    // `on_close`, or the sessions never hear about the termination.
}

void ConsensusModuleAgent::terminate_on_service_ack(std::int64_t log_position)
{
    if (nullptr != cluster_termination_)
    {
        cluster_termination_->on_services_terminated();
    }
    do_termination(log_position);
}

void ConsensusModuleAgent::connect_ingress()
{
    // C4b opened the ingress subscription in `open_session_layer`; the
    // reference's `connectIngress` re-adds a destination when the leader
    // changes. Single member: nothing to re-point.
}

void ConsensusModuleAgent::try_stop_log_recording()
{
    if (NULL_VALUE != log_recording_id_ && nullptr != archive_)
    {
        // The publication position is refreshed by the client's conductor, so
        // read it after a `do_work`, not straight after the last commit
        // (`append_cluster_action` commits before the position counter moves).
        (void)client_.do_work();
        const std::int64_t published_position =
            nullptr == log_publisher_ ? 0
                                      : std::max(log_publisher_->position(), log_publisher_->last_append_position());
        // `tryStopLogRecording` (`Agent:3364-3379`): stop by the recording
        // subscription when there is one, identity only as the fallback.
        if (NULL_VALUE != log_recording_subscription_id_)
        {
            (void)aeron_archive_stop_recording_subscription(archive_, log_recording_subscription_id_);
            log_recording_subscription_id_ = NULL_VALUE;
        }
        else
        {
            bool stopped = false;
            (void)aeron_archive_try_stop_recording_by_identity(&stopped, archive_, log_recording_id_);
        }

        // The archive records asynchronously: wait for the RecordingPos counter
        // to catch up with the publication, otherwise the recovery plan's
        // appended position trails a snapshot taken at the commit position.
        const std::int64_t deadline = axc::core::NanoClock::now() + 2'000'000'000;
        while (AERON_NULL_COUNTER_ID != append_position_counter_id_ && recording_position() < published_position &&
               axc::core::NanoClock::now() < deadline)
        {
            (void)client_.do_work();
        }

        log_recording_stop_position_ = recording_position();
        log_recording_id_ = NULL_VALUE;
        append_position_counter_id_ = AERON_NULL_COUNTER_ID;
    }
}

// ---- the snapshot take ----------------------------------------------------

std::expected<void, Error> ConsensusModuleAgent::snapshot_on_service_ack(std::int64_t log_position,
                                                                         std::int64_t timestamp,
                                                                         std::span<const ServiceAck> acks)
{
    if (!are_all_relevant_ids_non_null("failed to take snapshot", acks, [this](const Error& error) { report(error); }))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "failed to take snapshot"));
    }
    auto taken = take_snapshot(timestamp, log_position, acks);
    if (!taken)
    {
        report(taken.error());
    }
    session_manager_->update_time_of_last_activity();

    if (nullptr != cluster_termination_)
    {
        if (0 < config_.service_count && nullptr != service_proxy_)
        {
            service_proxy_->termination_position(termination_position_, true);
        }
        else if (nullptr != cluster_termination_)
        {
            cluster_termination_->on_services_terminated();
        }
        cluster_termination_->deadline_ns(now_ns() + config_.termination_timeout_ns);
        state(ConsensusModuleState::terminating, "null != clusterTermination");
    }
    else
    {
        state(ConsensusModuleState::active, "snapshot complete");
        if (ClusterRole::leader == cluster_role())
        {
            cluster_control_reset(control_toggle_counter_);
        }
    }
    return taken;
}

std::expected<void, Error> ConsensusModuleAgent::take_snapshot(std::int64_t timestamp, std::int64_t log_position,
                                                               std::span<const ServiceAck> acks)
{
    auto add = client_.add_exclusive_publication(config_.snapshot_channel, config_.snapshot_stream_id);
    auto publication = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
    if (!publication)
    {
        return std::unexpected(publication.error());
    }

    aeron_publication_constants_t constants{};
    (void)aeron_exclusive_publication_constants(publication->get(), &constants);
    std::string recording_channel = config_.snapshot_channel;
    recording_channel.push_back(std::string::npos == recording_channel.find('?') ? '?' : '|');
    recording_channel.append("session-id=").append(std::to_string(constants.session_id));

    std::int64_t subscription_id = NULL_VALUE;
    if (0 != aeron_archive_start_recording(&subscription_id, archive_, recording_channel.c_str(),
                                           config_.snapshot_stream_id, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, true))
    {
        return std::unexpected(
            make_error(ErrorCode::internal_error, std::string{"snapshot recording failed: "} + aeron_errmsg()));
    }

    std::int32_t counter_id = AERON_NULL_COUNTER_ID;
    const std::int64_t counter_deadline = axc::core::NanoClock::now() + 10'000'000'000;
    while (AERON_NULL_COUNTER_ID == counter_id)
    {
        (void)client_.do_work();
        counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(client_.counters_reader(),
                                                                               constants.session_id, archive_id_);
        if (AERON_NULL_COUNTER_ID == counter_id && axc::core::NanoClock::now() >= counter_deadline)
        {
            return std::unexpected(make_error(ErrorCode::timed_out, "snapshot recording counter not found"));
        }
    }
    const std::int64_t recording_id =
        aeron_archive_recording_pos_get_recording_id(client_.counters_reader(), counter_id);

    ExclusivePublicationSink sink(client_, publication->get());
    auto taken = snapshot_state(sink, log_position);
    if (!taken)
    {
        return taken;
    }

    // Close the publication exactly once: the `ExPubPtr` deleter would close
    // it again at scope exit.
    aeron_exclusive_publication_t* raw_publication = publication->release();
    (void)aeron_exclusive_publication_close(raw_publication, nullptr, nullptr);

    const common::RecordingLog::Entry* term = recording_log_.find_term_entry(leadership_term_id_);
    const std::int64_t term_base_log_position = nullptr == term ? 0 : term->term_base_log_position;

    for (std::size_t service_id = acks.size(); service_id-- > 0;)
    {
        (void)recording_log_.append_snapshot(acks[service_id].relevant_id, leadership_term_id_, term_base_log_position,
                                             log_position, timestamp, static_cast<std::int32_t>(service_id));
    }
    (void)recording_log_.append_snapshot(recording_id, leadership_term_id_, term_base_log_position, log_position,
                                         timestamp, static_cast<std::int32_t>(NULL_VALUE));
    (void)recording_log_.force(config_.file_sync_level);
    counter_increment(snapshot_counter_);
    return {};
}

std::expected<void, Error> ConsensusModuleAgent::snapshot_state(SnapshotPublicationSink& publication,
                                                                std::int64_t log_position)
{
    ConsensusModuleSnapshotTaker taker(publication, service::TimeUnit::milliseconds, 0);

    auto began = taker.mark_begin(log_position, leadership_term_id_);
    if (!began)
    {
        return began;
    }

    const std::int64_t next_service_session_id =
        pending_service_message_trackers_.empty() ? 0 : pending_service_message_trackers_[0].next_service_session_id();
    const std::int64_t log_service_session_id =
        pending_service_message_trackers_.empty() ? 0 : pending_service_message_trackers_[0].log_service_session_id();
    const std::int32_t pending_capacity = pending_service_message_trackers_.empty()
                                              ? 0
                                              : static_cast<std::int32_t>(pending_service_message_trackers_[0].size());
    auto wrote_state =
        taker.snapshot_consensus_module_state(session_manager_->next_committed_session_id(), next_service_session_id,
                                              log_service_session_id, pending_capacity);
    if (!wrote_state)
    {
        return wrote_state;
    }

    auto sessions = session_manager_->snapshot_sessions(taker);
    if (!sessions)
    {
        return sessions;
    }
    if (nullptr != timer_service_)
    {
        timer_service_->snapshot(taker);
    }
    for (const PendingServiceMessageTracker& tracker : pending_service_message_trackers_)
    {
        auto tracked = taker.snapshot_tracker(tracker, *this);
        if (!tracked)
        {
            return tracked;
        }
    }
    if (auto error = taker.take_error(); error.has_value())
    {
        return std::unexpected(*error);
    }
    return taker.mark_end(log_position, leadership_term_id_);
}

// ---- the log recording ----------------------------------------------------

std::expected<void, Error> ConsensusModuleAgent::start_log_recording(std::string_view channel, std::int32_t stream_id)
{
    const std::int64_t last_recording_id = recording_log_.find_last_term_recording_id();
    std::int64_t subscription_id = NULL_VALUE;
    int recording_result = 0;
    if (NULL_VALUE == last_recording_id)
    {
        recording_result = aeron_archive_start_recording(&subscription_id, archive_, std::string{channel}.c_str(),
                                                         stream_id, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, true);
        if (0 != recording_result)
        {
            return std::unexpected(
                make_error(ErrorCode::internal_error, std::string{"start recording failed: "} + aeron_errmsg()));
        }
    }
    else
    {
        recording_result =
            aeron_archive_extend_recording(&subscription_id, archive_, last_recording_id, std::string{channel}.c_str(),
                                           stream_id, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, true);
        if (0 != recording_result)
        {
            return std::unexpected(
                make_error(ErrorCode::internal_error, std::string{"extend recording failed: "} + aeron_errmsg()));
        }
    }
    log_recording_subscription_id_ = subscription_id;
    return {};
}

std::expected<bool, Error> ConsensusModuleAgent::try_create_append_position(std::int32_t log_session_id)
{
    const std::int32_t counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
        client_.counters_reader(), log_session_id, archive_id_);
    if (AERON_NULL_COUNTER_ID == counter_id)
    {
        return false;
    }
    const std::int64_t recording_id =
        aeron_archive_recording_pos_get_recording_id(client_.counters_reader(), counter_id);
    if (NULL_VALUE == recording_id)
    {
        return false;
    }
    log_recording_id_ = recording_id;
    append_position_counter_id_ = counter_id;
    return true;
}

// ---- the ten inbound consensus templates ---------------------------------

void ConsensusModuleAgent::on_canvass_position(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                               std::int64_t leadership_term_id, std::int32_t follower_member_id,
                                               std::int32_t protocol_version)
{
    (void)protocol_version;
    if (nullptr != election_)
    {
        auto handled = election_->on_canvass_position(log_leadership_term_id, log_position, leadership_term_id,
                                                      follower_member_id, 0);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_request_vote(std::int64_t log_leadership_term_id, std::int64_t log_position,
                                           std::int64_t candidate_term_id, std::int32_t candidate_member_id,
                                           std::int32_t protocol_version)
{
    (void)protocol_version;
    if (nullptr != election_)
    {
        auto handled =
            election_->on_request_vote(log_leadership_term_id, log_position, candidate_term_id, candidate_member_id, 0);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_vote(std::int64_t candidate_term_id, std::int64_t log_leadership_term_id,
                                   std::int64_t log_position, std::int32_t candidate_member_id,
                                   std::int32_t follower_member_id, bool vote)
{
    if (nullptr != election_)
    {
        auto handled = election_->on_vote(log_leadership_term_id, log_position, candidate_term_id, candidate_member_id,
                                          follower_member_id, vote);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_new_leadership_term(
    std::int64_t log_leadership_term_id, std::int64_t next_leadership_term_id, std::int64_t next_term_base_log_position,
    std::int64_t next_log_position, std::int64_t leadership_term_id, std::int64_t term_base_log_position,
    std::int64_t log_position, std::int64_t commit_position, std::int64_t leader_recording_id, std::int64_t timestamp,
    std::int32_t leader_member_id, std::int32_t log_session_id, std::int32_t app_version, bool is_startup)
{
    (void)app_version;
    if (nullptr != election_)
    {
        auto handled = election_->on_new_leadership_term(
            log_leadership_term_id, next_leadership_term_id, next_term_base_log_position, next_log_position,
            leadership_term_id, term_base_log_position, log_position, commit_position, leader_recording_id, timestamp,
            leader_member_id, log_session_id, is_startup);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_append_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                              std::int32_t follower_member_id, std::int16_t flags)
{
    if (nullptr != election_)
    {
        auto handled = election_->on_append_position(leadership_term_id, log_position, follower_member_id, flags);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_commit_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                              std::int32_t leader_member_id)
{
    (void)leader_member_id;
    if (nullptr != election_)
    {
        auto handled = election_->on_commit_position(leadership_term_id, log_position, leader_member_id);
        if (!handled)
        {
            report(handled.error());
        }
    }
}

void ConsensusModuleAgent::on_catchup_position(std::int64_t leadership_term_id, std::int64_t log_position,
                                               std::int32_t follower_member_id, std::string_view catchup_endpoint)
{
    (void)log_position;
    (void)follower_member_id;
    (void)catchup_endpoint;
    report(not_transcribed("onCatchupPosition", "C6"));
}

void ConsensusModuleAgent::on_stop_catchup(std::int64_t leadership_term_id, std::int32_t follower_member_id)
{
    (void)leadership_term_id;
    (void)follower_member_id;
    report(not_transcribed("onStopCatchup", "C6"));
}

void ConsensusModuleAgent::on_termination_position(std::int64_t leadership_term_id, std::int64_t log_position)
{
    if (leadership_term_id == leadership_term_id_ && ClusterRole::follower == cluster_role())
    {
        termination_position_ = log_position;
        termination_leadership_term_id_ = leadership_term_id;
        time_of_last_log_update_ns_ = now_ns();
    }
}

void ConsensusModuleAgent::on_termination_ack(std::int64_t leadership_term_id, std::int64_t log_position,
                                              std::int32_t member_id)
{
    if (leadership_term_id != leadership_term_id_ || log_position < termination_position_ ||
        ClusterRole::leader != cluster_role())
    {
        return;
    }
    const std::int32_t index = find_member_index(member_id);
    if (index >= 0)
    {
        active_members_[static_cast<std::size_t>(index)].has_terminated = true;
    }
    if (nullptr != cluster_termination_ && cluster_termination_->can_terminate(active_members_, now_ns()))
    {
        (void)recording_log_.commit_log_position(leadership_term_id_, termination_position_);
        close_and_terminate();
    }
}

void ConsensusModuleAgent::on_unhandled_template(std::int32_t template_id)
{
    report(make_error(ErrorCode::internal_error,
                      "consensus template " + std::to_string(template_id) + " is not transcribed yet (C9)"));
}

}  // namespace deepmsg::cluster::consensus
