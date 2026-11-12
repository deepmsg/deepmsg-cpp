#include "cluster/consensus/consensus_module_agent.h"

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
#include "cluster/consensus/standby_startup_check.h"
#include "core/io/fs.h"
#include "core/time/clock.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;

constexpr std::string_view VERSION = "1.54.0";
constexpr std::string_view GIT_SHA = "454da1e4e6";
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

}  // namespace

ConsensusModuleAgent::ConsensusModuleAgent(ConsensusModuleConfig config) : config_(std::move(config)) {}

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

    // C4d brings `recoverFromSnapshotAndLog` and the Election; until then the
    // reference's `election.doWork` / ACTIVE transition is the end of onStart.
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

    state(ConsensusModuleState::active, "started");
    role(ClusterRole::follower);
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

    auto ingress_add = client_.add_subscription(config_.ingress_channel, config_.ingress_stream_id);
    auto ingress = await_handle(client_, ingress_add, ASYNC_ADD_TIMEOUT_NS);
    if (!ingress)
    {
        return std::unexpected(ingress.error());
    }
    ingress_subscription_ = std::move(*ingress);

    if (config_.ingress_ipc_allowed)
    {
        auto ipc_add = client_.add_subscription("aeron:ipc", config_.ingress_stream_id);
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
        session_config, client_, *log_publisher_, *egress_publisher_, authenticator_, authorisation_service_,
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

    // The reference chooses between `election.doWork` and `consensusWork`
    // (`:426-437`); the election is C4d, so the archive-connected node runs
    // `consensusWork` today.
    work_count += consensus_work(timestamp_ms, now);

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
        if (state_ == ConsensusModuleState::active)
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

    const std::int64_t leader_append_position = log_publisher_->position();
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
    (void)leadership_term_id;
    (void)cluster_session_id;
    (void)correlation_id;
    report(make_error(ErrorCode::internal_error,
                      "admin request type=" + std::to_string(request_type) + " is not transcribed yet (C4e)"));
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

    // The reference records the log through the archive and waits for the
    // `RecordingPos` counter (`startLogRecording` / `tryCreateAppendPosition`,
    // C4d); the configured channel and the publication's own position carry
    // C4c.
    auto joined = service_proxy_->join_log(log_position, std::numeric_limits<std::int64_t>::max(), config_.member_id,
                                           log_session_id, config_.log_stream_id, is_startup, ClusterRole::leader,
                                           config_.log_channel, false);
    if (!joined)
    {
        return joined;
    }

    expected_ack_position_ = log_position;
    return {};
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

std::expected<void, Error> ConsensusModuleAgent::on_service_ack(std::int64_t log_position, std::int64_t /*timestamp*/,
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
        case ConsensusModuleState::quitting:
        case ConsensusModuleState::terminating:
            // `snapshotOnServiceAck` / `closeAndTerminate` /
            // `terminateOnServiceAck` are C4d/C4e; a state that needs them
            // must not be silent here.
            report(make_error(ErrorCode::internal_error, std::string{"service ack in state "} +
                                                             std::string{state_name(state_)} +
                                                             " is not transcribed yet (C4d/C4e)"));
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

    log_publisher_.reset();

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

}  // namespace deepmsg::cluster::consensus
