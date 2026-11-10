#include "cluster/consensus/consensus_module_agent.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>

#include <aeronc.h>

#include "cluster/common/cluster_counters.h"
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
[[nodiscard]] std::expected<axc::core::CounterPtr, Error> await_counter(axc::core::DeepmsgClient& client,
                                                                        axc::core::AsyncAdd<axc::core::CounterPtr>& add,
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
    state(ConsensusModuleState::active, "started");
    role(ClusterRole::follower);
    time_of_last_update_ns_ = now_ns();

    started_ = true;
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
        auto counter = await_counter(client_, add, ASYNC_ADD_TIMEOUT_NS);
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
        return await_counter(client_, add, ASYNC_ADD_TIMEOUT_NS);
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

int ConsensusModuleAgent::consensus_work(std::int64_t /*timestamp*/, std::int64_t /*now_ns*/) noexcept
{
    // C4b/C4d transcribe `ConsensusModuleAgent.consensusWork` (`:2499`):
    // ingress, log publication, commit position and the service ack path. The
    // skeleton has no log publication and no sessions yet.
    return 0;
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

void ConsensusModuleAgent::on_close() noexcept
{
    if (!started_ || closed_)
    {
        return;
    }

    closed_ = true;
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
}

}  // namespace deepmsg::cluster::consensus
