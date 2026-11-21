#include "cluster/backup/cluster_backup_agent.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <random>
#include <string>
#include <utility>

#include "cluster/backup/backup_publisher.h"
#include "cluster/backup/channel_uri.h"
#include "cluster/client/client_config.h"
#include "cluster/common/cluster_codecs.h"
#include "cluster/common/cluster_counters.h"
#include "core/time/clock.h"
#include "io_aeron_cluster_codecs/BackupResponse.h"
#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"

// The client's internal header has no `extern "C"` of its own
// (`ConsensusModuleAgent` includes it the same way).
extern "C"
{
#include <client/aeron_archive_async_connect.h>
#include <client/aeron_archive_client.h>
}

namespace deepmsg::cluster::backup
{
namespace
{

namespace codecs = ::io::aeron::cluster::codecs;

constexpr std::string_view VERSION = "1.54.0";
constexpr std::string_view GIT_SHA = "454da1e4e6";
constexpr std::int64_t ASYNC_ADD_TIMEOUT_NS = 5'000'000'000;
/// `ClusterBackupAgent.SLOW_TICK_INTERVAL_MS` (`:88`).
constexpr std::int64_t SLOW_TICK_INTERVAL_MS = 10;
/// `ClusterBackupAgent.MARK_FILE_UPDATE_INTERVAL_MS` (`:86`).
constexpr std::int64_t MARK_FILE_UPDATE_INTERVAL_MS = 1'000;
/// `LIVENESS_TIMEOUT_MS` (`ClusteredServiceContainer.java:213`).
constexpr std::int64_t LIVENESS_TIMEOUT_MS = 10'000;
/// `ConsensusAdapter.FRAGMENT_LIMIT`.
constexpr std::int32_t CONSENSUS_FRAGMENT_LIMIT = 10;
constexpr std::int64_t NULL_VALUE = -1;
constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int64_t NULL_TIMESTAMP = -1;
constexpr std::int32_t NULL_COUNTER_ID = AERON_NULL_COUNTER_ID;

using axc::core::ErrorCode;
using axc::core::make_error;

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
            return std::unexpected(make_error(ErrorCode::no_capacity, "async add timed out"));
        }
    }
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

[[nodiscard]] std::int64_t counter_value(const axc::core::CounterPtr& counter) noexcept
{
    if (!counter)
    {
        return 0;
    }
    std::int64_t* address = aeron_counter_addr(counter.get());
    return nullptr == address ? 0 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

bool counter_propose_max(const axc::core::CounterPtr& counter, std::int64_t value) noexcept
{
    if (!counter)
    {
        return false;
    }
    if (std::int64_t* address = aeron_counter_addr(counter.get()); nullptr != address)
    {
        std::int64_t current = __atomic_load_n(address, __ATOMIC_ACQUIRE);
        while (current < value)
        {
            if (__atomic_compare_exchange_n(address, &current, value, false, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))
            {
                return true;
            }
        }
    }
    return false;
}

[[nodiscard]] std::vector<std::string> split_endpoints(std::string_view value)
{
    std::vector<std::string> endpoints;
    std::size_t start = 0;
    while (start <= value.size())
    {
        const std::size_t comma = value.find(',', start);
        const std::string_view part =
            value.substr(start, std::string_view::npos == comma ? std::string_view::npos : comma - start);
        if (!part.empty())
        {
            endpoints.emplace_back(part);
        }
        if (std::string_view::npos == comma)
        {
            break;
        }
        start = comma + 1;
    }
    return endpoints;
}

[[nodiscard]] std::int32_t random_session_id()
{
    std::random_device device;
    std::mt19937 random{device()};
    return static_cast<std::int32_t>(random());
}

[[nodiscard]] std::expected<void, Error> write_mark_file_link(const ClusterBackupConfig& config)
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

    const std::filesystem::path link =
        std::filesystem::path{config.cluster_dir} / std::string{ClusterBackupConfig::mark_file_link_name()};
    if (cluster_canonical == mark_canonical)
    {
        std::filesystem::remove(link, error);
        return {};
    }

    std::ofstream out{link};
    if (!out)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "could not write the backup mark file link"));
    }
    out << mark_canonical.string();
    return {};
}

}  // namespace

ClusterBackupAgent::ClusterBackupAgent(ClusterBackupConfig config) : config_(std::move(config)) {}

ClusterBackupAgent::~ClusterBackupAgent() noexcept
{
    if (nullptr != consensus_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(consensus_assembler_);
        consensus_assembler_ = nullptr;
    }
}

std::int64_t ClusterBackupAgent::now_ms() const noexcept
{
    return config_.epoch_clock_ms ? config_.epoch_clock_ms() : axc::core::EpochClock::now_ms();
}

std::string_view ClusterBackupAgent::role_name() const noexcept
{
    return backup_thread_name(config_);
}

const common::RecordingLog* ClusterBackupAgent::recording_log() const noexcept
{
    return recording_log_.has_value() ? &*recording_log_ : nullptr;
}

std::expected<void, Error> ClusterBackupAgent::on_start()
{
    const std::int64_t now = now_ms();

    axc::core::ClientConfig client_config{config_.aeron_dir};
    // `ClusterBackupAgent.onUnavailableCounter` (`:394-404`): the live log's
    // `RecordingPos` counter disappearing resets the backup.
    client_config.on_unavailable_counter = &ClusterBackupAgent::unavailable_counter_trampoline;
    client_config.on_unavailable_counter_clientd = this;
    auto connected = axc::core::DeepmsgClient::connect(client_config);
    if (!connected)
    {
        return std::unexpected(connected.error());
    }
    client_ = std::move(*connected);

    auto counters = allocate_counters();
    if (!counters)
    {
        return std::unexpected(counters.error());
    }

    auto mark_file = open_mark_file();
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }

    auto log = open_recording_log();
    if (!log)
    {
        return std::unexpected(log.error());
    }

    auto archive = connect_backup_archive();
    if (!archive)
    {
        return std::unexpected(archive.error());
    }

    auto subscription = open_consensus_subscription();
    if (!subscription)
    {
        return std::unexpected(subscription.error());
    }

    auto endpoints = split_endpoints(config_.consensus_endpoints);
    if (endpoints.empty())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "aeron.cluster.consensus.endpoints must resolve for the backup"));
    }
    consensus_publications_ = std::make_unique<PublicationGroup>(std::move(endpoints), config_.consensus_channel,
                                                                 config_.consensus_stream_id);
    consensus_publications_->shuffle();

    // `ClusterBackupAgent.onStart` (`:194-196`).
    counter_set(state_counter_, backup_state_code(ClusterBackupState::backup_query));
    counter_set(next_query_deadline_ms_counter_, now - 1);
    state_ = ClusterBackupState::backup_query;

    time_of_last_progress_ms_ = now;
    slow_tick_deadline_ms_ = now;
    mark_file_update_deadline_ms_ = now;
    return {};
}

std::expected<void, Error> ClusterBackupAgent::allocate_counters()
{
    {
        auto add =
            common::allocate_versioned(client_, "ClusterBackup Errors", common::CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID,
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

    const Counter counters[] = {{&state_counter_, "ClusterBackup State", common::CLUSTER_BACKUP_STATE_TYPE_ID},
                                {&live_log_position_counter_, "ClusterBackup live log position",
                                 common::CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID},
                                {&next_query_deadline_ms_counter_, "ClusterBackup next query deadline in ms",
                                 common::CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID},
                                {&snapshot_retrieve_counter_, "ClusterBackup snapshots retrieved",
                                 common::CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID}};

    for (const Counter& counter : counters)
    {
        auto allocated = await_label(counter.name, counter.type_id);
        if (!allocated)
        {
            return std::unexpected(allocated.error());
        }
        *counter.field = std::move(*allocated);
    }
    return {};
}

std::expected<void, Error> ClusterBackupAgent::open_mark_file()
{
    common::ClusterMarkFile::Options options;
    options.path = config_.mark_file_path();
    options.component_type = common::ClusterComponentType::backup;
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

    // `ClusterBackup.java:2075-2087`.
    mark_file_.set_archive_stream_id(config_.archive_local_control_stream_id);
    mark_file_.set_service_stream_id(config_.service_stream_id);
    mark_file_.set_consensus_module_stream_id(config_.consensus_module_stream_id);
    mark_file_.set_ingress_stream_id(config_.ingress_stream_id);
    mark_file_.set_member_id(-1);
    mark_file_.set_service_id(-1);
    mark_file_.set_cluster_id(config_.cluster_id);
    mark_file_.set_channels(config_.aeron_dir, {}, {}, {}, {}, {});

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

std::expected<void, Error> ClusterBackupAgent::open_recording_log()
{
    auto log = common::RecordingLog::open(config_.cluster_dir, true);
    if (!log)
    {
        return std::unexpected(log.error());
    }
    recording_log_ = std::move(*log);
    return {};
}

std::expected<void, Error> ClusterBackupAgent::connect_backup_archive()
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
    (void)aeron_archive_context_set_recording_signal_consumer(archive_context_,
                                                              &ClusterBackupAgent::on_recording_signal, this);

    if (0 != aeron_archive_connect(&backup_archive_, archive_context_))
    {
        const std::string message = aeron_errmsg();
        (void)aeron_archive_context_close(archive_context_);
        archive_context_ = nullptr;
        return std::unexpected(make_error(ErrorCode::protocol_error, "backup archive connection failed: " + message));
    }

    backup_archive_id_ = aeron_archive_get_archive_id(backup_archive_);
    return {};
}

std::expected<void, Error> ClusterBackupAgent::open_consensus_subscription()
{
    auto add = client_.add_subscription(config_.consensus_channel, config_.consensus_stream_id);
    auto subscription = await_handle(client_, add, ASYNC_ADD_TIMEOUT_NS);
    if (!subscription)
    {
        return std::unexpected(subscription.error());
    }
    consensus_subscription_ = std::move(*subscription);

    if (0 != aeron_controlled_fragment_assembler_create(&consensus_assembler_, &ClusterBackupAgent::on_fragment, this))
    {
        consensus_assembler_ = nullptr;
        return std::unexpected(make_error(ErrorCode::internal_error, "fragment assembler create failed"));
    }
    return {};
}

int ClusterBackupAgent::do_work() noexcept
{
    if (ClusterBackupState::closed == state_)
    {
        return 0;
    }

    const std::int64_t timestamp_ms = now_ms();
    int work_count = 0;

    const int client_work = client_.do_work();
    if (client_work < 0)
    {
        on_error(make_error(ErrorCode::internal_error, std::string{"aeron client do_work failed: "} + aeron_errmsg()));
    }
    else
    {
        work_count += client_work;
    }

    if (timestamp_ms > slow_tick_deadline_ms_)
    {
        slow_tick_deadline_ms_ = timestamp_ms + SLOW_TICK_INTERVAL_MS;
        work_count += slow_tick(timestamp_ms);
    }

    if (nullptr != consensus_subscription_ && nullptr != consensus_assembler_)
    {
        const int polled = aeron_subscription_controlled_poll(consensus_subscription_.get(),
                                                              aeron_controlled_fragment_assembler_handler,
                                                              consensus_assembler_, CONSENSUS_FRAGMENT_LIMIT);
        if (polled < 0)
        {
            on_error(make_error(ErrorCode::internal_error, "consensus subscription poll failed"));
        }
        else
        {
            work_count += polled;
        }
    }

    switch (state_)
    {
        case ClusterBackupState::backup_query:
            work_count += backup_query_work(timestamp_ms);
            break;
        case ClusterBackupState::snapshot_retrieve:
            work_count += snapshot_retrieve_work(timestamp_ms);
            break;
        case ClusterBackupState::live_log_record:
            work_count += live_log_record_work(timestamp_ms);
            break;
        case ClusterBackupState::live_log_replay:
            work_count += live_log_replay_work(timestamp_ms);
            break;
        case ClusterBackupState::update_recording_log:
            work_count += update_recording_log_work(timestamp_ms);
            break;
        case ClusterBackupState::backing_up:
            work_count += backing_up_work(timestamp_ms);
            break;
        case ClusterBackupState::reset_backup:
            work_count += reset_backup_work(timestamp_ms);
            break;
        case ClusterBackupState::closed:
            return work_count;
    }

    if (has_progress_stalled(timestamp_ms))
    {
        on_error(make_error(ErrorCode::timed_out, "backup progress has stalled"));
        state(ClusterBackupState::reset_backup, timestamp_ms);
    }

    return work_count;
}

/// `ClusterBackupAgent.slowTick` (`:630-657`).
int ClusterBackupAgent::slow_tick(std::int64_t now) noexcept
{
    int work_count = 0;

    if (now >= mark_file_update_deadline_ms_)
    {
        mark_file_update_deadline_ms_ = now + MARK_FILE_UPDATE_INTERVAL_MS;
        mark_file_.update_activity_timestamp(now);
    }

    work_count += poll_backup_archive_events();

    // `ClusterBackupAgent.pollBackupArchiveEvents` (`:1052-1097`) also turns an
    // archive ControlResponse ERROR into a failure: a refused `startRecording`
    // is reported asynchronously.
    if (nullptr != backup_archive_)
    {
        std::array<char, 1024> buffer{};
        if (0 == aeron_archive_poll_for_error_response(backup_archive_, buffer.data(), buffer.size()) &&
            buffer[0] != '\0')
        {
            on_error(make_error(ErrorCode::protocol_error, std::string{"backup archive error: "} + buffer.data()));
            state(ClusterBackupState::reset_backup, now);
        }
    }

    if (NULL_VALUE == correlation_id_ && nullptr != cluster_archive_)
    {
        std::array<char, 1024> buffer{};
        if (0 == aeron_archive_poll_for_error_response(cluster_archive_, buffer.data(), buffer.size()) &&
            buffer[0] != '\0')
        {
            on_error(make_error(ErrorCode::protocol_error, std::string{"cluster archive error: "} + buffer.data()));
            state(ClusterBackupState::reset_backup, now);
        }
        work_count++;
    }
    return work_count;
}

/// `ClusterBackupAgent.backupQuery` (`:679-709`).
int ClusterBackupAgent::backup_query_work(std::int64_t now)
{
    if (nullptr == consensus_publications_->current() ||
        now > (time_of_last_backup_query_ms_ + (config_.response_timeout_ns / 1'000'000)))
    {
        consensus_publications_->next(client_);
        correlation_id_ = NULL_VALUE;
        time_of_last_backup_query_ms_ = now;
        const int polled = consensus_publications_->poll(client_);
        if (polled < 0)
        {
            on_error(make_error(ErrorCode::no_capacity, "consensus publication add failed"));
            return 1;
        }
        return 1 + polled;
    }

    if (NULL_VALUE == correlation_id_ && consensus_publications_->is_connected())
    {
        const std::int64_t correlation_id = client_.next_correlation_id();
        const auto credentials = config_.encoded_credentials();
        const bool offered = BackupPublisher::backup_query(
            consensus_publications_->current(), correlation_id, config_.consensus_stream_id,
            client::PROTOCOL_SEMANTIC_VERSION, std::numeric_limits<std::int64_t>::max(), config_.consensus_channel,
            credentials);
        if (offered)
        {
            time_of_last_backup_query_ms_ = now;
            correlation_id_ = correlation_id;
            return 1;
        }
    }
    const int polled = consensus_publications_->poll(client_);
    if (polled < 0)
    {
        on_error(make_error(ErrorCode::no_capacity, "consensus publication add failed"));
        return 0;
    }
    return polled;
}

/// `ClusterBackupAgent.snapshotRetrieve` (`:711-754`).
int ClusterBackupAgent::snapshot_retrieve_work(std::int64_t now)
{
    int work_count = 0;

    if (nullptr == cluster_archive_)
    {
        if (nullptr == cluster_archive_async_connect_)
        {
            return 0;
        }
        // The poll deletes the async handle only when it finishes (1 or -1);
        // clearing it while pending would strand the connection.
        const int result = aeron_archive_async_connect_poll(&cluster_archive_, cluster_archive_async_connect_);
        if (0 != result)
        {
            cluster_archive_async_connect_ = nullptr;
        }
        return result <= 0 ? 0 : 1;
    }

    if (nullptr == snapshot_replication_)
    {
        std::string replication_channel = channel_with_endpoint(config_.catchup_channel, config_.catchup_endpoint);
        const std::string src_control_channel =
            log_supplier_member_.has_value()
                ? channel_with_endpoint(config_.cluster_archive_control_channel, log_supplier_member_->archive_endpoint)
                : config_.cluster_archive_control_channel;
        snapshot_replication_ = std::make_unique<SnapshotReplication>(
            client_, backup_archive_, backup_archive_id_, config_.cluster_archive_control_stream_id,
            src_control_channel, std::move(replication_channel), config_.replication_progress_timeout_ns,
            config_.replication_progress_interval_ns);
        for (const auto& snapshot : snapshots_to_retrieve_)
        {
            snapshot_replication_->add_snapshot(snapshot);
        }
        work_count++;
    }

    auto polled = snapshot_replication_->poll(now * 1'000'000);
    if (!polled)
    {
        on_error(polled.error());
        state(ClusterBackupState::reset_backup, now);
        return work_count;
    }
    work_count += *polled;
    work_count += poll_backup_archive_events();
    time_of_last_progress_ms_ = now;

    if (snapshot_replication_->is_complete())
    {
        for (const auto& snapshot : snapshot_replication_->snapshots_retrieved())
        {
            snapshots_retrieved_.push_back(snapshot);
        }
        snapshot_replication_.reset();
        state(ClusterBackupState::live_log_record, now);
        work_count++;
    }
    return work_count;
}

/// `ClusterBackupAgent.liveLogRecord` (`:756-816`).
int ClusterBackupAgent::live_log_record_work(std::int64_t now)
{
    if (NULL_VALUE == live_log_recording_subscription_id_)
    {
        if (NULL_VALUE == live_log_recording_session_id_)
        {
            live_log_recording_session_id_ = random_session_id();
        }

        const std::string& catchup_endpoint = config_.catchup_endpoint;
        if (catchup_endpoint.size() >= 2 && catchup_endpoint.compare(catchup_endpoint.size() - 2, 2, ":0") == 0)
        {
            if (nullptr == recording_subscription_.get())
            {
                std::string channel = channel_without_param(config_.catchup_channel, "endpoint");
                channel = channel_with_param(channel, "tags",
                                             std::to_string(client_.next_correlation_id()) + "," +
                                                 std::to_string(client_.next_correlation_id()));
                channel = channel_with_param(channel, "session-id", std::to_string(live_log_recording_session_id_));
                recording_channel_ = channel;

                auto add =
                    client_.add_subscription(channel_with_endpoint(channel, catchup_endpoint), config_.log_stream_id);
                auto subscription = add.poll();
                if (!subscription || !subscription->has_value())
                {
                    return 0;
                }
                recording_subscription_ = std::move(**subscription);
                time_of_last_progress_ms_ = now;
                return 1;
            }

            std::array<char, 512> resolved{};
            if (0 !=
                aeron_subscription_resolved_endpoint(recording_subscription_.get(), resolved.data(), resolved.size()))
            {
                return 0;
            }
            replay_channel_ = channel_with_param(channel_with_endpoint(config_.catchup_channel, resolved.data()),
                                                 "session-id", std::to_string(live_log_recording_session_id_));
        }
        else
        {
            replay_channel_ = channel_with_param(channel_with_endpoint(config_.catchup_channel, catchup_endpoint),
                                                 "session-id", std::to_string(live_log_recording_session_id_));
            recording_channel_ = replay_channel_;
        }

        live_log_recording_subscription_id_ = start_log_recording();
        if (NULL_VALUE == live_log_recording_subscription_id_)
        {
            state(ClusterBackupState::reset_backup, now);
            return 1;
        }
    }

    time_of_last_progress_ms_ = now;
    state(ClusterBackupState::live_log_replay, now);
    return 1;
}

/// `ClusterBackupAgent.liveLogReplay` (`:818-907`).
int ClusterBackupAgent::live_log_replay_work(std::int64_t now)
{
    int work_count = 0;

    if (NULL_VALUE == live_log_recording_id_)
    {
        if (nullptr == cluster_archive_)
        {
            if (nullptr == cluster_archive_async_connect_)
            {
                return 0;
            }
            const int result = aeron_archive_async_connect_poll(&cluster_archive_, cluster_archive_async_connect_);
            if (0 != result)
            {
                cluster_archive_async_connect_ = nullptr;
            }
            return result <= 0 ? 0 : 1;
        }

        if (NULL_VALUE == live_log_replay_session_id_ && NULL_VALUE == correlation_id_)
        {
            const std::int64_t start_position = replay_start_position();

            aeron_archive_replay_params_t params{};
            if (0 != aeron_archive_replay_params_init(&params))
            {
                return 0;
            }
            params.position = start_position;
            params.length = -1;
            params.bounding_limit_counter_id = leader_commit_position_counter_id_;

            // The C client's `aeron_archive_start_replay` is synchronous: it
            // sends the request and polls the control response until the
            // archive answers, returning the replay session id
            // (`aeron_archive_client.c:1360-1425`, the same shape as
            // `AeronArchive.startReplay`). The lower 32 bits are the image
            // session id the RecordingPos counter is keyed by.
            std::int64_t replay_session_id = NULL_VALUE;
            if (0 == aeron_archive_start_replay(&replay_session_id, cluster_archive_, cluster_log_recording_id_,
                                                replay_channel_.c_str(), config_.log_stream_id, &params))
            {
                replay_channel_.clear();
                live_log_replay_session_id_ = replay_session_id;
                time_of_last_progress_ms_ = now;
                work_count++;
            }
            else
            {
                on_error(
                    make_error(ErrorCode::protocol_error, std::string{"live log replay failed: "} + aeron_errmsg()));
                state(ClusterBackupState::reset_backup, now);
                return work_count;
            }
        }
        else if (NULL_COUNTER_ID == live_log_recording_counter_id_)
        {
            aeron_counters_reader_t* reader = client_.counters_reader();
            live_log_recording_counter_id_ = aeron_archive_recording_pos_find_counter_id_by_session_id(
                reader, static_cast<std::int32_t>(live_log_replay_session_id_), backup_archive_id_);
            if (NULL_COUNTER_ID != live_log_recording_counter_id_)
            {
                if (std::int64_t* address = aeron_counters_reader_addr(reader, live_log_recording_counter_id_);
                    nullptr != address)
                {
                    counter_set(live_log_position_counter_, __atomic_load_n(address, __ATOMIC_ACQUIRE));
                }
                live_log_recording_id_ =
                    aeron_archive_recording_pos_get_recording_id(reader, live_log_recording_counter_id_);
                time_of_last_backup_query_ms_ = now;
                time_of_last_progress_ms_ = now;
                state(ClusterBackupState::update_recording_log, now);
                work_count++;
            }
        }
    }
    else
    {
        time_of_last_progress_ms_ = now;
        state(ClusterBackupState::update_recording_log, now);
    }
    return work_count;
}

/// `ClusterBackupAgent.updateRecordingLog` (`:916-996`).
int ClusterBackupAgent::update_recording_log_work(std::int64_t now)
{
    bool was_updated = false;

    const std::int64_t snapshot_leadership_term_id =
        snapshots_retrieved_.empty() ? NULL_VALUE : snapshots_retrieved_.front().leadership_term_id;

    if (leader_log_entry_.has_value() && recording_log_->is_unknown(leader_log_entry_->leadership_term_id) &&
        leader_log_entry_->leadership_term_id <= snapshot_leadership_term_id)
    {
        auto appended =
            recording_log_->append_term(live_log_recording_id_, leader_log_entry_->leadership_term_id,
                                        leader_log_entry_->term_base_log_position, leader_log_entry_->timestamp);
        if (!appended)
        {
            on_error(appended.error());
            state(ClusterBackupState::reset_backup, now);
            return 1;
        }
        was_updated = true;
        leader_log_entry_.reset();
    }

    if (!snapshots_retrieved_.empty())
    {
        for (auto it = snapshots_retrieved_.rbegin(); it != snapshots_retrieved_.rend(); ++it)
        {
            auto appended =
                recording_log_->append_snapshot(it->recording_id, it->leadership_term_id, it->term_base_log_position,
                                                it->log_position, it->timestamp, it->service_id);
            if (!appended)
            {
                on_error(appended.error());
                state(ClusterBackupState::reset_backup, now);
                return 1;
            }
        }
        was_updated = true;
    }

    if (leader_last_term_entry_.has_value() && recording_log_->is_unknown(leader_last_term_entry_->leadership_term_id))
    {
        auto appended = recording_log_->append_term(live_log_recording_id_, leader_last_term_entry_->leadership_term_id,
                                                    leader_last_term_entry_->term_base_log_position,
                                                    leader_last_term_entry_->timestamp);
        if (!appended)
        {
            on_error(appended.error());
            state(ClusterBackupState::reset_backup, now);
            return 1;
        }
        was_updated = true;
        leader_last_term_entry_.reset();
    }

    if (was_updated)
    {
        (void)recording_log_->force(2);
        // `onUpdatedRecordingLog` (`ClusterBackupAgent.java:982-985`).
        if (nullptr != config_.events_listener)
        {
            config_.events_listener->on_updated_recording_log(*recording_log_, snapshots_retrieved_);
        }
        if (!snapshots_retrieved_.empty() && snapshot_retrieve_counter_)
        {
            if (std::int64_t* address = aeron_counter_addr(snapshot_retrieve_counter_.get()); nullptr != address)
            {
                __atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
            }
        }
    }

    snapshots_retrieved_.clear();
    snapshots_to_retrieve_.clear();

    time_of_last_progress_ms_ = now;
    counter_set(next_query_deadline_ms_counter_, now + (config_.backup_interval_ns / 1'000'000));
    state(ClusterBackupState::backing_up, now);
    return 1;
}

/// `ClusterBackupAgent.backingUp` (`:998-1026`).
int ClusterBackupAgent::backing_up_work(std::int64_t now)
{
    int work_count = 0;

    if (now > counter_value(next_query_deadline_ms_counter_))
    {
        time_of_last_backup_query_ms_ = now;
        time_of_last_progress_ms_ = now;
        state(ClusterBackupState::backup_query, now);
        work_count++;
    }

    if (NULL_COUNTER_ID != live_log_recording_counter_id_)
    {
        if (std::int64_t* address =
                aeron_counters_reader_addr(client_.counters_reader(), live_log_recording_counter_id_);
            nullptr != address)
        {
            const std::int64_t live_position = __atomic_load_n(address, __ATOMIC_ACQUIRE);
            if (counter_propose_max(live_log_position_counter_, live_position))
            {
                // `onLiveLogProgress` (`ClusterBackupAgent.java:1016-1020`).
                if (nullptr != config_.events_listener)
                {
                    config_.events_listener->on_live_log_progress(live_log_recording_id_,
                                                                  live_log_recording_counter_id_, live_position);
                }
                work_count++;
            }
        }
    }
    return work_count;
}

/// `ClusterBackupAgent.resetBackup` (`:659-677`).
int ClusterBackupAgent::reset_backup_work(std::int64_t now)
{
    time_of_last_progress_ms_ = now;

    if (NULL_VALUE == cool_down_deadline_ms_)
    {
        cool_down_deadline_ms_ = now + (config_.cooldown_interval_ns / 1'000'000);
        reset();
        return 1;
    }
    if (now > cool_down_deadline_ms_)
    {
        cool_down_deadline_ms_ = NULL_VALUE;
        state(ClusterBackupState::backup_query, now);
        return 1;
    }
    return 0;
}

bool ClusterBackupAgent::has_progress_stalled(std::int64_t now) const noexcept
{
    return NULL_COUNTER_ID == live_log_recording_counter_id_ &&
           now > (time_of_last_progress_ms_ + (config_.progress_timeout_ns / 1'000'000));
}

bool ClusterBackupAgent::source_acceptable(std::int32_t leader_member_id, std::int32_t member_id) const noexcept
{
    switch (config_.source_type)
    {
        case BackupSourceType::leader:
            return NULL_VALUE != leader_member_id && leader_member_id == member_id;
        case BackupSourceType::follower:
            return NULL_VALUE == leader_member_id || leader_member_id != member_id;
        case BackupSourceType::any:
            return true;
    }
    return false;
}

namespace
{

std::int64_t archive_stop_position_thunk(void* context, std::int64_t recording_id) noexcept
{
    auto* archive = static_cast<aeron_archive_t*>(context);
    std::int64_t stop_position = NULL_POSITION;
    if (0 == aeron_archive_get_stop_position(&stop_position, archive, recording_id))
    {
        return stop_position;
    }
    return NULL_POSITION;
}

}  // namespace

std::int64_t replay_start_position(const common::RecordingLog::Entry* last_term,
                                   std::span<const common::RecordingLog::Snapshot> snapshots,
                                   BackupReplayStart initial_replay_start, StopPositionFn stop_position_of,
                                   void* context) noexcept
{
    if (nullptr != last_term)
    {
        if (nullptr == stop_position_of)
        {
            return NULL_POSITION;
        }
        return stop_position_of(context, last_term->recording_id);
    }

    if (BackupReplayStart::beginning == initial_replay_start)
    {
        return NULL_POSITION;
    }

    std::int64_t position = NULL_POSITION;
    for (const auto& snapshot : snapshots)
    {
        if (0 == snapshot.service_id && snapshot.log_position > position)
        {
            position = snapshot.log_position;
        }
    }
    return position;
}

/// `ClusterBackupAgent.replayStartPosition` (`:1120-1154`).
std::int64_t ClusterBackupAgent::replay_start_position()
{
    return backup::replay_start_position(recording_log_->find_last_term(), snapshots_retrieved_,
                                         config_.initial_replay_start, &archive_stop_position_thunk, backup_archive_);
}

void ClusterBackupAgent::reset()
{
    cluster_members_.clear();
    log_supplier_member_.reset();
    leader_log_entry_.reset();
    leader_last_term_entry_.reset();
    live_log_recording_counter_id_ = NULL_COUNTER_ID;
    live_log_recording_id_ = NULL_VALUE;
    live_log_recording_session_id_ = NULL_VALUE;

    snapshots_to_retrieve_.clear();
    snapshots_retrieved_.clear();

    snapshot_replication_.reset();
    stop_recording();
    stop_replay();

    consensus_publications_->close();
    consensus_publications_->shuffle();
    if (nullptr != cluster_archive_async_connect_)
    {
        aeron_archive_async_connect_delete(cluster_archive_async_connect_);
        cluster_archive_async_connect_ = nullptr;
    }
    if (nullptr != cluster_archive_)
    {
        (void)aeron_archive_close(cluster_archive_);
        cluster_archive_ = nullptr;
    }
    if (nullptr != cluster_archive_context_)
    {
        (void)aeron_archive_context_close(cluster_archive_context_);
        cluster_archive_context_ = nullptr;
    }
    recording_subscription_.reset();
}

void ClusterBackupAgent::stop_replay() noexcept
{
    if (NULL_VALUE != live_log_replay_session_id_)
    {
        if (nullptr != cluster_archive_)
        {
            (void)aeron_archive_stop_replay(cluster_archive_, live_log_replay_session_id_);
        }
        live_log_replay_session_id_ = NULL_VALUE;
    }
}

void ClusterBackupAgent::stop_recording() noexcept
{
    if (NULL_VALUE != live_log_recording_subscription_id_)
    {
        if (nullptr != backup_archive_)
        {
            bool stopped = false;
            (void)aeron_archive_try_stop_recording_subscription(&stopped, backup_archive_,
                                                                live_log_recording_subscription_id_);
        }
        live_log_recording_subscription_id_ = NULL_VALUE;
    }
}

/// `ClusterBackupAgent.startLogRecording` (`:1099-1112`).
std::int64_t ClusterBackupAgent::start_log_recording() noexcept
{
    const auto* log_entry = recording_log_->find_last_term();
    const std::int32_t stream_id = config_.log_stream_id;

    int64_t subscription_id = NULL_VALUE;
    const int result =
        nullptr == log_entry
            ? aeron_archive_start_recording(&subscription_id, backup_archive_, recording_channel_.c_str(), stream_id,
                                            AERON_ARCHIVE_SOURCE_LOCATION_REMOTE, true)
            : aeron_archive_extend_recording(&subscription_id, backup_archive_, log_entry->recording_id,
                                             recording_channel_.c_str(), stream_id,
                                             AERON_ARCHIVE_SOURCE_LOCATION_REMOTE, true);
    recording_channel_.clear();
    if (0 != result)
    {
        on_error(make_error(ErrorCode::protocol_error, std::string{"start log recording failed: "} + aeron_errmsg()));
        return NULL_VALUE;
    }
    return subscription_id;
}

/// `ClusterBackupAgent.pollBackupArchiveEvents` (`:1052-1097`).
int ClusterBackupAgent::poll_backup_archive_events() noexcept
{
    int work_count = 0;
    if (nullptr != backup_archive_)
    {
        std::int32_t signals = 0;
        if (0 == aeron_archive_poll_for_recording_signals(&signals, backup_archive_))
        {
            work_count += signals;
        }
        else
        {
            on_error(make_error(ErrorCode::protocol_error, std::string{"recording signals failed: "} + aeron_errmsg()));
        }
    }
    return work_count;
}

void ClusterBackupAgent::on_recording_signal(aeron_archive_recording_signal_t* signal, void* clientd) noexcept
{
    auto* self = static_cast<ClusterBackupAgent*>(clientd);
    if (nullptr == signal || nullptr == self)
    {
        return;
    }
    auto* poller =
        nullptr != self->backup_archive_ ? aeron_archive_control_response_poller(self->backup_archive_) : nullptr;
    const std::int64_t correlation_id = nullptr != poller ? poller->correlation_id : NULL_VALUE;
    self->handle_recording_signal(signal->recording_signal_code, correlation_id, signal->recording_id,
                                  signal->position);
}

void ClusterBackupAgent::handle_recording_signal(std::int32_t signal, std::int64_t correlation_id,
                                                 std::int64_t recording_id, std::int64_t position)
{
    if (nullptr != snapshot_replication_)
    {
        snapshot_replication_->on_signal(correlation_id, recording_id, position, signal);
    }
}

/// `ClusterBackupAgent.onFragment` (`:406-460`).
aeron_controlled_fragment_handler_action_t ClusterBackupAgent::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                           std::size_t length, aeron_header_t*)
{
    auto* agent = static_cast<ClusterBackupAgent*>(clientd);
    return agent->dispatch(std::span<const std::uint8_t>(buffer, length)) ? AERON_ACTION_CONTINUE : AERON_ACTION_ABORT;
}

bool ClusterBackupAgent::dispatch(std::span<const std::uint8_t> message)
{
    auto header = common::message_header(message);
    if (!header)
    {
        on_error(header.error());
        return false;
    }

    switch (header->templateId())
    {
        case codecs::BackupResponse::SBE_TEMPLATE_ID:
            on_backup_response(message);
            break;
        case codecs::Challenge::SBE_TEMPLATE_ID:
            on_challenge(message);
            break;
        case codecs::SessionEvent::SBE_TEMPLATE_ID:
            on_session_event(message);
            break;
        default:
            break;
    }
    return true;
}

/// `ClusterBackupAgent.onBackupResponse` (`:462-599`).
void ClusterBackupAgent::on_backup_response(std::span<const std::uint8_t> message)
{
    auto header = common::message_header(message);
    if (!header)
    {
        return;
    }
    auto decoder = common::wrap_body<codecs::BackupResponse>(message, *header);

    const std::int32_t member_id = codecs::BackupResponse::memberIdNullValue() != decoder.memberId()
                                       ? decoder.memberId()
                                       : static_cast<std::int32_t>(NULL_VALUE);

    if (NULL_VALUE == member_id)
    {
        on_error(make_error(ErrorCode::protocol_error,
                            "onBackupResponse(): memberId is null, retrying for compatible node"));
        state(ClusterBackupState::reset_backup, now_ms());
        return;
    }
    if (!source_acceptable(decoder.leaderMemberId(), member_id))
    {
        consensus_publications_->close_and_exclude_current();
        state(ClusterBackupState::reset_backup, now_ms());
        return;
    }
    if (log_supplier_member_.has_value() && log_supplier_member_->id != member_id)
    {
        state(ClusterBackupState::reset_backup, now_ms());
        return;
    }

    if (ClusterBackupState::backup_query != state_ || decoder.correlationId() != correlation_id_)
    {
        return;
    }

    auto snapshots = decoder.snapshots();
    for (std::uint64_t i = 0; i < snapshots.count(); ++i)
    {
        snapshots.next();
        const auto* latest = recording_log_->get_latest_snapshot(snapshots.serviceId());
        if (nullptr != latest && snapshots.logPosition() == latest->log_position)
        {
            continue;
        }
        snapshots_to_retrieve_.push_back(common::RecordingLog::Snapshot{
            snapshots.recordingId(), snapshots.leadershipTermId(), snapshots.termBaseLogPosition(),
            snapshots.logPosition(), snapshots.timestamp(), snapshots.serviceId()});
    }

    if (!log_supplier_member_.has_value() || member_id != log_supplier_member_->id ||
        decoder.logRecordingId() != cluster_log_recording_id_)
    {
        cluster_log_recording_id_ = decoder.logRecordingId();
        common::RecordingLog::Entry entry{};
        entry.recording_id = decoder.logRecordingId();
        entry.leadership_term_id = decoder.logLeadershipTermId();
        entry.term_base_log_position = decoder.logTermBaseLogPosition();
        entry.log_position = NULL_POSITION;
        entry.timestamp = NULL_TIMESTAMP;
        entry.service_id = static_cast<std::int32_t>(NULL_VALUE);
        entry.type = common::RecordingLog::ENTRY_TYPE_TERM;
        entry.is_valid = true;
        entry.entry_index = -1;
        leader_log_entry_ = entry;
    }

    const auto* last_term = recording_log_->find_last_term();
    if (nullptr == last_term || decoder.lastLeadershipTermId() != last_term->leadership_term_id)
    {
        common::RecordingLog::Entry entry{};
        entry.recording_id = decoder.logRecordingId();
        entry.leadership_term_id = decoder.lastLeadershipTermId();
        entry.term_base_log_position = decoder.lastTermBaseLogPosition();
        entry.log_position = NULL_POSITION;
        entry.timestamp = NULL_TIMESTAMP;
        entry.service_id = static_cast<std::int32_t>(NULL_VALUE);
        entry.type = common::RecordingLog::ENTRY_TYPE_TERM;
        entry.is_valid = true;
        entry.entry_index = -1;
        leader_last_term_entry_ = entry;
    }

    time_of_last_backup_query_ms_ = 0;
    correlation_id_ = NULL_VALUE;
    leader_commit_position_counter_id_ = decoder.commitPositionCounterId();

    auto members = common::parse_cluster_members(decoder.clusterMembers());
    if (!members)
    {
        on_error(members.error());
        state(ClusterBackupState::reset_backup, now_ms());
        return;
    }
    cluster_members_ = std::move(*members);
    for (auto& member : cluster_members_)
    {
        member.is_leader = member.id == decoder.leaderMemberId();
    }

    common::ClusterMember* supplier = common::find_member(cluster_members_, member_id);
    if (nullptr == supplier)
    {
        on_error(
            make_error(ErrorCode::protocol_error, std::to_string(member_id) + " not found in the cluster members"));
        state(ClusterBackupState::reset_backup, now_ms());
        return;
    }
    log_supplier_member_ = *supplier;
    log_supplier_member_->leadership_term_id = decoder.logLeadershipTermId();

    // `onBackupResponse` (`ClusterBackupAgent.java:569-572`).
    if (nullptr != config_.events_listener)
    {
        config_.events_listener->on_backup_response(cluster_members_, *log_supplier_member_, snapshots_to_retrieve_);
    }

    if (nullptr == cluster_archive_ && nullptr == cluster_archive_async_connect_)
    {
        if (0 != aeron_archive_context_init(&cluster_archive_context_))
        {
            on_error(make_error(ErrorCode::internal_error, "cluster archive context init failed"));
            state(ClusterBackupState::reset_backup, now_ms());
            return;
        }
        (void)aeron_archive_context_set_aeron(cluster_archive_context_, client_.handle());
        (void)aeron_archive_context_set_owns_aeron_client(cluster_archive_context_, false);
        (void)aeron_archive_context_set_message_timeout_ns(cluster_archive_context_, 5'000'000'000ULL);

        const std::string request_channel =
            channel_with_endpoint(config_.cluster_archive_control_channel, supplier->archive_endpoint);
        (void)aeron_archive_context_set_control_request_channel(cluster_archive_context_, request_channel.c_str());
        (void)aeron_archive_context_set_control_request_stream_id(cluster_archive_context_,
                                                                  config_.cluster_archive_control_stream_id);

        std::string response_channel = config_.cluster_archive_control_response_channel.empty()
                                           ? config_.cluster_archive_control_channel
                                           : config_.cluster_archive_control_response_channel;
        if (supplier->archive_response_endpoint.has_value())
        {
            response_channel = channel_without_param(response_channel, "endpoint");
            response_channel = channel_with_param(response_channel, "control-mode", "response");
            response_channel =
                channel_with_param(response_channel, "mdc-control", *supplier->archive_response_endpoint);
        }
        (void)aeron_archive_context_set_control_response_channel(cluster_archive_context_, response_channel.c_str());
        (void)aeron_archive_context_set_control_response_stream_id(cluster_archive_context_,
                                                                   config_.archive_control_response_stream_id);

        if (0 != aeron_archive_async_connect(&cluster_archive_async_connect_, cluster_archive_context_))
        {
            on_error(make_error(ErrorCode::protocol_error,
                                std::string{"cluster archive async connect failed: "} + aeron_errmsg()));
            state(ClusterBackupState::reset_backup, now_ms());
            return;
        }
    }

    time_of_last_progress_ms_ = now_ms();
    state(snapshots_to_retrieve_.empty() ? ClusterBackupState::live_log_record : ClusterBackupState::snapshot_retrieve,
          now_ms());
}

/// `ClusterBackupAgent.onChallenge` (`:601-608`).
void ClusterBackupAgent::on_challenge(std::span<const std::uint8_t> message)
{
    auto header = common::message_header(message);
    if (!header)
    {
        return;
    }
    auto decoder = common::wrap_body<codecs::Challenge>(message, *header);

    std::vector<std::uint8_t> challenge(decoder.encodedChallengeLength());
    if (!challenge.empty())
    {
        decoder.getEncodedChallenge(reinterpret_cast<char*>(challenge.data()), challenge.size());
    }

    const auto response = config_.on_challenge(challenge);
    correlation_id_ = client_.next_correlation_id();
    (void)BackupPublisher::challenge_response(consensus_publications_->current(), correlation_id_,
                                              decoder.clusterSessionId(), response);
}

/// `ClusterBackupAgent.onSessionEvent` (`:610-628`).
void ClusterBackupAgent::on_session_event(std::span<const std::uint8_t> message)
{
    auto header = common::message_header(message);
    if (!header)
    {
        return;
    }
    auto decoder = common::wrap_body<codecs::SessionEvent>(message, *header);

    if (correlation_id_ != decoder.correlationId())
    {
        return;
    }
    switch (decoder.code())
    {
        case codecs::EventCode::ERROR:
        case codecs::EventCode::AUTHENTICATION_REJECTED:
            on_error(
                make_error(ErrorCode::protocol_error, std::string{"backup session rejected: "} + decoder.detail()));
            state(ClusterBackupState::reset_backup, now_ms());
            break;
        case codecs::EventCode::REDIRECT:
            consensus_publications_->close_and_exclude_current();
            state(ClusterBackupState::reset_backup, now_ms());
            break;
        default:
            break;
    }
}

void ClusterBackupAgent::state(ClusterBackupState new_state, std::int64_t /*now_ms*/)
{
    // `state(...)` (`ClusterBackupAgent.java:1028-1040`): entering BACKUP_QUERY
    // is a listener event (`ClusterBackupEventsListener.onBackupQuery`).
    if (ClusterBackupState::backup_query == new_state && nullptr != config_.events_listener)
    {
        config_.events_listener->on_backup_query();
    }
    if (state_counter_)
    {
        counter_set(state_counter_, backup_state_code(new_state));
    }
    state_ = new_state;
    correlation_id_ = NULL_VALUE;
}

void ClusterBackupAgent::on_close() noexcept
{
    if (ClusterBackupState::closed == state_)
    {
        return;
    }

    state(ClusterBackupState::closed, now_ms());

    stop_replay();
    stop_recording();

    snapshot_replication_.reset();
    consensus_publications_.reset();
    consensus_subscription_.reset();
    if (nullptr != consensus_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(consensus_assembler_);
        consensus_assembler_ = nullptr;
    }
    if (nullptr != cluster_archive_async_connect_)
    {
        aeron_archive_async_connect_delete(cluster_archive_async_connect_);
        cluster_archive_async_connect_ = nullptr;
    }
    if (nullptr != cluster_archive_)
    {
        (void)aeron_archive_close(cluster_archive_);
        cluster_archive_ = nullptr;
    }
    if (nullptr != cluster_archive_context_)
    {
        (void)aeron_archive_context_close(cluster_archive_context_);
        cluster_archive_context_ = nullptr;
    }
    if (nullptr != backup_archive_)
    {
        (void)aeron_archive_close(backup_archive_);
        backup_archive_ = nullptr;
    }
    if (nullptr != archive_context_)
    {
        (void)aeron_archive_context_close(archive_context_);
        archive_context_ = nullptr;
    }

    recording_log_.reset();

    if (mark_file_open_)
    {
        (void)mark_file_.signal_terminated();
        mark_file_open_ = false;
    }
    error_counter_.reset();
    state_counter_.reset();
    live_log_position_counter_.reset();
    next_query_deadline_ms_counter_.reset();
    snapshot_retrieve_counter_.reset();
    client_.close();
}

void ClusterBackupAgent::unavailable_counter_trampoline(void* clientd, aeron_counters_reader_t* /*reader*/,
                                                        std::int64_t registration_id, std::int32_t counter_id) noexcept
{
    static_cast<ClusterBackupAgent*>(clientd)->on_unavailable_counter(registration_id, counter_id);
}

void ClusterBackupAgent::on_unavailable_counter(std::int64_t /*registration_id*/, std::int32_t counter_id)
{
    if (NULL_COUNTER_ID == live_log_recording_counter_id_ || counter_id != live_log_recording_counter_id_)
    {
        return;
    }
    on_error(make_error(ErrorCode::internal_error, "log recording counter unexpectedly unavailable"));
    state(ClusterBackupState::reset_backup, now_ms());
}

void ClusterBackupAgent::on_error(const Error& error) noexcept
{
    std::fprintf(stderr, "deepmsg-cluster-backup: %s\n", std::string{error.message.view()}.c_str());
    // `onPossibleFailure` (`ClusterBackupAgent.java:293`, `:307`, `:399`). Every
    // error the agent reports is a possible failure; the reference names three
    // sites, this one reports them all.
    if (nullptr != config_.events_listener)
    {
        config_.events_listener->on_possible_failure(error);
    }
    if (error_counter_)
    {
        std::int64_t* address = aeron_counter_addr(error_counter_.get());
        if (nullptr != address)
        {
            __atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
        }
    }
}

}  // namespace deepmsg::cluster::backup
