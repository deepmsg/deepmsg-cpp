#include "cluster/backup/cluster_backup_agent.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include "cluster/common/cluster_counters.h"
#include "core/time/clock.h"

namespace deepmsg::cluster::backup
{
namespace
{

constexpr std::string_view VERSION = "1.54.0";
constexpr std::string_view GIT_SHA = "454da1e4e6";
constexpr std::int64_t ASYNC_ADD_TIMEOUT_NS = 5'000'000'000;
/// `ClusterBackupAgent.SLOW_TICK_INTERVAL_MS` (`:88`).
constexpr std::int64_t SLOW_TICK_INTERVAL_MS = 10;
/// `ClusterBackupAgent.MARK_FILE_UPDATE_INTERVAL_MS` (`:86`).
constexpr std::int64_t MARK_FILE_UPDATE_INTERVAL_MS = 1'000;
/// `LIVENESS_TIMEOUT_MS` (`ClusteredServiceContainer.java:213`).
constexpr std::int64_t LIVENESS_TIMEOUT_MS = 10'000;

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
            return std::unexpected(make_error(ErrorCode::no_capacity, "async counter add timed out"));
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

/// `ClusterBackup.Context.concludeMarkFile` (`ClusterBackup.java:2071-2089`):
/// the fields a backup writes into its own mark file.
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
    if (mark_file_open_)
    {
        mark_file_open_ = false;
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

std::expected<void, Error> ClusterBackupAgent::on_start()
{
    const std::int64_t now = now_ms();

    auto connected = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{config_.aeron_dir});
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

    // `ClusterBackupAgent.onStart` (`:194-196`): a query is due immediately.
    counter_set(state_counter_, backup_state_code(ClusterBackupState::backup_query));
    counter_set(next_query_deadline_ms_counter_, now - 1);
    state_ = ClusterBackupState::backup_query;

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
        on_slow_tick(timestamp_ms);
        work_count += 1;
    }

    return work_count;
}

/// `ClusterBackupAgent.slowTick` (`:630-657`) for S2: drive the client,
/// refresh the mark file's activity timestamp.
void ClusterBackupAgent::on_slow_tick(std::int64_t now) noexcept
{
    if (now >= mark_file_update_deadline_ms_)
    {
        mark_file_update_deadline_ms_ = now + MARK_FILE_UPDATE_INTERVAL_MS;
        mark_file_.update_activity_timestamp(now);
    }
}

void ClusterBackupAgent::state(ClusterBackupState new_state) noexcept
{
    if (state_counter_)
    {
        counter_set(state_counter_, backup_state_code(new_state));
    }
    state_ = new_state;
}

void ClusterBackupAgent::on_close() noexcept
{
    if (ClusterBackupState::closed == state_)
    {
        return;
    }

    state(ClusterBackupState::closed);
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

void ClusterBackupAgent::on_error(const Error& error) noexcept
{
    std::fprintf(stderr, "deepmsg-cluster-backup: %s\n", std::string{error.message.view()}.c_str());
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
