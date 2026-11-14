#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <aeronc.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/cluster_mark_file.h"
#include "cluster/service/client_util.h"
#include "cluster/service/cluster.h"
#include "cluster/service/clustered_service.h"
#include "cluster/service/consensus_module_proxy.h"
#include "cluster/service/container_config.h"
#include "cluster/service/container_context.h"
#include "cluster/service/log_adapter.h"
#include "cluster/service/role.h"
#include "cluster/service/service_adapter.h"
#include "cluster/service/session.h"
#include "cluster/service/snapshot.h"
extern "C"
{
#include "concurrent/aeron_distinct_error_log.h"
}
#include "core/agent/idle_strategy.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"
#include "core/io/fs.h"
#include "core/time/clock.h"
#include "core/version.h"
#include "io_aeron_cluster_codecs/ClusterAction.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// The container agent: `launch`, the duty cycle, the log-to-business
/// dispatch, and the S2 direction to the consensus module.
///
/// Counterparts: `ClusteredServiceAgent.java` (the agent), `BoundedLogAdapter`,
/// `ServiceAdapter`, `ConsensusModuleProxy`, `Cluster`, `ClientSession`.
template <ClusteredService S>
class ClusteredServiceAgent
{
public:
    ClusteredServiceAgent(ServiceContainerConfig config, S service)
        : config_(std::move(config)),
          service_(std::move(service)),
          cm_proxy_(config_.service_id),
          log_adapter_(config_.log_fragment_limit)
    {
    }

    ClusteredServiceAgent(const ClusteredServiceAgent&) = delete;
    ClusteredServiceAgent& operator=(const ClusteredServiceAgent&) = delete;
    ClusteredServiceAgent(ClusteredServiceAgent&&) = delete;
    ClusteredServiceAgent& operator=(ClusteredServiceAgent&&) = delete;
    ~ClusteredServiceAgent() = default;

    [[nodiscard]] std::string_view role_name() const noexcept
    {
        return config_.service_name;
    }

    [[nodiscard]] const ServiceContainerConfig& config() const noexcept
    {
        return config_;
    }

    [[nodiscard]] S& service() noexcept
    {
        return service_;
    }

    [[nodiscard]] bool is_service_active() const noexcept
    {
        return is_service_active_;
    }

    [[nodiscard]] bool is_terminated() const noexcept
    {
        return terminated_;
    }

    [[nodiscard]] const std::optional<Error>& termination_error() const noexcept
    {
        return termination_error_;
    }

    void request_shutdown() noexcept
    {
        if (context_.has_value())
        {
            context_->request_shutdown();
        }
    }

    /// The reference's `ClusteredServiceContainer.Context.conclude` + `onStart`.
    [[nodiscard]] std::expected<void, Error> on_start();

    int do_work() noexcept;

    void on_close() noexcept;

private:
    // -- start-up ---------------------------------------------------------
    struct RecoveryKey
    {
        std::int64_t leadership_term_id{-1};
        std::int64_t log_position{-1};
        std::int64_t timestamp{0};
        std::int32_t cluster_id{0};
        std::int32_t service_count{0};
        std::int64_t snapshot_recording_id{-1};
    };

    [[nodiscard]] std::expected<common::ClusterMarkFile, Error> create_mark_file();
    [[nodiscard]] std::expected<void, Error> allocate_counters();
    [[nodiscard]] std::expected<CommitPosition, Error> await_commit_position_counter();
    [[nodiscard]] std::expected<RecoveryKey, Error> await_recovery_key();
    [[nodiscard]] std::expected<void, Error> recover_state();
    [[nodiscard]] std::expected<bool, Error> send_ack(std::int64_t position, std::int64_t timestamp,
                                                      std::int64_t relevant_id);
    class EventHandler;
    class SessionLoadHandler;

    // -- duty cycle -------------------------------------------------------
    [[nodiscard]] std::expected<int, Error> do_work_inner();
    [[nodiscard]] std::expected<std::size_t, Error> poll_service_adapter();
    [[nodiscard]] std::expected<std::size_t, Error> poll_log();
    [[nodiscard]] std::expected<std::int64_t, Error> commit_position_value();
    [[nodiscard]] std::expected<void, Error> join_active_log(const JoinLogEvent& event);
    [[nodiscard]] aeron_image_t* await_image(aeron_subscription_t* subscription, std::int32_t session_id);
    [[nodiscard]] Error terminate(bool expected, bool ack_required);
    void close_log();
    void disconnect_egress() noexcept;
    [[nodiscard]] std::expected<void, Error> set_role(Role new_role);
    [[nodiscard]] std::expected<void, Error> invoke_background_work(std::int64_t now_ns);

    struct SnapshotAction
    {
        std::int64_t log_position{-1};
        std::int64_t leadership_term_id{-1};
    };

    struct LoadedSnapshot
    {
        axc::core::SubPtr subscription{};
        aeron_image_t* image{nullptr};
        TimeUnit time_unit{TimeUnit::milliseconds};
    };

    [[nodiscard]] std::expected<LoadedSnapshot, Error> load_snapshot(std::int64_t recording_id);
    [[nodiscard]] std::expected<std::int64_t, Error> take_snapshot(std::int64_t log_position,
                                                                   std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> execute_snapshot_action(const SnapshotAction& action);

    void report(const Error& error) noexcept;
    void terminate_with(const Error& error) noexcept;

    [[nodiscard]] Cluster make_cluster() noexcept;
    [[nodiscard]] std::expected<void, Error> check_for_valid_invocation() const;

    // -- members (declaration order is the destruction order) -------------
    ServiceContainerConfig config_;
    S service_;
    std::optional<ContainerContext> context_{};
    std::optional<axc::core::DeepmsgClient> client_{};
    std::optional<ServiceAdapter> service_adapter_{};
    ConsensusModuleProxy cm_proxy_;
    BoundedLogAdapter log_adapter_;
    SessionTable sessions_{};

    axc::core::PubPtr cm_publication_{};
    axc::core::SubPtr service_subscription_{};
    axc::core::SubPtr log_subscription_{};
    axc::core::CounterPtr error_counter_{};
    axc::core::CounterPtr max_cycle_time_{};
    axc::core::CounterPtr cycle_threshold_exceeded_{};
    axc::core::CounterPtr max_snapshot_duration_{};
    axc::core::CounterPtr snapshot_threshold_exceeded_{};

    aeron_distinct_error_log_t error_log_{};
    bool error_log_initialised_{false};

    std::optional<JoinLogEvent> active_log_event_{};
    std::optional<SnapshotAction> pending_snapshot_{};
    std::int64_t snapshot_start_ns_{0};
    std::int64_t termination_position_{-1};
    bool termination_ack_required_{true};
    std::int64_t requested_ack_position_{-1};
    std::int64_t ack_id_{0};
    std::int64_t log_position_{-1};
    std::int64_t cluster_time_{0};
    TimeUnit time_unit_{TimeUnit::milliseconds};
    Role role_{Role::follower};
    std::int32_t member_id_{-1};
    bool is_service_active_{false};
    std::int32_t standby_snapshot_flags_{CLUSTER_ACTION_FLAGS_DEFAULT};
    std::int64_t last_cycle_ns_{0};
    bool terminated_{false};
    bool termination_expected_{false};
    bool termination_hooks_run_{false};
    std::optional<Error> termination_error_{};
};

}  // namespace deepmsg::cluster::service

// ---------------------------------------------------------------------------
// Implementation (template; header-only so every service instantiates inline).
// ---------------------------------------------------------------------------

namespace deepmsg::cluster::service
{

namespace detail
{

[[nodiscard]] inline std::int64_t get_le64(const std::uint8_t* data) noexcept
{
    std::int64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

[[nodiscard]] inline std::int32_t get_le32(const std::uint8_t* data) noexcept
{
    std::int32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

/// `new ChannelUriStringBuilder(channel).alias(alias).build()`: replace an
/// existing `alias` parameter, or append one.
[[nodiscard]] inline std::string with_aliased_channel(std::string_view channel, std::string_view alias)
{
    const std::size_t pipe = channel.find('|');
    const std::string base{channel.substr(0, pipe)};
    std::string result = base;

    bool replaced = false;
    if (std::string::npos != pipe)
    {
        std::size_t start = pipe + 1;
        while (start <= channel.size())
        {
            const std::size_t end = channel.find('|', start);
            const std::string_view parameter =
                channel.substr(start, std::string::npos == end ? std::string::npos : end - start);
            result.push_back('|');
            if (0 == parameter.rfind("alias=", 0))
            {
                result.append("alias=");
                result.append(alias);
                replaced = true;
            }
            else
            {
                result.append(parameter);
            }
            if (std::string::npos == end)
            {
                break;
            }
            start = end + 1;
        }
    }

    if (!replaced)
    {
        result.push_back('|');
        result.append("alias=");
        result.append(alias);
    }
    return result;
}

[[nodiscard]] inline std::string format_duration_ns(std::int64_t duration_ns)
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

struct RecoveryCounterScan
{
    std::int32_t cluster_id{0};
    std::int32_t found{AERON_NULL_COUNTER_ID};
};

inline void on_recovery_counter(std::int64_t /*value*/, std::int32_t id, std::int32_t type_id, const std::uint8_t* key,
                                std::size_t key_length, const char* /*label*/, std::size_t /*label_length*/,
                                void* clientd)
{
    auto* scan = static_cast<RecoveryCounterScan*>(clientd);
    if (AERON_NULL_COUNTER_ID != scan->found || type_id != common::CLUSTER_RECOVERY_STATE_TYPE_ID || key_length < 28)
    {
        return;
    }

    std::int32_t cluster_id = 0;
    std::memcpy(&cluster_id, key + 24, sizeof(cluster_id));
    if (cluster_id == scan->cluster_id)
    {
        scan->found = id;
    }
}

[[nodiscard]] inline std::int32_t find_recovery_counter(aeron_counters_reader_t* reader,
                                                        std::int32_t cluster_id) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }
    RecoveryCounterScan scan{cluster_id};
    aeron_counters_reader_foreach_counter(reader, &on_recovery_counter, &scan);
    return scan.found;
}

[[nodiscard]] inline bool should_snapshot(std::int32_t flags, std::int32_t standby_snapshot_flags) noexcept
{
    return CLUSTER_ACTION_FLAGS_DEFAULT == flags || 0 != (flags & standby_snapshot_flags);
}

}  // namespace detail

template <ClusteredService S>
std::expected<common::ClusterMarkFile, Error> ClusteredServiceAgent<S>::create_mark_file()
{
    auto cluster_dir = axc::core::create_directories(config_.cluster_dir);
    if (!cluster_dir)
    {
        return std::unexpected(cluster_dir.error());
    }

    const std::string mark_dir = config_.mark_file_dir.value_or(config_.cluster_dir);
    auto mark_dir_created = axc::core::create_directories(mark_dir);
    if (!mark_dir_created)
    {
        return std::unexpected(mark_dir_created.error());
    }

    common::ClusterMarkFile::Options options;
    options.path = config_.mark_file_path();
    options.component_type = common::ClusterComponentType::container;
    options.error_buffer_length = config_.error_buffer_length;
    options.timeout_ms = 1000;
    options.file_page_size = 4096;
    options.now_ms = axc::core::EpochClock::now_ms();

    auto mark_file = common::ClusterMarkFile::create_or_open(options);
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }

    mark_file->set_archive_stream_id(10);
    mark_file->set_service_stream_id(config_.service_stream_id);
    mark_file->set_consensus_module_stream_id(config_.consensus_module_stream_id);
    mark_file->set_ingress_stream_id(-1);
    mark_file->set_service_id(config_.service_id);
    mark_file->set_cluster_id(config_.cluster_id);
    mark_file->set_member_id(-1);

    std::error_code error;
    const std::filesystem::path cluster_canonical = std::filesystem::weakly_canonical(config_.cluster_dir, error);
    const std::filesystem::path mark_canonical = std::filesystem::weakly_canonical(mark_dir, error);
    const std::filesystem::path link = std::filesystem::path{config_.cluster_dir} / config_.mark_file_link_name();
    if (cluster_canonical == mark_canonical)
    {
        std::filesystem::remove(link, error);
    }
    else
    {
        std::ofstream out{link};
        if (!out)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "could not write the container mark file link"));
        }
        out << mark_canonical.string();
    }

    auto ready = mark_file->signal_ready(options.now_ms);
    if (!ready)
    {
        return std::unexpected(ready.error());
    }
    return mark_file;
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::allocate_counters()
{
    const auto version = std::string_view{axc::core::version_string()};
    constexpr std::string_view COMMIT = "deepmsg-cpp";

    {
        auto add =
            common::allocate_service_error_counter(*client_, config_.cluster_id, config_.service_id, version, COMMIT);
        auto counter = await_async(*client_, add, 5'000'000'000);
        if (!counter)
        {
            return std::unexpected(counter.error());
        }
        error_counter_ = std::move(*counter);
    }

    const auto await_label = [this](std::string&& name,
                                    std::int32_t type_id) -> std::expected<axc::core::CounterPtr, Error>
    {
        auto add = common::allocate_service_counter(*client_, name, type_id, config_.cluster_id, config_.service_id);
        return await_async(*client_, add, 5'000'000'000);
    };

    auto max_cycle =
        await_label("Cluster container max cycle time in ns", common::CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID);
    if (!max_cycle)
    {
        return std::unexpected(max_cycle.error());
    }
    max_cycle_time_ = std::move(*max_cycle);

    auto exceeded = await_label("Cluster container work cycle time exceeded count: threshold=" +
                                    detail::format_duration_ns(config_.cycle_threshold_ns),
                                common::CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID);
    if (!exceeded)
    {
        return std::unexpected(exceeded.error());
    }
    cycle_threshold_exceeded_ = std::move(*exceeded);

    auto max_snapshot = await_label("Clustered service max snapshot duration in ns",
                                    common::CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID);
    if (!max_snapshot)
    {
        return std::unexpected(max_snapshot.error());
    }
    max_snapshot_duration_ = std::move(*max_snapshot);

    auto snapshot_exceeded = await_label("Clustered service max snapshot duration exceeded count: threshold=" +
                                             detail::format_duration_ns(config_.snapshot_duration_threshold_ns),
                                         common::CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID);
    if (!snapshot_exceeded)
    {
        return std::unexpected(snapshot_exceeded.error());
    }
    snapshot_threshold_exceeded_ = std::move(*snapshot_exceeded);

    return {};
}

template <ClusteredService S>
std::expected<CommitPosition, Error> ClusteredServiceAgent<S>::await_commit_position_counter()
{
    context_->reset_idle();
    while (true)
    {
        auto* reader = client_->counters_reader();
        const std::int32_t counter_id =
            common::find_cluster_counter(reader, common::CLUSTER_COMMIT_POSITION_TYPE_ID, config_.cluster_id);
        if (AERON_NULL_COUNTER_ID != counter_id)
        {
            std::int64_t registration_id = 0;
            if (aeron_counters_reader_counter_registration_id(reader, counter_id, &registration_id) >= 0)
            {
                return CommitPosition{counter_id, registration_id};
            }
        }

        auto idled = context_->idle(*client_, 0);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
    }
}

template <ClusteredService S>
std::expected<typename ClusteredServiceAgent<S>::RecoveryKey, Error> ClusteredServiceAgent<S>::await_recovery_key()
{
    context_->reset_idle();
    while (true)
    {
        auto* reader = client_->counters_reader();
        const std::int32_t counter_id = detail::find_recovery_counter(reader, config_.cluster_id);
        if (AERON_NULL_COUNTER_ID != counter_id)
        {
            std::uint8_t* key = nullptr;
            if (aeron_counters_reader_metadata_key(reader, counter_id, &key) >= 0 && nullptr != key)
            {
                RecoveryKey recovery;
                recovery.leadership_term_id = detail::get_le64(key);
                recovery.log_position = detail::get_le64(key + 8);
                recovery.timestamp = detail::get_le64(key + 16);
                recovery.cluster_id = detail::get_le32(key + 24);
                recovery.service_count = detail::get_le32(key + 28);
                if (config_.service_id >= 0 && config_.service_id < recovery.service_count)
                {
                    recovery.snapshot_recording_id =
                        detail::get_le64(key + 32 + 8 * static_cast<std::size_t>(config_.service_id));
                }
                return recovery;
            }
        }

        auto idled = context_->idle(*client_, 0);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
    }
}

template <ClusteredService S>
std::expected<bool, Error> ClusteredServiceAgent<S>::send_ack(std::int64_t position, std::int64_t timestamp,
                                                              std::int64_t relevant_id)
{
    const std::int64_t ack_id = ack_id_++;
    while (true)
    {
        auto sent = cm_proxy_.ack(position, timestamp, ack_id, relevant_id);
        if (!sent)
        {
            return std::unexpected(sent.error());
        }
        if (*sent)
        {
            return true;
        }
        auto idled = context_->idle(*client_, 0);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
    }
}

template <ClusteredService S>
class ClusteredServiceAgent<S>::SessionLoadHandler final : public SnapshotLoadHandler
{
public:
    explicit SessionLoadHandler(SessionTable& sessions) : sessions_(sessions) {}

    std::expected<void, Error> on_load_begin_snapshot(std::int32_t, TimeUnit) override
    {
        return {};
    }

    std::expected<void, Error> on_load_cluster_session(std::int64_t cluster_session_id, std::int32_t response_stream_id,
                                                       std::string_view response_channel,
                                                       std::span<const std::uint8_t> encoded_principal) override
    {
        sessions_.add(
            ContainerClientSession{cluster_session_id, response_stream_id, std::string{response_channel},
                                   std::vector<std::uint8_t>(encoded_principal.begin(), encoded_principal.end())});
        return {};
    }

    std::expected<void, Error> on_load_end_snapshot() override
    {
        return {};
    }

private:
    SessionTable& sessions_;
};

template <ClusteredService S>
std::expected<typename ClusteredServiceAgent<S>::LoadedSnapshot, Error> ClusteredServiceAgent<S>::load_snapshot(
    std::int64_t recording_id)
{
    auto archive = ArchiveClient::connect(*client_, config_);
    if (!archive)
    {
        return std::unexpected(archive.error());
    }

    // The subscription is created **before** the replay request and named by
    // its registration id in the request. The C client's `aeron_archive_replay`
    // creates it after the replay has started, which can join a publication
    // that is already publishing and lose the beginning of a small snapshot
    // (deepmsg Rust track, P3-2b).
    axc::core::SubPtr subscription{};
    {
        auto add = client_->add_subscription(config_.replay_channel, config_.replay_stream_id);
        auto added = await_async(*client_, add, 5'000'000'000);
        if (!added)
        {
            return std::unexpected(added.error());
        }
        subscription = std::move(*added);
    }

    aeron_subscription_constants_t subscription_constants{};
    if (0 != aeron_subscription_constants(subscription.get(), &subscription_constants))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "replay subscription constants failed"));
    }

    auto replay_session = archive->start_replay(recording_id, config_.replay_channel, config_.replay_stream_id,
                                                subscription_constants.registration_id);
    if (!replay_session)
    {
        return std::unexpected(replay_session.error());
    }

    aeron_image_t* image = nullptr;
    {
        const auto session_id = static_cast<std::int32_t>(*replay_session);
        while (nullptr == image)
        {
            (void)client_->do_work();
            image = aeron_subscription_image_by_session_id(subscription.get(), session_id);
            if (nullptr == image)
            {
                auto checked = archive->check_error();
                if (!checked)
                {
                    return std::unexpected(checked.error());
                }
                auto idled = context_->idle(*client_, 0);
                if (!idled)
                {
                    return std::unexpected(idled.error());
                }
            }
        }
    }

    ServiceSnapshotLoader loader;
    SessionLoadHandler handler{sessions_};
    while (!loader.is_done())
    {
        auto work = loader.poll(image, handler);
        if (!work)
        {
            (void)aeron_subscription_image_release(subscription.get(), image);
            return std::unexpected(work.error());
        }

        if (0 == *work)
        {
            auto checked = archive->check_error();
            if (!checked)
            {
                (void)aeron_subscription_image_release(subscription.get(), image);
                return std::unexpected(checked.error());
            }
            if (aeron_image_is_closed(image))
            {
                (void)aeron_subscription_image_release(subscription.get(), image);
                return std::unexpected(axc::core::make_error(
                    axc::core::ErrorCode::protocol_error,
                    "snapshot recordingId=" + std::to_string(recording_id) + " ended unexpectedly"));
            }
        }

        auto idled = context_->idle(*client_, *work);
        if (!idled)
        {
            (void)aeron_subscription_image_release(subscription.get(), image);
            return std::unexpected(idled.error());
        }
    }

    constexpr auto MAJOR = [](std::int32_t version) { return (version >> 16) & 0xFF; };
    if (MAJOR(loader.app_version()) != MAJOR(config_.app_version))
    {
        (void)aeron_subscription_image_release(subscription.get(), image);
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                  "incompatible app version: container=" + std::to_string(config_.app_version) +
                                      " snapshot=" + std::to_string(loader.app_version())));
    }

    LoadedSnapshot loaded;
    loaded.subscription = std::move(subscription);
    loaded.image = image;
    loaded.time_unit = loader.time_unit();
    return loaded;
}

template <ClusteredService S>
std::expected<std::int64_t, Error> ClusteredServiceAgent<S>::take_snapshot(std::int64_t log_position,
                                                                           std::int64_t leadership_term_id)
{
    auto archive = ArchiveClient::connect(*client_, config_);
    if (!archive)
    {
        return std::unexpected(archive.error());
    }

    axc::core::ExPubPtr publication{};
    {
        auto add = client_->add_exclusive_publication(config_.snapshot_channel, config_.snapshot_stream_id);
        auto added = await_async(*client_, add, 5'000'000'000);
        if (!added)
        {
            return std::unexpected(added.error());
        }
        publication = std::move(*added);
    }

    SnapshotPublication writer{*client_, *context_, publication.get()};
    const std::int32_t session_id = writer.session_id();
    const std::string channel = detail::with_session_id(config_.snapshot_channel, session_id);

    auto recording = archive->start_recording(channel, config_.snapshot_stream_id);
    if (!recording)
    {
        return std::unexpected(recording.error());
    }

    const std::int64_t deadline = axc::core::NanoClock::now() + 30'000'000'000;
    std::int32_t counter_id = AERON_NULL_COUNTER_ID;
    while (AERON_NULL_COUNTER_ID == counter_id)
    {
        counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(client_->counters_reader(), session_id,
                                                                               archive->archive_id());
        if (AERON_NULL_COUNTER_ID == counter_id)
        {
            auto checked = archive->check_error();
            if (!checked)
            {
                return std::unexpected(checked.error());
            }
            if (axc::core::NanoClock::now() >= deadline)
            {
                return std::unexpected(axc::core::make_error(
                    axc::core::ErrorCode::internal_error, "the snapshot recording's position counter never appeared"));
            }
            auto idled = context_->idle(*client_, 0);
            if (!idled)
            {
                return std::unexpected(idled.error());
            }
        }
    }

    const std::int64_t recording_id =
        aeron_archive_recording_pos_get_recording_id(client_->counters_reader(), counter_id);

    ServiceSnapshotTaker taker{writer, time_unit_, config_.app_version};
    auto begun = taker.mark_begin(log_position, leadership_term_id);
    if (!begun)
    {
        return std::unexpected(begun.error());
    }
    for (ContainerClientSession& session : sessions_.sessions())
    {
        auto snapped = taker.snapshot_session(session);
        if (!snapped)
        {
            return std::unexpected(snapped.error());
        }
    }
    auto ended = taker.mark_end(log_position, leadership_term_id);
    if (!ended)
    {
        return std::unexpected(ended.error());
    }

    (void)context_->check_for_clock_tick(*client_, axc::core::NanoClock::now());
    auto checked = archive->check_error();
    if (!checked)
    {
        return std::unexpected(checked.error());
    }

    auto taken = service_.on_take_snapshot(writer);
    if (!taken)
    {
        return std::unexpected(taken.error());
    }

    const std::int64_t position = writer.position();
    while (true)
    {
        std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), counter_id);
        const std::int64_t value = nullptr == address ? 0 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
        if (value >= position)
        {
            break;
        }

        bool active = false;
        if (0 != aeron_archive_recording_pos_is_active(&active, client_->counters_reader(), counter_id, recording_id) ||
            !active)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                      "recording stopped unexpectedly: " + std::to_string(recording_id)));
        }

        auto idled = context_->idle(*client_, 0);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
        auto idle_checked = archive->check_error();
        if (!idle_checked)
        {
            return std::unexpected(idle_checked.error());
        }
    }

    publication.reset();
    return recording_id;
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::execute_snapshot_action(const SnapshotAction& action)
{
    snapshot_start_ns_ = axc::core::NanoClock::now();
    std::expected<std::int64_t, Error> taken = take_snapshot(action.log_position, action.leadership_term_id);

    const std::int64_t duration_ns = axc::core::NanoClock::now() - snapshot_start_ns_;
    if (max_snapshot_duration_)
    {
        if (std::int64_t* address = aeron_counter_addr(max_snapshot_duration_.get()); nullptr != address)
        {
            std::int64_t current = __atomic_load_n(address, __ATOMIC_ACQUIRE);
            while (duration_ns > current && !__atomic_compare_exchange_n(address, &current, duration_ns, false,
                                                                         __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            {
            }
        }
    }
    if (snapshot_threshold_exceeded_ && duration_ns > config_.snapshot_duration_threshold_ns)
    {
        if (std::int64_t* address = aeron_counter_addr(snapshot_threshold_exceeded_.get()); nullptr != address)
        {
            (void)__atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
        }
    }

    const std::int64_t relevant_id = taken.has_value() ? *taken : -1;
    if (!taken)
    {
        report(taken.error());
    }

    auto acked = send_ack(action.log_position, cluster_time_, relevant_id);
    if (!acked)
    {
        return std::unexpected(acked.error());
    }

    if (!taken)
    {
        return std::unexpected(taken.error());
    }
    return {};
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::recover_state()
{
    auto recovery = await_recovery_key();
    if (!recovery)
    {
        return std::unexpected(recovery.error());
    }

    log_position_ = recovery->log_position;
    cluster_time_ = recovery->timestamp;
    cm_proxy_.set_leadership_term_id(recovery->leadership_term_id);

    std::optional<LoadedSnapshot> loaded;
    std::expected<void, Error> result{};
    if (-1 != recovery->leadership_term_id && -1 != recovery->snapshot_recording_id)
    {
        auto snapshot = load_snapshot(recovery->snapshot_recording_id);
        if (!snapshot)
        {
            result = std::unexpected(snapshot.error());
        }
        else
        {
            time_unit_ = snapshot->time_unit;
            loaded = std::move(*snapshot);
        }
    }

    if (result.has_value())
    {
        context_->set_lifecycle(LifecycleCallback::on_start);
        Cluster cluster = make_cluster();
        if (loaded.has_value())
        {
            SnapshotImage image{loaded->image};
            result = service_.on_start(cluster, &image);
        }
        else
        {
            result = service_.on_start(cluster, nullptr);
        }
        context_->set_lifecycle(LifecycleCallback::none);
    }

    if (loaded.has_value())
    {
        (void)aeron_subscription_image_release(loaded->subscription.get(), loaded->image);
        loaded->subscription.reset();
    }

    const std::int64_t relevant_id = result.has_value() ? aeron_client_id(client_->handle()) : -1;
    auto acked = send_ack(log_position_, cluster_time_, relevant_id);
    if (!acked)
    {
        return std::unexpected(acked.error());
    }

    return result;
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::on_start()
{
    auto mark_file = create_mark_file();
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }

    auto idle = axc::core::IdleStrategy::by_agrona_name(config_.idle_strategy);
    if (!idle)
    {
        (void)mark_file->signal_failed_start();
        return std::unexpected(idle.error());
    }

    context_.emplace(*idle, config_.idle_strategy, std::move(*mark_file));
    context_->set_termination_hooks(config_.extended_termination_hook, config_.termination_hook);

    axc::core::ClientConfig client_config;
    client_config.dir = config_.aeron_dir;
    client_config.use_conductor_agent_invoker = true;
    // `ClusteredServiceContainer.Context.serviceName()`: the heartbeat
    // counter's label names the container (`shouldSetClientName`).
    client_config.client_name = config_.service_name;
    auto client = axc::core::DeepmsgClient::connect(client_config);
    if (!client)
    {
        (void)context_->mark_file().signal_failed_start();
        return std::unexpected(client.error());
    }
    client_.emplace(std::move(*client));

    auto buffer = context_->mark_file().error_buffer();
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
        (void)context_->mark_file().signal_failed_start();
        return std::unexpected(counters.error());
    }

    {
        auto add = client_->add_publication(config_.control_channel, config_.consensus_module_stream_id);
        auto publication = await_async(*client_, add, 5'000'000'000);
        if (!publication)
        {
            (void)context_->mark_file().signal_failed_start();
            return std::unexpected(publication.error());
        }
        cm_publication_ = std::move(*publication);
        cm_proxy_.set_publication(cm_publication_.get());
    }

    {
        auto add = client_->add_subscription(config_.control_channel, config_.service_stream_id);
        auto subscription = await_async(*client_, add, 5'000'000'000);
        if (!subscription)
        {
            (void)context_->mark_file().signal_failed_start();
            return std::unexpected(subscription.error());
        }
        service_subscription_ = std::move(*subscription);
        service_adapter_.emplace(service_subscription_.get());
    }

    auto commit = await_commit_position_counter();
    if (!commit)
    {
        (void)context_->mark_file().signal_failed_start();
        return std::unexpected(commit.error());
    }
    context_->set_commit_position(commit->counter_id, commit->registration_id);

    auto recovered = recover_state();
    if (!recovered)
    {
        (void)context_->mark_file().signal_failed_start();
        return std::unexpected(recovered.error());
    }

    last_cycle_ns_ = axc::core::NanoClock::now();
    is_service_active_ = true;
    return {};
}

template <ClusteredService S>
Cluster ClusteredServiceAgent<S>::make_cluster() noexcept
{
    return Cluster{*client_,   sessions_,     cm_proxy_,     *context_, config_,
                   time_unit_, cluster_time_, log_position_, role_,     member_id_};
}

template <ClusteredService S>
std::expected<std::int64_t, Error> ClusteredServiceAgent<S>::commit_position_value()
{
    const auto& commit = context_->commit_position();
    if (!commit.has_value())
    {
        return std::int64_t{0};
    }

    std::int64_t* address = aeron_counters_reader_addr(client_->counters_reader(), commit->counter_id);
    if (nullptr == address)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "commit-pos counter unexpectedly closed, terminating"));
    }
    return static_cast<std::int64_t>(__atomic_load_n(address, __ATOMIC_ACQUIRE));
}

template <ClusteredService S>
class ClusteredServiceAgent<S>::EventHandler final : public LogHandler
{
public:
    explicit EventHandler(ClusteredServiceAgent& agent) : agent_(agent) {}

    std::expected<void, Error> on_log_event(const LogEvent& event) override
    {
        agent_.log_position_ = event.position;
        const LogHeader header{event.position, event.session_id, event.stream_id};
        const LogMessage& message = event.message;

        switch (message.kind)
        {
            case LogMessage::Kind::session_message:
            {
                agent_.cluster_time_ = message.timestamp;
                Cluster cluster = agent_.make_cluster();
                ContainerClientSession* raw = agent_.sessions_.get(message.cluster_session_id);
                if (nullptr != raw)
                {
                    ClientSession view{cluster, *raw};
                    return agent_.service_.on_session_message(cluster, &view, message.timestamp, event.payload, header);
                }
                return agent_.service_.on_session_message(cluster, nullptr, message.timestamp, event.payload, header);
            }
            case LogMessage::Kind::timer_event:
            {
                agent_.cluster_time_ = message.timestamp;
                Cluster cluster = agent_.make_cluster();
                return agent_.service_.on_timer_event(cluster, message.correlation_id, message.timestamp);
            }
            case LogMessage::Kind::session_open:
            {
                agent_.cluster_time_ = message.timestamp;
                if (nullptr != agent_.sessions_.get(message.cluster_session_id))
                {
                    return std::unexpected(axc::core::make_error(
                        axc::core::ErrorCode::protocol_error,
                        "clashing open clusterSessionId=" + std::to_string(message.cluster_session_id) +
                            " leadershipTermId=" + std::to_string(message.leadership_term_id) +
                            " logPosition=" + std::to_string(agent_.log_position_)));
                }

                ContainerClientSession session{message.cluster_session_id, message.response_stream_id,
                                               message.response_channel, message.encoded_principal};
                if (Role::leader == agent_.role_ && agent_.config_.responding_service)
                {
                    auto connected = session.connect(*agent_.client_);
                    if (!connected)
                    {
                        agent_.report(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                            "failed to connect session response publication: " +
                                                                std::string{connected.error().message.view()}));
                    }
                }

                agent_.sessions_.add(std::move(session));
                ContainerClientSession* raw = agent_.sessions_.get(message.cluster_session_id);
                Cluster cluster = agent_.make_cluster();
                ClientSession view{cluster, *raw};
                return agent_.service_.on_session_open(view, message.timestamp);
            }
            case LogMessage::Kind::session_close:
            {
                agent_.cluster_time_ = message.timestamp;
                auto removed = agent_.sessions_.remove(message.cluster_session_id);
                if (!removed.has_value())
                {
                    agent_.report(axc::core::make_error(
                        axc::core::ErrorCode::protocol_error,
                        "unknown session close: clusterSessionId=" + std::to_string(message.cluster_session_id) +
                            " leadershipTermId=" + std::to_string(message.leadership_term_id) +
                            " logPosition=" + std::to_string(agent_.log_position_)));
                    return {};
                }

                (void)removed->disconnect();
                Cluster cluster = agent_.make_cluster();
                ClientSession view{cluster, *removed};
                return agent_.service_.on_session_close(view, message.timestamp, message.close_reason);
            }
            case LogMessage::Kind::cluster_action:
            {
                agent_.cluster_time_ = message.timestamp;
                if (static_cast<std::int32_t>(::io::aeron::cluster::codecs::ClusterAction::SNAPSHOT) ==
                        message.action &&
                    detail::should_snapshot(message.flags, agent_.standby_snapshot_flags_))
                {
                    agent_.pending_snapshot_ = SnapshotAction{message.log_position, message.leadership_term_id};
                }
                return {};
            }
            case LogMessage::Kind::new_leadership_term:
            {
                constexpr auto MAJOR = [](std::int32_t version) { return (version >> 16) & 0xFF; };
                if (MAJOR(message.app_version) != MAJOR(agent_.config_.app_version))
                {
                    return std::unexpected(axc::core::make_error(
                        axc::core::ErrorCode::protocol_error,
                        "incompatible version: log appVersion=" + std::to_string(message.app_version) +
                            " container appVersion=" + std::to_string(agent_.config_.app_version)));
                }

                agent_.cm_proxy_.set_leadership_term_id(message.leadership_term_id);
                agent_.log_position_ = message.log_position;
                agent_.cluster_time_ = message.timestamp;
                agent_.time_unit_ = time_unit_from_code(message.time_unit_code);

                if constexpr (HasNewLeadershipTermEvent<S>)
                {
                    NewLeadershipTermEvent leadership_event;
                    leadership_event.leadership_term_id = message.leadership_term_id;
                    leadership_event.log_position = message.log_position;
                    leadership_event.timestamp = message.timestamp;
                    leadership_event.term_base_log_position = message.term_base_log_position;
                    leadership_event.leader_member_id = message.leader_member_id;
                    leadership_event.log_session_id = message.log_session_id;
                    leadership_event.time_unit = agent_.time_unit_;
                    leadership_event.app_version = message.app_version;

                    Cluster cluster = agent_.make_cluster();
                    return agent_.service_.on_new_leadership_term_event(cluster, leadership_event);
                }
                return {};
            }
        }
        return {};
    }

private:
    ClusteredServiceAgent& agent_;
};

template <ClusteredService S>
std::expected<std::size_t, Error> ClusteredServiceAgent<S>::poll_log()
{
    auto commit = commit_position_value();
    if (!commit)
    {
        return std::unexpected(commit.error());
    }

    EventHandler handler{*this};
    auto read = log_adapter_.poll(*commit, handler);
    if (!read)
    {
        return std::unexpected(read.error());
    }

    if (pending_snapshot_.has_value())
    {
        const SnapshotAction action = *pending_snapshot_;
        pending_snapshot_.reset();
        auto executed = execute_snapshot_action(action);
        if (!executed)
        {
            return std::unexpected(executed.error());
        }
    }

    if (0 == *read && log_adapter_.is_done())
    {
        close_log();
    }
    return *read;
}

template <ClusteredService S>
std::expected<std::size_t, Error> ClusteredServiceAgent<S>::poll_service_adapter()
{
    ServiceEvents events;
    auto work = service_adapter_->poll(events);
    if (!work)
    {
        return std::unexpected(work.error());
    }

    if (events.termination_position.has_value())
    {
        termination_position_ = events.termination_position->first;
        termination_ack_required_ = events.termination_position->second;
    }
    if (events.requested_ack_position.has_value())
    {
        requested_ack_position_ = *events.requested_ack_position;
    }
    if (events.join_log.has_value())
    {
        log_adapter_.set_max_log_position(events.join_log->log_position);
        active_log_event_ = std::move(events.join_log);
    }

    if (active_log_event_.has_value() && !log_adapter_.has_image())
    {
        JoinLogEvent event = std::move(*active_log_event_);
        active_log_event_.reset();
        auto joined = join_active_log(event);
        if (!joined)
        {
            return std::unexpected(joined.error());
        }
    }

    if (-1 != termination_position_ && log_position_ >= termination_position_)
    {
        if (log_position_ > termination_position_)
        {
            report(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                         "service terminate: logPosition=" + std::to_string(log_position_) +
                                             " > terminationPosition=" + std::to_string(termination_position_)));
        }
        return std::unexpected(terminate(log_position_ == termination_position_, termination_ack_required_));
    }

    if (-1 != requested_ack_position_ && log_position_ >= requested_ack_position_)
    {
        if (log_position_ > requested_ack_position_)
        {
            report(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                         "invalid ack request: logPosition=" + std::to_string(log_position_) +
                                             " > requestedAckPosition=" + std::to_string(requested_ack_position_)));
        }
        auto acked = send_ack(log_position_, cluster_time_, -1);
        if (!acked)
        {
            return std::unexpected(acked.error());
        }
        requested_ack_position_ = -1;
    }

    return *work;
}

template <ClusteredService S>
aeron_image_t* ClusteredServiceAgent<S>::await_image(aeron_subscription_t* subscription, std::int32_t session_id)
{
    context_->reset_idle();
    while (true)
    {
        (void)client_->do_work();
        aeron_image_t* image = aeron_subscription_image_by_session_id(subscription, session_id);
        if (nullptr != image)
        {
            return image;
        }
        (void)context_->idle(*client_, 0);
    }
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::join_active_log(const JoinLogEvent& event)
{
    if (Role::leader != role_)
    {
        disconnect_egress();
    }

    standby_snapshot_flags_ = event.is_standby ? CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT : CLUSTER_ACTION_FLAGS_DEFAULT;

    const std::string channel =
        detail::with_aliased_channel(event.log_channel, "log-sc-" + std::to_string(config_.service_id));

    {
        auto add = client_->add_subscription(channel, event.log_stream_id);
        auto subscription = await_async(*client_, add, 5'000'000'000);
        if (!subscription)
        {
            return std::unexpected(subscription.error());
        }
        log_subscription_ = std::move(*subscription);
    }

    aeron_image_t* image = await_image(log_subscription_.get(), event.log_session_id);
    aeron_image_constants_t constants{};
    if (0 != aeron_image_constants(image, &constants))
    {
        (void)aeron_subscription_image_release(log_subscription_.get(), image);
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "log image constants are unavailable"));
    }

    if (constants.join_position != log_position_)
    {
        (void)aeron_subscription_image_release(log_subscription_.get(), image);
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::protocol_error,
            "Cluster log must be contiguous for joining image: expectedPosition=" + std::to_string(log_position_) +
                " joinPosition=" + std::to_string(constants.join_position)));
    }
    if (event.log_position != log_position_)
    {
        (void)aeron_subscription_image_release(log_subscription_.get(), image);
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::protocol_error,
            "Cluster log must be contiguous for active log event: expectedPosition=" + std::to_string(log_position_) +
                " eventPosition=" + std::to_string(event.log_position)));
    }

    log_adapter_.set_image(log_subscription_.get(), image);
    log_adapter_.set_max_log_position(event.max_log_position);

    auto acked = send_ack(event.log_position, cluster_time_, -1);
    if (!acked)
    {
        return std::unexpected(acked.error());
    }

    member_id_ = event.member_id;
    context_->mark_file().set_member_id(member_id_);

    if (static_cast<std::int32_t>(role_code(Role::leader)) == event.role)
    {
        for (ContainerClientSession& session : sessions_.sessions())
        {
            if (config_.responding_service && !event.is_startup)
            {
                auto connected = session.connect(*client_);
                if (!connected)
                {
                    report(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                 "failed to connect session response publication: " +
                                                     std::string{connected.error().message.view()}));
                }
            }
            session.reset_closing();
        }
    }

    auto role = role_from_code(event.role);
    if (!role)
    {
        return std::unexpected(role.error());
    }
    return set_role(*role);
}

template <ClusteredService S>
void ClusteredServiceAgent<S>::close_log()
{
    if (nullptr != log_adapter_.image())
    {
        const std::int64_t position = aeron_image_position(log_adapter_.image());
        log_position_ = std::max(log_position_, position);
    }

    log_adapter_.clear_image();
    log_subscription_.reset();
    disconnect_egress();
    (void)set_role(Role::follower);
}

template <ClusteredService S>
void ClusteredServiceAgent<S>::disconnect_egress() noexcept
{
    for (ContainerClientSession& session : sessions_.sessions())
    {
        if (session.has_publication())
        {
            const auto disconnected = session.disconnect();
            if (!disconnected)
            {
                report(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                             "failed to disconnect session response publication"));
            }
        }
    }
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::set_role(Role new_role)
{
    if (new_role == role_)
    {
        return {};
    }

    role_ = new_role;
    context_->set_lifecycle(LifecycleCallback::on_role_change);
    auto result = service_.on_role_change(new_role);
    context_->set_lifecycle(LifecycleCallback::none);
    return result;
}

template <ClusteredService S>
std::expected<void, Error> ClusteredServiceAgent<S>::invoke_background_work(std::int64_t now_ns)
{
    if constexpr (HasBackgroundWork<S>)
    {
        context_->set_lifecycle(LifecycleCallback::do_background_work);
        Cluster cluster = make_cluster();
        const int work = service_.do_background_work(cluster, now_ns);
        context_->set_lifecycle(LifecycleCallback::none);
        if (work < 0)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                         "doBackgroundWork returned a negative count"));
        }
    }
    return {};
}

template <ClusteredService S>
Error ClusteredServiceAgent<S>::terminate(bool expected, bool ack_required)
{
    // `ClusterTerminationException(expected)` (`CSA:1240-1270`): the flag
    // reaches `runTerminationHooks` through the error that ends `doWork`.
    termination_expected_ = expected;
    is_service_active_ = false;
    context_->set_lifecycle(LifecycleCallback::on_terminate);
    Cluster cluster = make_cluster();
    auto result = service_.on_terminate(cluster);
    context_->set_lifecycle(LifecycleCallback::none);
    if (!result)
    {
        report(result.error());
    }

    if (ack_required)
    {
        int attempts = 5;
        while (attempts > 0)
        {
            auto acked = send_ack(log_position_, cluster_time_, -1);
            if (acked && *acked)
            {
                break;
            }
            if (!acked)
            {
                break;
            }
            if (0 == --attempts)
            {
                break;
            }
        }
    }

    termination_position_ = -1;
    return axc::core::make_error(axc::core::ErrorCode::internal_error,
                                 "service terminated, isExpected=" + std::string{expected ? "true" : "false"});
}

template <ClusteredService S>
void ClusteredServiceAgent<S>::report(const Error& error) noexcept
{
    if (error_counter_)
    {
        if (std::int64_t* address = aeron_counter_addr(error_counter_.get()); nullptr != address)
        {
            (void)__atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
        }
    }

    if (error_log_initialised_)
    {
        const std::string description{error.message.view()};
        (void)aeron_distinct_error_log_record(&error_log_, static_cast<std::int32_t>(error.code), description.c_str());
    }
}

template <ClusteredService S>
void ClusteredServiceAgent<S>::terminate_with(const Error& error) noexcept
{
    if (!terminated_)
    {
        report(error);
        terminated_ = true;
        termination_error_ = error;
        is_service_active_ = false;

        // `ClusteredServiceAgent.doWork`'s `catch (AgentTerminationException)`
        // (`CSA:275-279`) runs the hooks before the exception leaves the agent;
        // the ordinary `onClose` path does not.
        if (!termination_hooks_run_)
        {
            termination_hooks_run_ = true;
            const common::TerminationCause cause{termination_expected_, error};
            common::run_termination_hooks(context_->extended_termination_hook(), context_->termination_hook(), cause,
                                          [this](const Error& hook_error) { report(hook_error); });
        }
    }
}

template <ClusteredService S>
std::expected<int, Error> ClusteredServiceAgent<S>::do_work_inner()
{
    const std::int64_t now_ns = axc::core::NanoClock::now();

    const std::int64_t cycle_ns = now_ns - last_cycle_ns_;
    last_cycle_ns_ = now_ns;
    if (max_cycle_time_)
    {
        if (std::int64_t* address = aeron_counter_addr(max_cycle_time_.get()); nullptr != address)
        {
            std::int64_t current = __atomic_load_n(address, __ATOMIC_ACQUIRE);
            while (cycle_ns > current &&
                   !__atomic_compare_exchange_n(address, &current, cycle_ns, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            {
            }
        }
    }
    if (cycle_threshold_exceeded_ && cycle_ns > config_.cycle_threshold_ns)
    {
        if (std::int64_t* address = aeron_counter_addr(cycle_threshold_exceeded_.get()); nullptr != address)
        {
            (void)__atomic_add_fetch(address, 1, __ATOMIC_RELEASE);
        }
    }

    auto ticked = context_->check_for_clock_tick(*client_, now_ns);
    if (!ticked)
    {
        return std::unexpected(ticked.error());
    }

    int work = 0;
    if (*ticked)
    {
        auto polled = poll_service_adapter();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        work += static_cast<int>(*polled);
    }

    if (log_adapter_.has_image())
    {
        auto read = poll_log();
        if (!read)
        {
            return std::unexpected(read.error());
        }
        work += static_cast<int>(*read);
    }

    auto background = invoke_background_work(now_ns);
    if (!background)
    {
        return std::unexpected(background.error());
    }
    return work;
}

template <ClusteredService S>
int ClusteredServiceAgent<S>::do_work() noexcept
{
    if (terminated_)
    {
        return 0;
    }

    if (context_->is_shutdown_requested())
    {
        terminate_with(axc::core::make_error(axc::core::ErrorCode::internal_error, "shutdown requested"));
        return 0;
    }

    try
    {
        auto work = do_work_inner();
        if (!work)
        {
            terminate_with(work.error());
            return 0;
        }
        return *work;
    }
    catch (const std::exception& ex)
    {
        terminate_with(axc::core::make_error(axc::core::ErrorCode::internal_error, ex.what()));
        return 0;
    }
}

template <ClusteredService S>
void ClusteredServiceAgent<S>::on_close() noexcept
{
    try
    {
        if (is_service_active_)
        {
            is_service_active_ = false;
            context_->set_lifecycle(LifecycleCallback::on_terminate);
            Cluster cluster = make_cluster();
            auto result = service_.on_terminate(cluster);
            context_->set_lifecycle(LifecycleCallback::none);
            if (!result)
            {
                report(result.error());
            }
        }

        log_adapter_.clear_image();
        log_subscription_.reset();
        disconnect_egress();
        sessions_.sessions().clear();

        service_adapter_.reset();
        service_subscription_.reset();
        cm_publication_.reset();

        if (error_log_initialised_)
        {
            aeron_distinct_error_log_close(&error_log_);
            error_log_initialised_ = false;
        }

        error_counter_.reset();
        max_cycle_time_.reset();
        cycle_threshold_exceeded_.reset();
        max_snapshot_duration_.reset();
        snapshot_threshold_exceeded_.reset();

        if (context_.has_value())
        {
            auto terminated = context_->mark_file().signal_terminated();
            (void)terminated;
        }

        if (client_.has_value())
        {
            client_->close();
        }
    }
    catch (...)
    {
    }
}

}  // namespace deepmsg::cluster::service
