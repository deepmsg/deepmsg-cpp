#pragma once

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

extern "C"
{
#include <concurrent/aeron_distinct_error_log.h>
}

#include <client/aeron_archive.h>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/node_state_file.h"
#include "cluster/common/recording_log.h"
#include "cluster/consensus/consensus_module_context.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error_handler.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS` (`:124`).
inline constexpr std::int64_t SLOW_TICK_INTERVAL_NS = 10'000'000;
/// `ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS`
/// (`ClusteredServiceContainer.java:208`).
inline constexpr std::int64_t MARK_FILE_UPDATE_INTERVAL_NS = 1'000'000'000;
/// `LIVENESS_TIMEOUT_MS` (`ClusteredServiceContainer.java:213`).
inline constexpr std::int64_t LIVENESS_TIMEOUT_MS = 10'000;

/// The C4a consensus-module skeleton: launch, the main loop and its slow tick,
/// counters 200-241, the local archive connection and the Q6 standby startup
/// check.
///
/// Transcribed from `ConsensusModuleAgent.onStart`/`doWork`/`slowTickWork`
/// (Aeron 1.54.0) where the parts they call are available at this card. The
/// election (`election_`), sessions, adapters, the log publisher and the
/// recovery plan are C4b/C4d; each absence is loud in the record, not silent in
/// the code:
///   * `state(ACTIVE)` is set at the end of `on_start` because the reference
///     reaches it through `election.doWork`;
///   * `consensus_work()` returns 0;
///   * no log publication is created, so the archive connection is the only
///     criterion that the archive can observe.
class ConsensusModuleAgent final : public axc::core::ErrorHandler
{
public:
    explicit ConsensusModuleAgent(ConsensusModuleConfig config);
    ConsensusModuleAgent(const ConsensusModuleAgent&) = delete;
    ConsensusModuleAgent& operator=(const ConsensusModuleAgent&) = delete;
    ConsensusModuleAgent(ConsensusModuleAgent&&) = delete;
    ConsensusModuleAgent& operator=(ConsensusModuleAgent&&) = delete;
    ~ConsensusModuleAgent() noexcept override;

    // `axc::core::Agent` surface (`consensus/CMakeLists.txt` drives it through
    // `axc::core::AgentRunner` in `launch()`).
    [[nodiscard]] std::expected<void, Error> on_start();
    [[nodiscard]] int do_work() noexcept;
    void on_close() noexcept;
    [[nodiscard]] std::string_view role_name() const noexcept;

    // `axc::core::ErrorHandler`; the runner hands agent-loop errors here.
    void on_error(const Error& error) noexcept override;

    [[nodiscard]] const ConsensusModuleConfig& config() const noexcept
    {
        return config_;
    }

    [[nodiscard]] ConsensusModuleState state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] ClusterRole role() const noexcept
    {
        return role_;
    }

    [[nodiscard]] std::int64_t commit_position() const noexcept;
    [[nodiscard]] std::int64_t archive_id() const noexcept
    {
        return archive_id_;
    }

    [[nodiscard]] axc::core::DeepmsgClient& client() noexcept
    {
        return client_;
    }

    [[nodiscard]] bool is_started() const noexcept
    {
        return started_;
    }

private:
    [[nodiscard]] std::expected<void, Error> open_mark_file();
    [[nodiscard]] std::expected<void, Error> open_node_files();
    [[nodiscard]] std::expected<void, Error> allocate_counters();
    [[nodiscard]] std::expected<void, Error> connect_archive();
    [[nodiscard]] int slow_tick_work(std::int64_t now_ns) noexcept;
    [[nodiscard]] int consensus_work(std::int64_t timestamp, std::int64_t now_ns) noexcept;
    [[nodiscard]] int poll_archive_error_response() noexcept;

    void report(const Error& error) noexcept;
    void state(ConsensusModuleState new_state, const char* reason) noexcept;
    void role(ClusterRole new_role) noexcept;
    void measure_duty_cycle(std::int64_t now_ns) noexcept;
    void close_resources() noexcept;

    [[nodiscard]] std::int64_t now_ns() const noexcept;
    [[nodiscard]] std::int64_t now_ms() const noexcept;

    ConsensusModuleConfig config_;
    axc::core::DeepmsgClient client_{};

    common::ClusterMarkFile mark_file_{};
    bool mark_file_open_{false};
    common::NodeStateFile node_state_{};
    common::RecordingLog recording_log_{};

    // `AeronArchive.connect` resources; the C API has no move-friendly value
    // type, so the pair is owned here.
    aeron_archive_context_t* archive_context_{nullptr};
    aeron_archive_t* archive_{nullptr};
    std::int64_t archive_id_{NULL_VALUE};

    axc::core::CounterPtr error_counter_{};
    axc::core::CounterPtr module_state_counter_{};
    axc::core::CounterPtr role_counter_{};
    axc::core::CounterPtr commit_position_counter_{};
    axc::core::CounterPtr control_toggle_counter_{};
    axc::core::CounterPtr node_control_toggle_counter_{};
    axc::core::CounterPtr election_state_counter_{};
    axc::core::CounterPtr election_count_counter_{};
    axc::core::CounterPtr leadership_term_counter_{};
    axc::core::CounterPtr snapshot_counter_{};
    axc::core::CounterPtr timed_out_client_counter_{};
    axc::core::CounterPtr max_cycle_time_counter_{};
    axc::core::CounterPtr cycle_threshold_exceeded_counter_{};

    aeron_distinct_error_log_t error_log_{};
    bool error_log_initialised_{false};

    ConsensusModuleState state_{ConsensusModuleState::init};
    ClusterRole role_{ClusterRole::follower};
    std::int64_t slow_tick_deadline_ns_{0};
    std::int64_t mark_file_update_deadline_ns_{0};
    std::int64_t time_of_last_update_ns_{0};
    bool started_{false};
    bool closed_{false};
};

}  // namespace deepmsg::cluster::consensus
