#pragma once

#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C"
{
#include <concurrent/aeron_distinct_error_log.h>
}

#include <client/aeron_archive.h>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_member.h"
#include "cluster/common/node_state_file.h"
#include "cluster/common/recording_log.h"
#include "cluster/common/timer/timer_service.h"
#include "cluster/common/timer/timer_supplier.h"
#include "cluster/consensus/authenticator.h"
#include "cluster/consensus/consensus_module_adapter.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/consensus/egress_publisher.h"
#include "cluster/consensus/ingress_adapter.h"
#include "cluster/consensus/log_publisher.h"
#include "cluster/consensus/pending_service_message_tracker.h"
#include "cluster/consensus/service_ack.h"
#include "cluster/consensus/service_proxy.h"
#include "cluster/consensus/session_manager.h"
#include "core/collections/long2long_hash_map.h"
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
class ConsensusModuleAgent final : public axc::core::ErrorHandler,
                                   public IngressHandler,
                                   public ServiceControlHandler,
                                   public common::timer::TimerHandler
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

    // `IngressHandler`: the C4b session surface. The role/term guards are the
    // reference's (`ConsensusModuleAgent.java:772-880`); until the Election
    // wiring lands in C4d the module is a follower and these paths stay shut.
    [[nodiscard]] bool on_ingress_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                          const std::uint8_t* payload, std::size_t payload_length) override;
    void on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id, std::int32_t version,
                            std::string_view response_channel, std::span<const std::uint8_t> encoded_credentials,
                            std::string_view client_info, const IngressImageInfo& image_info) override;
    void on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id) override;
    void on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                               std::int64_t image_correlation_id) override;
    void on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                               std::span<const std::uint8_t> encoded_credentials) override;
    void on_admin_request(std::int64_t leadership_term_id, std::int64_t cluster_session_id, std::int64_t correlation_id,
                          std::int32_t request_type, std::span<const std::uint8_t> payload) override;
    void on_extension_message(std::int32_t schema_id, std::int32_t template_id,
                              std::span<const std::uint8_t> message) override;

    // `ServiceControlHandler`: the S2 container -> consensus module direction.
    // The two unknown-service-id guards are 1.54.0's `8554648566`: report and
    // drop, never index outside the per-service arrays.
    [[nodiscard]] std::expected<void, Error> on_service_message(std::int64_t cluster_session_id,
                                                                std::span<const std::uint8_t> message) override;
    [[nodiscard]] std::expected<void, Error> on_service_close_session(std::int64_t cluster_session_id) override;
    void on_schedule_timer(std::int64_t correlation_id, std::int64_t deadline) override;
    void on_cancel_timer(std::int64_t correlation_id) override;
    [[nodiscard]] std::expected<void, Error> on_service_ack(std::int64_t log_position, std::int64_t timestamp,
                                                            std::int64_t ack_id, std::int64_t relevant_id,
                                                            std::int32_t service_id) override;
    [[nodiscard]] std::expected<void, Error> on_cluster_members_query(std::int64_t correlation_id,
                                                                      bool is_extended_request) override;

    // `TimerService.TimerHandler`: an expired timer becomes a `TimerEvent` in
    // the log (`ConsensusModuleAgent.onTimerEvent`, `:889-900`).
    [[nodiscard]] bool on_timer_event(std::int64_t correlation_id) override;

    /// `ConsensusModuleAgent.joinLogAsLeader` (`:1704-1715`), for the shape
    /// C4c can support: no archive recording yet, so the log publication's
    /// position is the append position and C4d replaces `tryCreateAppendPosition`.
    /// Nothing calls this in product code until the Election lands in C4d; the
    /// interop test drives it, as the reference's own tests drive `role()`.
    [[nodiscard]] std::expected<void, Error> join_log_as_leader(std::int64_t leadership_term_id,
                                                                std::int64_t log_position, std::int32_t log_session_id,
                                                                bool is_startup);

    /// `awaitServicesReady`'s completion check (`:1817-1838`) without the
    /// blocking: when every service has acked `expectedAckPosition`, consumes
    /// the queue heads and advances `serviceAckId`.
    [[nodiscard]] std::expected<bool, Error> service_acks_reached();

    /// The reference's package-private `role(Cluster.Role)`; the Election is
    /// its only caller in C4d.
    void role(ClusterRole new_role) noexcept;

    /// `leadershipTermId(long)` (`:2150-2156`): the counter and every tracker.
    void leadership_term_id(std::int64_t leadership_term_id);

    [[nodiscard]] std::int64_t leadership_term_id() const noexcept
    {
        return leadership_term_id_;
    }

    [[nodiscard]] std::int64_t append_position() const noexcept;
    [[nodiscard]] std::int64_t service_ack_id() const noexcept
    {
        return service_ack_id_;
    }

    [[nodiscard]] common::timer::TimerService* timer_service() noexcept
    {
        return timer_service_.get();
    }

    [[nodiscard]] const std::vector<PendingServiceMessageTracker>& pending_service_message_trackers() const noexcept
    {
        return pending_service_message_trackers_;
    }

    [[nodiscard]] SessionManager* session_manager() noexcept
    {
        return session_manager_.get();
    }

    [[nodiscard]] LogPublisher* log_publisher() noexcept
    {
        return log_publisher_.get();
    }

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
    [[nodiscard]] std::expected<void, Error> open_session_layer();
    [[nodiscard]] std::expected<void, Error> open_service_control_layer();
    [[nodiscard]] int slow_tick_work(std::int64_t now_ns) noexcept;
    [[nodiscard]] int consensus_work(std::int64_t timestamp, std::int64_t now_ns) noexcept;
    [[nodiscard]] int poll_archive_error_response() noexcept;

    [[nodiscard]] int update_leader_position(std::int64_t now_ns) noexcept;
    void sweep_uncommitted_entries_to(std::int64_t commit_position) noexcept;
    [[nodiscard]] std::expected<void, Error> capture_service_ack(std::int64_t log_position, std::int64_t ack_id,
                                                                 std::int64_t relevant_id, std::int32_t service_id);

    void report(const Error& error) noexcept;
    void state(ConsensusModuleState new_state, const char* reason) noexcept;
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
    axc::core::CounterPtr recovery_state_counter_{};
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

    // C4b: sessions, ingress, log and egress. Declaration order matters:
    // the session manager refers to the publishers, so it is destroyed first.
    DefaultAuthenticator authenticator_{};
    AllowAllAuthorisationService authorisation_service_{};
    std::unique_ptr<LogPublisher> log_publisher_{};
    std::unique_ptr<EgressPublisher> egress_publisher_{};
    std::unique_ptr<IngressAdapter> ingress_adapter_{};
    std::unique_ptr<SessionManager> session_manager_{};
    axc::core::SubPtr ingress_subscription_{};
    axc::core::SubPtr ipc_ingress_subscription_{};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::string ingress_endpoints_{};

    // C4c: the S2 seam, timers and commit advance. `ServiceProxy` refers to
    // `service_send_`, which refers to `control_publication_`, so it is
    // declared between them (members are destroyed in reverse order).
    common::ClusterMembers active_members_{};
    common::ClusterMember this_member_{};
    std::size_t this_member_index_{0};
    common::ClusterMember leader_member_{};
    std::vector<std::int64_t> ranked_positions_{};
    axc::core::PubPtr control_publication_{};
    std::unique_ptr<PublicationSend> service_send_{};
    std::unique_ptr<ServiceProxy> service_proxy_{};
    std::unique_ptr<ConsensusModuleAdapter> control_adapter_{};
    std::unique_ptr<common::timer::TimerService> timer_service_{};
    std::vector<PendingServiceMessageTracker> pending_service_message_trackers_{};
    ServiceAckQueues service_ack_queues_{};
    std::vector<std::int64_t> service_client_ids_{};
    std::int64_t service_ack_id_{0};
    std::int64_t expected_ack_position_{0};
    axc::core::Long2LongHashMap expired_timer_count_by_correlation_id_{
        axc::core::Long2LongHashMap::MIN_CAPACITY, axc::core::Long2LongHashMap::DEFAULT_LOAD_FACTOR, 0};
    std::deque<std::pair<std::int64_t, std::int64_t>> uncommitted_timers_{};
    std::int64_t time_of_last_log_update_ns_{0};
    std::int64_t last_quorum_backtrack_commit_position_{NULL_POSITION};

    ConsensusModuleState state_{ConsensusModuleState::init};
    ClusterRole role_{ClusterRole::follower};
    std::int64_t slow_tick_deadline_ns_{0};
    std::int64_t mark_file_update_deadline_ns_{0};
    std::int64_t time_of_last_update_ns_{0};
    bool started_{false};
    bool closed_{false};
};

}  // namespace deepmsg::cluster::consensus
