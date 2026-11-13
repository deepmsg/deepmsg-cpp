#pragma once

#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <mutex>
#include <random>
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
#include "cluster/common/recovery_state.h"
#include "cluster/common/timer/timer_service.h"
#include "cluster/common/timer/timer_supplier.h"
#include "cluster/consensus/authenticator.h"
#include "cluster/consensus/cluster_termination.h"
#include "cluster/consensus/cm_snapshot.h"
#include "cluster/consensus/consensus_adapter.h"
#include "cluster/consensus/consensus_module_adapter.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/consensus/consensus_publisher.h"
#include "cluster/consensus/egress_publisher.h"
#include "cluster/consensus/ingress_adapter.h"
#include "cluster/consensus/log_adapter.h"
#include "cluster/consensus/log_publisher.h"
#include "cluster/consensus/log_replay.h"
#include "cluster/consensus/pending_service_message_tracker.h"
#include "cluster/consensus/service_ack.h"
#include "cluster/consensus/service_proxy.h"
#include "cluster/consensus/session_manager.h"
#include "cluster/election/election.h"
#include "cluster/service/role.h"
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
                                   public common::timer::TimerHandler,
                                   public election::ElectionHost,
                                   public LogMessageHandler,
                                   public ConsensusMessageHandler,
                                   public SnapshotLoadHandler,
                                   public LogReplayHost
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
                                                                bool is_startup) override;

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

    [[nodiscard]] ConsensusModuleState module_state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] ClusterRole cluster_role() const noexcept
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

    /// Test seam: the JoinLog channel the leader advertises to its services.
    [[nodiscard]] std::string local_log_channel_copy() const
    {
        std::lock_guard<std::mutex> lock(local_log_mutex_);
        return local_log_channel_;
    }

    // ---- C4d: ElectionHost (`election_host.h`) -------------------------

    void set_role(service::Role new_role) override;
    [[nodiscard]] service::Role role() const override;
    [[nodiscard]] election::ModuleState state() const override;
    [[nodiscard]] std::int64_t log_recording_id() const override
    {
        return log_recording_id_;
    }
    void set_log_recording_id(std::int64_t recording_id) override
    {
        log_recording_id_ = recording_id;
    }
    [[nodiscard]] std::int64_t time_of_last_leader_update_ns() const override
    {
        return time_of_last_leader_update_ns_;
    }
    [[nodiscard]] std::int64_t quorum_position_bounded_by_leader_log(std::int64_t leader_append_position,
                                                                     std::int64_t now_ns) override;
    void update_member_log_position(common::ClusterMember& member, std::int64_t leadership_term_id,
                                    std::int64_t log_position) override;
    [[nodiscard]] std::expected<int, Error> update_leader_position(std::int64_t now_ns, std::int64_t append_position,
                                                                   std::int64_t quorum_position) override;
    [[nodiscard]] std::expected<void, Error> publish_commit_position(std::int64_t commit_position,
                                                                     std::int64_t leadership_term_id) override;
    [[nodiscard]] std::expected<bool, Error> try_join_log_as_follower(aeron_image_t* image, bool is_leader_startup,
                                                                      std::int64_t now_ns) override;
    [[nodiscard]] std::expected<std::int32_t, Error> add_log_publication(std::int64_t append_position) override;
    [[nodiscard]] std::expected<std::unique_ptr<election::LogReplay>, Error> new_log_replay(
        std::int64_t log_position, std::int64_t append_position) override;
    [[nodiscard]] std::expected<std::unique_ptr<election::RecordingReplication>, Error> new_log_replication(
        std::string_view leader_archive_endpoint, std::optional<std::string> response_archive_endpoint,
        std::int64_t leader_recording_id, std::int64_t stop_position, std::int64_t now_ns) override;
    [[nodiscard]] std::expected<void, Error> truncate_log_entry(std::int64_t leadership_term_id,
                                                                std::int64_t log_position) override;
    [[nodiscard]] std::expected<bool, Error> append_new_leadership_term_event(std::int64_t now_ns) override;
    [[nodiscard]] std::expected<std::int64_t, Error> prepare_for_new_leadership(std::int64_t log_position,
                                                                                std::int64_t now_ns) override;
    [[nodiscard]] std::expected<void, Error> catchup_initiated(std::int64_t now_ns) override;
    [[nodiscard]] std::expected<int, Error> catchup_poll(std::int64_t limit_position, std::int64_t now_ns) override;
    [[nodiscard]] bool is_catchup_near_live(std::int64_t position) override;
    [[nodiscard]] std::expected<void, Error> track_catchup_completion(common::ClusterMember& member,
                                                                      std::int64_t leadership_term_id,
                                                                      std::int16_t append_position_flags) override;
    [[nodiscard]] std::expected<void, Error> stop_all_catchups() override;
    [[nodiscard]] std::string_view live_log_destination() const override
    {
        return live_log_destination_;
    }
    [[nodiscard]] std::expected<void, Error> set_live_log_destination(std::string_view destination) override;
    [[nodiscard]] std::string_view catchup_log_destination() const override
    {
        return catchup_log_destination_;
    }
    [[nodiscard]] std::expected<void, Error> set_catchup_log_destination(std::string_view destination) override;
    [[nodiscard]] std::expected<void, Error> await_local_sockets_closed(std::int64_t registration_id) override;
    [[nodiscard]] std::expected<void, Error> election_complete(std::int64_t leadership_term_id,
                                                               std::int32_t leader_member_id, std::int64_t log_position,
                                                               std::int64_t now_ns) override;
    [[nodiscard]] std::expected<int, Error> poll_archive_events() override;
    [[nodiscard]] bool member_has_publication(std::int32_t member_id) const override;
    [[nodiscard]] std::expected<void, Error> try_add_member_publication(std::int32_t member_id) override;
    [[nodiscard]] std::expected<std::int64_t, Error> add_log_subscription(std::int32_t log_session_id) override;
    [[nodiscard]] std::expected<void, Error> remove_log_subscription(std::int64_t registration_id) override;
    [[nodiscard]] std::expected<void, Error> add_log_destination(std::int64_t registration_id,
                                                                 std::string_view destination) override;
    [[nodiscard]] std::optional<std::string> log_subscription_resolved_endpoint(
        std::int64_t registration_id) const override;
    [[nodiscard]] bool log_subscription_channel_errored(std::int64_t registration_id) const override;
    [[nodiscard]] std::string_view log_subscription_channel(std::int64_t registration_id) const override;
    [[nodiscard]] std::optional<election::LogImage> log_image_by_session(std::int64_t registration_id,
                                                                         std::int32_t session_id) override;

    // ---- C4d: LogMessageHandler (the six replay handlers) --------------

    void on_replay_session_message(std::int64_t cluster_session_id, std::int64_t timestamp) override;
    void on_replay_timer_event(std::int64_t correlation_id) override;
    void on_replay_session_open(std::int64_t log_position, std::int64_t correlation_id, std::int64_t cluster_session_id,
                                std::int64_t timestamp, std::int32_t response_stream_id,
                                std::string_view response_channel) override;
    void on_replay_session_close(std::int64_t cluster_session_id, codecs::CloseReason::Value close_reason) override;
    [[nodiscard]] std::expected<void, Error> on_replay_cluster_action(std::int64_t leadership_term_id,
                                                                      std::int64_t log_position, std::int64_t timestamp,
                                                                      std::int32_t action, std::int32_t flags) override;
    [[nodiscard]] std::expected<void, Error> on_replay_new_leadership_term_event(
        std::int64_t leadership_term_id, std::int64_t log_position, std::int64_t timestamp,
        std::int64_t term_base_log_position, std::int32_t time_unit_code, std::int32_t app_version) override;

    // ---- C4d: SnapshotLoadHandler -------------------------------------

    [[nodiscard]] std::expected<void, Error> on_load_begin_snapshot(std::int32_t app_version,
                                                                    service::TimeUnit time_unit) override;
    [[nodiscard]] std::expected<void, Error> on_load_end_snapshot() override;
    [[nodiscard]] std::expected<void, Error> on_load_cluster_session(
        std::int64_t cluster_session_id, std::int64_t correlation_id, std::int64_t opened_log_position,
        std::int64_t time_of_last_activity, codecs::CloseReason::Value close_reason, std::int32_t response_stream_id,
        std::string_view response_channel) override;
    [[nodiscard]] std::expected<void, Error> on_load_consensus_module_state(
        std::int64_t next_session_id, std::int64_t next_service_session_id, std::int64_t log_service_session_id,
        std::int32_t pending_message_capacity) override;
    [[nodiscard]] std::expected<void, Error> on_load_pending_message_tracker(std::int64_t next_service_session_id,
                                                                             std::int64_t log_service_session_id,
                                                                             std::int32_t pending_message_capacity,
                                                                             std::int32_t service_id) override;
    [[nodiscard]] std::expected<void, Error> on_load_pending_message(std::int64_t cluster_session_id,
                                                                     std::span<const std::uint8_t> message) override;
    [[nodiscard]] std::expected<void, Error> on_load_timer(std::int64_t correlation_id, std::int64_t deadline) override;

    // ---- C4d: LogReplayHost -------------------------------------------

    [[nodiscard]] std::expected<void, Error> await_services_ready(std::string_view log_channel, std::int32_t stream_id,
                                                                  std::int32_t log_session_id,
                                                                  std::int64_t log_position,
                                                                  std::int64_t max_log_position, bool is_startup,
                                                                  service::Role role) override;
    [[nodiscard]] std::expected<int, Error> replay_log_poll(LogAdapter& log_adapter,
                                                            std::int64_t stop_position) override;
    [[nodiscard]] bool is_snapshot_state() const override
    {
        return ConsensusModuleState::snapshot == state_;
    }
    void on_log_replay_error(const Error& error) override;

    // ---- C4d: ConsensusMessageHandler (the ten inbound templates) -----

    void on_canvass_position(std::int64_t log_leadership_term_id, std::int64_t log_position,
                             std::int64_t leadership_term_id, std::int32_t follower_member_id,
                             std::int32_t protocol_version) override;
    void on_request_vote(std::int64_t log_leadership_term_id, std::int64_t log_position, std::int64_t candidate_term_id,
                         std::int32_t candidate_member_id, std::int32_t protocol_version) override;
    void on_vote(std::int64_t candidate_term_id, std::int64_t log_leadership_term_id, std::int64_t log_position,
                 std::int32_t candidate_member_id, std::int32_t follower_member_id, bool vote) override;
    void on_new_leadership_term(std::int64_t log_leadership_term_id, std::int64_t next_leadership_term_id,
                                std::int64_t next_term_base_log_position, std::int64_t next_log_position,
                                std::int64_t leadership_term_id, std::int64_t term_base_log_position,
                                std::int64_t log_position, std::int64_t commit_position,
                                std::int64_t leader_recording_id, std::int64_t timestamp, std::int32_t leader_member_id,
                                std::int32_t log_session_id, std::int32_t app_version, bool is_startup) override;
    void on_append_position(std::int64_t leadership_term_id, std::int64_t log_position, std::int32_t follower_member_id,
                            std::int16_t flags) override;
    void on_commit_position(std::int64_t leadership_term_id, std::int64_t log_position,
                            std::int32_t leader_member_id) override;
    void on_catchup_position(std::int64_t leadership_term_id, std::int64_t log_position,
                             std::int32_t follower_member_id, std::string_view catchup_endpoint) override;
    void on_stop_catchup(std::int64_t leadership_term_id, std::int32_t follower_member_id) override;
    void on_termination_position(std::int64_t leadership_term_id, std::int64_t log_position) override;
    void on_termination_ack(std::int64_t leadership_term_id, std::int64_t log_position,
                            std::int32_t member_id) override;
    void on_unhandled_template(std::int32_t template_id) override;

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
    void restore_uncommitted_entries(std::int64_t commit_position);
    [[nodiscard]] std::expected<void, Error> capture_service_ack(std::int64_t log_position, std::int64_t ack_id,
                                                                 std::int64_t relevant_id, std::int32_t service_id);

    // ---- C4d: recovery, snapshot and the log recording ---------------

    [[nodiscard]] std::expected<void, Error> recover_from_snapshot_and_log();
    [[nodiscard]] std::expected<axc::core::CounterPtr, Error> add_recovery_state_counter(
        const common::RecordingLog::RecoveryPlan& plan);
    [[nodiscard]] std::expected<void, Error> load_snapshot(const common::RecordingLog::Snapshot& snapshot);
    [[nodiscard]] std::expected<void, Error> start_log_recording(std::string_view channel, std::int32_t stream_id);
    [[nodiscard]] std::expected<bool, Error> try_create_append_position(std::int32_t log_session_id);
    [[nodiscard]] std::int64_t recording_position();
    [[nodiscard]] std::expected<void, Error> snapshot_on_service_ack(std::int64_t log_position, std::int64_t timestamp,
                                                                     std::span<const ServiceAck> acks);
    [[nodiscard]] std::expected<void, Error> take_snapshot(std::int64_t timestamp, std::int64_t log_position,
                                                           std::span<const ServiceAck> acks);
    [[nodiscard]] std::expected<void, Error> snapshot_state(SnapshotPublicationSink& publication,
                                                            std::int64_t log_position);
    [[nodiscard]] std::expected<void, Error> take_snapshot_recording();

    // ---- C4d: control toggles and termination ------------------------

    [[nodiscard]] std::expected<bool, Error> append_action(std::int32_t action, std::int64_t timestamp,
                                                           std::int32_t flags);
    void offer_position_and_previous_state(std::int64_t position, ConsensusModuleState state);
    [[nodiscard]] int check_cluster_control_toggle(std::int64_t now_ns);
    void enter_election(bool is_log_end_of_stream, std::string_view reason);
    void do_termination(std::int64_t log_position);
    void close_and_terminate();
    void terminate_on_service_ack(std::int64_t log_position);
    void try_stop_log_recording();
    void connect_ingress();
    std::int64_t get_last_appended_position();

    // ---- C4d: small conversions and helpers --------------------------

    void update_member_details(const common::ClusterMember& leader);
    [[nodiscard]] std::int32_t find_member_index(std::int32_t member_id) const noexcept;
    [[nodiscard]] std::string member_consensus_channel(const common::ClusterMember& member) const;
    [[nodiscard]] std::int64_t read_counter_value(std::int32_t counter_id) noexcept;

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

    // C4d: the election, the consensus channel, the log recording, recovery
    // and termination. Declaration order follows construction order.
    std::unique_ptr<ConsensusPublisher> consensus_publisher_{};
    std::unique_ptr<ConsensusAdapter> consensus_adapter_{};
    axc::core::SubPtr consensus_subscription_{};
    std::unique_ptr<election::Election> election_{};
    std::unique_ptr<LogAdapter> log_adapter_{};
    std::unique_ptr<ClusterTermination> cluster_termination_{};
    common::RecordingLog::RecoveryPlan recovery_plan_{};
    std::int64_t log_recording_id_{NULL_VALUE};
    std::int64_t log_recording_subscription_id_{NULL_VALUE};
    std::int64_t log_recording_stop_position_{0};
    std::int32_t append_position_counter_id_{AERON_NULL_COUNTER_ID};
    std::int64_t last_append_position_{0};
    std::int64_t termination_position_{NULL_POSITION};
    std::int64_t termination_leadership_term_id_{NULL_VALUE};
    std::int64_t time_of_last_leader_update_ns_{0};
    std::int64_t last_append_position_send_ns_{0};
    std::string local_log_channel_{};
    mutable std::mutex local_log_mutex_{};
    std::string log_publication_tag_{};
    std::string live_log_destination_{};
    std::string catchup_log_destination_{};
    std::deque<std::pair<std::int64_t, ConsensusModuleState>> uncommitted_previous_state_{};
    bool is_node_startup_{true};
    bool election_reset_pending_{false};
    std::int64_t notified_commit_position_{0};
    std::mt19937_64 random_{std::random_device{}()};
};

}  // namespace deepmsg::cluster::consensus
