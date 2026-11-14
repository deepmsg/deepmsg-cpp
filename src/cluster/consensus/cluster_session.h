#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/consensus_constants.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"
#include "io_aeron_cluster_codecs/CloseReason.h"
#include "io_aeron_cluster_codecs/EventCode.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;
namespace codecs = ::io::aeron::cluster::codecs;

/// `ClusterSession.MAX_ENCODED_PRINCIPAL_LENGTH`.
inline constexpr std::size_t MAX_ENCODED_PRINCIPAL_LENGTH = 4 * 1024;

/// `ClusterSession.State` (`ClusterSession.java:58-63`).
enum class SessionState
{
    init,
    connecting,
    connected,
    challenged,
    authenticated,
    rejected,
    open,
    closing,
    invalid,
    closed
};

[[nodiscard]] std::string_view session_state_name(SessionState state) noexcept;

/// `ClusterSession.Action`: backup and standby sessions belong to C9; only the
/// client action is transcribed by C4b.
enum class SessionAction
{
    client,
    backup,
    heartbeat,
    standby_snapshot
};

/// One client session on the consensus module (`ClusterSession.java`).
///
/// The session owns its response publication and the `cluster-session: …`
/// counter 241. Resource creation is two-phase through `AsyncAdd`, polled by
/// `is_response_publication_connected`, exactly as the reference's
/// `asyncAddPublication` / `asyncAddCounter`.
class ClusterSession
{
public:
    ClusterSession(std::int32_t cluster_member_id, std::int64_t session_id, std::int32_t response_stream_id,
                   std::string response_channel, std::string session_info);

    ClusterSession(const ClusterSession&) = delete;
    ClusterSession& operator=(const ClusterSession&) = delete;
    ClusterSession(ClusterSession&&) noexcept = default;
    ClusterSession& operator=(ClusterSession&&) noexcept = default;
    ~ClusterSession() = default;

    [[nodiscard]] std::int64_t id() const noexcept
    {
        return id_;
    }

    [[nodiscard]] std::int32_t response_stream_id() const noexcept
    {
        return response_stream_id_;
    }

    [[nodiscard]] const std::string& response_channel() const noexcept
    {
        return response_channel_;
    }

    [[nodiscard]] const std::string& session_info() const noexcept
    {
        return session_info_;
    }

    [[nodiscard]] SessionState state() const noexcept
    {
        return state_;
    }

    void state(SessionState new_state, std::string_view reason);

    [[nodiscard]] bool is_open() const noexcept
    {
        return SessionState::open == state_;
    }

    [[nodiscard]] std::int64_t correlation_id() const noexcept
    {
        return correlation_id_;
    }

    [[nodiscard]] const std::span<const std::uint8_t> encoded_principal() const noexcept
    {
        return encoded_principal_;
    }

    [[nodiscard]] std::int64_t opened_log_position() const noexcept
    {
        return opened_log_position_;
    }

    void opened_log_position(std::int64_t position) noexcept
    {
        opened_log_position_ = position;
    }

    [[nodiscard]] std::int64_t closed_log_position() const noexcept
    {
        return closed_log_position_;
    }

    void closed_log_position(std::int64_t position) noexcept
    {
        closed_log_position_ = position;
    }

    [[nodiscard]] std::int64_t time_of_last_activity_ns() const noexcept
    {
        return time_of_last_activity_ns_;
    }

    void time_of_last_activity_ns(std::int64_t now_ns) noexcept
    {
        time_of_last_activity_ns_ = now_ns;
    }

    void last_activity_ns(std::int64_t now_ns, std::int64_t correlation_id) noexcept
    {
        time_of_last_activity_ns_ = now_ns;
        correlation_id_ = correlation_id;
    }

    [[nodiscard]] bool has_timed_out(std::int64_t now_ns, std::int64_t session_timeout_ns) const noexcept
    {
        return SessionState::init != state_ && (time_of_last_activity_ns_ + session_timeout_ns) < now_ns;
    }

    [[nodiscard]] std::int64_t ingress_image_correlation_id() const noexcept
    {
        return ingress_image_correlation_id_;
    }

    void link_ingress_image(std::int64_t image_correlation_id) noexcept;
    void unlink_ingress_image() noexcept
    {
        ingress_image_correlation_id_ = NULL_VALUE;
    }

    [[nodiscard]] bool has_open_event_pending() const noexcept
    {
        return has_open_event_pending_;
    }

    void clear_open_event_pending() noexcept
    {
        has_open_event_pending_ = false;
    }

    [[nodiscard]] bool has_new_leader_event_pending() const noexcept
    {
        return has_new_leader_event_pending_;
    }

    void has_new_leader_event_pending(bool flag) noexcept
    {
        has_new_leader_event_pending_ = flag;
    }

    [[nodiscard]] SessionAction action() const noexcept
    {
        return action_;
    }

    void action(SessionAction action) noexcept
    {
        action_ = action;
    }

    [[nodiscard]] codecs::CloseReason::Value close_reason() const noexcept
    {
        return close_reason_;
    }

    [[nodiscard]] codecs::EventCode::Value event_code() const noexcept
    {
        return event_code_;
    }

    [[nodiscard]] const std::string& response_detail() const noexcept
    {
        return response_detail_;
    }

    /// `ClusterSession.loadSnapshotState` (`ClusterSession.java:139-160`).
    void load_snapshot_state(std::int64_t correlation_id, std::int64_t opened_log_position,
                             std::int64_t time_of_last_activity_ns, codecs::CloseReason::Value close_reason);

    /// `ClusterSession.closing(CloseReason)`.
    void closing(codecs::CloseReason::Value close_reason);

    /// `ClusterSession.authenticate(encodedPrincipal)`.
    void authenticate(std::span<const std::uint8_t> encoded_principal);

    /// `ClusterSession.open(openedLogPosition)`.
    void open(std::int64_t opened_log_position);

    /// `ClusterSession.reject(code, detail, errorLog)`; the error log is the
    /// caller's (the agent's counted handler).
    void reject(codecs::EventCode::Value code, std::string response_detail);

    /// `ClusterSession.redirect(ingressEndpoints)`.
    void redirect(std::string ingress_endpoints);

    /// `ClusterSession.asyncConnect` (`ClusterSession.java:186-190`).
    void async_connect(axc::core::DeepmsgClient& client, std::int32_t cluster_id);

    /// `ClusterSession.isResponsePublicationConnected` (`:229-283`): polls the
    /// two async adds; once the publication exists the session is CONNECTING.
    [[nodiscard]] bool is_response_publication_connected(axc::core::DeepmsgClient& client, std::int64_t now_ns);

    /// `ClusterSession.close(aeron, errorHandler, reason)`.
    void close(axc::core::DeepmsgClient& client, std::string_view reason);

    /// `ClusterSession.disconnect`.
    void disconnect(axc::core::DeepmsgClient& client);

    [[nodiscard]] bool has_response_publication() const noexcept
    {
        return nullptr != response_publication_;
    }

    [[nodiscard]] std::int64_t max_payload_length() const noexcept;

    [[nodiscard]] long try_claim(std::size_t length, aeron_buffer_claim_t& claim) noexcept;
    [[nodiscard]] long offer(const std::uint8_t* buffer, std::size_t length) noexcept;

    /// `AeronCounters.setReferenceId` (`AeronCounters.java:1648-1656`): the
    /// session counter's reference id is the publication's registration id.
    void set_counter_reference(std::int64_t reference_id) noexcept;

    void set_counter_value(std::int64_t value) noexcept;

    /// `CountersReader.REFERENCE_ID_OFFSET` from the counter value (byte
    /// offset 8; owner id is at 4, value at 0).
    static constexpr std::size_t REFERENCE_ID_OFFSET = 8;

private:
    [[maybe_unused]] std::int32_t cluster_member_id_{0};
    std::int64_t id_{0};
    std::int32_t response_stream_id_{0};
    std::string response_channel_{};
    std::string session_info_{};
    bool has_new_leader_event_pending_{false};
    bool has_open_event_pending_{true};
    std::int64_t correlation_id_{0};
    std::int64_t opened_log_position_{NULL_POSITION};
    std::int64_t closed_log_position_{NULL_POSITION};
    std::int64_t time_of_last_activity_ns_{0};
    std::int64_t ingress_image_correlation_id_{NULL_VALUE};
    SessionState state_{SessionState::init};
    std::string response_detail_{};
    codecs::EventCode::Value event_code_{codecs::EventCode::NULL_VALUE};
    codecs::CloseReason::Value close_reason_{codecs::CloseReason::NULL_VALUE};
    std::vector<std::uint8_t> encoded_principal_{};
    SessionAction action_{SessionAction::client};

    axc::core::AsyncAdd<axc::core::CounterPtr> counter_add_{};
    axc::core::AsyncAdd<axc::core::PubPtr> publication_add_{};
    axc::core::CounterPtr session_counter_{};
    axc::core::PubPtr response_publication_{};
};

}  // namespace deepmsg::cluster::consensus
