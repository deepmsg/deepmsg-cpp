#pragma once

#include <concepts>
#include <cstdint>
#include <expected>
#include <span>

#include "cluster/service/role.h"
#include "core/error/error.h"
#include "io_aeron_cluster_codecs/CloseReason.h"

namespace deepmsg::cluster::service
{

using CloseReason = ::io::aeron::cluster::codecs::CloseReason::Value;

/// A cluster session's id (`ClientSession.id()`).
struct SessionId
{
    std::int64_t value{0};

    [[nodiscard]] friend constexpr bool operator==(const SessionId&, const SessionId&) noexcept = default;
};

/// The Aeron header a session message arrived with (`Header.java`), reduced to
/// what a business can act on.
struct LogHeader
{
    std::int64_t position{0};
    std::int32_t session_id{0};
    std::int32_t stream_id{0};
};

/// A `NewLeadershipTermEvent(24)`, as the business sees it.
struct NewLeadershipTermEvent
{
    std::int64_t leadership_term_id{-1};
    std::int64_t log_position{-1};
    std::int64_t timestamp{0};
    std::int64_t term_base_log_position{0};
    std::int32_t leader_member_id{-1};
    std::int32_t log_session_id{0};
    TimeUnit time_unit{TimeUnit::milliseconds};
    std::int32_t app_version{0};
};

class Cluster;
class ClientSession;
class SnapshotPublication;
class SnapshotImage;

/// `ClusteredService` (`ClusteredService.java:36-167`).
///
/// Session callbacks receive the session view itself (as Java does); a service
/// message has no client session, hence `ClientSession*`. A callback that fails
/// returns the error instead of throwing, and the container terminates on it.
template <class S>
concept ClusteredService = requires(S& service, Cluster& cluster, ClientSession& session, ClientSession* maybe_session,
                                    std::span<const std::uint8_t> message, const LogHeader& header,
                                    std::int64_t timestamp, std::int64_t correlation_id, SnapshotPublication& writer,
                                    SnapshotImage* image, CloseReason reason, Role role) {
    { service.on_start(cluster, image) } -> std::same_as<std::expected<void, Error>>;
    { service.on_session_open(session, timestamp) } -> std::same_as<std::expected<void, Error>>;
    { service.on_session_close(session, timestamp, reason) } -> std::same_as<std::expected<void, Error>>;
    {
        service.on_session_message(cluster, maybe_session, timestamp, message, header)
    } -> std::same_as<std::expected<void, Error>>;
    { service.on_timer_event(cluster, correlation_id, timestamp) } -> std::same_as<std::expected<void, Error>>;
    { service.on_take_snapshot(writer) } -> std::same_as<std::expected<void, Error>>;
    { service.on_role_change(role) } -> std::same_as<std::expected<void, Error>>;
    { service.on_terminate(cluster) } -> std::same_as<std::expected<void, Error>>;
};

/// The optional extension callbacks, detected without requiring them.
template <class S>
concept HasNewLeadershipTermEvent = requires(S& service, Cluster& cluster, const NewLeadershipTermEvent& event) {
    { service.on_new_leadership_term_event(cluster, event) } -> std::same_as<std::expected<void, Error>>;
};

template <class S>
concept HasBackgroundWork = requires(S& service, Cluster& cluster, std::int64_t now_ns) {
    { service.do_background_work(cluster, now_ns) } -> std::same_as<int>;
};

}  // namespace deepmsg::cluster::service
