#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "cluster/service/cluster.h"
#include "cluster/service/clustered_service.h"
#include "cluster/service/snapshot.h"

namespace deepmsg::apps
{

/// The `echo` business (`EchoClusteredService.java` plus the reference test
/// business's service-message round trip, `TestNode.java:669-690`).
///
/// `Echo as Service IPC ingress` is offered back into the cluster; the service
/// message that comes out of the log (no client session) is echoed to every
/// session. Any other message is echoed to the session that sent it. A 16-byte
/// message is a timer command (`command: i32 @0`, `correlationId: i64 @4`,
/// `delayMillis: i32 @12`); a fired timer echoes its 8-byte correlation id to
/// the session that scheduled it.
class EchoService
{
public:
    std::expected<void, deepmsg::cluster::service::Error> on_start(deepmsg::cluster::service::Cluster&,
                                                                   deepmsg::cluster::service::SnapshotImage*);
    std::expected<void, deepmsg::cluster::service::Error> on_session_open(deepmsg::cluster::service::ClientSession&,
                                                                          std::int64_t);
    std::expected<void, deepmsg::cluster::service::Error> on_session_close(deepmsg::cluster::service::ClientSession&,
                                                                           std::int64_t,
                                                                           deepmsg::cluster::service::CloseReason);
    std::expected<void, deepmsg::cluster::service::Error> on_session_message(
        deepmsg::cluster::service::Cluster&, deepmsg::cluster::service::ClientSession*, std::int64_t,
        std::span<const std::uint8_t>, const deepmsg::cluster::service::LogHeader&);
    std::expected<void, deepmsg::cluster::service::Error> on_timer_event(deepmsg::cluster::service::Cluster&,
                                                                         std::int64_t, std::int64_t);
    std::expected<void, deepmsg::cluster::service::Error> on_take_snapshot(
        deepmsg::cluster::service::SnapshotPublication&);
    std::expected<void, deepmsg::cluster::service::Error> on_role_change(deepmsg::cluster::service::Role);
    std::expected<void, deepmsg::cluster::service::Error> on_terminate(deepmsg::cluster::service::Cluster&);

private:
    using Cluster = deepmsg::cluster::service::Cluster;
    using ClientSession = deepmsg::cluster::service::ClientSession;
    using SessionId = deepmsg::cluster::service::SessionId;
    using Error = deepmsg::cluster::service::Error;

    struct PendingTimer
    {
        std::int64_t correlation_id;
        bool has_session;
        std::int64_t session_id;
    };

    [[nodiscard]] static std::expected<void, Error> echo_to_session(Cluster& cluster, SessionId session,
                                                                    std::span<const std::uint8_t> payload);
    [[nodiscard]] std::expected<void, Error> send_service_message(Cluster& cluster, ClientSession* session,
                                                                  std::span<const std::uint8_t> payload);

    std::vector<PendingTimer> pending_{};
};

}  // namespace deepmsg::apps
