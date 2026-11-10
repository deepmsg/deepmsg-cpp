#pragma once

#include "cluster/service/clustered_service.h"
#include "cluster/service/snapshot.h"

namespace deepmsg::apps
{

/// The container with no business state in the way.
class NoopService
{
public:
    std::expected<void, deepmsg::cluster::service::Error> on_start(deepmsg::cluster::service::Cluster&,
                                                                   deepmsg::cluster::service::SnapshotImage*)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_session_open(deepmsg::cluster::service::ClientSession&,
                                                                          std::int64_t)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_session_close(deepmsg::cluster::service::ClientSession&,
                                                                           std::int64_t,
                                                                           deepmsg::cluster::service::CloseReason)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_session_message(
        deepmsg::cluster::service::Cluster&, deepmsg::cluster::service::ClientSession*, std::int64_t,
        std::span<const std::uint8_t>, const deepmsg::cluster::service::LogHeader&)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_timer_event(deepmsg::cluster::service::Cluster&,
                                                                         std::int64_t, std::int64_t)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_take_snapshot(
        deepmsg::cluster::service::SnapshotPublication&)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_role_change(deepmsg::cluster::service::Role)
    {
        return {};
    }

    std::expected<void, deepmsg::cluster::service::Error> on_terminate(deepmsg::cluster::service::Cluster&)
    {
        return {};
    }
};

}  // namespace deepmsg::apps
