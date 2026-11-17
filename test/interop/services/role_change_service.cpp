// C6c: a business whose only job is recording `on_role_change` (the
// reference's `TestNode.TestService.roleChangedTo()`) so the scenario can read
// it from outside the container. Loaded through
// `deepmsg.cluster.service.library`, like `echo_service_dynamic`.

#include <cstdint>
#include <cstdlib>
#include <expected>
#include <fstream>
#include <span>
#include <string>

#include "cluster/service/cluster.h"
#include "cluster/service/clustered_service.h"
#include "cluster/service/dynamic_service.h"

#if defined(__GNUC__)
#define AXC_DYNAMIC_EXPORT extern "C" __attribute__((visibility("default")))
#else
#define AXC_DYNAMIC_EXPORT extern "C"
#endif

namespace
{

namespace service = deepmsg::cluster::service;

class RoleChangeService
{
public:
    std::expected<void, service::Error> on_start(service::Cluster&, service::SnapshotImage*)
    {
        return {};
    }

    std::expected<void, service::Error> on_session_open(service::ClientSession&, std::int64_t)
    {
        return {};
    }

    std::expected<void, service::Error> on_session_close(service::ClientSession&, std::int64_t, service::CloseReason)
    {
        return {};
    }

    std::expected<void, service::Error> on_session_message(service::Cluster&, service::ClientSession*, std::int64_t,
                                                           std::span<const std::uint8_t>, const service::LogHeader&)
    {
        return {};
    }

    std::expected<void, service::Error> on_timer_event(service::Cluster&, std::int64_t, std::int64_t)
    {
        return {};
    }

    std::expected<void, service::Error> on_take_snapshot(service::SnapshotPublication&)
    {
        return {};
    }

    /// Append one line per callback: `<role code>`. The fixture sets
    /// `AXC_TEST_ROLE_FILE` per container (the callback can run before
    /// `on_start`, so the service has no cluster context to ask for a member id).
    std::expected<void, service::Error> on_role_change(service::Role role)
    {
        const char* file = std::getenv("AXC_TEST_ROLE_FILE");
        if (nullptr != file)
        {
            std::ofstream out{file, std::ios::app};
            out << static_cast<std::int32_t>(role) << "\n";
        }
        return {};
    }

    std::expected<void, service::Error> on_terminate(service::Cluster&)
    {
        return {};
    }
};

}  // namespace

AXC_DYNAMIC_EXPORT deepmsg::cluster::service::ClusteredServiceBase* deepmsg_cluster_service_create()
{
    return new deepmsg::cluster::service::DynamicServiceAdapter<RoleChangeService>{};
}
