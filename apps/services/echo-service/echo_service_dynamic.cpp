// The `.so` form of the echo business (Q8): a factory symbol and nothing else.
// The container binary loads it with `deepmsg.cluster.service.library=<path>`.

#include "apps/services/echo-service/echo_service.h"
#include "cluster/service/dynamic_service.h"

#if defined(__GNUC__)
#define AXC_DYNAMIC_EXPORT extern "C" __attribute__((visibility("default")))
#else
#define AXC_DYNAMIC_EXPORT extern "C"
#endif

AXC_DYNAMIC_EXPORT deepmsg::cluster::service::ClusteredServiceBase* deepmsg_cluster_service_create()
{
    return new deepmsg::cluster::service::DynamicServiceAdapter<deepmsg::apps::EchoService>{};
}
