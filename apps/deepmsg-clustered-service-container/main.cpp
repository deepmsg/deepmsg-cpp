// One service container process: the counterpart of
// `io.aeron.cluster.service.ClusteredServiceContainer.main`.
//
// Arguments are properties files; `-Dname=value` is accepted too (applied
// first, so a file overrides it). The business is selected by
// `deepmsg.cluster.service` from the compiled-in registry (`echo`,
// `basic-auction`, `noop`), or by `deepmsg.cluster.service.library=<path>` for
// a `.so` exporting `deepmsg_cluster_service_create`. A Java class name is
// refused by name. The media driver and the archive are separate processes.

#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "apps/services/basic-auction-service/basic_auction_service.h"
#include "apps/services/echo-service/echo_service.h"
#include "apps/services/noop-service/noop_service.h"
#include "cluster/service/clustered_service_agent.h"
#include "cluster/service/container_config.h"
#include "cluster/service/dynamic_service.h"
#include "core/agent/agent_runner.h"
#include "core/agent/idle_strategy.h"
#include "core/config/property_source.h"
#include "core/error/error_handler.h"

namespace
{

using deepmsg::cluster::service::ClusteredServiceAgent;
using deepmsg::cluster::service::DynamicService;
using deepmsg::cluster::service::ServiceContainerConfig;
using deepmsg::cluster::service::ServiceLibrary;

volatile std::sig_atomic_t g_stop = 0;

void handle_signal(int) noexcept
{
    g_stop = 1;
}

void install_signal_handlers() noexcept
{
    struct sigaction action{};
    action.sa_handler = &handle_signal;
    (void)sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    (void)::sigaction(SIGINT, &action, nullptr);
    (void)::sigaction(SIGTERM, &action, nullptr);
}

[[nodiscard]] bool is_termination_message(std::string_view message) noexcept
{
    return std::string::npos != message.find("service terminated") ||
           std::string::npos != message.find("shutdown requested");
}

template <class S>
int run_agent(const ServiceContainerConfig& config, S service)
{
    ClusteredServiceAgent<S> agent{config, std::move(service)};

    auto idle = axc::core::IdleStrategy::by_agrona_name(config.idle_strategy);
    if (!idle)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: aeron.cluster.idle.strategy=%s names no strategy\n",
                     config.idle_strategy.c_str());
        return 1;
    }

    axc::core::NoOpErrorHandler errors;
    axc::core::AgentRunner<ClusteredServiceAgent<S>> runner{agent, *idle, errors};

    auto started = runner.start();
    if (!started)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: %s\n",
                     std::string{started.error().message.view()}.c_str());
        return 1;
    }

    std::printf("STARTED serviceId=%d\n", config.service_id);
    std::fflush(stdout);

    while (0 == g_stop && !agent.is_terminated())
    {
        const timespec delay{0, 50'000'000};
        (void)::nanosleep(&delay, nullptr);
    }

    if (0 != g_stop)
    {
        agent.request_shutdown();
    }
    runner.close();

    if (agent.is_terminated() && agent.termination_error().has_value())
    {
        const std::string message{agent.termination_error()->message.view()};
        if (!is_termination_message(message))
        {
            std::fprintf(stderr, "deepmsg-clustered-service-container: %s\n", message.c_str());
            return 1;
        }
    }
    return 0;
}

[[nodiscard]] int run_dynamic(const ServiceContainerConfig& config, std::string_view library_path)
{
    auto library = ServiceLibrary::open(library_path);
    if (!library)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: %s\n",
                     std::string{library.error().message.view()}.c_str());
        return 1;
    }

    deepmsg::cluster::service::ClusteredServiceBase* service = library->factory()();
    if (nullptr == service)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: %s returned null\n",
                     std::string{library_path}.c_str());
        return 1;
    }

    return run_agent(config, DynamicService{std::unique_ptr<deepmsg::cluster::service::ClusteredServiceBase>(service)});
}

}  // namespace

int main(int argc, char* argv[])
{
    install_signal_handlers();

    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: %s\n",
                     std::string{properties.error().message.view()}.c_str());
        return 1;
    }

    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument{argv[index]};
        if (0 == argument.rfind("-D", 0))
        {
            continue;
        }
        if ("-h" == argument || "--help" == argument)
        {
            std::printf(
                "Usage: deepmsg-clustered-service-container [-Dname=value ...] [properties file ...]\n"
                "  deepmsg.cluster.service=echo|basic-auction|noop\n"
                "  deepmsg.cluster.service.library=<path to a .so exporting %s>\n",
                std::string{deepmsg::cluster::service::SERVICE_FACTORY_SYMBOL}.c_str());
            return 0;
        }
        if (0 == argument.rfind('-', 0))
        {
            std::fprintf(stderr, "deepmsg-clustered-service-container: unknown option '%s'\n", argv[index]);
            return 1;
        }
        auto loaded = properties->load_file(argument);
        if (!loaded)
        {
            std::fprintf(stderr, "deepmsg-clustered-service-container: %s\n",
                         std::string{loaded.error().message.view()}.c_str());
            return 1;
        }
    }

    auto config = deepmsg::cluster::service::container_config_from_properties(*properties);
    if (!config)
    {
        std::fprintf(stderr, "deepmsg-clustered-service-container: the configuration is refused: %s\n",
                     std::string{config.error().message.view()}.c_str());
        return 1;
    }

    if (const auto library = properties->get("deepmsg.cluster.service.library"); library.has_value())
    {
        return run_dynamic(*config, *library);
    }

    const std::string name = config->service.value_or("");
    if (0 == name.compare("echo"))
    {
        return run_agent(*config, deepmsg::apps::EchoService{});
    }
    if (0 == name.compare("basic-auction"))
    {
        return run_agent(*config, deepmsg::apps::BasicAuctionService{});
    }
    if (0 == name.compare("noop"))
    {
        return run_agent(*config, deepmsg::apps::NoopService{});
    }

    std::fprintf(stderr,
                 "deepmsg-clustered-service-container: deepmsg.cluster.service='%s' names no business; "
                 "it must be echo, basic-auction or noop (or set deepmsg.cluster.service.library)\n",
                 name.c_str());
    return 1;
}
