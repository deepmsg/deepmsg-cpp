// `ConsensusModule.main` (`ConsensusModule.java:267-277`): one consensus module
// process against an external media driver and archive.
//
// Arguments are `-Dname=value` and `*.properties` files (the Aeron launch
// style). The reference's `main` waits on a shutdown barrier that the
// termination hook signals; this binary does the same with a flag, and adds the
// SIGINT/SIGTERM graceful stop the other deepmsg binaries already use.

#include <atomic>
#include <csignal>
#include <cstdio>
#include <string>
#include <string_view>

#include <time.h>

#include "cluster/consensus/consensus_module.h"
#include "cluster/consensus/consensus_module_context.h"
#include "core/config/property_source.h"

namespace
{

using deepmsg::cluster::common::TerminationCause;
using deepmsg::cluster::consensus::consensus_module_config_from_properties;
using deepmsg::cluster::consensus::ConsensusModule;

volatile std::sig_atomic_t g_signal_stop = 0;
std::atomic<bool> g_terminated{false};
std::atomic<bool> g_expected{true};

extern "C" void on_signal(int /*signal_number*/)
{
    g_signal_stop = 1;
}

void install_signal_handlers() noexcept
{
    struct sigaction action{};
    action.sa_handler = &on_signal;
    (void)sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    (void)::sigaction(SIGINT, &action, nullptr);
    (void)::sigaction(SIGTERM, &action, nullptr);
}

[[nodiscard]] bool stopped() noexcept
{
    return 0 != g_signal_stop || g_terminated.load(std::memory_order_acquire);
}

}  // namespace

int main(int argc, char* argv[])
{
    install_signal_handlers();

    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        std::fprintf(stderr, "deepmsg-consensus-module: %s\n", std::string{properties.error().message.view()}.c_str());
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
            std::printf("Usage: deepmsg-consensus-module [-Dname=value ...] [properties file ...]\n");
            return 0;
        }
        if (!argument.empty() && '-' == argument[0])
        {
            std::fprintf(stderr, "deepmsg-consensus-module: unknown option '%s'\n", argv[index]);
            return 1;
        }
        auto loaded = properties->load_file(argument);
        if (!loaded)
        {
            std::fprintf(stderr, "deepmsg-consensus-module: %s\n", std::string{loaded.error().message.view()}.c_str());
            return 1;
        }
    }

    auto config = consensus_module_config_from_properties(*properties);
    if (!config)
    {
        std::fprintf(stderr, "deepmsg-consensus-module: the configuration is refused: %s\n",
                     std::string{config.error().message.view()}.c_str());
        return 1;
    }

    // `ConsensusModule.main`'s `terminationHook(barrier::signalAll)`: the agent's
    // termination wakes the process.
    config->termination_hook = []() { g_terminated.store(true, std::memory_order_release); };
    config->extended_termination_hook = [](const TerminationCause& cause)
    {
        g_expected.store(cause.expected, std::memory_order_release);
        if (!cause.expected)
        {
            std::fprintf(stderr, "deepmsg-consensus-module: %s\n", std::string{cause.error.message.view()}.c_str());
        }
        g_terminated.store(true, std::memory_order_release);
    };

    auto module = ConsensusModule::launch(std::move(*config));
    if (!module)
    {
        std::fprintf(stderr, "deepmsg-consensus-module: %s\n", std::string{module.error().message.view()}.c_str());
        return 1;
    }

    std::printf("STARTED consensus module\n");
    std::fflush(stdout);

    while (!stopped())
    {
        const timespec delay{0, 50'000'000};
        (void)::nanosleep(&delay, nullptr);
    }

    module->close();

    return g_expected.load(std::memory_order_acquire) ? 0 : 1;
}
