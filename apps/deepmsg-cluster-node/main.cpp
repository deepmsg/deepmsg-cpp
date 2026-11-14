// `ClusteredMediaDriver` / `ClusteredArchive`
// (`ClusteredMediaDriver.java:51-64`, `ClusteredArchive.java:46-59`): the
// archive + consensus module aggregate, with the media driver embedded
// (`--embedded-driver`, the ClusteredMediaDriver shape) or external (the
// ClusteredArchive shape).
//
// The service container is NOT part of this aggregate: the reference keeps
// `ClusteredServiceContainer` as its own main, and so does this repository.
//
// Arguments follow the Aeron launch style: `-Dname=value`, `*.properties`
// files, `--embedded-driver`, and the Java launcher options the interop shim
// may pass through (ignored).

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>

#include <sched.h>
#include <time.h>

extern "C"
{
#include "aeronmd.h"
#include "util/aeron_properties_util.h"
}

#include "archive/server/archive_launch.h"
#include "cluster/consensus/consensus_module.h"
#include "cluster/consensus/consensus_module_context.h"
#include "core/config/property_source.h"

namespace
{

using deepmsg::archive::server::ArchiveLaunchConfig;
using deepmsg::archive::server::ArchiveServer;
using deepmsg::archive::server::ArchiveThreadingMode;
using deepmsg::cluster::common::TerminationCause;
using deepmsg::cluster::consensus::ConsensusModule;

volatile std::sig_atomic_t g_signal_stop = 0;
std::atomic<bool> g_terminated{false};
std::atomic<bool> g_expected{true};

extern "C" void on_signal(int /*signal_number*/)
{
    g_signal_stop = 1;
}

void on_driver_termination(void* /*state*/)
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

[[noreturn]] void fail(const std::string& message)
{
    std::fprintf(stderr, "deepmsg-cluster-node: %s\n", message.c_str());
    std::exit(EXIT_FAILURE);
}

/// Java launcher options that consume the following token (the interop shim
/// may leave them in place when a binary is used as a Java main).
[[nodiscard]] bool takes_value(std::string_view option) noexcept
{
    return option == "-cp" || option == "-classpath" || option == "--class-path" || option == "--module-path" ||
           option == "-p" || option == "--add-opens" || option == "--add-exports" || option == "--add-reads" ||
           option == "--add-modules" || option == "--limit-modules" || option == "--upgrade-module-path" ||
           option == "--patch-module" || option == "--enable-native-access" || option == "-javaagent" ||
           option == "-agentlib";
}

/// The reference C driver's termination validator names
/// (`aeron_termination_validator.c:25-45`).
[[nodiscard]] std::string_view driver_property_value(std::string_view name, std::string_view value) noexcept
{
    if (name == "aeron.driver.termination.validator")
    {
        if (value == "io.aeron.driver.DefaultAllowTerminationValidator")
        {
            return "allow";
        }
        if (value == "io.aeron.driver.DefaultDenyTerminationValidator")
        {
            return "deny";
        }
    }
    return value;
}

}  // namespace

int main(int argc, char* argv[])
{
    install_signal_handlers();

    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        fail(std::string{properties.error().message.view()});
    }

    bool embedded_driver = false;
    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument{argv[index]};
        if (argument == "--embedded-driver")
        {
            embedded_driver = true;
            continue;
        }
        if (argument == "-h" || argument == "--help")
        {
            std::printf(
                "Usage: deepmsg-cluster-node [--embedded-driver] [-Dname=value ...] [properties file ...]\n"
                "  --embedded-driver  the ClusteredMediaDriver shape (driver + archive + CM)\n"
                "  (default)          the ClusteredArchive shape (external driver + archive + CM)\n");
            return 0;
        }
        if (argument.empty() || argument.substr(0, 2) == "-D")
        {
            // Consumed by `PropertySource::from_args`.
            continue;
        }
        if (argument[0] == '-')
        {
            if (takes_value(argument) && index + 1 < argc)
            {
                index++;
            }
            continue;
        }
        if (argument.size() > 11 && argument.substr(argument.size() - 11) == ".properties")
        {
            auto loaded = properties->load_file(argument);
            if (!loaded)
            {
                fail(std::string{loaded.error().message.view()});
            }
        }
    }
    (void)properties->load_environment("AERON_");

    // The embedded driver reads its configuration from the environment, as
    // `aeronmd` does with `-D` (same mapping as the archive server binary).
    if (embedded_driver)
    {
        properties->for_each(
            [](std::string_view name, std::string_view value)
            {
                const std::string_view driver_value = driver_property_value(name, value);
                (void)aeron_properties_setenv(std::string{name}.c_str(), std::string{driver_value}.c_str());
            });
    }

    auto archive_config = deepmsg::archive::server::parse_launch_config(*properties);
    if (!archive_config)
    {
        fail(std::string{archive_config.error().message.view()});
    }
    if (ArchiveThreadingMode::dedicated != archive_config->threading.mode)
    {
        // A SHARED/INVOKER archive must be driven by the caller, but the CM's
        // `on_start` blocks this thread while it joins the archive. Refuse the
        // combination rather than deadlock; the reference default is DEDICATED.
        fail("aeron.archive.threading.mode must be DEDICATED in the cluster node aggregate");
    }

    if (auto prepared = deepmsg::archive::server::prepare_archive_directories(*archive_config); !prepared)
    {
        fail(std::string{prepared.error().message.view()});
    }

    aeron_driver_context_t* driver_context = nullptr;
    aeron_driver_t* driver = nullptr;
    if (embedded_driver)
    {
        if (0 != aeron_driver_context_init(&driver_context))
        {
            fail(std::string{"driver context init: "} + aeron_errmsg());
        }
        (void)aeron_driver_context_set_driver_termination_hook(driver_context, &on_driver_termination, nullptr);
        if (0 != aeron_driver_init(&driver, driver_context))
        {
            fail(std::string{"driver init: "} + aeron_errmsg());
        }
        if (0 != aeron_driver_start(driver, false))
        {
            fail(std::string{"driver start: "} + aeron_errmsg());
        }
    }

    auto created = ArchiveServer::create(*archive_config);
    if (!created)
    {
        if (embedded_driver)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{created.error().message.view()});
    }
    std::unique_ptr<ArchiveServer> archive = std::move(*created);

    if (auto started = archive->start(); !started)
    {
        archive.reset();
        if (embedded_driver)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{started.error().message.view()});
    }

    auto config = deepmsg::cluster::consensus::consensus_module_config_from_properties(*properties);
    if (!config)
    {
        archive->close();
        archive.reset();
        if (embedded_driver)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{config.error().message.view()});
    }

    // The aggregate's three termination hooks all wake the process
    // (`ClusteredMediaDriver.java:55-59`); the archive has none exposed, so the
    // driver hook and SIGINT/SIGTERM cover that side.
    config->termination_hook = []() { g_terminated.store(true, std::memory_order_release); };
    config->extended_termination_hook = [](const TerminationCause& cause)
    {
        g_expected.store(cause.expected, std::memory_order_release);
        if (!cause.expected)
        {
            std::fprintf(stderr, "deepmsg-cluster-node: %s\n", std::string{cause.error.message.view()}.c_str());
        }
        g_terminated.store(true, std::memory_order_release);
    };

    auto module = ConsensusModule::launch(std::move(*config));
    if (!module)
    {
        archive->close();
        archive.reset();
        if (embedded_driver)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{module.error().message.view()});
    }

    std::printf("STARTED cluster node\n");
    std::fflush(stdout);

    while (!stopped())
    {
        const timespec delay{0, 50'000'000};
        (void)::nanosleep(&delay, nullptr);
    }

    module->close();
    archive->close();
    archive.reset();

    int exit_status = g_expected.load(std::memory_order_acquire) ? EXIT_SUCCESS : EXIT_FAILURE;
    if (embedded_driver)
    {
        if (0 != aeron_driver_close(driver))
        {
            std::fprintf(stderr, "deepmsg-cluster-node: driver close: %s\n", aeron_errmsg());
            exit_status = EXIT_FAILURE;
        }
        if (0 != aeron_driver_context_close(driver_context))
        {
            std::fprintf(stderr, "deepmsg-cluster-node: driver context close: %s\n", aeron_errmsg());
            exit_status = EXIT_FAILURE;
        }
    }

    return exit_status;
}
