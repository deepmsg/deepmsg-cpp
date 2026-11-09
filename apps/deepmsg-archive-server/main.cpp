// `ArchivingMediaDriver` (`Archive.java` + the embedded driver): the SHARED,
// single-process archive the reference C suite spawns for every case.
//
// Arguments follow the Aeron launch style the reference C suite passes when this
// binary is configured as its `JAVA_EXECUTABLE`: `--add-opens <module>`,
// `-Dname=value`, `-cp <jar>` and the mapped main class. `-D` options are read
// as properties, `*.properties` arguments are loaded as files, everything else
// is ignored. (The suite spawns the archive with `posix_spawn(..., envp=NULL)`,
// i.e. an **empty environment**, so environment-driven launchers cannot be used
// for it; see records/a2b.md.)

#include <pthread.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include <sched.h>

extern "C"
{
#include "aeronmd.h"
#include "util/aeron_properties_util.h"
}

#include "archive/server/archive_launch.h"
#include "core/config/property_source.h"

namespace
{

volatile std::sig_atomic_t g_running = 1;

extern "C" void on_signal(int /*signal_number*/)
{
    g_running = 0;
}

void on_driver_termination(void* /*state*/)
{
    g_running = 0;
}

[[noreturn]] void fail(const std::string& message)
{
    std::fprintf(stderr, "deepmsg-archive-server: %s\n", message.c_str());
    std::exit(EXIT_FAILURE);
}

/// The reference test harness passes the Java termination validator class;
/// the embedded C driver's symbol table knows `allow` and `deny`
/// (`aeron_termination_validator.c:25-45`).
std::string_view driver_property_value(std::string_view name, std::string_view value) noexcept
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

/// Java launcher options that consume the following token; skipping the value
/// keeps the jar path in `-cp <jar>` from being mistaken for a property file.
bool takes_value(std::string_view option) noexcept
{
    return option == "-cp" || option == "-classpath" || option == "--class-path" || option == "--module-path" ||
           option == "-p" || option == "--add-opens" || option == "--add-exports" || option == "--add-reads" ||
           option == "--add-modules" || option == "--limit-modules" || option == "--upgrade-module-path" ||
           option == "--patch-module" || option == "--enable-native-access" || option == "-javaagent" ||
           option == "-agentlib";
}

/// `io.aeron.archive.Archive` is the standalone shape (`Archive.launch`),
/// `ArchivingMediaDriver` embeds the driver; anything else keeps the historical
/// embedded behaviour.
constexpr std::string_view STANDALONE_MAIN_CLASS{"io.aeron.archive.Archive"};

}  // namespace

int main(int argc, char** argv)
{
    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        fail(std::string{properties.error().message.view()});
    }

    bool standalone = false;
    for (int index = 1; index < argc; index++)
    {
        const std::string_view argument{argv[index]};
        if (argument == STANDALONE_MAIN_CLASS)
        {
            standalone = true;
            continue;
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

        // A `*.properties` file (the Aeron launch style); every other positional
        // argument is the mapped main class or a jar path.
        if (argument.size() > 11 && argument.substr(argument.size() - 11) == ".properties")
        {
            if (auto loaded = properties->load_file(argument); !loaded)
            {
                fail(std::string{loaded.error().message.view()});
            }
        }
    }
    (void)properties->load_environment("AERON_");

    // The embedded driver reads its configuration from the environment
    // (`aeron_driver_context_init`), exactly as `aeronmd` does with `-D`.
    properties->for_each(
        [](std::string_view name, std::string_view value)
        {
            const std::string_view driver_value = driver_property_value(name, value);
            (void)aeron_properties_setenv(std::string{name}.c_str(), std::string{driver_value}.c_str());
        });

    auto config = deepmsg::archive::server::parse_launch_config(*properties);
    if (!config)
    {
        fail(std::string{config.error().message.view()});
    }

    if (auto prepared = deepmsg::archive::server::prepare_archive_directories(*config); !prepared)
    {
        fail(std::string{prepared.error().message.view()});
    }

    std::signal(SIGINT, &on_signal);
    std::signal(SIGTERM, &on_signal);

    // The embedded shape starts the driver in this process; the standalone
    // shape (`io.aeron.archive.Archive`) connects to the driver already running
    // in `aeron.dir` and never closes it.
    aeron_driver_context_t* driver_context = nullptr;
    aeron_driver_t* driver = nullptr;
    if (!standalone)
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
        // The driver runs its agents on its own threads.
        if (0 != aeron_driver_start(driver, false))
        {
            fail(std::string{"driver start: "} + aeron_errmsg());
        }
    }

    auto created = deepmsg::archive::server::ArchiveServer::create(*config);
    if (!created)
    {
        if (!standalone)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{created.error().message.view()});
    }
    std::unique_ptr<deepmsg::archive::server::ArchiveServer> server = std::move(*created);

    if (auto started = server->start(); !started)
    {
        server.reset();
        if (!standalone)
        {
            (void)aeron_driver_close(driver);
            (void)aeron_driver_context_close(driver_context);
        }
        fail(std::string{started.error().message.view()});
    }

    if (deepmsg::archive::server::ArchiveThreadingMode::dedicated == server->threading_mode())
    {
        // `DedicatedModeArchiveConductor`: the three agents have their own
        // threads; the launcher only keeps the mark file alive and waits for a
        // signal.
        while (0 != g_running)
        {
            (void)server->do_work();
            ::usleep(10000);
        }
    }
    else
    {
        if (deepmsg::archive::server::ArchiveThreadingMode::shared == server->threading_mode())
        {
            // `Archive.java:148-160`: SHARED runs the conductor on a named
            // thread; this binary's caller loop is that thread.
            const char* name = deepmsg::archive::server::thread_name(config->threading.naming,
                                                                     deepmsg::archive::server::AgentRole::shared)
                                   .data();
            (void)::pthread_setname_np(::pthread_self(), std::string{name}.substr(0, 15).c_str());
        }
        axc::core::IdleStrategy idle = server->idle_strategy();
        while (0 != g_running)
        {
            idle.idle(server->do_work());
        }
    }

    server->close();
    server.reset();

    int exit_status = EXIT_SUCCESS;
    if (!standalone && 0 != aeron_driver_close(driver))
    {
        std::fprintf(stderr, "deepmsg-archive-server: driver close: %s\n", aeron_errmsg());
        exit_status = EXIT_FAILURE;
    }
    if (!standalone && 0 != aeron_driver_context_close(driver_context))
    {
        std::fprintf(stderr, "deepmsg-archive-server: driver context close: %s\n", aeron_errmsg());
        exit_status = EXIT_FAILURE;
    }

    return exit_status;
}
