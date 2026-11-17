// The media driver on its own (`aeronmd`'s shape): no archive, no consensus
// module, just the C driver and the media it was compiled with.
//
// Arguments follow the Aeron launch style this repository's binaries already
// accept: `-Dname=value` properties and `*.properties` files. Everything under
// `aeron.*` is handed to the C driver's environment; everything under
// `deepmsg.dpdk.*` is the media's own settings.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

#include <unistd.h>

extern "C"
{
#include "aeronmd.h"
#include "util/aeron_properties_util.h"
}

#include "core/config/property_source.h"
#include "core/driver/embedded_driver.h"

namespace
{

volatile std::sig_atomic_t g_running = 1;

extern "C" void on_signal(int /*signal_number*/)
{
    g_running = 0;
}

extern "C" void on_termination(void* /*state*/)
{
    g_running = 0;
}

[[noreturn]] void fail(const std::string& message)
{
    std::fprintf(stderr, "deepmsg-media-driver: %s\n", message.c_str());
    std::exit(EXIT_FAILURE);
}

}  // namespace

int main(int argc, char** argv)
{
    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        fail(std::string{properties.error().message.view()});
    }

    for (int index = 1; index < argc; index++)
    {
        const std::string_view argument{argv[index]};
        if (argument.empty() || argument.substr(0, 2) == "-D")
        {
            // Consumed by `PropertySource::from_args`.
            continue;
        }
        if (argument[0] == '-')
        {
            // No Java launcher options: this binary has no mapped main class.
            continue;
        }
        if (argument.size() > 11 && argument.substr(argument.size() - 11) == ".properties")
        {
            if (auto loaded = properties->load_file(argument); !loaded)
            {
                fail(std::string{loaded.error().message.view()});
            }
        }
    }

    if (auto loaded = properties->load_environment("AERON_"); !loaded)
    {
        fail(std::string{loaded.error().message.view()});
    }
    if (auto loaded = properties->load_environment("DEEPMSG_", "deepmsg."); !loaded)
    {
        fail(std::string{loaded.error().message.view()});
    }

    // All four of the reference's threading modes are served. INVOKER is the
    // one that cannot start a runner of its own: the driver refuses it unless
    // the caller runs the first agent (`aeron_driver.c:1380-1386`), and the
    // reference's own launcher answers by running the main loop itself
    // (`aeronmd.c:153-168`). The other three start that runner on a thread and
    // this loop only waits, which is how this binary has always run them.
    const std::string_view threading_mode = properties->get("aeron.threading.mode").value_or("DEDICATED");
    const bool invoker = "INVOKER" == threading_mode;

    // The reference's own namespace goes back into the environment the C driver
    // reads (`aeron_driver_context_init`); `deepmsg.*` stays ours.
    properties->for_each(
        [](std::string_view name, std::string_view value)
        {
            if (!name.starts_with("aeron."))
            {
                return;
            }
            (void)aeron_properties_setenv(std::string{name}.c_str(), std::string{value}.c_str());
        });

    std::signal(SIGINT, &on_signal);
    std::signal(SIGTERM, &on_signal);

    auto driver = axc::core::driver::EmbeddedDriver::create(
        *properties, &on_termination, nullptr,
        invoker ? axc::core::driver::StartMode::caller_thread : axc::core::driver::StartMode::own_threads);
    if (!driver)
    {
        fail(std::string{driver.error().message.view()});
    }

    while (0 != g_running)
    {
        if (invoker)
        {
            // `aeronmd.c:165-168`: with a manual main loop the calling thread
            // *is* the driver's first agent, so it must not sleep.
            aeron_driver_main_idle_strategy(driver->driver(), aeron_driver_main_do_work(driver->driver()));
        }
        else
        {
            ::usleep(10000);
        }
    }

    if (auto closed = driver->close(); !closed)
    {
        fail(std::string{closed.error().message.view()});
    }
    return EXIT_SUCCESS;
}
