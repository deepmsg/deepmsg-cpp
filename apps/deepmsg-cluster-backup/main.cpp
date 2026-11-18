// `ClusterBackupMediaDriver` (`ClusterBackupMediaDriver.java:32-161`): the
// media driver + archive + `ClusterBackup` aggregate in one process. The
// reference always embeds the driver and the archive, so this binary has no
// separate-driver switch.
//
// Arguments follow the Aeron launch style: `-Dname=value`, `*.properties`
// files, and the Java launcher options the interop shim may pass through
// (ignored).

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
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
#include "cluster/backup/cluster_backup.h"
#include "cluster/backup/cluster_backup_context.h"
#include "cluster/backup/cluster_backup_events_listener.h"
#include "cluster/common/termination_hook.h"
#include "core/config/property_source.h"
#include "core/driver/embedded_driver.h"

namespace
{

using deepmsg::archive::server::ArchiveServer;
using deepmsg::archive::server::ArchiveThreadingMode;
using deepmsg::cluster::backup::ClusterBackup;
using deepmsg::cluster::common::TerminationCause;

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
    std::fprintf(stderr, "deepmsg-cluster-backup: %s\n", message.c_str());
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

namespace
{

/// The reference's `ClusterBackupEventsListener` (five callbacks) written to
/// stdout, one `BACKUP_EVENT` line each: the operator's view of a backup, and
/// what the interop tests read from the process's log.
class LoggingBackupEvents final : public deepmsg::cluster::backup::ClusterBackupEventsListener
{
public:
    void on_backup_query() override
    {
        line("QUERY");
    }

    void on_possible_failure(const axc::core::Error& error) override
    {
        line("FAILURE " + std::string{error.message.view()});
    }

    void on_backup_response(
        std::span<const deepmsg::cluster::common::ClusterMember> /*cluster_members*/,
        const deepmsg::cluster::common::ClusterMember& log_source_member,
        std::span<const deepmsg::cluster::common::RecordingLog::Snapshot> snapshots_to_retrieve) override
    {
        line("RESPONSE member=" + std::to_string(log_source_member.id) +
             " snapshots=" + std::to_string(snapshots_to_retrieve.size()));
    }

    void on_updated_recording_log(
        const deepmsg::cluster::common::RecordingLog& /*recording_log*/,
        std::span<const deepmsg::cluster::common::RecordingLog::Snapshot> snapshots_retrieved) override
    {
        line("RECORDING_LOG snapshots=" + std::to_string(snapshots_retrieved.size()));
    }

    void on_live_log_progress(std::int64_t recording_id, std::int32_t /*recording_pos_counter_id*/,
                              std::int64_t log_position) override
    {
        line("LIVE recordingId=" + std::to_string(recording_id) + " logPosition=" + std::to_string(log_position));
    }

private:
    static void line(const std::string& text)
    {
        std::printf("BACKUP_EVENT %s\n", text.c_str());
        std::fflush(stdout);
    }
};

}  // namespace

int main(int argc, char* argv[])
{
    install_signal_handlers();

    auto properties = axc::core::PropertySource::from_args(argc, argv);
    if (!properties)
    {
        fail(std::string{properties.error().message.view()});
    }

    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument{argv[index]};
        if (argument == "-h" || argument == "--help")
        {
            std::printf(
                "Usage: deepmsg-cluster-backup [-Dname=value ...] [properties file ...]\n"
                "  the ClusterBackupMediaDriver shape: driver + archive + backup in one process\n");
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
    if (auto loaded = properties->load_environment("DEEPMSG_", "deepmsg."); !loaded)
    {
        fail(std::string{loaded.error().message.view()});
    }
    properties->for_each(
        [](std::string_view name, std::string_view value)
        {
            if (!name.starts_with("aeron."))
            {
                return;
            }
            const std::string_view driver_value = driver_property_value(name, value);
            (void)aeron_properties_setenv(std::string{name}.c_str(), std::string{driver_value}.c_str());
        });

    auto archive_config = deepmsg::archive::server::parse_launch_config(*properties);
    if (!archive_config)
    {
        fail(std::string{archive_config.error().message.view()});
    }
    if (ArchiveThreadingMode::dedicated != archive_config->threading.mode)
    {
        fail("aeron.archive.threading.mode must be DEDICATED in the cluster backup aggregate");
    }

    if (auto prepared = deepmsg::archive::server::prepare_archive_directories(*archive_config); !prepared)
    {
        fail(std::string{prepared.error().message.view()});
    }

    auto created_driver = axc::core::driver::EmbeddedDriver::create(*properties, &on_driver_termination, nullptr);
    if (!created_driver)
    {
        fail(std::string{created_driver.error().message.view()});
    }
    std::optional<axc::core::driver::EmbeddedDriver> driver;
    driver.emplace(std::move(*created_driver));

    auto created_archive = ArchiveServer::create(*archive_config);
    if (!created_archive)
    {
        driver.reset();
        fail(std::string{created_archive.error().message.view()});
    }
    std::unique_ptr<ArchiveServer> archive = std::move(*created_archive);

    if (auto started = archive->start(); !started)
    {
        archive.reset();
        driver.reset();
        fail(std::string{started.error().message.view()});
    }

    auto config = deepmsg::cluster::backup::cluster_backup_config_from_properties(*properties);
    if (!config)
    {
        archive->close();
        archive.reset();
        driver.reset();
        fail(std::string{config.error().message.view()});
    }

    LoggingBackupEvents events;
    config->events_listener = &events;

    // `ClusterBackupMediaDriver.main` wires the three termination hooks into
    // the shutdown barrier.
    config->termination_hook = []() { g_terminated.store(true, std::memory_order_release); };
    config->extended_termination_hook = [](const TerminationCause& cause)
    {
        g_expected.store(cause.expected, std::memory_order_release);
        if (!cause.expected)
        {
            std::fprintf(stderr, "deepmsg-cluster-backup: %s\n", std::string{cause.error.message.view()}.c_str());
        }
        g_terminated.store(true, std::memory_order_release);
    };

    auto backup = ClusterBackup::launch(std::move(*config));
    if (!backup)
    {
        archive->close();
        archive.reset();
        driver.reset();
        fail(std::string{backup.error().message.view()});
    }

    std::printf("STARTED cluster backup\n");
    std::fflush(stdout);

    while (!stopped())
    {
        const timespec delay{0, 50'000'000};
        (void)::nanosleep(&delay, nullptr);
    }

    backup->close();
    archive->close();
    archive.reset();

    int exit_status = g_expected.load(std::memory_order_acquire) ? EXIT_SUCCESS : EXIT_FAILURE;
    if (driver)
    {
        if (auto closed = driver->close(); !closed)
        {
            std::fprintf(stderr, "deepmsg-cluster-backup: %s\n", std::string{closed.error().message.view()}.c_str());
            exit_status = EXIT_FAILURE;
        }
        driver.reset();
    }

    return exit_status;
}
