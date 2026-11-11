#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "archive/server/archive_conductor.h"
#include "archive/server/archive_config.h"
#include "archive/server/archive_mark_file.h"
#include "archive/server/authenticator.h"
#include "archive/server/catalog.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/threading.h"
#include "core/agent/agent_runner.h"
#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `Archive.Context` for the SHARED single-process binary
/// (`Archive.java:1140-1400`), read from the reference property names.
struct ArchiveLaunchConfig
{
    /// Empty ⇒ the driver/client default (`/dev/shm/aeron-<user>`).
    std::string aeron_dir;
    std::string archive_dir;
    /// Empty ⇒ `archive_dir` (`Archive.java:1325-1330`).
    std::string mark_file_dir;
    std::string control_channel{"aeron:udp?endpoint=localhost:8010"};
    std::int32_t control_stream_id{10};
    /// `aeron.archive.control.channel.enabled` (`Archive.java:692`).
    bool control_channel_enabled{true};
    /// `aeron.archive.control.response.channel` / `.stream.id`
    /// (`AeronArchive.java:2752-2758`): the archive client's response
    /// channel. Empty ⇒ derived from `control_channel`
    /// (`Archive.java:1587-1610`).
    std::string control_response_channel;
    std::int32_t control_response_stream_id{20};
    /// `aeron.archive.message.timeout` / `.retry.attempts`
    /// (`AeronArchive.java:2674-2692`).
    std::int64_t message_timeout_ms{10'000};
    std::int32_t message_retry_attempts{3};
    std::string local_control_channel{"aeron:ipc?term-length=64k"};
    std::int32_t local_control_stream_id{10};
    /// `aeron.archive.replication.channel` (`Archive.java:613`, `:1301-1303`):
    /// the fallback channel when a `ReplicateRequest2` names none. Empty ⇒ a
    /// replicate request without its own channel is refused.
    std::string replication_channel;
    std::int64_t archive_id{-1};
    bool delete_archive_on_start{false};
    std::int64_t connect_timeout_ms{5000};
    std::int64_t session_liveness_check_interval_ms{1000};
    std::int32_t control_term_buffer_length{64 * 1024};
    std::int32_t control_mtu_length{1408};
    bool control_term_buffer_sparse{true};
    std::int32_t error_buffer_length{1024 * 1024};
    std::int64_t catalog_capacity{1024 * 1024};
    int catalog_file_sync_level{0};
    RecordingSettings recording;
    ReplaySettings replay;
    /// `aeron.archive.threading.mode` and the three agent settings.
    ThreadingSettings threading;
    /// `aeron.archive.authenticator.supplier`; empty ⇒ `DefaultAuthenticator`.
    std::string authenticator_supplier;
};

/// Reads the launch names and applies the reference's startup checks
/// (`Archive.java:1250-1400`).
[[nodiscard]] std::expected<ArchiveLaunchConfig, axc::core::Error> parse_launch_config(
    const axc::core::PropertySource& source) noexcept;

/// `aeron.archive.dir.delete.on.start`: remove the archive directory tree when
/// the property asks for it (`Archive.java:1275-1279`).
[[nodiscard]] std::expected<void, axc::core::Error> prepare_archive_directories(const ArchiveLaunchConfig& config);

/// Creates the mark file, the catalog and the conductor, in that order, and
/// closes them in reverse. Owns what `Archive` owns in the reference.
class ArchiveServer
{
public:
    static std::expected<std::unique_ptr<ArchiveServer>, axc::core::Error> create(const ArchiveLaunchConfig& config);

    ArchiveServer(const ArchiveServer&) = delete;
    ArchiveServer& operator=(const ArchiveServer&) = delete;
    ~ArchiveServer();

    /// `Archive.launch` (`Archive.java:141-170`): DEDICATED starts the three
    /// agents on their own threads; SHARED/INVOKER leave the caller in charge
    /// (`do_work`).
    [[nodiscard]] std::expected<void, axc::core::Error> start();

    /// One duty cycle: the conductor, then the mark-file activity timestamp
    /// (`ArchiveMarkFile.java:365-376`). In DEDICATED the agents run on their
    /// own threads and this returns 0.
    [[nodiscard]] int do_work();

    void close();

    [[nodiscard]] ArchiveThreadingMode threading_mode() const noexcept
    {
        return config_.threading.mode;
    }

    /// The idle strategy the caller loop uses in SHARED/INVOKER
    /// (`Configuration.idleStrategySupplier`, `Archive.java:832-847`). A copy:
    /// the strategy carries the backoff state of one loop.
    [[nodiscard]] axc::core::IdleStrategy idle_strategy() const noexcept
    {
        return config_.threading.idle;
    }

    [[nodiscard]] ArchiveMarkFile& mark_file() noexcept
    {
        return mark_file_;
    }

    [[nodiscard]] const ArchiveMarkFile& mark_file() const noexcept
    {
        return mark_file_;
    }

private:
    /// `DedicatedModeArchiveConductor`'s three agents; the conductor agent
    /// wraps the whole conductor (its `do_work`), the other two their worker
    /// halves.
    class ConductorAgent
    {
    public:
        ConductorAgent(ArchiveConductor& conductor, std::string_view role) : conductor_(conductor), role_(role) {}

        [[nodiscard]] std::expected<void, axc::core::Error> on_start()
        {
            return {};
        }

        [[nodiscard]] int do_work() noexcept
        {
            return conductor_.do_work();
        }

        void on_close() noexcept {}

        [[nodiscard]] std::string_view role_name() noexcept
        {
            return role_;
        }

    private:
        ArchiveConductor& conductor_;
        std::string role_;
    };

    class RecorderAgent
    {
    public:
        RecorderAgent(Recorder& recorder, std::string_view role) : recorder_(recorder), role_(role) {}

        [[nodiscard]] std::expected<void, axc::core::Error> on_start()
        {
            recorder_.on_start_threaded();
            return {};
        }

        [[nodiscard]] int do_work() noexcept
        {
            return recorder_.do_work_threaded();
        }

        void on_close() noexcept
        {
            recorder_.close_threaded();
        }

        [[nodiscard]] std::string_view role_name() noexcept
        {
            return role_;
        }

    private:
        Recorder& recorder_;
        std::string role_;
    };

    class ReplayerAgent
    {
    public:
        ReplayerAgent(Replayer& replayer, std::string_view role) : replayer_(replayer), role_(role) {}

        [[nodiscard]] std::expected<void, axc::core::Error> on_start()
        {
            replayer_.on_start_threaded();
            return {};
        }

        [[nodiscard]] int do_work() noexcept
        {
            return replayer_.do_work_threaded();
        }

        void on_close() noexcept
        {
            replayer_.close_threaded();
        }

        [[nodiscard]] std::string_view role_name() noexcept
        {
            return role_;
        }

    private:
        Replayer& replayer_;
        std::string role_;
    };

    class LoggingErrorHandler final : public axc::core::ErrorHandler
    {
    public:
        void on_error(const axc::core::Error& error) noexcept override;
    };

    explicit ArchiveServer(const ArchiveLaunchConfig& config);

    [[nodiscard]] std::expected<void, axc::core::Error> init();

    ArchiveLaunchConfig config_;
    SystemEpochClock epoch_clock_;
    ArchiveMarkFile mark_file_;
    SampleAuthenticator sample_authenticator_;
    DefaultAuthenticator default_authenticator_;
    std::unique_ptr<ArchiveConductor> conductor_;
    LoggingErrorHandler error_handler_;
    std::unique_ptr<ConductorAgent> conductor_agent_;
    /// `deepmsg.archive.recorder.threads` recorder agents (one in the reference
    /// shape; P-A9).
    std::vector<std::unique_ptr<RecorderAgent>> recorder_agents_;
    std::unique_ptr<ReplayerAgent> replayer_agent_;
    std::unique_ptr<axc::core::AgentRunner<ConductorAgent>> conductor_runner_;
    std::vector<std::unique_ptr<axc::core::AgentRunner<RecorderAgent>>> recorder_runners_;
    std::unique_ptr<axc::core::AgentRunner<ReplayerAgent>> replayer_runner_;
    /// `init()` signalled the mark file ready.
    bool started_{false};
    /// `start()` created the agent threads.
    bool runtime_started_{false};
};

}  // namespace deepmsg::archive::server
