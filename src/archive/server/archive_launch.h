#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "archive/server/archive_conductor.h"
#include "archive/server/archive_config.h"
#include "archive/server/archive_mark_file.h"
#include "archive/server/authenticator.h"
#include "archive/server/catalog.h"
#include "archive/server/epoch_clock_source.h"
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

    /// One duty cycle: the conductor, then the mark-file activity timestamp
    /// (`ArchiveMarkFile.java:365-376`).
    [[nodiscard]] int do_work();

    void close();

    [[nodiscard]] ArchiveMarkFile& mark_file() noexcept
    {
        return mark_file_;
    }

    [[nodiscard]] const ArchiveMarkFile& mark_file() const noexcept
    {
        return mark_file_;
    }

private:
    explicit ArchiveServer(const ArchiveLaunchConfig& config);

    [[nodiscard]] std::expected<void, axc::core::Error> init();

    ArchiveLaunchConfig config_;
    SystemEpochClock epoch_clock_;
    ArchiveMarkFile mark_file_;
    SampleAuthenticator sample_authenticator_;
    DefaultAuthenticator default_authenticator_;
    std::unique_ptr<ArchiveConductor> conductor_;
    bool started_{false};
};

}  // namespace deepmsg::archive::server
