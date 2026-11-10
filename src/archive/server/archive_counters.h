#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "core/counters/counter_directory.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"

namespace deepmsg::archive::server
{

/// `io.aeron.archive.ArchiveCounters` (`ArchiveCounters.java:28-130`) and the
/// four archive-level counters the recorder publishes
/// (`Archive.java:1633-1693`).
///
/// Every counter here has an **8-byte key that is only the archive id** and a
/// label of `<name> - archiveId=<id>`; the type-100 recording position counter
/// has its own composite key and lives in `RecordingPos`.
class ArchiveCounters
{
public:
    /// `AeronCounters.java:751-812`.
    static constexpr std::int32_t CONTROL_SESSIONS_TYPE_ID = 102;
    /// `AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID` / `..._THRESHOLD_EXCEEDED_...`
    /// (`AeronCounters.java:757,764`): duty-cycle counters 103/104.
    static constexpr std::int32_t MAX_CYCLE_TIME_TYPE_ID = 103;
    static constexpr std::int32_t CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 104;
    static constexpr std::int32_t MAX_WRITE_TIME_TYPE_ID = 105;
    static constexpr std::int32_t TOTAL_WRITE_BYTES_TYPE_ID = 106;
    static constexpr std::int32_t TOTAL_WRITE_TIME_TYPE_ID = 107;
    static constexpr std::int32_t REPLAYER_MAX_READ_TIME_TYPE_ID = 108;
    static constexpr std::int32_t REPLAYER_TOTAL_READ_BYTES_TYPE_ID = 109;
    static constexpr std::int32_t REPLAYER_TOTAL_READ_TIME_TYPE_ID = 110;
    static constexpr std::int32_t RECORDING_SESSION_COUNT_TYPE_ID = 111;
    static constexpr std::int32_t REPLAY_SESSION_COUNT_TYPE_ID = 112;
    /// `AeronCounters.ARCHIVE_CONTROL_SESSION_TYPE_ID` (`AeronCounters.java:826`).
    static constexpr std::int32_t CONTROL_SESSION_TYPE_ID = 113;

    static constexpr std::string_view CONTROL_SESSIONS_NAME{"Archive Control Sessions"};
    static constexpr std::string_view MAX_WRITE_TIME_NAME{"archive-recorder max write time in ns"};
    static constexpr std::string_view TOTAL_WRITE_BYTES_NAME{"archive-recorder total write bytes"};
    static constexpr std::string_view TOTAL_WRITE_TIME_NAME{"archive-recorder total write time in ns"};
    static constexpr std::string_view REPLAYER_MAX_READ_TIME_NAME{"archive-replayer max read time in ns"};
    static constexpr std::string_view REPLAYER_TOTAL_READ_BYTES_NAME{"archive-replayer total read bytes"};
    static constexpr std::string_view REPLAYER_TOTAL_READ_TIME_NAME{"archive-replayer total read time in ns"};
    static constexpr std::string_view RECORDING_SESSION_COUNT_NAME{"Archive Recording Sessions"};
    static constexpr std::string_view REPLAY_SESSION_COUNT_NAME{"Archive Replay Sessions"};
    static constexpr std::string_view CONTROL_SESSION_NAME{"control-session"};

    /// `ArchiveCounters.allocate`'s key (`ArchiveCounters.java:52-58`).
    [[nodiscard]] static std::array<std::uint8_t, sizeof(std::int64_t)> archive_id_key(
        std::int64_t archive_id) noexcept;

    /// `ArchiveCounters.allocate`'s label: `<name> - archiveId=<id>`.
    [[nodiscard]] static std::string make_label(std::string_view name, std::int64_t archive_id);

    /// `ControlSessionCounter`'s key (`ControlSessionCounter.java:46-72`):
    /// archive id and control session id.
    [[nodiscard]] static std::array<std::uint8_t, 2 * sizeof(std::int64_t)> control_session_key(
        std::int64_t archive_id, std::int64_t control_session_id) noexcept;

    /// `ControlSessionCounter.allocate`'s label: `control-session: <clientInfo>
    /// - archiveId=<id>`.
    [[nodiscard]] static std::string control_session_label(std::string_view client_info, std::int64_t archive_id);

    /// `ArchiveCounters.find(countersReader, typeId, archiveId)`
    /// (`ArchiveCounters.java:124-146`); `AERON_NULL_COUNTER_ID` when absent.
    [[nodiscard]] static std::int32_t find(aeron_counters_reader_t* reader, std::int32_t type_id,
                                           std::int64_t archive_id) noexcept;

    /// Two-phase allocation (never block the agent on the driver).
    [[nodiscard]] static axc::core::AsyncAdd<axc::core::CounterPtr> allocate(axc::core::DeepmsgClient& client,
                                                                             std::int32_t type_id,
                                                                             std::string_view name,
                                                                             std::int64_t archive_id);
};

}  // namespace deepmsg::archive::server
