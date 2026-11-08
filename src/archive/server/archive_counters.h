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
    /// `AeronCounters.java:771-812`.
    static constexpr std::int32_t MAX_WRITE_TIME_TYPE_ID = 105;
    static constexpr std::int32_t TOTAL_WRITE_BYTES_TYPE_ID = 106;
    static constexpr std::int32_t TOTAL_WRITE_TIME_TYPE_ID = 107;
    static constexpr std::int32_t RECORDING_SESSION_COUNT_TYPE_ID = 111;

    static constexpr std::string_view MAX_WRITE_TIME_NAME{"archive-recorder max write time in ns"};
    static constexpr std::string_view TOTAL_WRITE_BYTES_NAME{"archive-recorder total write bytes"};
    static constexpr std::string_view TOTAL_WRITE_TIME_NAME{"archive-recorder total write time in ns"};
    static constexpr std::string_view RECORDING_SESSION_COUNT_NAME{"Archive Recording Sessions"};

    /// `ArchiveCounters.allocate`'s key (`ArchiveCounters.java:52-58`).
    [[nodiscard]] static std::array<std::uint8_t, sizeof(std::int64_t)> archive_id_key(
        std::int64_t archive_id) noexcept;

    /// `ArchiveCounters.allocate`'s label: `<name> - archiveId=<id>`.
    [[nodiscard]] static std::string make_label(std::string_view name, std::int64_t archive_id);

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
