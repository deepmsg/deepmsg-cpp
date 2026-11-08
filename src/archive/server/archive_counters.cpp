#include "archive/server/archive_counters.h"

#include <cstring>
#include <span>

namespace deepmsg::archive::server
{
namespace
{

constexpr std::string_view ARCHIVE_ID_LABEL_SUFFIX{" - archiveId="};

}  // namespace

std::array<std::uint8_t, sizeof(std::int64_t)> ArchiveCounters::archive_id_key(std::int64_t archive_id) noexcept
{
    std::array<std::uint8_t, sizeof(std::int64_t)> key{};
    std::memcpy(key.data(), &archive_id, sizeof(archive_id));
    return key;
}

std::array<std::uint8_t, 2 * sizeof(std::int64_t)> ArchiveCounters::control_session_key(
    std::int64_t archive_id, std::int64_t control_session_id) noexcept
{
    std::array<std::uint8_t, 2 * sizeof(std::int64_t)> key{};
    std::memcpy(key.data(), &archive_id, sizeof(archive_id));
    std::memcpy(key.data() + sizeof(archive_id), &control_session_id, sizeof(control_session_id));
    return key;
}

std::string ArchiveCounters::control_session_label(std::string_view client_info, std::int64_t archive_id)
{
    std::string label{CONTROL_SESSION_NAME};
    label += ": ";
    label += client_info;
    label += ARCHIVE_ID_LABEL_SUFFIX;
    label += std::to_string(archive_id);
    return label;
}

std::string ArchiveCounters::make_label(std::string_view name, std::int64_t archive_id)
{
    std::string label{name};
    label += ARCHIVE_ID_LABEL_SUFFIX;
    label += std::to_string(archive_id);
    return label;
}

std::int32_t ArchiveCounters::find(aeron_counters_reader_t* reader, std::int32_t type_id,
                                   std::int64_t archive_id) noexcept
{
    const auto key = archive_id_key(archive_id);
    return axc::core::find_counter_id_by_type_and_key(reader, type_id, {key.data(), key.size()});
}

axc::core::AsyncAdd<axc::core::CounterPtr> ArchiveCounters::allocate(axc::core::DeepmsgClient& client,
                                                                     std::int32_t type_id, std::string_view name,
                                                                     std::int64_t archive_id)
{
    const auto key = archive_id_key(archive_id);
    const std::string label = make_label(name, archive_id);
    return client.add_counter(type_id, {key.data(), key.size()}, label);
}

}  // namespace deepmsg::archive::server
