#include "archive/server/recording_pos.h"

#include <algorithm>
#include <cstring>
#include <string>

namespace deepmsg::archive::server
{
namespace
{

/// `Aeron.NULL_VALUE`.
constexpr std::int64_t NULL_VALUE = -1;

/// `ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX` = `" - archiveId="`.
constexpr std::string_view ARCHIVE_ID_LABEL_SUFFIX{" - archiveId="};

template <typename T>
[[nodiscard]] T read_value(const std::uint8_t* address) noexcept
{
    T value{};
    std::memcpy(&value, address, sizeof(T));
    return value;
}

template <typename T>
void write_value(std::uint8_t* address, T value) noexcept
{
    std::memcpy(address, &value, sizeof(T));
}

struct FindContext
{
    std::int64_t recording_id{0};
    std::int32_t session_id{0};
    bool match_recording{false};
    bool match_session{false};
    std::int64_t archive_id{NULL_VALUE};
    std::int32_t found{AERON_NULL_COUNTER_ID};
};

void on_counter(int64_t /*value*/, int32_t id, int32_t type_id, const uint8_t* key, size_t key_length,
                const char* /*label*/, size_t /*label_length*/, void* clientd)
{
    auto* context = static_cast<FindContext*>(clientd);
    if (context->found != AERON_NULL_COUNTER_ID || type_id != RecordingPos::TYPE_ID)
    {
        return;
    }

    const auto view = RecordingPos::parse_key({key, key_length});
    if (!view.has_value())
    {
        return;
    }

    if (NULL_VALUE != context->archive_id && view->archive_id != context->archive_id)
    {
        return;
    }

    if (context->match_recording && view->recording_id != context->recording_id)
    {
        return;
    }
    if (context->match_session && view->session_id != context->session_id)
    {
        return;
    }

    context->found = id;
}

}  // namespace

RecordingPos::Key RecordingPos::make_key(std::int64_t archive_id, std::int64_t recording_id, std::int32_t session_id,
                                         std::string_view source_identity) noexcept
{
    Key key;

    const auto identity_length = static_cast<std::int32_t>(
        std::min(source_identity.size(), MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET - sizeof(std::int64_t)));

    write_value<std::int64_t>(key.bytes.data() + RECORDING_ID_OFFSET, recording_id);
    write_value<std::int32_t>(key.bytes.data() + SESSION_ID_OFFSET, session_id);
    write_value<std::int32_t>(key.bytes.data() + SOURCE_IDENTITY_LENGTH_OFFSET, identity_length);
    std::memcpy(key.bytes.data() + SOURCE_IDENTITY_OFFSET, source_identity.data(),
                static_cast<std::size_t>(identity_length));

    const auto archive_id_offset = SOURCE_IDENTITY_OFFSET + identity_length;
    write_value<std::int64_t>(key.bytes.data() + archive_id_offset, archive_id);
    key.length = archive_id_offset + static_cast<std::int32_t>(sizeof(std::int64_t));

    return key;
}

std::string RecordingPos::make_label(std::int64_t archive_id, std::int64_t recording_id, std::int32_t session_id,
                                     std::int32_t stream_id, std::string_view stripped_channel)
{
    std::string label;
    label += NAME;
    label += ": ";
    label += std::to_string(recording_id);
    label += ' ';
    label += std::to_string(session_id);
    label += ' ';
    label += std::to_string(stream_id);
    label += ' ';

    const auto archive_id_label_length = ARCHIVE_ID_LABEL_SUFFIX.size() + std::to_string(archive_id).size();
    const auto room = label.size() + archive_id_label_length < MAX_LABEL_LENGTH
                          ? MAX_LABEL_LENGTH - label.size() - archive_id_label_length
                          : 0;
    label.append(stripped_channel.substr(0, std::min(stripped_channel.size(), room)));

    label += ARCHIVE_ID_LABEL_SUFFIX;
    label += std::to_string(archive_id);
    return label;
}

std::optional<RecordingPos::View> RecordingPos::parse_key(std::span<const std::uint8_t> key) noexcept
{
    if (key.size() < SOURCE_IDENTITY_OFFSET)
    {
        return std::nullopt;
    }

    const auto identity_length = read_value<std::int32_t>(key.data() + SOURCE_IDENTITY_LENGTH_OFFSET);
    if (identity_length < 0 ||
        static_cast<std::size_t>(SOURCE_IDENTITY_OFFSET + identity_length + sizeof(std::int64_t)) > key.size())
    {
        return std::nullopt;
    }

    View view;
    view.recording_id = read_value<std::int64_t>(key.data() + RECORDING_ID_OFFSET);
    view.session_id = read_value<std::int32_t>(key.data() + SESSION_ID_OFFSET);
    view.source_identity = {reinterpret_cast<const char*>(key.data() + SOURCE_IDENTITY_OFFSET),
                            static_cast<std::size_t>(identity_length)};
    view.archive_id = read_value<std::int64_t>(key.data() + SOURCE_IDENTITY_OFFSET + identity_length);
    return view;
}

std::int32_t RecordingPos::find_by_recording(aeron_counters_reader_t* reader, std::int64_t archive_id,
                                             std::int64_t recording_id) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }

    FindContext context{};
    context.match_recording = true;
    context.recording_id = recording_id;
    context.archive_id = archive_id;
    aeron_counters_reader_foreach_counter(reader, &on_counter, &context);
    return context.found;
}

std::int32_t RecordingPos::find_by_session(aeron_counters_reader_t* reader, std::int64_t archive_id,
                                           std::int32_t session_id) noexcept
{
    if (nullptr == reader)
    {
        return AERON_NULL_COUNTER_ID;
    }

    FindContext context{};
    context.match_session = true;
    context.session_id = session_id;
    context.archive_id = archive_id;
    aeron_counters_reader_foreach_counter(reader, &on_counter, &context);
    return context.found;
}

std::int64_t RecordingPos::recording_id(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept
{
    if (nullptr == reader || counter_id < 0 || counter_id > aeron_counters_reader_max_counter_id(reader))
    {
        return NULL_VALUE;
    }

    std::int32_t type_id = 0;
    if (0 != aeron_counters_reader_counter_type_id(reader, counter_id, &type_id) || TYPE_ID != type_id)
    {
        return NULL_VALUE;
    }

    std::uint8_t* key = nullptr;
    if (0 != aeron_counters_reader_metadata_key(reader, counter_id, &key) || nullptr == key)
    {
        return NULL_VALUE;
    }

    return read_value<std::int64_t>(key + RECORDING_ID_OFFSET);
}

bool RecordingPos::is_active(aeron_counters_reader_t* reader, std::int32_t counter_id,
                             std::int64_t recording_id_value) noexcept
{
    return recording_id(reader, counter_id) == recording_id_value;
}

}  // namespace deepmsg::archive::server
