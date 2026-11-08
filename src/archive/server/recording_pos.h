#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <aeronc.h>

namespace deepmsg::archive::server
{

/// `io.aeron.archive.status.RecordingPos` (`RecordingPos.java:33-271`): the
/// type-100 counter a live recording publishes its position through.
class RecordingPos
{
public:
    /// `AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID` (`AeronCounters.java:739`).
    static constexpr std::int32_t TYPE_ID = 100;
    static constexpr std::string_view NAME = "rec-pos";

    static constexpr std::int32_t RECORDING_ID_OFFSET = 0;
    static constexpr std::int32_t SESSION_ID_OFFSET = 8;
    static constexpr std::int32_t SOURCE_IDENTITY_LENGTH_OFFSET = 12;
    static constexpr std::int32_t SOURCE_IDENTITY_OFFSET = 16;
    /// `CountersReader.MAX_KEY_LENGTH` / `MAX_LABEL_LENGTH` (Agrona's metadata
    /// descriptor is 112 bytes of key and 380 bytes of label).
    static constexpr std::size_t MAX_KEY_LENGTH = 112;
    static constexpr std::size_t MAX_LABEL_LENGTH = 380;

    struct Key
    {
        std::array<std::uint8_t, MAX_KEY_LENGTH> bytes{};
        std::int32_t length{0};

        [[nodiscard]] std::span<const std::uint8_t> view() const noexcept
        {
            return {bytes.data(), static_cast<std::size_t>(length)};
        }
    };

    /// Decoded key (`RecordingPos.java:53-57`).
    struct View
    {
        std::int64_t recording_id{0};
        std::int32_t session_id{0};
        std::string_view source_identity{};
        std::int64_t archive_id{0};
    };

    /// `RecordingPos.allocate`'s metadata (`RecordingPos.java:93-132`).
    [[nodiscard]] static Key make_key(std::int64_t archive_id, std::int64_t recording_id, std::int32_t session_id,
                                      std::string_view source_identity) noexcept;

    /// `RecordingPos.allocate`'s label (`RecordingPos.java:114-128`).
    [[nodiscard]] static std::string make_label(std::int64_t archive_id, std::int64_t recording_id,
                                                std::int32_t session_id, std::int32_t stream_id,
                                                std::string_view stripped_channel);

    /// Nullopt when the key is not a well-formed type-100 key.
    [[nodiscard]] static std::optional<View> parse_key(std::span<const std::uint8_t> key) noexcept;

    /// `RecordingPos.findCounterIdByRecording` (`:157-188`); an `archive_id` of
    /// `Aeron.NULL_VALUE` (-1) matches any archive.
    [[nodiscard]] static std::int32_t find_by_recording(aeron_counters_reader_t* reader, std::int64_t archive_id,
                                                        std::int64_t recording_id) noexcept;

    /// `RecordingPos.findCounterIdBySession` (`:213-244`).
    [[nodiscard]] static std::int32_t find_by_session(aeron_counters_reader_t* reader, std::int64_t archive_id,
                                                      std::int32_t session_id) noexcept;

    /// `RecordingPos.getRecordingId` (`:258-270`).
    [[nodiscard]] static std::int64_t recording_id(aeron_counters_reader_t* reader, std::int32_t counter_id) noexcept;

    /// `RecordingPos.isActive` (`:275-284`).
    [[nodiscard]] static bool is_active(aeron_counters_reader_t* reader, std::int32_t counter_id,
                                        std::int64_t recording_id) noexcept;
};

}  // namespace deepmsg::archive::server
