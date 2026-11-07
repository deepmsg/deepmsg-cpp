#pragma once

#include <cstdint>

namespace deepmsg::archive::server
{

/// Aeron frame header constants shared by the catalog's recovery scan, the
/// recording writer and the recording reader.
///
/// Sources: `FrameDescriptor` (`FrameDescriptor.java:66`, `:96`, `:111`, `:116`)
/// and `DataHeaderFlyweight` (`DataHeaderFlyweight.java:38`, `:88`), Aeron 1.54.0.

/// `FrameDescriptor.FRAME_ALIGNMENT` — 32 since 1.44.
inline constexpr std::int32_t FRAME_ALIGNMENT = 32;
/// `DataHeaderFlyweight.HEADER_LENGTH`.
inline constexpr std::int32_t DATA_HEADER_LENGTH = 32;

inline constexpr std::int32_t FRAME_LENGTH_OFFSET = 0;
inline constexpr std::int32_t FRAME_VERSION_OFFSET = 4;
inline constexpr std::int32_t FRAME_FLAGS_OFFSET = 5;
/// `DataHeaderFlyweight.TYPE_FIELD_OFFSET`; `frameType` reads a 16-bit value
/// here, so the byte after the type (reserved) is part of the comparison.
inline constexpr std::int32_t FRAME_TYPE_OFFSET = 6;
inline constexpr std::int32_t TERM_OFFSET_FIELD_OFFSET = 8;
inline constexpr std::int32_t SESSION_ID_OFFSET = 12;
inline constexpr std::int32_t STREAM_ID_OFFSET = 16;
inline constexpr std::int32_t TERM_ID_OFFSET = 20;
inline constexpr std::int32_t RESERVED_VALUE_OFFSET = 24;

/// `HeaderFlyweight.HDR_TYPE_DATA` / `HDR_TYPE_PAD`.
inline constexpr std::int32_t DATA_FRAME_TYPE = 0x01;
inline constexpr std::int32_t PADDING_FRAME_TYPE = 0x02;

/// `BitUtil.align(value, alignment)`; the reference always passes a power of two.
[[nodiscard]] constexpr std::int32_t frame_align(std::int32_t value, std::int32_t alignment) noexcept
{
    return (value + alignment - 1) & ~(alignment - 1);
}

}  // namespace deepmsg::archive::server
