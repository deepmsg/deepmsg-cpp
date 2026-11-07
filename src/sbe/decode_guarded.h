#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace axc::sbe
{

/// Bounds check for an inbound SBE message before any flyweight is constructed.
///
/// X0-3/A1 must replace this placeholder with a real guard that validates the
/// SBE header's blockLength and every var-data/group length against the buffer
/// (the Rust track hit a generator without guards that panicked on truncated
/// frames).
///
/// Rules:
///   * every inbound decode goes through this function;
///   * it never reads past `buffer.size()`;
///   * it returns false for a truncated or inconsistent frame.
inline bool decode_guarded(std::span<const std::uint8_t> buffer, std::size_t required_length) noexcept
{
    return required_length <= buffer.size();
}

}  // namespace axc::sbe
