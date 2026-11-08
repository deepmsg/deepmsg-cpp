#pragma once

#include <cstdint>
#include <span>

#include <aeronc.h>

namespace axc::core
{

/// `CountersReader.findByTypeIdAndKey` equivalent: walks the counter metadata
/// once and returns the first allocated counter with that type id and key, or
/// `AERON_NULL_COUNTER_ID`.
[[nodiscard]] std::int32_t find_counter_id_by_type_and_key(aeron_counters_reader_t* reader, std::int32_t type_id,
                                                           std::span<const std::uint8_t> key) noexcept;

}  // namespace axc::core
