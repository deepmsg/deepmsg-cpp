#pragma once

#include <cstdint>
#include <string_view>

namespace axc::alloc_test
{

/// The labels the cluster zero-allocation test reports. `other` is every
/// thread that never called `label_thread` (driver and archive agents).
enum class Label : int
{
    other = 0,
    cm = 1,
    container = 2,
    client = 3
};

/// Tags the calling thread. Cheap and allocation-free; call it first thing on
/// every thread whose allocations matter.
void label_thread(std::string_view name) noexcept;

/// Zeroes every counter. Called after the warm-up traffic.
void reset() noexcept;

[[nodiscard]] std::int64_t count(Label label) noexcept;
[[nodiscard]] std::int64_t total() noexcept;

}  // namespace axc::alloc_test
