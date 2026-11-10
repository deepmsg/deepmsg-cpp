#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core
{

/// CPU pinning for an agent thread.
///
/// Reference semantics (Aeron 1.54.0 `*.cpu.affinity`): a single integer is an
/// **index into the cpuset the process is allowed to run on**
/// (`ThreadAffinity.NO_AFFINITY = -1`); the index is resolved when the affinity
/// is applied.
///
/// Extension, documented as a deviation: a comma/range list (`"0,2,4-5"`) pins
/// to those absolute CPU numbers. It is accepted so tests and manual runs do not
/// have to reason about the allowed set.
class CpuAffinity
{
public:
    static constexpr int NO_AFFINITY = -1;
    static constexpr int MAX_CPUS = 1024;

    static CpuAffinity none() noexcept
    {
        return {};
    }

    /// "" and "-1" mean no affinity; "N" is an index into the allowed cpuset;
    /// a list/range is an absolute CPU set.
    [[nodiscard]] static std::expected<CpuAffinity, Error> parse(std::string_view spec) noexcept;

    [[nodiscard]] bool any() const noexcept
    {
        return mode_ != Mode::none;
    }

    [[nodiscard]] bool is_index() const noexcept
    {
        return mode_ == Mode::index;
    }

    [[nodiscard]] int index() const noexcept
    {
        return index_;
    }

    /// Absolute CPU-set mode only; index mode must be resolved first.
    [[nodiscard]] bool contains(int cpu) const noexcept;

    [[nodiscard]] std::size_t count() const noexcept;

    /// The `n`-th CPU of this affinity: the set mode's n-th member, or
    /// `index + n` for index mode. `none()` for no affinity or out of range.
    /// A6f uses it to give each recorder agent its own core from the one
    /// configured `aeron.archive.recorder.cpu.affinity`.
    [[nodiscard]] CpuAffinity nth(std::size_t n) const noexcept;

    /// Applies to the calling thread. Returns false and leaves the thread
    /// unpinned when the kernel refuses (seccomp/cpuset restrictions).
    [[nodiscard]] bool apply_to_current_thread() const noexcept;

private:
    enum class Mode
    {
        none,
        index,
        set
    };

    Mode mode_{Mode::none};
    int index_{0};
    std::array<std::uint64_t, MAX_CPUS / 64> bits_{};
};

}  // namespace axc::core
