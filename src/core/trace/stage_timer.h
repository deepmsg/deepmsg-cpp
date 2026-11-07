#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#if defined(AXC_STAGE_TIMING) && (defined(__x86_64__) || defined(__i386__))
#include <x86intrin.h>
#endif

namespace axc::core
{

enum class Stage : std::size_t
{
    archive_recorder_poll = 0,
    archive_recorder_write,
    archive_recorder_fsync,
    archive_recorder_position,
    cluster_ingress_to_log,
    cluster_quorum_to_commit,
    cluster_service_to_egress,
    count
};

struct StageStats
{
    std::uint64_t count{0};
    std::uint64_t total_cycles{0};
    std::uint64_t min_cycles{0};
    std::uint64_t max_cycles{0};
};

/// Compile-time-gated stage timing (`AXC_STAGE_TIMING`). Header-inline so the
/// switch is resolved in the translation unit that uses the timer; when it is
/// off every method is a no-op and the object stays cheap to carry.
class StageTimer
{
public:
    void begin(Stage stage) noexcept
    {
#if defined(AXC_STAGE_TIMING)
        started_cycles_ = read_cycles();
        current_stage_ = static_cast<std::size_t>(stage);
        active_ = true;
#else
        (void)stage;
#endif
    }

    void end(Stage stage) noexcept
    {
#if defined(AXC_STAGE_TIMING)
        if (!active_ || current_stage_ != static_cast<std::size_t>(stage))
        {
            return;
        }

        const std::uint64_t elapsed = read_cycles() - started_cycles_;
        auto& entry = stats_[current_stage_];
        ++entry.count;
        entry.total_cycles += elapsed;
        if (0 == entry.min_cycles || elapsed < entry.min_cycles)
        {
            entry.min_cycles = elapsed;
        }
        if (elapsed > entry.max_cycles)
        {
            entry.max_cycles = elapsed;
        }
        active_ = false;
#else
        (void)stage;
#endif
    }

    [[nodiscard]] StageStats stats(Stage stage) const noexcept
    {
        const auto index = static_cast<std::size_t>(stage);
        return index < STAGE_COUNT ? stats_[index] : StageStats{};
    }

    [[nodiscard]] static std::uint64_t read_cycles() noexcept
    {
#if defined(AXC_STAGE_TIMING) && (defined(__x86_64__) || defined(__i386__))
        return __rdtsc();
#else
        return 0;
#endif
    }

    void reset() noexcept
    {
        stats_.fill(StageStats{});
        started_cycles_ = 0;
        current_stage_ = 0;
        active_ = false;
    }

private:
    static constexpr std::size_t STAGE_COUNT = static_cast<std::size_t>(Stage::count);

    std::array<StageStats, STAGE_COUNT> stats_{};
    std::uint64_t started_cycles_{0};
    std::size_t current_stage_{0};
    bool active_{false};
};

}  // namespace axc::core
