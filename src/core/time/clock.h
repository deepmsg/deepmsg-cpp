#pragma once

#include <cstdint>

namespace axc::core
{

/// Monotonic nanosecond clock (`CLOCK_MONOTONIC`, vDSO).
class NanoClock
{
public:
    [[nodiscard]] static std::int64_t now() noexcept;
};

/// Wall-clock milliseconds (`CLOCK_REALTIME`).
class EpochClock
{
public:
    [[nodiscard]] static std::int64_t now_ms() noexcept;
};

/// A clock read once per `do_work` pass and reused by everything in that pass
/// (reference: `ConsensusModuleAgent.doWork` reads the clock once).
class CachedNanoClock
{
public:
    explicit CachedNanoClock(std::int64_t now = NanoClock::now()) noexcept : now_(now) {}

    void update() noexcept
    {
        now_ = NanoClock::now();
    }

    [[nodiscard]] std::int64_t time() const noexcept
    {
        return now_;
    }

private:
    std::int64_t now_;
};

}  // namespace axc::core
