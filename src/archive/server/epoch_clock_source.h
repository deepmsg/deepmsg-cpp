#pragma once

#include <cstdint>

namespace deepmsg::archive::server
{

/// Millisecond wall clock, the reference's `EpochClock` functional interface;
/// injectable so time-dependent behaviour (catalog repair, mark file activity,
/// recording start timestamps) can be pinned in a test.
class EpochClockSource
{
public:
    EpochClockSource() = default;
    virtual ~EpochClockSource() = default;

    EpochClockSource(const EpochClockSource&) = delete;
    EpochClockSource& operator=(const EpochClockSource&) = delete;

    [[nodiscard]] virtual std::int64_t time_ms() const noexcept = 0;
};

/// `EpochClock` as production code gets it (`CLOCK_REALTIME`).
class SystemEpochClock final : public EpochClockSource
{
public:
    [[nodiscard]] std::int64_t time_ms() const noexcept override;
};

/// `org.agrona.concurrent.CachedEpochClock`: read once per pass and reuse, so a
/// whole `doWork` pass sees one time.
class CachedEpochClock final : public EpochClockSource
{
public:
    explicit CachedEpochClock(std::int64_t now_ms = 0) noexcept : now_ms_(now_ms) {}

    [[nodiscard]] std::int64_t time_ms() const noexcept override
    {
        return now_ms_;
    }

    void update(std::int64_t now_ms) noexcept
    {
        now_ms_ = now_ms;
    }

private:
    std::int64_t now_ms_{0};
};

}  // namespace deepmsg::archive::server
