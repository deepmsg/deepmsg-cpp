#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core
{

enum class IdleKind
{
    busy_spin,
    yielding,
    sleeping_ns,
    sleeping_ms,
    backoff,
    noop
};

/// Idle strategy, value type (no virtual dispatch, single-threaded use).
///
/// Names and behaviour follow `io.aeron.driver.Configuration.agentIdleStrategy`
/// (Aeron 1.54.0): the alias and the fully-qualified class name are both
/// accepted, and the backoff defaults are the driver's constants
/// (`IDLE_MAX_SPINS=10`, `IDLE_MAX_YIELDS=20`, `IDLE_MIN_PARK_NS=1000`,
/// `IDLE_MAX_PARK_NS=1ms`).
class IdleStrategy
{
public:
    static constexpr std::int64_t DEFAULT_MAX_SPINS = 10;
    static constexpr std::int64_t DEFAULT_MAX_YIELDS = 20;
    static constexpr std::int64_t DEFAULT_MIN_PARK_NS = 1000;
    static constexpr std::int64_t DEFAULT_MAX_PARK_NS = 1'000'000;
    static constexpr std::int64_t DEFAULT_SLEEP_NS = 1000;
    static constexpr std::int64_t DEFAULT_SLEEP_MS = 1;

    IdleKind kind{IdleKind::busy_spin};
    std::int64_t max_spins{DEFAULT_MAX_SPINS};
    std::int64_t max_yields{DEFAULT_MAX_YIELDS};
    std::int64_t min_park_ns{DEFAULT_MIN_PARK_NS};
    std::int64_t max_park_ns{DEFAULT_MAX_PARK_NS};
    std::int64_t sleep_period_ns{DEFAULT_SLEEP_NS};
    std::int64_t sleep_period_ms{DEFAULT_SLEEP_MS};

    /// Accepts the reference names: `spin`, `yield`, `sleep-ns`, `sleep-ms`,
    /// `backoff`, `noop` and their `org.agrona.concurrent.*` class names.
    ///
    /// `controllable` is refused loudly: it needs a StatusIndicator that only the
    /// media driver has, and this build does not provide one yet.
    [[nodiscard]] static std::expected<IdleStrategy, Error> by_agrona_name(std::string_view name) noexcept;

    /// `io.aeron.driver.Configuration.agentIdleStrategy` from a configured
    /// string; null/empty is a configuration error (the caller supplies the
    /// archive/cluster default).
    void idle(int work_count) noexcept;

    void reset() noexcept;

private:
    void backoff_idle() noexcept;

    // Agrona BackoffIdleStrategy state machine.
    int state_{0};  // 0 = not idle, 1 = spinning, 2 = yielding, 3 = parking
    std::int64_t spins_{0};
    std::int64_t yields_{0};
    std::int64_t park_period_ns_{DEFAULT_MIN_PARK_NS};
};

}  // namespace axc::core
