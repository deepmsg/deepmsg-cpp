#pragma once

#include <cstdint>
#include <utility>

#include "core/counters/position_writer.h"

namespace axc::core
{

/// A value a pass publishes into a counter (`Counter.setRelease`); the seam lets
/// the recorder's publish condition be exercised in a unit test.
class CounterTarget
{
public:
    CounterTarget() = default;
    virtual ~CounterTarget() = default;

    CounterTarget(const CounterTarget&) = delete;
    CounterTarget& operator=(const CounterTarget&) = delete;

    virtual void set_value(std::int64_t value) noexcept = 0;
    [[nodiscard]] virtual std::int64_t value() const noexcept = 0;

    /// Identity of the underlying counter when there is one (the replay limit
    /// counter needs both, `ReplaySession.java:550-556`).
    [[nodiscard]] virtual std::int32_t counter_id() const noexcept
    {
        return -1;
    }
    [[nodiscard]] virtual std::int64_t registration_id() const noexcept
    {
        return -1;
    }
};

class PositionWriterTarget final : public CounterTarget
{
public:
    explicit PositionWriterTarget(PositionWriter writer) noexcept : writer_(std::move(writer)) {}

    void set_value(std::int64_t value) noexcept override
    {
        writer_.set_release(value);
    }

    [[nodiscard]] std::int64_t value() const noexcept override
    {
        return writer_.get();
    }

    [[nodiscard]] std::int32_t counter_id() const noexcept override
    {
        return writer_.id();
    }

    [[nodiscard]] std::int64_t registration_id() const noexcept override
    {
        return writer_.registration_id();
    }

    [[nodiscard]] const PositionWriter& writer() const noexcept
    {
        return writer_;
    }

private:
    PositionWriter writer_;
};

}  // namespace axc::core
