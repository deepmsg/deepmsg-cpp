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

    [[nodiscard]] const PositionWriter& writer() const noexcept
    {
        return writer_;
    }

private:
    PositionWriter writer_;
};

}  // namespace axc::core
