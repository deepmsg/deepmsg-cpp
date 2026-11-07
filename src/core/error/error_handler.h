#pragma once

#include "core/error/error.h"

namespace axc::core
{

/// Sink for agent errors. The real implementation records into the distinct
/// error log and increments the error counter (X0-3d / X0-3c wiring).
class ErrorHandler
{
public:
    ErrorHandler() = default;
    ErrorHandler(const ErrorHandler&) = delete;
    ErrorHandler& operator=(const ErrorHandler&) = delete;
    virtual ~ErrorHandler() = default;

    virtual void on_error(const Error& error) noexcept = 0;
};

class NoOpErrorHandler final : public ErrorHandler
{
public:
    void on_error(const Error&) noexcept override {}
};

}  // namespace axc::core
