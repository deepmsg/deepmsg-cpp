#pragma once

#include <cstdint>
#include <stdexcept>

#include "core/collections/fixed_string.h"

namespace axc::core
{

enum class ErrorCode : std::int32_t
{
    none = 0,
    invalid_argument = 1,
    not_found = 2,
    no_capacity = 3,
    io_error = 4,
    protocol_error = 5,
    config_error = 6,
    internal_error = 7
};

/// Heap-free error value used by hot paths (`std::expected<T, Error>`).
struct Error
{
    ErrorCode code{ErrorCode::none};
    std::int32_t errcode{0};  // reference error-code domain (archive adds its +200 offsets)
    FixedString<256> message;

    [[nodiscard]] bool ok() const noexcept
    {
        return ErrorCode::none == code;
    }
};

[[nodiscard]] inline Error make_error(ErrorCode code, std::string_view message, std::int32_t errcode = 0) noexcept
{
    Error error;
    error.code = code;
    error.errcode = errcode;
    error.message.assign(message);
    return error;
}

/// Startup/configuration failure. Only caught in `main`/`launch`.
class ConfigError : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

}  // namespace axc::core
