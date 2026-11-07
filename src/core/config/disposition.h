#pragma once

#include "core/collections/fixed_string.h"

namespace axc::core
{

enum class DispositionKind
{
    honoured,
    accepted_ignored,
    refused,
    unknown
};

/// What we do with a reference configuration name. Every name must have one;
/// there are no silent ignores.
struct Disposition
{
    DispositionKind kind{DispositionKind::unknown};
    FixedString<128> reason;

    [[nodiscard]] bool is_known() const noexcept
    {
        return DispositionKind::unknown != kind;
    }
};

}  // namespace axc::core
