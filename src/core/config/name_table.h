#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "core/config/disposition.h"
#include "core/config/property_source.h"

namespace axc::core
{

/// Disposition table for reference configuration names (XA-4 / XC-6).
///
/// Unknown `aeron.*` names are reported, never silently ignored.
class NameTable
{
public:
    NameTable() = default;

    void add_honoured(std::string_view name);
    void add_accepted_ignored(std::string_view name, std::string_view reason);
    void add_refused(std::string_view name, std::string_view reason);

    [[nodiscard]] Disposition lookup(std::string_view name) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept
    {
        return entries_.size();
    }

    /// Names present in the source that have no disposition (sorted).
    [[nodiscard]] std::vector<std::string> unknown_names(const PropertySource& source) const;

    /// Names present in the source whose disposition is `refused` (sorted).
    [[nodiscard]] std::vector<std::string> refused_names(const PropertySource& source) const;

private:
    void add(std::string_view name, DispositionKind kind, std::string_view reason);

    std::map<std::string, Disposition, std::less<>> entries_;
};

}  // namespace axc::core
