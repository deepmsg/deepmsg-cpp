#pragma once

#include <cstddef>
#include <span>
#include <string_view>

namespace deepmsg::archive::server
{

/// What this implementation does with a reference property name (XA-4).
enum class ConfigDisposition
{
    /// Read and applied.
    honoured,
    /// Parsed/accepted but deliberately not applied; `reason` says why.
    accepted_ignored
};

struct ConfigEntry
{
    std::string_view name;
    ConfigDisposition disposition;
    std::string_view reason;
};

/// Every `aeron.archive.*` name in Aeron 1.54.0 (`454da1e4e6`), recounted by
/// X0-6: **48** names (`analysis/cpp/records/x0-6/config-names.tsv`; the card's
/// 45 is the pre-1.54 reading, the three `*.cpu.affinity` names are new).
///
/// XA-4 is "no `Pending`": every name has a disposition and a reason.
[[nodiscard]] std::span<const ConfigEntry> archive_config_names() noexcept;

/// Null when the name is not a reference archive name.
[[nodiscard]] const ConfigEntry* find_archive_config(std::string_view name) noexcept;

[[nodiscard]] std::string_view disposition_name(ConfigDisposition disposition) noexcept;

}  // namespace deepmsg::archive::server
