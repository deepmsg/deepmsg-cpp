#pragma once

#include <cstddef>
#include <expected>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "core/error/error.h"

namespace axc::core
{

/// Configuration values in Java property form (`aeron.archive.dir`, ...).
///
/// Precedence, highest first: `-D` arguments, properties file, environment.
///
/// Startup/config path only -- this is not a hot-path container.
class PropertySource
{
public:
    PropertySource() = default;

    /// Collects `-Dname=value` from the argument vector. Other arguments are
    /// left to the application.
    [[nodiscard]] static std::expected<PropertySource, Error> from_args(int argc, const char* const argv[]) noexcept;

    /// `key=value` lines; `#` and `!` comments; whitespace trimmed. Values already
    /// present (from `-D`) are not overwritten.
    [[nodiscard]] std::expected<void, Error> load_file(std::string_view path) noexcept;

    /// `AERON_DIR` -> `aeron.dir`, `AERON_ARCHIVE_MAX_CONCURRENT_RECORDINGS` ->
    /// `aeron.archive.max.concurrent.recordings`. `name_prefix` is the property
    /// namespace the variable is filed under, so `DEEPMSG_DPDK_ENABLED` with
    /// prefix `DEEPMSG_` and name prefix `deepmsg.` becomes
    /// `deepmsg.dpdk.enabled`. Existing values win.
    [[nodiscard]] std::expected<void, Error> load_environment(std::string_view prefix = "AERON_",
                                                              std::string_view name_prefix = "aeron.") noexcept;

    /// Explicit override (highest precedence).
    void set(std::string_view name, std::string_view value);

    [[nodiscard]] std::optional<std::string_view> get(std::string_view name) const noexcept;
    [[nodiscard]] std::string get_or(std::string_view name, std::string_view fallback) const;
    [[nodiscard]] bool contains(std::string_view name) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept
    {
        return values_.size();
    }

    template <class Consumer>
    void for_each(Consumer&& consumer) const
    {
        for (const auto& [name, value] : values_)
        {
            consumer(name, value);
        }
    }

private:
    void set_if_absent(std::string_view name, std::string_view value);

    std::map<std::string, std::string, std::less<>> values_;
};

}  // namespace axc::core
