#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace axc::core
{

[[nodiscard]] bool file_exists(std::string_view path) noexcept;
[[nodiscard]] bool directory_exists(std::string_view path) noexcept;
[[nodiscard]] std::expected<std::size_t, Error> file_size(std::string_view path) noexcept;
[[nodiscard]] std::expected<void, Error> remove_file(std::string_view path) noexcept;

/// `mkdir -p`: creates every missing component. Existing directories are fine.
[[nodiscard]] std::expected<void, Error> create_directories(std::string_view path) noexcept;

/// `aeron_usable_fs_space` (`aeron_fileutil.c:620`): `f_frsize * f_bavail` for
/// the filesystem holding `path`, 0 when the path cannot be queried.
[[nodiscard]] std::int64_t usable_space(std::string_view path) noexcept;

}  // namespace axc::core
