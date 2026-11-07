#pragma once

#include <cstddef>
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

}  // namespace axc::core
