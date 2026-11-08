#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "core/error/error.h"

namespace axc::core
{

/// Shared read/write mapping of a file (`mmap(MAP_SHARED)`).
///
/// Startup/data-structure path only: creating and growing map or unmap, never
/// inside a hot loop. After `grow()` every previously obtained pointer is
/// invalid; callers re-take `mapping()`.
class MappedFile
{
public:
    static std::expected<MappedFile, Error> create(std::string_view path, std::size_t length) noexcept;
    static std::expected<MappedFile, Error> open(std::string_view path) noexcept;
    /// `writable = false` opens the file `O_RDONLY` and maps it `PROT_READ`, so a
    /// read-only file or mount can be inspected (reference: `Catalog`'s read-only
    /// open, `Catalog.java:270-276`).
    static std::expected<MappedFile, Error> open(std::string_view path, bool writable) noexcept;

    MappedFile() noexcept = default;
    MappedFile(MappedFile&& other) noexcept;
    MappedFile& operator=(MappedFile&& other) noexcept;
    ~MappedFile();

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    [[nodiscard]] bool valid() const noexcept
    {
        return nullptr != address_;
    }

    [[nodiscard]] std::size_t length() const noexcept
    {
        return length_;
    }

    [[nodiscard]] std::span<std::uint8_t> mapping() noexcept
    {
        return {address_, length_};
    }

    [[nodiscard]] std::span<const std::uint8_t> mapping() const noexcept
    {
        return {address_, length_};
    }

    /// `msync(MS_SYNC)` over the whole mapping.
    [[nodiscard]] std::expected<void, Error> flush() noexcept;

    /// Grows the file and remaps it. Data already written is preserved.
    [[nodiscard]] std::expected<void, Error> grow(std::size_t new_length) noexcept;

private:
    void close() noexcept;

    int fd_{-1};
    std::uint8_t* address_{nullptr};
    std::size_t length_{0};
};

}  // namespace axc::core
