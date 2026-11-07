#pragma once

#include <cstdint>
#include <expected>
#include <string_view>

#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// Reference: `io.aeron.archive.checksum.Checksum` (`Checksum.java:24-40`).
///
/// Instances are stateless and thread-safe, as the reference requires. The
/// signature keeps the reference's `(address, offset, length)` shape so call
/// sites read like the Java ones.
class Checksum
{
public:
    Checksum() = default;
    virtual ~Checksum() = default;

    Checksum(const Checksum&) = delete;
    Checksum& operator=(const Checksum&) = delete;

    /// `int Checksum.compute(long address, int offset, int length)`.
    [[nodiscard]] virtual std::int32_t compute(const std::uint8_t* address, std::int32_t offset,
                                               std::int32_t length) const noexcept = 0;
};

/// IEEE 802.3 CRC-32 with the reflected polynomial `0xEDB88320`, i.e. exactly
/// what `java.util.zip.CRC32` produces (`Checksums.crc32()`, `Checksums.java:39`).
/// Slicing-by-8; x86 has no single instruction for this polynomial, see
/// `records/a1b.md`.
class Crc32 final : public Checksum
{
public:
    [[nodiscard]] std::int32_t compute(const std::uint8_t* address, std::int32_t offset,
                                       std::int32_t length) const noexcept override;
};

/// CRC-32C (Castagnoli, reflected polynomial `0x82F63B78`), what
/// `Checksums.crc32c()` (`Checksums.java:49`) and `java.util.zip.CRC32C`
/// produce. Uses the SSE4.2 `crc32` instruction when the CPU has it, and a table
/// otherwise.
class Crc32c final : public Checksum
{
public:
    [[nodiscard]] std::int32_t compute(const std::uint8_t* address, std::int32_t offset,
                                       std::int32_t length) const noexcept override;
};

/// `Checksums.crc32()`: a process-wide stateless instance.
[[nodiscard]] const Checksum& crc32() noexcept;

/// `Checksums.crc32c()`: a process-wide stateless instance.
[[nodiscard]] const Checksum& crc32c() noexcept;

/// `Checksums.newInstance` (`Checksums.java:65-90`): the two aliases and the two
/// class names the reference accepts for each implementation.
[[nodiscard]] std::expected<const Checksum*, axc::core::Error> checksum_by_name(std::string_view name);

namespace detail
{

/// The table implementation behind `Crc32c`, exposed so the criterion can
/// exercise it on hosts where the SSE4.2 path is taken.
[[nodiscard]] std::int32_t crc32c_tables(const std::uint8_t* address, std::int32_t offset,
                                         std::int32_t length) noexcept;

/// True when `Crc32c` uses the SSE4.2 instruction on this host.
[[nodiscard]] bool crc32c_has_hardware() noexcept;

}  // namespace detail

}  // namespace deepmsg::archive::server
