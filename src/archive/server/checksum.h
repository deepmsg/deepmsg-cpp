#pragma once

#include <cstdint>

namespace deepmsg::archive::server
{

/// Reference: `io.aeron.archive.checksum.Checksum` (`Checksum.java:24-40`).
///
/// Instances are stateless and thread-safe, as the reference requires. The
/// signature keeps the reference's `(address, offset, length)` shape so call
/// sites read like the Java ones.
///
/// A1a ships CRC-32 only because the catalog needs it. A1b adds CRC-32C and the
/// hardware paths; the interface does not change.
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
class Crc32 final : public Checksum
{
public:
    [[nodiscard]] std::int32_t compute(const std::uint8_t* address, std::int32_t offset,
                                       std::int32_t length) const noexcept override;
};

/// `Checksums.crc32()`: a process-wide stateless instance.
[[nodiscard]] const Checksum& crc32() noexcept;

}  // namespace deepmsg::archive::server
