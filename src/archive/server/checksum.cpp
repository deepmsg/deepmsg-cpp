#include "archive/server/checksum.h"

#include <array>
#include <string>

#if defined(__x86_64__) || defined(__i386__)
#include <nmmintrin.h>
#define AXC_HAVE_X86_CRC32 1
#endif

namespace deepmsg::archive::server
{
namespace
{

constexpr std::uint32_t CRC32_POLYNOMIAL = 0xEDB88320u;
constexpr std::uint32_t CRC32C_POLYNOMIAL = 0x82F63B78u;

/// Slicing-by-8 tables: `table[0]` is the plain byte table, `table[n]` the same
/// table advanced by `n` bytes, so eight bytes are consumed per iteration.
template <std::uint32_t Polynomial>
constexpr std::array<std::array<std::uint32_t, 256>, 8> make_slicing_tables() noexcept
{
    std::array<std::array<std::uint32_t, 256>, 8> tables{};

    for (std::uint32_t index = 0; index < 256; index++)
    {
        std::uint32_t crc = index;
        for (int bit = 0; bit < 8; bit++)
        {
            crc = (0 != (crc & 1u)) ? ((crc >> 1) ^ Polynomial) : (crc >> 1);
        }
        tables[0][index] = crc;
    }

    for (std::size_t slice = 1; slice < tables.size(); slice++)
    {
        for (std::uint32_t index = 0; index < 256; index++)
        {
            const std::uint32_t previous = tables[slice - 1][index];
            tables[slice][index] = (previous >> 8) ^ tables[0][previous & 0xFFu];
        }
    }

    return tables;
}

constexpr auto CRC32_TABLES = make_slicing_tables<CRC32_POLYNOMIAL>();
constexpr auto CRC32C_TABLES = make_slicing_tables<CRC32C_POLYNOMIAL>();

template <const auto& Tables>
[[nodiscard]] std::uint32_t slicing_by_8(const std::uint8_t* data, std::int32_t length) noexcept
{
    std::uint32_t crc = 0xFFFFFFFFu;

    while (length >= 8)
    {
        std::uint32_t low = 0;
        std::uint32_t high = 0;
        for (int index = 0; index < 4; index++)
        {
            low |= static_cast<std::uint32_t>(data[index]) << (8 * index);
            high |= static_cast<std::uint32_t>(data[index + 4]) << (8 * index);
        }

        crc ^= low;
        crc = Tables[7][crc & 0xFFu] ^ Tables[6][(crc >> 8) & 0xFFu] ^ Tables[5][(crc >> 16) & 0xFFu] ^
              Tables[4][(crc >> 24) & 0xFFu] ^ Tables[3][high & 0xFFu] ^ Tables[2][(high >> 8) & 0xFFu] ^
              Tables[1][(high >> 16) & 0xFFu] ^ Tables[0][(high >> 24) & 0xFFu];

        data += 8;
        length -= 8;
    }

    while (length > 0)
    {
        crc = Tables[0][(crc ^ *data) & 0xFFu] ^ (crc >> 8);
        data++;
        length--;
    }

    return crc ^ 0xFFFFFFFFu;
}

#if defined(AXC_HAVE_X86_CRC32)

/// CRC-32C via the SSE4.2 `crc32` instruction (which is Castagnoli, not IEEE).
[[nodiscard]] __attribute__((target("sse4.2"))) std::uint32_t crc32c_hardware(const std::uint8_t* data,
                                                                              std::int32_t length) noexcept
{
    std::uint64_t crc = 0xFFFFFFFFu;

    while (length >= 8)
    {
        std::uint64_t value = 0;
        for (int index = 0; index < 8; index++)
        {
            value |= static_cast<std::uint64_t>(data[index]) << (8 * index);
        }
        crc = _mm_crc32_u64(crc, value);
        data += 8;
        length -= 8;
    }

    auto crc32 = static_cast<std::uint32_t>(crc);
    while (length > 0)
    {
        crc32 = _mm_crc32_u8(crc32, *data);
        data++;
        length--;
    }

    return crc32 ^ 0xFFFFFFFFu;
}

[[nodiscard]] bool has_sse42() noexcept
{
    static const bool supported = 0 != __builtin_cpu_supports("sse4.2");
    return supported;
}

#endif

}  // namespace

std::int32_t Crc32::compute(const std::uint8_t* address, std::int32_t offset, std::int32_t length) const noexcept
{
    return static_cast<std::int32_t>(slicing_by_8<CRC32_TABLES>(address + offset, length));
}

std::int32_t Crc32c::compute(const std::uint8_t* address, std::int32_t offset, std::int32_t length) const noexcept
{
    const std::uint8_t* data = address + offset;
#if defined(AXC_HAVE_X86_CRC32)
    if (has_sse42())
    {
        return static_cast<std::int32_t>(crc32c_hardware(data, length));
    }
#endif
    return detail::crc32c_tables(address, offset, length);
}

namespace detail
{

std::int32_t crc32c_tables(const std::uint8_t* address, std::int32_t offset, std::int32_t length) noexcept
{
    return static_cast<std::int32_t>(slicing_by_8<CRC32C_TABLES>(address + offset, length));
}

bool crc32c_has_hardware() noexcept
{
#if defined(AXC_HAVE_X86_CRC32)
    return has_sse42();
#else
    return false;
#endif
}

}  // namespace detail

const Checksum& crc32() noexcept
{
    static const Crc32 instance;
    return instance;
}

const Checksum& crc32c() noexcept
{
    static const Crc32c instance;
    return instance;
}

std::expected<const Checksum*, axc::core::Error> checksum_by_name(std::string_view name)
{
    // `Checksums.newInstance` (`Checksums.java:65-90`).
    if (name.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument, "className is empty"));
    }
    if (name == "CRC-32" || name == "io.aeron.archive.checksum.Crc32" || name == "org.agrona.checksum.Crc32")
    {
        return &crc32();
    }
    if (name == "CRC-32C" || name == "io.aeron.archive.checksum.Crc32c" || name == "org.agrona.checksum.Crc32c")
    {
        return &crc32c();
    }
    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                 "failed to create Checksum instance for class: " + std::string{name}));
}

}  // namespace deepmsg::archive::server
