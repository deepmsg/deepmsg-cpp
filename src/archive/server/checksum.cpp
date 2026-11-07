#include "archive/server/checksum.h"

#include <array>

namespace deepmsg::archive::server
{
namespace
{

constexpr std::array<std::uint32_t, 256> make_crc32_table() noexcept
{
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t index = 0; index < table.size(); index++)
    {
        std::uint32_t crc = index;
        for (int bit = 0; bit < 8; bit++)
        {
            crc = (0 != (crc & 1u)) ? ((crc >> 1) ^ 0xEDB88320u) : (crc >> 1);
        }
        table[index] = crc;
    }
    return table;
}

constexpr std::array<std::uint32_t, 256> CRC32_TABLE = make_crc32_table();

}  // namespace

std::int32_t Crc32::compute(const std::uint8_t* address, std::int32_t offset, std::int32_t length) const noexcept
{
    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::int32_t index = 0; index < length; index++)
    {
        crc = CRC32_TABLE[(crc ^ address[offset + index]) & 0xFFu] ^ (crc >> 8);
    }
    return static_cast<std::int32_t>(crc ^ 0xFFFFFFFFu);
}

const Checksum& crc32() noexcept
{
    static const Crc32 instance;
    return instance;
}

}  // namespace deepmsg::archive::server
