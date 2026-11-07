// CRC criterion for card A1b.
//
// The expected values come from the reference (`Checksums.crc32()` and
// `Checksums.crc32c()`, Aeron 1.54.0 = 454da1e4e6) over the byte pattern
// `byte(i) = (i * 31 + 7) & 0xFF`, generated with
// `analysis/cpp/records/a1b/CrcGoldenGen.java`.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

#include "archive/server/checksum.h"

namespace
{

using deepmsg::archive::server::checksum_by_name;
using deepmsg::archive::server::crc32;
using deepmsg::archive::server::crc32c;

struct Vector
{
    std::int32_t length;
    std::int32_t crc32;
    std::int32_t crc32c;
};

// From the reference run (see the header comment).
constexpr Vector VECTORS[] = {
    {0, 0, 0},
    {1, 1281784366, -2034812998},
    {7, 1044920610, 1360044306},
    {8, -1487535064, 1081695346},
    {9, -904724738, 1877171622},
    {15, -1887962437, -1693706985},
    {16, 104245397, -814201436},
    {31, 1171692296, 390269331},
    {32, 1227094950, -1698274896},
    {63, 2034968221, -687787184},
    {64, -2067242872, 723346904},
    {100, -1247201796, -496221156},
    {137, 985618193, 950103570},
    {1024, 2083658589, -1511672651},
};

[[nodiscard]] std::vector<std::uint8_t> pattern(std::size_t length)
{
    std::vector<std::uint8_t> bytes(length);
    for (std::size_t index = 0; index < length; index++)
    {
        bytes[index] = static_cast<std::uint8_t>((index * 31 + 7) & 0xFF);
    }
    return bytes;
}

TEST(ChecksumTest, matchesTheReferenceCrc32AndCrc32c)
{
    const std::vector<std::uint8_t> bytes = pattern(2048);
    for (const Vector& vector : VECTORS)
    {
        EXPECT_EQ(vector.crc32, crc32().compute(bytes.data(), 0, vector.length)) << "crc32 length " << vector.length;
        EXPECT_EQ(vector.crc32c, crc32c().compute(bytes.data(), 0, vector.length)) << "crc32c length " << vector.length;
    }
}

TEST(ChecksumTest, matchesTheReferenceFromAnOffset)
{
    const std::vector<std::uint8_t> bytes = pattern(2048);
    // Reference: `c32.compute(addr, 32, 64)` / `c32c.compute(addr, 32, 64)`.
    EXPECT_EQ(-146519094, crc32().compute(bytes.data(), 32, 64));
    EXPECT_EQ(29663986, crc32c().compute(bytes.data(), 32, 64));
}

TEST(ChecksumTest, theCrc32cTablePathAgreesWithTheHardwarePath)
{
    // The reference does not pin the table path, but a host without SSE4.2 takes
    // it, so it has to produce the same bytes.
    const std::vector<std::uint8_t> bytes = pattern(4096);
    for (const Vector& vector : VECTORS)
    {
        EXPECT_EQ(vector.crc32c, deepmsg::archive::server::detail::crc32c_tables(bytes.data(), 0, vector.length))
            << "length " << vector.length;
    }
    for (std::int32_t length = 0; length < 128; length++)
    {
        EXPECT_EQ(crc32c().compute(bytes.data(), 3, length),
                  deepmsg::archive::server::detail::crc32c_tables(bytes.data(), 3, length))
            << "length " << length;
    }
}

TEST(ChecksumTest, knownCheckValue)
{
    // The CRC catalogue's check value for "123456789".
    const char* input = "123456789";
    const auto* bytes = reinterpret_cast<const std::uint8_t*>(input);
    EXPECT_EQ(static_cast<std::int32_t>(0xCBF43926u), crc32().compute(bytes, 0, 9));
    EXPECT_EQ(static_cast<std::int32_t>(0xE3069283u), crc32c().compute(bytes, 0, 9));
}

TEST(ChecksumTest, nameResolutionFollowsTheReference)
{
    for (const char* name : {"CRC-32", "io.aeron.archive.checksum.Crc32", "org.agrona.checksum.Crc32"})
    {
        auto resolved = checksum_by_name(name);
        ASSERT_TRUE(resolved) << name;
        EXPECT_EQ(&crc32(), *resolved) << name;
    }
    for (const char* name : {"CRC-32C", "io.aeron.archive.checksum.Crc32c", "org.agrona.checksum.Crc32c"})
    {
        auto resolved = checksum_by_name(name);
        ASSERT_TRUE(resolved) << name;
        EXPECT_EQ(&crc32c(), *resolved) << name;
    }

    EXPECT_FALSE(checksum_by_name(""));
    EXPECT_FALSE(checksum_by_name("com.example.NotAChecksum"));
    auto rejected = checksum_by_name("");
    ASSERT_FALSE(rejected);
    EXPECT_EQ("className is empty", rejected.error().message.view());
}

}  // namespace
