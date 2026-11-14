#include <gtest/gtest.h>

#include "core/media/dpdk/stack/packet_io.h"

namespace
{

using axc::core::media::dpdk::frame::Checksums;
using axc::core::media::dpdk::packet_io::Offloads;
using axc::core::media::dpdk::packet_io::Verdict;

constexpr Offloads JUDGING{true, true};

TEST(PacketIo, APortThatJudgesBothChecksumsHasItsVerdictTaken)
{
    EXPECT_TRUE(JUDGING.judges_both());
    EXPECT_EQ(Checksums::verified, JUDGING.checksums(Verdict::passed));
}

TEST(PacketIo, APortThatJudgesOneChecksumVouchesForNothing)
{
    const Offloads cases[] = {Offloads::none(), Offloads{true, false}, Offloads{false, true}};
    for (const auto& offloads : cases)
    {
        EXPECT_FALSE(offloads.judges_both());
        EXPECT_EQ(Checksums::software, offloads.checksums(Verdict::passed));
    }
}

TEST(PacketIo, AFrameTheHardwareRejectedIsTheStacksToCheck)
{
    EXPECT_EQ(Checksums::software, JUDGING.checksums(Verdict::failed));
    EXPECT_EQ(Checksums::software, JUDGING.checksums(Verdict::not_judged));
}

}  // namespace
