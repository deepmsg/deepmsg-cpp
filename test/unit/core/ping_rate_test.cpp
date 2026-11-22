// The P6 ping probe's achieved-rate denominator. The probe is a bench binary
// outside the gate, so the arithmetic that decides whether an arm kept its pace
// lives in `bench/dpdk-p6/probes/rate.h` and is pinned here.

#include <gtest/gtest.h>

#include <cstdint>

#include "rate.h"

TEST(PingRate, TheSpanIsTheSendSide)
{
    // 100 messages, the first is the unpaced handshake: 99 paced messages over
    // 990 ms of send-side span is 100/s. The last message's 10 ms round trip is
    // not part of the span; a receive-side span (1 s) would report 99/s.
    EXPECT_EQ(100, p6_achieved_rate(100, 0, 990'000'000));
    EXPECT_EQ(99, p6_achieved_rate(100, 0, 1'000'000'000));
}

TEST(PingRate, DegenerateSamples)
{
    EXPECT_EQ(0, p6_achieved_rate(100, 5'000, 5'000)) << "a zero span has no rate";
    EXPECT_EQ(0, p6_achieved_rate(1, 0, 0)) << "a lone message has no span";
    EXPECT_EQ(0, p6_achieved_rate(0, 0, 1'000'000'000)) << "an empty run has no paced count";
}
