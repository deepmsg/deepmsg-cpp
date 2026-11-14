// C4e: `runTerminationHooks` (`ConsensusModuleAgent.java:3457-3470`,
// `ClusteredServiceAgent.java:1302-1315`): extended first, termination second,
// each exception counted without stopping the other hook.

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "cluster/common/termination_hook.h"

using deepmsg::cluster::common::ExtendedTerminationHook;
using deepmsg::cluster::common::run_termination_hooks;
using deepmsg::cluster::common::TerminationCause;
using deepmsg::cluster::common::TerminationHook;

TEST(TerminationHookTest, runsExtendedThenTermination)
{
    std::vector<std::string> order;
    ExtendedTerminationHook extended = [&order](const TerminationCause&) { order.emplace_back("extended"); };
    TerminationHook termination = [&order]() { order.emplace_back("termination"); };

    int reported = 0;
    run_termination_hooks(extended, termination, TerminationCause{}, [&reported](const auto&) { reported++; });

    ASSERT_EQ(2U, order.size());
    EXPECT_EQ("extended", order[0]);
    EXPECT_EQ("termination", order[1]);
    EXPECT_EQ(0, reported);
}

TEST(TerminationHookTest, aThrowingExtendedStillRunsTermination)
{
    std::vector<std::string> order;
    ExtendedTerminationHook extended = [&order](const TerminationCause&)
    {
        order.emplace_back("extended");
        throw std::runtime_error("hook exploded");
    };
    TerminationHook termination = [&order]() { order.emplace_back("termination"); };

    std::vector<std::string> reported;
    run_termination_hooks(extended, termination, TerminationCause{},
                          [&reported](const deepmsg::cluster::common::Error& error)
                          { reported.emplace_back(error.message.view()); });

    ASSERT_EQ(2U, order.size());
    EXPECT_EQ("extended", order[0]);
    EXPECT_EQ("termination", order[1]);
    ASSERT_EQ(1U, reported.size());
    EXPECT_EQ("hook exploded", reported[0]);
}

TEST(TerminationHookTest, theCauseCarriesExpectedAndTheError)
{
    bool seen_expected = false;
    std::string seen_message;

    const TerminationCause cause{false, axc::core::make_error(axc::core::ErrorCode::protocol_error, "archive gone")};
    ExtendedTerminationHook extended = [&](const TerminationCause& received)
    {
        seen_expected = received.expected;
        seen_message = received.error.message.view();
    };

    int reported = 0;
    run_termination_hooks(extended, {}, cause, [&reported](const auto&) { reported++; });

    EXPECT_FALSE(seen_expected);
    EXPECT_EQ("archive gone", seen_message);
    EXPECT_EQ(0, reported);
}

TEST(TerminationHookTest, emptyHooksAreANoOp)
{
    int reported = 0;
    run_termination_hooks({}, {}, TerminationCause{}, [&reported](const auto&) { reported++; });
    EXPECT_EQ(0, reported);
}
