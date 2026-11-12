// Port of `ClusterMemberTest` (Aeron 1.54.0, `454da1e4e6`). Parameterised
// cases are expanded; names keep the reference method names.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "cluster/common/cluster_member.h"

namespace
{

using deepmsg::cluster::common::are_same_endpoints;
using deepmsg::cluster::common::become_candidate;
using deepmsg::cluster::common::ClusterMember;
using deepmsg::cluster::common::ClusterMembers;
using deepmsg::cluster::common::compare_log;
using deepmsg::cluster::common::determine_member;
using deepmsg::cluster::common::has_active_quorum;
using deepmsg::cluster::common::has_quorum_at_position;
using deepmsg::cluster::common::is_quorum_candidate;
using deepmsg::cluster::common::is_quorum_leader;
using deepmsg::cluster::common::is_unanimous_candidate;
using deepmsg::cluster::common::is_unanimous_leader;
using deepmsg::cluster::common::parse_cluster_members;
using deepmsg::cluster::common::parse_member_endpoints;
using deepmsg::cluster::common::quorum_position;
using deepmsg::cluster::common::quorum_threshold;
using deepmsg::cluster::common::set_is_leader;

constexpr std::int64_t NULL_VALUE = -1;
constexpr std::int64_t NULL_POSITION = -1;

[[nodiscard]] ClusterMember new_member(std::int32_t id)
{
    ClusterMember member;
    member.id = id;
    return member;
}

[[nodiscard]] ClusterMember new_member(std::int32_t id, std::int64_t leadership_term_id, std::int64_t log_position,
                                       std::int64_t time_of_last_append_position_ns)
{
    ClusterMember member = new_member(id);
    member.leadership_term_id = leadership_term_id;
    member.log_position = log_position;
    member.time_of_last_append_position_ns = time_of_last_append_position_ns;
    return member;
}

class ClusterMemberTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto parsed = parse_cluster_members(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");
        ASSERT_TRUE(parsed) << parsed.error().message.view();
        members_ = std::move(*parsed);

        auto archive = parse_cluster_members(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|"
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|"
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|");
        ASSERT_TRUE(archive) << archive.error().message.view();
        members_with_archive_response_ = std::move(*archive);

        auto egress = parse_cluster_members(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint,"
            "egressResponseEndpoint|"
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint,"
            "egressResponseEndpoint|"
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint,"
            "egressResponseEndpoint|");
        ASSERT_TRUE(egress) << egress.error().message.view();
        members_with_egress_response_ = std::move(*egress);

        ranked_positions_.assign(quorum_threshold(static_cast<std::int32_t>(members_.size())), 0);
    }

    ClusterMembers members_{};
    ClusterMembers members_with_archive_response_{};
    ClusterMembers members_with_egress_response_{};
    std::vector<std::int64_t> ranked_positions_{};
};

TEST_F(ClusterMemberTest, shouldParseCorrectly)
{
    for (const ClusterMember& member : members_)
    {
        EXPECT_EQ("ingressEndpoint", member.ingress_endpoint);
        EXPECT_EQ("consensusEndpoint", member.consensus_endpoint);
        EXPECT_EQ("logEndpoint", member.log_endpoint);
        EXPECT_EQ("catchupEndpoint", member.catchup_endpoint);
        EXPECT_EQ("archiveEndpoint", member.archive_endpoint);
        EXPECT_FALSE(member.archive_response_endpoint.has_value());
        EXPECT_FALSE(member.egress_response_endpoint.has_value());
        EXPECT_EQ("ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint", member.endpoints);
    }
}

TEST_F(ClusterMemberTest, shouldParseCorrectlyOptionalArchiveResponse)
{
    for (const ClusterMember& member : members_with_archive_response_)
    {
        EXPECT_EQ("archiveResponseEndpoint", member.archive_response_endpoint);
        EXPECT_FALSE(member.egress_response_endpoint.has_value());
        EXPECT_EQ(
            "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint",
            member.endpoints);
    }
}

TEST_F(ClusterMemberTest, shouldParseCorrectlyOptionalEgressResponseEntry)
{
    for (const ClusterMember& member : members_with_egress_response_)
    {
        EXPECT_EQ("archiveResponseEndpoint", member.archive_response_endpoint);
        EXPECT_EQ("egressResponseEndpoint", member.egress_response_endpoint);
        EXPECT_EQ(
            "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,"
            "archiveResponseEndpoint,egressResponseEndpoint",
            member.endpoints);
    }
}

TEST_F(ClusterMemberTest, shouldDetermineQuorumSize)
{
    EXPECT_EQ(1, quorum_threshold(1));
    EXPECT_EQ(2, quorum_threshold(2));
    EXPECT_EQ(2, quorum_threshold(3));
    EXPECT_EQ(3, quorum_threshold(4));
    EXPECT_EQ(3, quorum_threshold(5));
    EXPECT_EQ(4, quorum_threshold(6));
    EXPECT_EQ(4, quorum_threshold(7));
}

TEST_F(ClusterMemberTest, shouldCheckMemberIsActive)
{
    struct Case
    {
        std::int64_t time_of_last_append_position_ns;
        std::int64_t now_ns;
        std::int64_t timeout_ns;
        bool expected;
    };

    const Case cases[] = {{0, 0, 0, false}, {0, 0, 1, true},   {0, 1, 1, false},  {0, 2, 1, false},
                          {1, 2, 1, false}, {1, 2, 5, true},   {1, 5, 5, true},   {1, 8, 5, false},
                          {10, 8, 1, true}, {10, 10, 1, true}, {10, 12, 1, false}};

    for (const Case& test_case : cases)
    {
        ClusterMember member = new_member(0);
        member.time_of_last_append_position_ns = test_case.time_of_last_append_position_ns;
        EXPECT_EQ(test_case.expected, member.is_active(test_case.now_ns, test_case.timeout_ns));
    }
}

TEST_F(ClusterMemberTest, shouldRankClusterStart)
{
    EXPECT_EQ(0, quorum_position(members_, ranked_positions_, 0, 10));
}

TEST_F(ClusterMemberTest, shouldDetermineQuorumPosition)
{
    struct Case
    {
        std::int64_t member0;
        std::int64_t member1;
        std::int64_t member2;
        std::int64_t expected;
    };

    const Case cases[] = {{0, 0, 0, 0},       {123, 0, 0, 0}, {123, 123, 0, 123}, {123, 123, 123, 123},
                          {0, 123, 123, 123}, {0, 0, 123, 0}, {0, 123, 200, 123}, {5, 3, 1, 3},
                          {5, 1, 3, 3},       {1, 3, 5, 3},   {1, 5, 3, 3},       {3, 1, 5, 3},
                          {3, 5, 1, 3}};

    for (const Case& test_case : cases)
    {
        members_[0].log_position = test_case.member0;
        members_[1].log_position = test_case.member1;
        members_[2].log_position = test_case.member2;
        EXPECT_EQ(test_case.expected, quorum_position(members_, ranked_positions_, 0, 10));
    }
}

TEST_F(ClusterMemberTest, shouldOnlyConsiderActiveNodesWhenDeterminingQuorumPosition)
{
    struct Case
    {
        std::int64_t positions[5];
        std::int64_t timestamps[5];
        std::int64_t now_ns;
        std::int64_t timeout_ns;
        std::int64_t expected;
    };

    const Case cases[] = {{{0, 0, 0, 0, 0}, {0, 0, 0, 0, 0}, 10, 0, 0}, {{1, 2, 3, 4, 5}, {0, 0, 0, 0, 0}, 5, 10, 3},
                          {{1, 2, 3, 2, 5}, {0, 0, 0, 0, 0}, 5, 10, 2}, {{1, 2, 3, 2, 5}, {1, 0, 1, 0, 1}, 5, 5, 1},
                          {{5, 2, 3, 2, 1}, {1, 0, 1, 0, 1}, 5, 5, 1},  {{5, 2, 3, 2, 1}, {0, 1, 0, 1, 0}, 5, 5, 0},
                          {{5, 2, 3, 2, 1}, {0, 1, 0, 1, 1}, 5, 5, 1},  {{5, 2, 3, 2, 1}, {0, 1, 1, 1, 0}, 5, 5, 2},
                          {{5, 2, 3, 2, 1}, {0, 2, 3, 1, 5}, 8, 7, 1},  {{5, 2, 3, 2, 1}, {0, 0, 0, 0, 0}, 8, 7, 0},
                          {{5, 2, 3, 2, 1}, {5, 0, 0, 0, 0}, 8, 7, 0},  {{5, 2, 3, 2, 1}, {5, 5, 0, 0, 0}, 8, 7, 0},
                          {{5, 2, 3, 2, 1}, {5, 5, 5, 0, 0}, 8, 7, 2},  {{4, 2, 4, 2, 1}, {5, 5, 5, 0, 0}, 8, 7, 2},
                          {{4, 2, 4, 2, 3}, {5, 5, 5, 0, 3}, 8, 7, 3}};

    for (const Case& test_case : cases)
    {
        ClusterMembers cluster_members;
        for (std::int32_t i = 0; i < 5; ++i)
        {
            cluster_members.push_back(new_member(i, 0, test_case.positions[i], test_case.timestamps[i]));
        }

        std::vector<std::int64_t> positions(
            static_cast<std::size_t>(quorum_threshold(static_cast<std::int32_t>(cluster_members.size()))));
        EXPECT_EQ(test_case.expected,
                  quorum_position(cluster_members, positions, test_case.now_ns, test_case.timeout_ns));
    }
}

TEST_F(ClusterMemberTest, isUnanimousCandidateReturnFalseIfThereIsAMemberWithoutLogPosition)
{
    const ClusterMember candidate = new_member(4, 100, 1000, 0);
    const ClusterMembers members{new_member(1, 2, 100, 0), new_member(2, 8, NULL_POSITION, 0), new_member(3, 1, 1, 0)};

    EXPECT_FALSE(is_unanimous_candidate(members, candidate, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateReturnFalseIfThereIsAMemberWithMoreUpToDateLog)
{
    const ClusterMember candidate = new_member(4, 10, 800, 0);
    const ClusterMembers members{new_member(1, 2, 100, 0), new_member(2, 8, 6, 0), new_member(3, 11, 1000, 0)};

    EXPECT_FALSE(is_unanimous_candidate(members, candidate, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateReturnFalseIfLeaderClosesGracefully)
{
    const ClusterMember candidate = new_member(2, 2, 100, 0);
    const ClusterMembers members{new_member(1, 2, 100, 0), new_member(2, 2, 100, 0)};

    EXPECT_FALSE(is_unanimous_candidate(members, candidate, 1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateReturnTrueIfTheCandidateHasTheMostUpToDateLog)
{
    const ClusterMember candidate = new_member(2, 10, 800, 0);
    const ClusterMembers members{new_member(10, 2, 100, 0), new_member(20, 8, 6, 0), new_member(30, 10, 800, 0)};

    EXPECT_TRUE(is_unanimous_candidate(members, candidate, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isQuorumCandidateReturnFalseWhenQuorumIsNotReached)
{
    const ClusterMember candidate = new_member(2, 10, 800, 0);
    const ClusterMembers members{new_member(10, 2, 100, 0), new_member(20, 18, 600, 0), new_member(30, 10, 800, 0),
                                 new_member(40, 19, 800, 0), new_member(50, 10, 1000, 0)};

    EXPECT_FALSE(is_quorum_candidate(members, candidate));
}

TEST_F(ClusterMemberTest, isQuorumCandidateReturnTrueWhenQuorumIsReached)
{
    const ClusterMember candidate = new_member(2, 10, 800, 0);
    const ClusterMembers members{new_member(10, 2, 100, 0), new_member(20, 18, 600, 0), new_member(30, 10, 800, 0),
                                 new_member(40, 9, 800, 0), new_member(50, 10, 700, 0)};

    EXPECT_TRUE(is_quorum_candidate(members, candidate));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsTrueWhenQuorumIsReached)
{
    const std::int64_t candidate_term_id = -5;
    ClusterMembers members{new_member(1), new_member(2), new_member(3), new_member(4), new_member(5)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id * 2;
    members[1].vote = false;
    members[2].candidate_term_id = candidate_term_id;
    members[3].candidate_term_id = candidate_term_id;
    members[3].vote = true;
    members[4].candidate_term_id = candidate_term_id;
    members[4].vote = true;

    EXPECT_TRUE(is_quorum_leader(members, candidate_term_id));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsFalseIfAtLeastOneNegativeVoteIsDetected)
{
    const std::int64_t candidate_term_id = 8;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id;
    members[1].vote = false;
    members[2].candidate_term_id = candidate_term_id;
    members[2].vote = true;

    EXPECT_FALSE(is_quorum_leader(members, candidate_term_id));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsFalseWhenQuorumIsNotReached)
{
    const std::int64_t candidate_term_id = 2;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id;
    members[2].candidate_term_id = candidate_term_id + 5;
    members[2].vote = true;

    EXPECT_FALSE(is_quorum_leader(members, candidate_term_id));
}

TEST_F(ClusterMemberTest, isUnanimousLeaderReturnsFalseIfThereIsAtLeastOneNegativeVoteForAGivenCandidateTerm)
{
    const std::int64_t candidate_term_id = 42;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id;
    members[1].vote = true;
    members[2].candidate_term_id = candidate_term_id;
    members[2].vote = false;

    EXPECT_FALSE(is_unanimous_leader(members, candidate_term_id, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isUnanimousLeaderReturnsFalseIfLeaderClosesGracefully)
{
    const std::int64_t candidate_term_id = 7;
    ClusterMembers members{new_member(1), new_member(2)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id;
    members[1].vote = true;

    EXPECT_FALSE(is_unanimous_leader(members, candidate_term_id, 1));
}

TEST_F(ClusterMemberTest, isUnanimousLeaderReturnsFalseIfNotAllNodesVotedPositively)
{
    const std::int64_t candidate_term_id = 2;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id;
    members[2].candidate_term_id = candidate_term_id;
    members[2].vote = true;

    EXPECT_FALSE(is_unanimous_leader(members, candidate_term_id, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isUnanimousLeaderReturnsFalseIfNotAllNodesHadTheExpectedCandidateTermId)
{
    const std::int64_t candidate_term_id = 2;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    members[0].candidate_term_id = candidate_term_id;
    members[0].vote = true;
    members[1].candidate_term_id = candidate_term_id + 1;
    members[1].vote = true;
    members[2].candidate_term_id = candidate_term_id;
    members[2].vote = true;

    EXPECT_FALSE(is_unanimous_leader(members, candidate_term_id, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, isUnanimousLeaderReturnsTrueIfAllNodesVotedWithTrue)
{
    const std::int64_t candidate_term_id = 42;
    ClusterMembers members{new_member(1), new_member(2), new_member(3)};
    for (ClusterMember& member : members)
    {
        member.candidate_term_id = candidate_term_id;
        member.vote = true;
    }

    EXPECT_TRUE(is_unanimous_leader(members, candidate_term_id, static_cast<std::int32_t>(NULL_VALUE)));
}

TEST_F(ClusterMemberTest, compareLogReturnsResult)
{
    struct Case
    {
        std::int64_t lhs_term;
        std::int64_t lhs_position;
        std::int64_t rhs_term;
        std::int64_t rhs_position;
        std::int32_t expected;
    };

    const Case cases[] = {{5, 1000, 3, 999999, 1},
                          {-100, 99999, 4, 0, -1},
                          {42, 371239192371239, 42, 1001, 1},
                          {3, -777, 3, 273291846723894, -1},
                          {1, 1024, 1, 1024, 0}};

    for (const Case& test_case : cases)
    {
        EXPECT_EQ(test_case.expected,
                  compare_log(test_case.lhs_term, test_case.lhs_position, test_case.rhs_term, test_case.rhs_position));
        EXPECT_EQ(test_case.expected, compare_log(new_member(5, test_case.lhs_term, test_case.lhs_position, 0),
                                                  new_member(100, test_case.rhs_term, test_case.rhs_position, 0)));
    }
}

TEST_F(ClusterMemberTest, shouldNotVoteIfHasNoPosition)
{
    const ClusterMember member = new_member(1, 0, NULL_POSITION, 0);
    const ClusterMember candidate = new_member(2, 0, 100, 0);
    EXPECT_FALSE(member.will_vote_for(candidate));
}

TEST_F(ClusterMemberTest, shouldNotVoteIfHasMoreLog)
{
    const ClusterMember member = new_member(1, 0, 500, 0);
    const ClusterMember candidate = new_member(2, 0, 100, 0);
    EXPECT_FALSE(member.will_vote_for(candidate));
}

TEST_F(ClusterMemberTest, shouldVoteIfHasLessOrTheSameAmountOfLog)
{
    for (const std::int64_t log_position : {std::int64_t{0}, std::int64_t{900}, std::int64_t{1024}})
    {
        const ClusterMember member = new_member(1, 0, log_position, 0);
        const ClusterMember candidate = new_member(2, 0, 1024, 0);
        EXPECT_TRUE(member.will_vote_for(candidate));
    }
}

TEST_F(ClusterMemberTest, shouldReturnFalseIfNotActiveWhenDoingPositionChecks)
{
    constexpr std::int64_t leadership_term_id = 42;
    constexpr std::int64_t log_position = 500;
    const ClusterMember member = new_member(1, leadership_term_id, log_position, 0);
    EXPECT_FALSE(member.has_reached_position(leadership_term_id, log_position, 5, 3));
}

TEST_F(ClusterMemberTest, shouldReturnFalseIfLeadershipTermIdDoesNotMatch)
{
    constexpr std::int64_t log_position = 500;
    const ClusterMember member = new_member(1, 6, log_position, 0);
    for (const std::int64_t leadership_term_id : {std::int64_t{0}, std::int64_t{10}})
    {
        EXPECT_FALSE(member.has_reached_position(leadership_term_id, log_position, 5, 10));
    }
}

TEST_F(ClusterMemberTest, shouldReturnFalseIfLogPositionIsLessThan)
{
    constexpr std::int64_t leadership_term_id = 6;
    constexpr std::int64_t log_position = 500;
    const ClusterMember member = new_member(1, leadership_term_id, 100, 0);
    EXPECT_FALSE(member.has_reached_position(leadership_term_id, log_position, 5, 10));
}

TEST_F(ClusterMemberTest, shouldReturnTrueIfLogPositionIsEqualOrGreaterThan)
{
    constexpr std::int64_t leadership_term_id = 42;
    for (const std::int64_t log_position : {std::int64_t{100}, std::numeric_limits<std::int64_t>::max()})
    {
        const ClusterMember member = new_member(1, leadership_term_id, log_position, 10);
        EXPECT_TRUE(member.has_reached_position(leadership_term_id, 100, 12, 5));
    }
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionReturnFalseIfNotAQuorum)
{
    const ClusterMembers members{new_member(10, 2, 100, 0), new_member(20, 18, 600, 0), new_member(30, 10, 800, 0),
                                 new_member(40, 19, 800, 0), new_member(50, 10, 1000, 0)};

    EXPECT_FALSE(has_quorum_at_position(members, 10, 800, 0, 10));
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionReturnTrueIfQuorumIsAtPosition)
{
    const ClusterMembers members{new_member(10, 2, 100, 0), new_member(20, 10, 600, 0), new_member(30, 10, 800, 0),
                                 new_member(40, 19, 800, 0), new_member(50, 10, 1000, 0)};

    EXPECT_TRUE(has_quorum_at_position(members, 10, 600, 5, 10));
}

TEST_F(ClusterMemberTest, shouldRejectInvalidMemberValues)
{
    auto too_few = parse_cluster_members("0,a,b,c,d");
    ASSERT_FALSE(too_few);
    EXPECT_EQ("invalid member value: 0,a,b,c,d within: 0,a,b,c,d", too_few.error().message.view());

    auto bad_id = parse_cluster_members("x,a,b,c,d,e");
    ASSERT_FALSE(bad_id);
    EXPECT_EQ("invalid cluster member id, must be an integer value", bad_id.error().message.view());

    auto bad_endpoints = parse_member_endpoints(0, "a,b,c,d");
    ASSERT_FALSE(bad_endpoints);
    EXPECT_EQ("invalid member value: a,b,c,d", bad_endpoints.error().message.view());
}

TEST_F(ClusterMemberTest, shouldEncodeAndFindMembers)
{
    EXPECT_EQ(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint",
        deepmsg::cluster::common::encode_cluster_members(members_));

    EXPECT_EQ("0=ingressEndpoint,1=ingressEndpoint,2=ingressEndpoint",
              deepmsg::cluster::common::ingress_endpoints(members_));

    ASSERT_NE(nullptr, deepmsg::cluster::common::find_member(members_, 1));
    EXPECT_EQ(1, deepmsg::cluster::common::find_member(members_, 1)->id);
    EXPECT_EQ(nullptr, deepmsg::cluster::common::find_member(members_, 9));

    set_is_leader(members_, 1);
    EXPECT_FALSE(members_[0].is_leader);
    EXPECT_TRUE(members_[1].is_leader);

    become_candidate(members_, 7, 2);
    EXPECT_EQ(7, members_[2].candidate_term_id);
    EXPECT_EQ(std::optional<bool>{true}, members_[2].vote);
    EXPECT_TRUE(members_[2].is_ballot_sent);
    EXPECT_FALSE(members_[0].vote.has_value());

    deepmsg::cluster::common::reset_members(members_);
    EXPECT_FALSE(members_[2].is_ballot_sent);
    EXPECT_EQ(ClusterMember::NULL_VALUE, members_[2].candidate_term_id);
}

TEST_F(ClusterMemberTest, shouldDetermineMemberFromMembershipOrEndpoints)
{
    auto from_members = determine_member(members_, 1, "");
    ASSERT_TRUE(from_members) << from_members.error().message.view();
    EXPECT_EQ(1, from_members->id);

    auto from_endpoints = determine_member(ClusterMembers{}, static_cast<std::int32_t>(NULL_VALUE), "a,b,c,d,e");
    ASSERT_TRUE(from_endpoints) << from_endpoints.error().message.view();
    EXPECT_EQ("a,b,c,d,e", from_endpoints->endpoints);

    auto unknown = determine_member(members_, 9, "");
    ASSERT_FALSE(unknown);
    EXPECT_EQ("memberId=9 not found in clusterMembers", unknown.error().message.view());

    auto mismatch = determine_member(members_, 1, "a,b,c,d,e");
    ASSERT_FALSE(mismatch);
    EXPECT_EQ(
        "clusterMembers and endpoints differ: "
        "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint != a,b,c,d,e",
        mismatch.error().message.view());

    const ClusterMember lhs = determine_member(members_, 1, "").value();
    const ClusterMember rhs = determine_member(members_, 2, "").value();
    EXPECT_TRUE(are_same_endpoints(lhs, rhs));
}

}  // namespace
