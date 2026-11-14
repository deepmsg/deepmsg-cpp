// C5a: the election seam and the states it transcribes, ported from
// `ElectionTest` (`aeron-cluster/src/test/java/io/aeron/cluster/ElectionTest.java`)
// by name.
//
// The reference mocks `ConsensusModuleAgent`, `ConsensusPublisher` and its
// `RecordingLog` / `NodeStateFile`. The first two are our frozen `ElectionHost`
// and `ConsensusSender`, so gmock stands in for them exactly as Mockito does.
// The last two are concrete C1 types here, so the tests open them on a temp
// directory and assert their effect instead of a call: the observable is
// `recording.log` / `node-state.dat`, which is what the reference's
// `verify(recordingLog).ensureCoherent(...)` is really about.
//
// C5a's scope is `INIT`, the single-member short circuit and the `LEADER_*`
// states. A multi-member cluster reaches `CANVASS`, which is C5b; that path
// fails loudly and one test pins the failure.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cluster/common/cluster_member.h"
#include "cluster/common/node_state_file.h"
#include "cluster/common/recording_log.h"
#include "cluster/election/election.h"
#include "cluster/election/election_host.h"

namespace
{

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::ByMove;
using ::testing::Eq;
using ::testing::NiceMock;
using ::testing::Return;

using axc::core::Error;
using deepmsg::cluster::common::ClusterMember;
using deepmsg::cluster::common::ClusterMembers;
using deepmsg::cluster::common::NodeStateFile;
using deepmsg::cluster::common::RecordingLog;
using deepmsg::cluster::election::APPEND_POSITION_FLAG_NONE;
using deepmsg::cluster::election::ConsensusSender;
using deepmsg::cluster::election::Election;
using deepmsg::cluster::election::ElectionContext;
using deepmsg::cluster::election::ElectionHost;
using deepmsg::cluster::election::ElectionState;
using deepmsg::cluster::election::ModuleState;
using deepmsg::cluster::election::NULL_POSITION;
using deepmsg::cluster::election::NULL_VALUE;
using deepmsg::cluster::service::Role;
using deepmsg::cluster::service::TimeUnit;

constexpr std::int64_t RECORDING_ID = 600;
constexpr std::int32_t LOG_SESSION_ID = 777;

class MockElectionHost : public ElectionHost
{
public:
    MOCK_METHOD(void, set_role, (Role), (override));
    MOCK_METHOD(Role, role, (), (const, override));
    MOCK_METHOD(ModuleState, state, (), (const, override));
    MOCK_METHOD(std::int64_t, log_recording_id, (), (const, override));
    MOCK_METHOD(void, set_log_recording_id, (std::int64_t), (override));
    MOCK_METHOD(std::int64_t, time_of_last_leader_update_ns, (), (const, override));
    MOCK_METHOD(std::int64_t, quorum_position_bounded_by_leader_log, (std::int64_t, std::int64_t), (override));
    MOCK_METHOD(void, update_member_log_position, (ClusterMember&, std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<int, Error>), update_leader_position, (std::int64_t, std::int64_t, std::int64_t),
                (override));
    MOCK_METHOD((std::expected<void, Error>), publish_commit_position, (std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<void, Error>), join_log_as_leader, (std::int64_t, std::int64_t, std::int32_t, bool),
                (override));
    MOCK_METHOD((std::expected<bool, Error>), try_join_log_as_follower, (aeron_image_t*, bool, std::int64_t),
                (override));
    MOCK_METHOD((std::expected<std::int32_t, Error>), add_log_publication, (std::int64_t), (override));
    MOCK_METHOD((std::expected<std::unique_ptr<deepmsg::cluster::election::LogReplay>, Error>), new_log_replay,
                (std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<std::unique_ptr<deepmsg::cluster::election::RecordingReplication>, Error>),
                new_log_replication,
                (std::string_view, std::optional<std::string>, std::int64_t, std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<void, Error>), truncate_log_entry, (std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<bool, Error>), append_new_leadership_term_event, (std::int64_t), (override));
    MOCK_METHOD((std::expected<std::int64_t, Error>), prepare_for_new_leadership, (std::int64_t, std::int64_t),
                (override));
    MOCK_METHOD((std::expected<void, Error>), catchup_initiated, (std::int64_t), (override));
    MOCK_METHOD((std::expected<int, Error>), catchup_poll, (std::int64_t, std::int64_t), (override));
    MOCK_METHOD(bool, is_catchup_near_live, (std::int64_t), (override));
    MOCK_METHOD((std::expected<void, Error>), track_catchup_completion, (ClusterMember&, std::int64_t, std::int16_t),
                (override));
    MOCK_METHOD((std::expected<void, Error>), stop_all_catchups, (), (override));
    MOCK_METHOD(std::string_view, live_log_destination, (), (const, override));
    MOCK_METHOD((std::expected<void, Error>), set_live_log_destination, (std::string_view), (override));
    MOCK_METHOD(std::string_view, catchup_log_destination, (), (const, override));
    MOCK_METHOD((std::expected<void, Error>), set_catchup_log_destination, (std::string_view), (override));
    MOCK_METHOD((std::expected<void, Error>), await_local_sockets_closed, (std::int64_t), (override));
    MOCK_METHOD((std::expected<void, Error>), election_complete,
                (std::int64_t, std::int32_t, std::int64_t, std::int64_t), (override));
    MOCK_METHOD((std::expected<int, Error>), poll_archive_events, (), (override));

    // C5b's additions: the member publications and the log subscription.
    MOCK_METHOD(bool, member_has_publication, (std::int32_t), (const, override));
    MOCK_METHOD((std::expected<void, Error>), try_add_member_publication, (std::int32_t), (override));
    MOCK_METHOD((std::expected<std::int64_t, Error>), add_log_subscription, (std::int32_t), (override));
    MOCK_METHOD((std::expected<void, Error>), remove_log_subscription, (std::int64_t), (override));
    MOCK_METHOD((std::expected<void, Error>), add_log_destination, (std::int64_t, std::string_view), (override));
    MOCK_METHOD((std::optional<std::string>), log_subscription_resolved_endpoint, (std::int64_t), (const, override));
    MOCK_METHOD(bool, log_subscription_channel_errored, (std::int64_t), (const, override));
    MOCK_METHOD(std::string_view, log_subscription_channel, (std::int64_t), (const, override));
    MOCK_METHOD((std::optional<deepmsg::cluster::election::LogImage>), log_image_by_session,
                (std::int64_t, std::int32_t), (override));
};

class MockConsensusSender : public ConsensusSender
{
public:
    MOCK_METHOD(void, canvass_position, (std::int32_t, std::int64_t, std::int64_t, std::int64_t, std::int32_t),
                (override));
    MOCK_METHOD(bool, request_vote, (std::int32_t, std::int64_t, std::int64_t, std::int64_t, std::int32_t), (override));
    MOCK_METHOD(void, place_vote,
                (std::int32_t, std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t, bool), (override));
    MOCK_METHOD(void, new_leadership_term,
                (std::int32_t, std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int64_t,
                 std::int64_t, std::int64_t, std::int64_t, std::int64_t, std::int32_t, std::int32_t, std::int32_t,
                 bool),
                (override));
    MOCK_METHOD(bool, append_position, (std::int32_t, std::int64_t, std::int64_t, std::int32_t, std::int16_t),
                (override));
    MOCK_METHOD(bool, catchup_position, (std::int32_t, std::int64_t, std::int64_t, std::int32_t, std::string_view),
                (override));
};

class MockLogReplay : public deepmsg::cluster::election::LogReplay
{
public:
    MOCK_METHOD(int, do_work, (), (override));
    MOCK_METHOD(bool, is_done, (), (const, override));
    MOCK_METHOD(std::int64_t, position, (), (const, override));
    MOCK_METHOD(void, close, (), (override));
};

class MockRecordingReplication : public deepmsg::cluster::election::RecordingReplication
{
public:
    MOCK_METHOD(void, on_signal, (std::int64_t, std::int64_t, std::int64_t, std::int32_t), (override));
    MOCK_METHOD((std::expected<int, Error>), poll, (std::int64_t), (override));
    MOCK_METHOD(bool, has_replication_ended, (), (const, override));
    MOCK_METHOD(bool, has_stopped, (), (const, override));
    MOCK_METHOD(bool, has_synced, (), (const, override));
    MOCK_METHOD(std::string_view, src_archive_channel, (), (const, override));
    MOCK_METHOD(std::int64_t, recording_id, (), (const, override));
    MOCK_METHOD(std::int64_t, position, (), (const, override));
    MOCK_METHOD(void, close, (), (override));
};

class ElectionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        directory_ = std::filesystem::temp_directory_path() / ("axc-c5a-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);

        auto log = RecordingLog::open(directory_.string(), true);
        ASSERT_TRUE(log) << log.error().message.view();
        recording_log_ = std::move(*log);

        auto node_state = NodeStateFile::open(directory_.string(), true, 0);
        ASSERT_TRUE(node_state) << node_state.error().message.view();
        node_state_ = std::move(*node_state);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] ElectionContext context()
    {
        ElectionContext context;
        context.node_state_file = &node_state_;
        context.recording_log = &recording_log_;
        context.now_ns = [this] { return now_ns_; };
        context.epoch_ms = [this] { return now_ns_ / 1'000'000; };
        context.time_unit = TimeUnit::nanoseconds;
        context.file_sync_level = 0;
        context.set_election_state = [this](ElectionState state) { states_.push_back(state); };
        context.increment_election_counter = [this] { ++election_count_; };
        return context;
    }

    /// The reference's `ElectionTest.before()`: a recording, a log publication
    /// session id, and `updateMemberLogPosition` really moving the member (its
    /// `doAnswer`), which the vote predicates read back.
    void script_host(MockElectionHost& host, MockConsensusSender& sender)
    {
        ON_CALL(host, log_recording_id()).WillByDefault(Return(RECORDING_ID));
        ON_CALL(host, add_log_publication(_)).WillByDefault(Return(std::expected<std::int32_t, Error>{LOG_SESSION_ID}));
        ON_CALL(sender, new_leadership_term(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _))
            .WillByDefault(
                [this](std::int32_t member_id, std::int64_t log_leadership_term_id,
                       std::int64_t next_leadership_term_id, std::int64_t next_term_base_log_position,
                       std::int64_t next_log_position, std::int64_t leadership_term_id,
                       std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t commit_position,
                       std::int64_t leader_recording_id, std::int64_t timestamp, std::int32_t leader_member_id,
                       std::int32_t log_session_id, std::int32_t app_version, bool is_startup)
                {
                    new_leadership_terms_.push_back(NewLeadershipTermCall{
                        member_id, log_leadership_term_id, next_leadership_term_id, next_term_base_log_position,
                        next_log_position, leadership_term_id, term_base_log_position, log_position, commit_position,
                        leader_recording_id, timestamp, leader_member_id, log_session_id, app_version, is_startup});
                });
        ON_CALL(host, update_member_log_position(_, _, _))
            .WillByDefault(
                [this](ClusterMember& member, std::int64_t leadership_term_id, std::int64_t log_position)
                {
                    member.leadership_term_id = leadership_term_id;
                    member.log_position = log_position;
                    member.time_of_last_append_position_ns = now_ns_;
                });
    }

    [[nodiscard]] ClusterMembers single_member_cluster()
    {
        auto parsed = deepmsg::cluster::common::parse_cluster_members(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint");
        EXPECT_TRUE(parsed) << (parsed ? std::string{} : std::string{parsed.error().message.view()});
        return parsed ? std::move(*parsed) : ClusterMembers{};
    }

    std::filesystem::path directory_{};
    RecordingLog recording_log_{};
    NodeStateFile node_state_{};
    std::int64_t now_ns_{0};
    /// A recorded `new_leadership_term` call: the reference verifies these by
    /// argument, and the leader states emit several with different
    /// `isStartup` values, so the test records them all and picks.
    struct NewLeadershipTermCall
    {
        std::int32_t member_id;
        std::int64_t log_leadership_term_id;
        std::int64_t next_leadership_term_id;
        std::int64_t next_term_base_log_position;
        std::int64_t next_log_position;
        std::int64_t leadership_term_id;
        std::int64_t term_base_log_position;
        std::int64_t log_position;
        std::int64_t commit_position;
        std::int64_t leader_recording_id;
        std::int64_t timestamp;
        std::int32_t leader_member_id;
        std::int32_t log_session_id;
        std::int32_t app_version;
        bool is_startup;
    };

    std::vector<NewLeadershipTermCall> new_leadership_terms_{};
    std::vector<ElectionState> states_{};
    int election_count_{0};
};

TEST_F(ElectionTest, shouldElectSingleNodeClusterLeader)
{
    const std::int64_t leadership_term_id = NULL_VALUE;
    const std::int64_t log_position = 0;

    ClusterMembers cluster_members = single_member_cluster();
    ASSERT_EQ(1U, cluster_members.size());
    ClusterMember& this_member = cluster_members[0];

    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;

    ON_CALL(host, log_recording_id()).WillByDefault(Return(RECORDING_ID));
    ON_CALL(host, add_log_publication(_)).WillByDefault(Return(std::expected<std::int32_t, Error>{LOG_SESSION_ID}));

    // The constructor takes the single member straight to
    // `LEADER_LOG_REPLICATION`, whose transition opens the log publication
    // (`Election.java:1398-1402`).
    EXPECT_CALL(host, set_role(Role::leader)).Times(::testing::AtLeast(1));
    EXPECT_CALL(host, add_log_publication(Eq(log_position))).Times(1);
    // `joinLogAsLeader(newLeadershipTermId, logPosition, logSessionId, true)`
    // (`Election.java:870`), reached through `LEADER_INIT`.
    EXPECT_CALL(host, join_log_as_leader(Eq(leadership_term_id + 1), Eq(log_position), Eq(LOG_SESSION_ID), Eq(true)))
        .Times(1);
    // The election completes only when the leader writes the term-start event;
    // the reference test leaves `appendNewLeadershipTermEvent` false, so the
    // run stops at `LEADER_READY`.
    EXPECT_CALL(host, election_complete(_, _, _, _)).Times(0);

    ElectionContext election_context = context();
    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, std::move(election_context));

    const std::int64_t new_leadership_term_id = leadership_term_id + 1;

    now_ns_ = 1'000'000'007;
    for (int i = 0; i < 4; ++i)
    {
        auto work = election.do_work(now_ns_);
        ASSERT_TRUE(work) << work.error().message.view() << " in state "
                          << deepmsg::cluster::election::election_state_name(election.state());
    }

    EXPECT_EQ(ElectionState::leader_ready, election.state());
    EXPECT_EQ(new_leadership_term_id, election.leadership_term_id());
    EXPECT_EQ(LOG_SESSION_ID, election.log_session_id());

    ASSERT_FALSE(states_.empty());
    EXPECT_EQ(ElectionState::leader_ready, states_.back());
    EXPECT_EQ(1, election_count_);

    // `updateCandidateTermId(candidateTermId + 1, logPosition, epochTime)` went
    // into `node-state.dat` — the single-member short circuit's safety write.
    EXPECT_EQ(new_leadership_term_id, node_state_.candidate_term().candidate_term_id);
    EXPECT_EQ(log_position, node_state_.candidate_term().log_position);

    // `recordingLog.ensureCoherent(RECORDING_ID, NULL_VALUE, logPosition,
    // newLeadershipTermId, logPosition, NULL_VALUE, now, timestamp, sync)`.
    const RecordingLog::Entry* term_entry = recording_log_.find_term_entry(new_leadership_term_id);
    ASSERT_NE(nullptr, term_entry);
    EXPECT_EQ(RECORDING_ID, term_entry->recording_id);
    EXPECT_EQ(log_position, term_entry->term_base_log_position);
    EXPECT_EQ(NULL_VALUE, term_entry->log_position);
    // The cluster clock unit is nanoseconds here (`TestClusterClock(NANOSECONDS)`
    // in the reference), so `nanosToTimestamp` is the identity.
    EXPECT_EQ(now_ns_, term_entry->timestamp);
    EXPECT_EQ(log_position, election.log_position());
}

TEST_F(ElectionTest, theLeaderWaitsForTheTermStartEventBeforeCompleting)
{
    // The reference has no case for `LEADER_READY`'s completion step: its
    // leader tests leave `appendNewLeadershipTermEvent` false and stop at
    // `LEADER_READY`. C5a transcribes that state, so this case pins it: the
    // election completes only on the pass where the term-start event was
    // really appended (`Election.java:885-894`).
    const std::int64_t leadership_term_id = NULL_VALUE;
    const std::int64_t log_position = 0;

    ClusterMembers cluster_members = single_member_cluster();
    ASSERT_EQ(1U, cluster_members.size());
    ClusterMember& this_member = cluster_members[0];

    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    ON_CALL(host, log_recording_id()).WillByDefault(Return(RECORDING_ID));
    ON_CALL(host, add_log_publication(_)).WillByDefault(Return(std::expected<std::int32_t, Error>{LOG_SESSION_ID}));

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    now_ns_ = 1'000'000'007;
    for (int i = 0; i < 4; ++i)
    {
        auto work = election.do_work(now_ns_);
        ASSERT_TRUE(work) << work.error().message.view();
    }
    ASSERT_EQ(ElectionState::leader_ready, election.state());

    // No term-start event in the log: the election stays in LEADER_READY.
    EXPECT_CALL(host, append_new_leadership_term_event(_)).WillOnce(Return(std::expected<bool, Error>{false}));
    EXPECT_CALL(host, election_complete(_, _, _, _)).Times(0);
    auto waiting = election.do_work(now_ns_);
    ASSERT_TRUE(waiting) << waiting.error().message.view();
    EXPECT_EQ(ElectionState::leader_ready, election.state());

    // The event lands: it completes with the election's own term and leader.
    EXPECT_CALL(host, append_new_leadership_term_event(_)).WillOnce(Return(std::expected<bool, Error>{true}));
    EXPECT_CALL(host, election_complete(Eq(leadership_term_id + 1), Eq(this_member.id), Eq(log_position), Eq(now_ns_)))
        .Times(1);
    auto completed = election.do_work(now_ns_);
    ASSERT_TRUE(completed) << completed.error().message.view();
    EXPECT_EQ(ElectionState::closed, election.state());
}

TEST_F(ElectionTest, shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified)
{
    // `Election.ensureRecordingLogCoherent` returns without touching the
    // recording log when the recording id is NULL_POSITION (`Election.java:1495-1500`).
    ElectionContext election_context = context();

    auto first = Election::ensure_recording_log_coherent(election_context, NULL_POSITION, 0, 0, 0, 0, 0, 1);
    ASSERT_TRUE(first) << first.error().message.view();
    auto second = Election::ensure_recording_log_coherent(election_context, NULL_POSITION, 0, 0, 0, 0, 1000, 1);
    ASSERT_TRUE(second) << second.error().message.view();

    EXPECT_TRUE(recording_log_.entries().empty());
}

// ---------------------------------------------------------------------------
// C5b: the ballot and follower scenarios, ported from `ElectionTest` by name.
// ---------------------------------------------------------------------------

constexpr std::int32_t PROTOCOL_VERSION = 768;  // `PROTOCOL_SEMANTIC_VERSION` 0.3.0

// `ElectionContext`'s defaults, which are `ConsensusModule.Configuration`'s.
constexpr std::int64_t STARTUP_CANVASS_TIMEOUT_NS = 20'000'000'000;
constexpr std::int64_t LEADER_HEARTBEAT_TIMEOUT_NS = 10'000'000'000;
constexpr std::int64_t ELECTION_TIMEOUT_NS = 1'000'000'000;
constexpr std::int64_t LEADER_HEARTBEAT_INTERVAL_NS = 100'000'000;

[[nodiscard]] ClusterMembers three_member_cluster()
{
    auto parsed = deepmsg::cluster::common::parse_cluster_members(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");
    EXPECT_TRUE(parsed) << (parsed ? std::string{} : std::string{parsed.error().message.view()});
    return parsed ? std::move(*parsed) : ClusterMembers{};
}

/// Every member except `this_member_id`, in ascending order — the loop the
/// reference writes out by hand.
[[nodiscard]] std::vector<std::int32_t> other_member_ids(std::int32_t this_member_id)
{
    std::vector<std::int32_t> ids;
    for (const std::int32_t id : {0, 1, 2})
    {
        if (id != this_member_id)
        {
            ids.push_back(id);
        }
    }
    return ids;
}

void canvass_from_peers(Election& election, std::int32_t this_member_id, std::int64_t leadership_term_id,
                        std::int64_t log_position)
{
    for (const std::int32_t id : other_member_ids(this_member_id))
    {
        auto canvassed =
            election.on_canvass_position(leadership_term_id, log_position, leadership_term_id, id, PROTOCOL_VERSION);
        ASSERT_TRUE(canvassed) << canvassed.error().message.view();
    }
}

TEST_F(ElectionTest, shouldCanvassMembersInSuccessfulLeadershipBid)
{
    constexpr std::int64_t log_position = 0;
    constexpr std::int64_t leadership_term_id = NULL_VALUE;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    EXPECT_CALL(sender, canvass_position(0, leadership_term_id, log_position, leadership_term_id, this_member.id));
    EXPECT_CALL(sender, canvass_position(2, leadership_term_id, log_position, leadership_term_id, this_member.id));

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    auto init = election.do_work(0);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    now_ns_ = 1;
    auto canvassed = election.do_work(now_ns_);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();
    EXPECT_EQ(ElectionState::canvass, states_.back());

    canvass_from_peers(election, this_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    EXPECT_EQ(ElectionState::nominate, election.state());
    EXPECT_EQ(ElectionState::nominate, states_.back());
}

TEST_F(ElectionTest, shouldVoteForCandidateDuringNomination)
{
    constexpr std::int64_t log_position = 0;
    constexpr std::int64_t leadership_term_id = NULL_VALUE;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    const std::int64_t candidate_term_id = leadership_term_id + 1;
    EXPECT_CALL(sender, place_vote(0, candidate_term_id, leadership_term_id, log_position, 0, this_member.id, true));

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    now_ns_ += 1;
    auto canvassed = election.do_work(now_ns_);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();

    canvass_from_peers(election, this_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += 1;
    auto voted = election.on_request_vote(leadership_term_id, log_position, candidate_term_id, 0, PROTOCOL_VERSION);
    ASSERT_TRUE(voted) << voted.error().message.view();
    auto followed = election.do_work(now_ns_);
    ASSERT_TRUE(followed) << followed.error().message.view();
    EXPECT_EQ(ElectionState::follower_ballot, election.state());
}

TEST_F(ElectionTest, shouldTimeoutCanvassWithMajority)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto appended = election.on_append_position(leadership_term_id, log_position, 0, APPEND_POSITION_FLAG_NONE);
    ASSERT_TRUE(appended) << appended.error().message.view();

    now_ns_ += 1;
    auto canvassed = election.do_work(now_ns_);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();

    now_ns_ += STARTUP_CANVASS_TIMEOUT_NS;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    EXPECT_EQ(ElectionState::nominate, election.state());
    EXPECT_EQ(ElectionState::nominate, states_.back());
}

TEST_F(ElectionTest, shouldWinCandidateBallotWithMajority)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(false, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    canvass_from_peers(election, this_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS;
    const std::int64_t candidate_term_id = leadership_term_id + 1;
    auto vote = election.on_vote(leadership_term_id, log_position, candidate_term_id, this_member.id, 2, true);
    ASSERT_TRUE(vote) << vote.error().message.view();
    auto elected = election.do_work(now_ns_);
    ASSERT_TRUE(elected) << elected.error().message.view();
    EXPECT_EQ(ElectionState::leader_log_replication, election.state());
}

TEST_F(ElectionTest, shouldElectCandidateWithFullVote)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& candidate_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      candidate_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    canvass_from_peers(election, candidate_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    now_ns_ += 1;
    const std::int64_t candidate_term_id = leadership_term_id + 1;
    for (const std::int32_t voter : {0, 2})
    {
        auto vote =
            election.on_vote(leadership_term_id, log_position, candidate_term_id, candidate_member.id, voter, true);
        ASSERT_TRUE(vote) << vote.error().message.view();
    }
    auto elected = election.do_work(now_ns_);
    ASSERT_TRUE(elected) << elected.error().message.view();
    EXPECT_EQ(ElectionState::leader_log_replication, election.state());
}

TEST_F(ElectionTest, shouldTimeoutCandidateBallotWithoutMajority)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& candidate_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      candidate_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    canvass_from_peers(election, candidate_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS;
    auto timed_out = election.do_work(now_ns_);
    ASSERT_TRUE(timed_out) << timed_out.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
    EXPECT_EQ(leadership_term_id, election.leadership_term_id());
}

TEST_F(ElectionTest, shouldTimeoutFollowerBallotWithoutLeaderEmerging)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& follower_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      follower_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    const std::int64_t candidate_term_id = leadership_term_id + 1;
    auto voted = election.on_request_vote(leadership_term_id, log_position, candidate_term_id, 0, PROTOCOL_VERSION);
    ASSERT_TRUE(voted) << voted.error().message.view();

    now_ns_ += 1;
    auto followed = election.do_work(now_ns_);
    ASSERT_TRUE(followed) << followed.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS;
    auto timed_out = election.do_work(now_ns_);
    ASSERT_TRUE(timed_out) << timed_out.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
    EXPECT_EQ(leadership_term_id, election.leadership_term_id());
}

TEST_F(ElectionTest, shouldBecomeFollowerIfEnteringNewElection)
{
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t log_position = 120;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    EXPECT_CALL(host, set_role(Role::leader)).Times(::testing::AnyNumber());
    EXPECT_CALL(host, prepare_for_new_leadership(Eq(log_position), Eq(1)))
        .WillOnce(Return(std::expected<std::int64_t, Error>{NULL_POSITION}));
    EXPECT_CALL(host, set_role(Role::follower)).Times(::testing::AtLeast(1));

    Election election(false, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
    EXPECT_EQ(ElectionState::canvass, states_.back());
}

TEST_F(ElectionTest, shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& candidate_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      candidate_member, sender, host, context());

    now_ns_ += 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto canvass_one =
        election.on_canvass_position(leadership_term_id, log_position, leadership_term_id, 0, PROTOCOL_VERSION);
    ASSERT_TRUE(canvass_one) << canvass_one.error().message.view();

    now_ns_ += STARTUP_CANVASS_TIMEOUT_NS;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    now_ns_ += 1;
    auto split =
        election.on_vote(leadership_term_id, log_position, leadership_term_id + 1, candidate_member.id, 2, false);
    ASSERT_TRUE(split) << split.error().message.view();
    auto split_work = election.do_work(now_ns_);
    ASSERT_TRUE(split_work) << split_work.error().message.view();

    now_ns_ += ELECTION_TIMEOUT_NS;
    auto timed_out = election.do_work(now_ns_);
    ASSERT_TRUE(timed_out) << timed_out.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto canvass_again =
        election.on_canvass_position(leadership_term_id, log_position, leadership_term_id, 0, PROTOCOL_VERSION);
    ASSERT_TRUE(canvass_again) << canvass_again.error().message.view();

    now_ns_ += LEADER_HEARTBEAT_TIMEOUT_NS;
    auto nominated_again = election.do_work(now_ns_);
    ASSERT_TRUE(nominated_again) << nominated_again.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS;
    auto balloted_again = election.do_work(now_ns_);
    ASSERT_TRUE(balloted_again) << balloted_again.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    const std::int64_t candidate_term_id = leadership_term_id + 2;
    auto won = election.on_vote(leadership_term_id + 1, log_position, candidate_term_id, candidate_member.id, 2, true);
    ASSERT_TRUE(won) << won.error().message.view();

    now_ns_ += ELECTION_TIMEOUT_NS;
    auto leader = election.do_work(now_ns_);
    ASSERT_TRUE(leader) << leader.error().message.view();
    ASSERT_EQ(ElectionState::leader_log_replication, election.state());

    auto replay = election.do_work(now_ns_);
    ASSERT_TRUE(replay) << replay.error().message.view();
    ASSERT_EQ(ElectionState::leader_replay, election.state());

    now_ns_ += 1;
    auto init_leader = election.do_work(now_ns_);
    ASSERT_TRUE(init_leader) << init_leader.error().message.view();
    auto ready = election.do_work(now_ns_);
    ASSERT_TRUE(ready) << ready.error().message.view();
    EXPECT_EQ(candidate_term_id, election.leadership_term_id());
    EXPECT_EQ(ElectionState::leader_ready, election.state());
}

TEST_F(ElectionTest, shouldElectAppointedLeader)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;
    constexpr std::int64_t commit_position = 42;
    constexpr std::int32_t app_version = -98;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& candidate_member = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(commit_position));

    ElectionContext election_context = context();
    election_context.appointed_leader_id = candidate_member.id;
    election_context.app_version = app_version;

    const std::int64_t candidate_term_id = leadership_term_id + 1;
    now_ns_ = 1;

    EXPECT_CALL(sender, request_vote(1, leadership_term_id, log_position, candidate_term_id, candidate_member.id));
    EXPECT_CALL(sender, request_vote(2, leadership_term_id, log_position, candidate_term_id, candidate_member.id));
    EXPECT_CALL(host, join_log_as_leader(Eq(candidate_term_id), Eq(log_position), Eq(LOG_SESSION_ID), Eq(true)))
        .Times(1);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      candidate_member, sender, host, std::move(election_context));

    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    canvass_from_peers(election, candidate_member.id, leadership_term_id, log_position);

    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    // One more pass sends the ballots (the candidate's own is already cast).
    auto ballots_sent = election.do_work(now_ns_);
    ASSERT_TRUE(ballots_sent) << ballots_sent.error().message.view();

    now_ns_ += 1;
    for (const std::int32_t voter : {1, 2})
    {
        auto vote =
            election.on_vote(leadership_term_id, log_position, candidate_term_id, candidate_member.id, voter, true);
        ASSERT_TRUE(vote) << vote.error().message.view();
    }
    now_ns_ += 1;
    auto elected = election.do_work(now_ns_);
    ASSERT_TRUE(elected) << elected.error().message.view();
    ASSERT_EQ(ElectionState::leader_log_replication, election.state());

    auto replicated = election.do_work(now_ns_);
    ASSERT_TRUE(replicated) << replicated.error().message.view();
    ASSERT_EQ(ElectionState::leader_replay, election.state());

    // Nothing to replay: the leader's log is the append position.
    auto replayed = election.do_work(now_ns_);
    ASSERT_TRUE(replayed) << replayed.error().message.view();
    ASSERT_EQ(ElectionState::leader_init, election.state());
    auto joined = election.do_work(now_ns_);
    ASSERT_TRUE(joined) << joined.error().message.view();
    ASSERT_EQ(ElectionState::leader_ready, election.state());

    EXPECT_EQ(0, cluster_members[1].log_position);
    EXPECT_EQ(0, cluster_members[2].log_position);
    EXPECT_EQ(candidate_term_id, election.leadership_term_id());

    // `verify(consensusPublisher).newLeadershipTerm(...)` for both followers,
    // with the term the election settled on and `isLeaderStartup`.
    std::int32_t matched = 0;
    for (const NewLeadershipTermCall& call : new_leadership_terms_)
    {
        if (call.leadership_term_id == candidate_term_id && call.commit_position == commit_position &&
            call.leader_member_id == candidate_member.id && call.log_session_id == LOG_SESSION_ID &&
            call.app_version == app_version && call.is_startup)
        {
            EXPECT_TRUE(call.member_id == 1 || call.member_id == 2);
            EXPECT_EQ(leadership_term_id, call.log_leadership_term_id);
            EXPECT_EQ(0, call.next_leadership_term_id);
            EXPECT_EQ(0, call.next_term_base_log_position);
            EXPECT_EQ(NULL_POSITION, call.next_log_position);
            EXPECT_EQ(log_position, call.term_base_log_position);
            EXPECT_EQ(log_position, call.log_position);
            EXPECT_EQ(RECORDING_ID, call.leader_recording_id);
            ++matched;
        }
    }
    EXPECT_EQ(2, matched);

    now_ns_ += 1;
    auto waiting = election.do_work(now_ns_);
    ASSERT_TRUE(waiting) << waiting.error().message.view();
    ASSERT_EQ(ElectionState::leader_ready, election.state());

    ON_CALL(host, append_new_leadership_term_event(_)).WillByDefault(Return(std::expected<bool, Error>{true}));
    EXPECT_CALL(host, election_complete(Eq(candidate_term_id), Eq(candidate_member.id), Eq(log_position), _)).Times(1);

    now_ns_ += 1;
    for (const std::int32_t follower : {1, 2})
    {
        auto appended =
            election.on_append_position(candidate_term_id, log_position, follower, APPEND_POSITION_FLAG_NONE);
        ASSERT_TRUE(appended) << appended.error().message.view();
    }
    auto completed = election.do_work(now_ns_);
    ASSERT_TRUE(completed) << completed.error().message.view();
    EXPECT_EQ(ElectionState::closed, election.state());
}

TEST_F(ElectionTest, shouldVoteForAppointedLeader)
{
    constexpr std::int64_t leadership_term_id = NULL_VALUE;
    constexpr std::int64_t log_position = 0;
    constexpr std::int32_t candidate_id = 0;
    constexpr std::int64_t leader_recording_id = 983724;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& follower_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    const std::int64_t candidate_term_id = leadership_term_id + 1;
    const std::int32_t log_session_id = -7;
    constexpr std::int64_t commit_position = 100;

    EXPECT_CALL(sender, place_vote(candidate_id, candidate_term_id, leadership_term_id, log_position, candidate_id,
                                   follower_member.id, true))
        .Times(1);
    EXPECT_CALL(sender, append_position(candidate_id, candidate_term_id, log_position, follower_member.id,
                                        APPEND_POSITION_FLAG_NONE))
        .WillOnce(Return(true));
    EXPECT_CALL(host, add_log_subscription(Eq(log_session_id))).WillOnce(Return(std::expected<std::int64_t, Error>{7}));
    EXPECT_CALL(host, try_join_log_as_follower(_, Eq(false), _)).WillOnce(Return(std::expected<bool, Error>{true}));
    EXPECT_CALL(host, election_complete(Eq(candidate_term_id), Eq(candidate_id), Eq(log_position), _)).Times(1);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      follower_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto vote_request =
        election.on_request_vote(leadership_term_id, log_position, candidate_term_id, candidate_id, PROTOCOL_VERSION);
    ASSERT_TRUE(vote_request) << vote_request.error().message.view();

    now_ns_ += 1;
    auto followed = election.do_work(now_ns_);
    ASSERT_TRUE(followed) << followed.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    auto new_term = election.on_new_leadership_term(leadership_term_id, NULL_VALUE, NULL_POSITION, NULL_POSITION,
                                                    candidate_term_id, log_position, log_position, commit_position,
                                                    leader_recording_id, now_ns_, candidate_id, log_session_id, false);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_replay, election.state());

    auto replayed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replayed) << replayed.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_init, election.state());

    auto subscribed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(subscribed) << subscribed.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_await, election.state());

    ON_CALL(host, log_image_by_session(_, _))
        .WillByDefault(Return(std::optional<deepmsg::cluster::election::LogImage>{
            deepmsg::cluster::election::LogImage{nullptr, log_position}}));

    auto joined = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(joined) << joined.error().message.view();
    ASSERT_EQ(ElectionState::follower_ready, election.state());

    auto ready = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(ready) << ready.error().message.view();
    EXPECT_EQ(ElectionState::closed, election.state());
}

TEST_F(ElectionTest, followerShouldReplicateLogBeforeReplayDuringElection)
{
    constexpr std::int64_t term0_id = 0;
    constexpr std::int64_t term1_id = 1;
    constexpr std::int64_t term1_base_log_position = 60;
    constexpr std::int64_t term2_id = 2;
    constexpr std::int64_t term2_base_log_position = 120;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, prepare_for_new_leadership(_, _))
        .WillByDefault(Return(std::expected<std::int64_t, Error>{term1_base_log_position}));
    ON_CALL(sender, append_position(_, _, _, _, _)).WillByDefault(Return(true));

    auto* replication = new NiceMock<MockRecordingReplication>();
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(false));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(false));
    ON_CALL(*replication, position()).WillByDefault(Return(term2_base_log_position));
    ON_CALL(*replication, recording_id()).WillByDefault(Return(RECORDING_ID));
    EXPECT_CALL(host, new_log_replication(Eq(live_leader.archive_endpoint), _, Eq(RECORDING_ID),
                                          Eq(term2_base_log_position), _))
        .Times(1)
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(true, NULL_VALUE, term0_id, term1_base_log_position, term1_base_log_position,
                      term1_base_log_position, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto vote = election.on_request_vote(term1_id, term2_base_log_position, term2_id, live_leader.id, PROTOCOL_VERSION);
    ASSERT_TRUE(vote) << vote.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    // A term the follower has no history for: the message is stale, so it
    // canvasses again.
    auto stale = election.on_new_leadership_term(
        term1_id, term2_id, term2_base_log_position, term2_base_log_position, term2_id, term2_base_log_position,
        term2_base_log_position, term1_base_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(stale) << stale.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto canvassed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();

    // Now the term whose base is the follower's own append position: it must
    // replicate before it can replay.
    auto replicated = election.on_new_leadership_term(
        term0_id, term1_id, term1_base_log_position, term2_base_log_position, term2_id, term2_base_log_position,
        term2_base_log_position, term1_base_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(replicated) << replicated.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();

    // While the replication runs, the follower keeps polling and reporting.
    for (int i = 0; i < 4; ++i)
    {
        auto polling = election.do_work(now_ns_ += 1);
        ASSERT_TRUE(polling) << polling.error().message.view();
        ASSERT_EQ(ElectionState::follower_log_replication, election.state());
    }

    // The replication completes at the leader's position; the election waits
    // for the commit position that covers it.
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(true));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(true));
    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    auto waiting = election.do_work(now_ns_);
    ASSERT_TRUE(waiting) << waiting.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto commit = election.on_commit_position(term2_id, term2_base_log_position, live_leader.id);
    ASSERT_TRUE(commit) << commit.error().message.view();
    auto done = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(done) << done.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
    EXPECT_EQ(term2_base_log_position, election.notified_commit_position());
}

TEST_F(ElectionTest, followerShouldReplayAndCatchupWhenLateJoiningClusterInSameTerm)
{
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t leader_log_position = 120;
    constexpr std::int64_t follower_log_position = 60;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, prepare_for_new_leadership(_, _))
        .WillByDefault(Return(std::expected<std::int64_t, Error>{follower_log_position}));

    Election election(true, NULL_VALUE, leadership_term_id, follower_log_position, follower_log_position,
                      follower_log_position, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto new_term = election.on_new_leadership_term(
        leadership_term_id, NULL_VALUE, NULL_POSITION, NULL_POSITION, leadership_term_id, follower_log_position,
        leader_log_position, leader_log_position, RECORDING_ID, now_ns_, live_leader.id, LOG_SESSION_ID, true);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_replay, election.state());

    // Nothing to replay: the follower's log is already at the append position,
    // and a catchup join position was recorded.
    auto replayed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replayed) << replayed.error().message.view();
    EXPECT_EQ(ElectionState::follower_catchup_init, election.state());
}

TEST_F(ElectionTest, shouldBaseStartupValueOnLeader)
{
    for (const bool is_leader_start : {true, false})
    {
        for (const bool is_node_start : {true, false})
        {
            SCOPED_TRACE(std::string{"isLeaderStart="} + (is_leader_start ? "true" : "false") +
                         " isNodeStart=" + (is_node_start ? "true" : "false"));

            constexpr std::int64_t leadership_term_id = 0;
            constexpr std::int64_t log_position = 0;
            constexpr std::int64_t leader_recording_id = 367234;
            constexpr std::int64_t commit_position = 1024;

            ClusterMembers cluster_members = three_member_cluster();
            ClusterMember& follower_member = cluster_members[1];
            NiceMock<MockElectionHost> host;
            NiceMock<MockConsensusSender> sender;
            script_host(host, sender);

            Election election(is_node_start, NULL_VALUE, leadership_term_id, log_position, log_position, log_position,
                              cluster_members, follower_member, sender, host, context());

            now_ns_ = 1;
            auto init = election.do_work(now_ns_);
            ASSERT_TRUE(init) << init.error().message.view();
            ASSERT_EQ(ElectionState::canvass, election.state());

            const std::int32_t leader_member_id = cluster_members[0].id;
            auto new_term = election.on_new_leadership_term(
                leadership_term_id, NULL_VALUE, NULL_POSITION, NULL_POSITION, leadership_term_id, log_position,
                log_position, commit_position, leader_recording_id, now_ns_, leader_member_id, 0, is_leader_start);
            ASSERT_TRUE(new_term) << new_term.error().message.view();

            auto nothing_to_replay = election.do_work(now_ns_ += 1);
            ASSERT_TRUE(nothing_to_replay) << nothing_to_replay.error().message.view();
            ASSERT_EQ(ElectionState::follower_log_init, election.state());

            auto subscribed = election.do_work(now_ns_ += 1);
            ASSERT_TRUE(subscribed) << subscribed.error().message.view();
            ASSERT_EQ(ElectionState::follower_log_await, election.state());

            constexpr std::int64_t time_at_follower_log_await_ns = 4;
            now_ns_ = time_at_follower_log_await_ns;
            ON_CALL(host, log_image_by_session(_, _))
                .WillByDefault(Return(std::optional<deepmsg::cluster::election::LogImage>{
                    deepmsg::cluster::election::LogImage{nullptr, log_position}}));
            EXPECT_CALL(host, try_join_log_as_follower(_, Eq(is_leader_start), Eq(time_at_follower_log_await_ns)))
                .WillOnce(Return(std::expected<bool, Error>{true}));

            auto joined = election.do_work(now_ns_);
            ASSERT_TRUE(joined) << joined.error().message.view();
            EXPECT_EQ(ElectionState::follower_ready, election.state());
        }
    }
}

TEST_F(ElectionTest, followerShouldReplicateReplayAndCatchupWhenLateJoiningClusterInLaterTerm)
{
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t leader_log_position = 120;
    constexpr std::int64_t term_base_log_position = 60;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(sender, append_position(_, _, _, _, _)).WillByDefault(Return(true));

    auto* replication = new NiceMock<MockRecordingReplication>();
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(true));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(true));
    ON_CALL(*replication, position()).WillByDefault(Return(term_base_log_position));
    ON_CALL(*replication, recording_id()).WillByDefault(Return(RECORDING_ID));
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(term_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(true, NULL_VALUE, 0, 0, 0, 0, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto new_term = election.on_new_leadership_term(
        0, 1, term_base_log_position, leader_log_position, leadership_term_id, term_base_log_position,
        leader_log_position, leader_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, false);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();

    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    auto replicated = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replicated) << replicated.error().message.view();

    auto commit = election.on_commit_position(leadership_term_id, leader_log_position, live_leader.id);
    ASSERT_TRUE(commit) << commit.error().message.view();
    auto canvassed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
}

TEST_F(ElectionTest, followerShouldReplicateAndSendAppendPositionWhenLogReplicationDone)
{
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t leader_log_position = 120;
    constexpr std::int64_t term_base_log_position = 60;
    constexpr std::int64_t next_term_base_log_position = 60;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    auto* replication = new NiceMock<MockRecordingReplication>();
    std::int64_t replication_position = 0;
    ON_CALL(*replication, position())
        .WillByDefault(::testing::Invoke([&replication_position] { return replication_position; }));
    ON_CALL(*replication, poll(_))
        .WillByDefault(::testing::Invoke(
            [&replication_position](std::int64_t) -> std::expected<int, Error>
            {
                replication_position = next_term_base_log_position;
                return 1;
            }));
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(true));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(true));
    ON_CALL(*replication, recording_id()).WillByDefault(Return(RECORDING_ID));
    ON_CALL(sender, append_position(_, _, _, _, _)).WillByDefault(Return(true));
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(term_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(true, NULL_VALUE, 0, 0, 0, 0, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto new_term = election.on_new_leadership_term(
        0, 1, next_term_base_log_position, NULL_POSITION, leadership_term_id, term_base_log_position,
        leader_log_position, leader_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, false);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();

    // The replication finishes inside this pass, and the position it reaches is
    // what goes out as the `AppendPosition` — the last position of the term.
    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    EXPECT_CALL(sender, append_position(live_leader.id, leadership_term_id, Eq(next_term_base_log_position),
                                        this_member.id, APPEND_POSITION_FLAG_NONE))
        .WillOnce(Return(true));
    auto replicated = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replicated) << replicated.error().message.view();

    auto commit = election.on_commit_position(leadership_term_id, leader_log_position, live_leader.id);
    ASSERT_TRUE(commit) << commit.error().message.view();
    auto canvassed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(canvassed) << canvassed.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());
}

TEST_F(ElectionTest, followerShouldTimeoutLeaderIfReplicateLogPositionIsNotCommittedByLeader)
{
    constexpr std::int64_t term1_id = 1;
    constexpr std::int64_t term2_id = 2;
    constexpr std::int64_t term1_base_log_position = 60;
    constexpr std::int64_t term2_base_log_position = 120;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, prepare_for_new_leadership(_, _))
        .WillByDefault(Return(std::expected<std::int64_t, Error>{term1_base_log_position}));
    ON_CALL(sender, append_position(_, _, _, _, _)).WillByDefault(Return(true));

    auto* replication = new NiceMock<MockRecordingReplication>();
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(true));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(true));
    ON_CALL(*replication, position()).WillByDefault(Return(term2_base_log_position));
    ON_CALL(*replication, recording_id()).WillByDefault(Return(RECORDING_ID));
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(term2_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(true, NULL_VALUE, term1_id, term1_base_log_position, term1_base_log_position,
                      term1_base_log_position, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto vote = election.on_request_vote(term1_id, term2_base_log_position, term2_id, live_leader.id, PROTOCOL_VERSION);
    ASSERT_TRUE(vote) << vote.error().message.view();

    auto new_term = election.on_new_leadership_term(
        term1_id, term2_id, term2_base_log_position, term2_base_log_position, term2_id, term2_base_log_position,
        term2_base_log_position, NULL_POSITION, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();

    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    auto waiting = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(waiting) << waiting.error().message.view();
    EXPECT_EQ(ElectionState::follower_log_replication, election.state());

    // The leader never commits the replicated position: the election times out
    // rather than replaying.
    now_ns_ += LEADER_HEARTBEAT_TIMEOUT_NS;
    auto timed_out = election.do_work(now_ns_);
    ASSERT_FALSE(timed_out);
    EXPECT_NE(std::string::npos,
              std::string{timed_out.error().message.view()}.find("timeout awaiting commit position"));
}

TEST_F(ElectionTest, followerShouldProgressThroughFailedElectionsTermsImmediatelyPriorToCurrent)
{
    constexpr std::int64_t term1_id = 1;
    constexpr std::int64_t term10_id = 10;
    constexpr std::int64_t term1_base_log_position = 60;
    constexpr std::int64_t term10_base_log_position = 120;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, prepare_for_new_leadership(_, _))
        .WillByDefault(Return(std::expected<std::int64_t, Error>{term1_base_log_position}));

    auto* replication = new NiceMock<MockRecordingReplication>();
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(term10_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(true, NULL_VALUE, term1_id, term1_base_log_position, term1_base_log_position,
                      term1_base_log_position, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto vote =
        election.on_request_vote(term1_id, term10_base_log_position, term10_id, live_leader.id, PROTOCOL_VERSION);
    ASSERT_TRUE(vote) << vote.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    auto new_term = election.on_new_leadership_term(
        term1_id, term10_id, term10_base_log_position, term10_base_log_position, term10_id, term10_base_log_position,
        term10_base_log_position, term10_base_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();
}

TEST_F(ElectionTest, followerShouldProgressThroughInterimElectionsTerms)
{
    constexpr std::int64_t term9_id = 9;
    constexpr std::int64_t term10_id = 10;
    constexpr std::int64_t term9_base_log_position = 60;
    constexpr std::int64_t term10_base_log_position = 120;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, 0, 0, 0, 0, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto vote =
        election.on_request_vote(term9_id, term10_base_log_position, term10_id, live_leader.id, PROTOCOL_VERSION);
    ASSERT_TRUE(vote) << vote.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    auto stale = election.on_new_leadership_term(
        term9_id, term10_id, term10_base_log_position, term10_base_log_position, term10_id, term10_base_log_position,
        term10_base_log_position, term10_base_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(stale) << stale.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    // Each interim term is replicated from its base to the next term's base,
    // and the election returns to canvass with the log it built.
    for (std::int64_t i = 0; i < term9_id - 1; ++i)
    {
        auto interim = election.on_new_leadership_term(i, i + 1, 0, 0, term10_id, term10_base_log_position,
                                                       term10_base_log_position, term10_base_log_position, RECORDING_ID,
                                                       now_ns_, live_leader.id, 0, true);
        ASSERT_TRUE(interim) << interim.error().message.view();
        ASSERT_EQ(ElectionState::follower_log_replication, election.state());

        auto replicated = election.do_work(now_ns_ += 1);
        ASSERT_TRUE(replicated) << replicated.error().message.view();
        ASSERT_EQ(ElectionState::canvass, election.state());
    }

    auto* replication = new NiceMock<MockRecordingReplication>();
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(term9_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    auto last_term = election.on_new_leadership_term(
        term9_id - 1, term9_id, term9_base_log_position, term10_base_log_position, term10_id, term10_base_log_position,
        term10_base_log_position, term10_base_log_position, RECORDING_ID, now_ns_, live_leader.id, 0, true);
    ASSERT_TRUE(last_term) << last_term.error().message.view();
    ASSERT_EQ(ElectionState::follower_log_replication, election.state());

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();
}

TEST_F(ElectionTest, followerShouldUseInitialLeadershipTermIdAndInitialTermBaseLogPositionWhenRecordingLogIsEmpty)
{
    constexpr std::int64_t initial_leadership_term_id = 2;
    constexpr std::int64_t initial_term_base_log_position = 120;
    constexpr std::int64_t snapshot_log_position = 150;
    constexpr std::int64_t leader_leadership_term_id = 3;
    constexpr std::int64_t leader_term_base_log_position = 500;
    constexpr std::int64_t leader_log_position = 600;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[1];
    ClusterMember& live_leader = cluster_members[0];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(sender, append_position(_, _, _, _, _)).WillByDefault(Return(true));

    auto* replication = new NiceMock<MockRecordingReplication>();
    ON_CALL(*replication, has_replication_ended()).WillByDefault(Return(true));
    ON_CALL(*replication, has_stopped()).WillByDefault(Return(true));
    ON_CALL(*replication, position()).WillByDefault(Return(leader_term_base_log_position));
    ON_CALL(*replication, recording_id()).WillByDefault(Return(RECORDING_ID));
    EXPECT_CALL(host, new_log_replication(_, _, Eq(RECORDING_ID), Eq(leader_term_base_log_position), _))
        .WillOnce(Return(ByMove(std::unique_ptr<deepmsg::cluster::election::RecordingReplication>(replication))));

    Election election(false, NULL_VALUE, initial_leadership_term_id, initial_term_base_log_position,
                      snapshot_log_position, snapshot_log_position, cluster_members, this_member, sender, host,
                      context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto new_term = election.on_new_leadership_term(
        initial_leadership_term_id, leader_leadership_term_id, leader_term_base_log_position, leader_log_position,
        leader_leadership_term_id, leader_term_base_log_position, leader_log_position, leader_log_position,
        RECORDING_ID, now_ns_, live_leader.id, 0, false);
    ASSERT_TRUE(new_term) << new_term.error().message.view();

    auto started = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(started) << started.error().message.view();

    auto commit = election.on_commit_position(leader_leadership_term_id, leader_term_base_log_position, live_leader.id);
    ASSERT_TRUE(commit) << commit.error().message.view();

    // The recording log gets the term the follower actually has (the initial
    // one), not the leader's, with the initial base position.
    auto replicated = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replicated) << replicated.error().message.view();

    const RecordingLog::Entry* term_entry = recording_log_.find_term_entry(initial_leadership_term_id);
    ASSERT_NE(nullptr, term_entry);
    EXPECT_EQ(initial_term_base_log_position, term_entry->term_base_log_position);
    EXPECT_EQ(leader_term_base_log_position, term_entry->log_position);
}

TEST_F(ElectionTest, leaderShouldMoveToLogReplicationThenWaitForCommitPosition)
{
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t leader_log_position = 120;
    constexpr std::int64_t follower_log_position = 60;
    const std::int64_t candidate_term_id = leadership_term_id + 1;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& this_member = cluster_members[0];
    ClusterMember& live_follower = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, role()).WillByDefault(Return(Role::leader));

    Election election(true, NULL_VALUE, leadership_term_id, leader_log_position, leader_log_position,
                      leader_log_position, cluster_members, this_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    auto canvass_leader = election.on_canvass_position(leadership_term_id, leader_log_position, leadership_term_id,
                                                       this_member.id, PROTOCOL_VERSION);
    ASSERT_TRUE(canvass_leader) << canvass_leader.error().message.view();
    auto canvass_follower = election.on_canvass_position(leadership_term_id, follower_log_position, leadership_term_id,
                                                         live_follower.id, PROTOCOL_VERSION);
    ASSERT_TRUE(canvass_follower) << canvass_follower.error().message.view();

    now_ns_ += 2 * STARTUP_CANVASS_TIMEOUT_NS;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += 2 * ELECTION_TIMEOUT_NS;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    for (const std::int32_t voter : {this_member.id, live_follower.id})
    {
        auto vote =
            election.on_vote(leadership_term_id, leader_log_position, candidate_term_id, this_member.id, voter, true);
        ASSERT_TRUE(vote) << vote.error().message.view();
    }

    now_ns_ += 2 * ELECTION_TIMEOUT_NS;
    auto elected = election.do_work(now_ns_);
    ASSERT_TRUE(elected) << elected.error().message.view();
    ASSERT_EQ(ElectionState::leader_log_replication, election.state());

    // Until the quorum position reaches the leader's append position it stays
    // put and keeps emitting new leadership terms.
    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(follower_log_position));
    auto waiting = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(waiting) << waiting.error().message.view();
    ASSERT_EQ(ElectionState::leader_log_replication, election.state());

    std::int32_t emitted = 0;
    for (const NewLeadershipTermCall& call : new_leadership_terms_)
    {
        if (call.leadership_term_id == candidate_term_id && call.commit_position == follower_log_position)
        {
            ++emitted;
        }
    }
    EXPECT_EQ(2, emitted);  // one per follower

    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(leader_log_position));
    auto replayed = election.do_work(now_ns_ += 1);
    ASSERT_TRUE(replayed) << replayed.error().message.view();
    EXPECT_EQ(ElectionState::leader_replay, election.state());
}

TEST_F(ElectionTest, shouldSendCommitPositionAndNewLeadershipTermEventsWithTheSameLeadershipTerm)
{
    constexpr std::int64_t log_position = 100;
    constexpr std::int64_t leadership_term_id = 42;
    constexpr std::int64_t quorum_position_1 = 1024 * 1024;
    constexpr std::int64_t quorum_position_2 = 2 * quorum_position_1;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& leader_member = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);
    ON_CALL(host, role()).WillByDefault(Return(Role::leader));
    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(0));
    EXPECT_CALL(host, publish_commit_position(_, _)).Times(::testing::AnyNumber());

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      leader_member, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    canvass_from_peers(election, leader_member.id, leadership_term_id, log_position);
    now_ns_ += 1;
    auto nominated = election.do_work(now_ns_);
    ASSERT_TRUE(nominated) << nominated.error().message.view();
    ASSERT_EQ(ElectionState::nominate, election.state());

    now_ns_ += ELECTION_TIMEOUT_NS >> 1;
    auto balloted = election.do_work(now_ns_);
    ASSERT_TRUE(balloted) << balloted.error().message.view();
    ASSERT_EQ(ElectionState::candidate_ballot, election.state());

    const std::int64_t candidate_term_id = leadership_term_id + 1;
    for (const std::int32_t voter : other_member_ids(leader_member.id))
    {
        auto vote =
            election.on_vote(leadership_term_id, log_position, candidate_term_id, leader_member.id, voter, true);
        ASSERT_TRUE(vote) << vote.error().message.view();
    }
    now_ns_ += 1;
    auto elected = election.do_work(now_ns_);
    ASSERT_TRUE(elected) << elected.error().message.view();
    ASSERT_EQ(ElectionState::leader_log_replication, election.state());

    // Scripted quorum positions that keep the leader in replication.
    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(quorum_position_1));
    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    auto replicated = election.do_work(now_ns_);
    ASSERT_TRUE(replicated) << replicated.error().message.view();

    ON_CALL(host, quorum_position_bounded_by_leader_log(_, _)).WillByDefault(Return(quorum_position_2));
    now_ns_ += LEADER_HEARTBEAT_INTERVAL_NS;
    auto committed = election.do_work(now_ns_);
    ASSERT_TRUE(committed) << committed.error().message.view();

    // The commit position and the new leadership term carry the same term.
    std::int32_t leadership_terms_with_q1 = 0;
    std::int32_t leadership_terms_with_q2 = 0;
    for (const NewLeadershipTermCall& call : new_leadership_terms_)
    {
        EXPECT_EQ(election.leadership_term_id(), call.leadership_term_id);
        if (call.commit_position == quorum_position_1)
        {
            ++leadership_terms_with_q1;
        }
        if (call.commit_position == quorum_position_2)
        {
            ++leadership_terms_with_q2;
        }
    }
    EXPECT_GE(leadership_terms_with_q1, 1);
    EXPECT_GE(leadership_terms_with_q2, 1);
}

TEST_F(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition)
{
    constexpr std::int64_t log_position = 100;
    constexpr std::int64_t leadership_term_id = 7;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& follower = cluster_members[0];
    ClusterMember& leader = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      follower, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());

    // The reference sets the leader by reflection; here the same state comes
    // from the message that names it.
    auto new_term = election.on_new_leadership_term(leadership_term_id, leadership_term_id + 1, NULL_POSITION, 0,
                                                    leadership_term_id, log_position, log_position, 0, 9, 100,
                                                    leader.id, 42, false);
    ASSERT_TRUE(new_term) << new_term.error().message.view();
    EXPECT_EQ(0, election.notified_commit_position());

    auto commit = [&](std::int64_t term, std::int64_t position, std::int32_t leader_id)
    {
        auto result = election.on_commit_position(term, position, leader_id);
        EXPECT_TRUE(result) << (result ? std::string{} : std::string{result.error().message.view()});
    };

    commit(leadership_term_id, 2048, leader.id);
    EXPECT_EQ(2048, election.notified_commit_position());
    commit(leadership_term_id, 5000, leader.id);
    EXPECT_EQ(5000, election.notified_commit_position());
    commit(leadership_term_id, 100, leader.id);
    EXPECT_EQ(5000, election.notified_commit_position());
    commit(leadership_term_id, 4999, leader.id);
    EXPECT_EQ(5000, election.notified_commit_position());
    commit(leadership_term_id, std::numeric_limits<std::int64_t>::min(), leader.id);
    EXPECT_EQ(5000, election.notified_commit_position());

    // A legacy `leadershipTermId` is accepted while the leader matches.
    commit(leadership_term_id - 5, 10000, leader.id);
    EXPECT_EQ(10000, election.notified_commit_position());
    commit(leadership_term_id + 10, 20000, leader.id);
    EXPECT_EQ(20000, election.notified_commit_position());

    // Another member's commit position is ignored.
    commit(leadership_term_id, 13131, leader.id + 5);
    EXPECT_EQ(20000, election.notified_commit_position());

    // INIT is a no-op.
    const Error event = axc::core::make_error(axc::core::ErrorCode::internal_error, "test");
    auto errored = election.handle_error(now_ns_, event);
    ASSERT_TRUE(errored) << errored.error().message.view();
    ASSERT_EQ(ElectionState::init, election.state());
    commit(leadership_term_id, std::numeric_limits<std::int64_t>::max(), leader.id);
    EXPECT_EQ(20000, election.notified_commit_position());

    // No leader at all: a fresh election in CANVASS ignores the message.
    Election leaderless(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                        follower, sender, host, context());
    auto leaderless_init = leaderless.do_work(now_ns_ + 1);
    ASSERT_TRUE(leaderless_init) << leaderless_init.error().message.view();
    auto ignored = leaderless.on_commit_position(leadership_term_id, 65535, leader.id);
    ASSERT_TRUE(ignored) << ignored.error().message.view();
    EXPECT_EQ(0, leaderless.notified_commit_position());
}

TEST_F(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm)
{
    constexpr std::int64_t log_position = 100;
    constexpr std::int64_t leadership_term_id = 7;
    constexpr std::int64_t log_leadership_term_id = leadership_term_id;

    ClusterMembers cluster_members = three_member_cluster();
    ClusterMember& follower = cluster_members[0];
    ClusterMember& leader = cluster_members[1];
    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    script_host(host, sender);

    Election election(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                      follower, sender, host, context());

    now_ns_ = 1;
    auto init = election.do_work(now_ns_);
    ASSERT_TRUE(init) << init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, election.state());
    EXPECT_EQ(0, election.notified_commit_position());

    // FOLLOWER_BALLOT: the message is accepted because its term matches the
    // candidate term the follower voted for.
    auto vote_request = election.on_request_vote(log_leadership_term_id, log_position, 111, 2, PROTOCOL_VERSION);
    ASSERT_TRUE(vote_request) << vote_request.error().message.view();
    ASSERT_EQ(ElectionState::follower_ballot, election.state());

    auto ballot_term =
        election.on_new_leadership_term(log_leadership_term_id, log_leadership_term_id + 1, NULL_POSITION, 0, 111,
                                        log_position, 1073741824, 333, 9, 100, leader.id, 42, false);
    ASSERT_TRUE(ballot_term) << ballot_term.error().message.view();
    EXPECT_EQ(333, election.notified_commit_position());
    EXPECT_EQ(111, election.leadership_term_id());

    // CANVASS does not check the candidate term, so a stale term still moves
    // the value (which cannot go backwards) and the term — the reference's own
    // quirk, reached here through a fresh election rather than reflection.
    Election canvassing(true, NULL_VALUE, leadership_term_id, log_position, log_position, log_position, cluster_members,
                        follower, sender, host, context());
    auto canvassing_init = canvassing.do_work(now_ns_ + 1);
    ASSERT_TRUE(canvassing_init) << canvassing_init.error().message.view();
    ASSERT_EQ(ElectionState::canvass, canvassing.state());

    auto stale_term =
        canvassing.on_new_leadership_term(log_leadership_term_id, log_leadership_term_id + 1, NULL_POSITION, 0, 2,
                                          log_position, 1073741824, 222, 9, 100, leader.id, 42, false);
    ASSERT_TRUE(stale_term) << stale_term.error().message.view();
    EXPECT_EQ(222, canvassing.notified_commit_position());
    EXPECT_EQ(2, canvassing.leadership_term_id());

    // ... and a message whose value is lower does not take it backwards.
    auto lower_value =
        canvassing.on_new_leadership_term(log_leadership_term_id, log_leadership_term_id + 1, NULL_POSITION, 0, 2,
                                          log_position, 1073741824, 100, 9, 100, leader.id, 42, false);
    ASSERT_TRUE(lower_value) << lower_value.error().message.view();
    EXPECT_EQ(222, canvassing.notified_commit_position());

    // INIT is a no-op.
    const Error event = axc::core::make_error(axc::core::ErrorCode::internal_error, "test");
    auto errored = canvassing.handle_error(now_ns_, event);
    ASSERT_TRUE(errored) << errored.error().message.view();
    auto init_term =
        canvassing.on_new_leadership_term(log_leadership_term_id, log_leadership_term_id + 1, NULL_POSITION, 0, 222,
                                          log_position, 1073741824, 777, 9, 100, leader.id, 42, false);
    ASSERT_TRUE(init_term) << init_term.error().message.view();
    EXPECT_EQ(222, canvassing.notified_commit_position());
}

}  // namespace
