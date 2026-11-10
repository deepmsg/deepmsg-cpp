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
using ::testing::Eq;
using ::testing::NiceMock;
using ::testing::Return;

using axc::core::Error;
using deepmsg::cluster::common::ClusterMember;
using deepmsg::cluster::common::ClusterMembers;
using deepmsg::cluster::common::NodeStateFile;
using deepmsg::cluster::common::RecordingLog;
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

TEST_F(ElectionTest, aMultiMemberClusterFailsLoudlyUntilTheBallotIsTranscribed)
{
    auto parsed = deepmsg::cluster::common::parse_cluster_members(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|"
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");
    ASSERT_TRUE(parsed) << parsed.error().message.view();
    ClusterMembers cluster_members = std::move(*parsed);
    ClusterMember& this_member = cluster_members[0];

    NiceMock<MockElectionHost> host;
    NiceMock<MockConsensusSender> sender;
    Election election(true, NULL_VALUE, NULL_VALUE, 0, 0, 0, cluster_members, this_member, sender, host, context());

    auto init = election.do_work(1);
    ASSERT_TRUE(init) << init.error().message.view();
    EXPECT_EQ(ElectionState::canvass, election.state());

    auto work = election.do_work(1);
    ASSERT_FALSE(work);
    EXPECT_NE(std::string::npos, std::string{work.error().message.view()}.find("CANVASS"));
    EXPECT_EQ(ElectionState::canvass, election.state());
}

}  // namespace
