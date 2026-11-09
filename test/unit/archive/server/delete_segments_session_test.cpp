// A4a-2 criteria: `DeleteSegmentsSession` (`DeleteSegmentsSession.java:29-177`,
// 1.54.0 = 454da1e4e6). The C suite observes the DELETE signal and the file
// counts, but not the state machine, the `.del` fallback or the error path.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "archive/server/delete_segments_session.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::DeleteSegmentsControl;
using deepmsg::archive::server::DeleteSegmentsResponder;
using deepmsg::archive::server::DeleteSegmentsSession;
using deepmsg::archive::server::segment_file_name;

class FakeResponder final : public DeleteSegmentsResponder
{
public:
    void send_delete_signal(std::int64_t control_session_id, std::int64_t correlation_id,
                            std::int64_t recording_id) override
    {
        delete_signals.push_back({control_session_id, correlation_id, recording_id});
    }

    void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::string_view message) override
    {
        errors.push_back({control_session_id, correlation_id, std::string{message}});
    }

    struct Signal
    {
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::int64_t recording_id{0};
    };

    struct Error
    {
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::string message;
    };

    std::vector<Signal> delete_signals;
    std::vector<Error> errors;
};

class FakeControl final : public DeleteSegmentsControl
{
public:
    bool has_in_progress_replays(std::int64_t recording_id) override
    {
        last_has_replays_recording_id = recording_id;
        return replays_in_progress;
    }

    void remove_delete_segments_session(std::int64_t recording_id) override
    {
        removed_recordings.push_back(recording_id);
    }

    void log_error(std::string_view message) override
    {
        logged_errors.emplace_back(message);
    }

    bool replays_in_progress{false};
    std::int64_t last_has_replays_recording_id{-1};
    std::vector<std::int64_t> removed_recordings;
    std::vector<std::string> logged_errors;
};

class DeleteSegmentsSessionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::error_code error;
        archive_dir_ = std::filesystem::temp_directory_path(error) / "axc-delete-segments-test";
        std::filesystem::remove_all(archive_dir_, error);
        std::filesystem::create_directories(archive_dir_, error);
        ASSERT_FALSE(error);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    void create_file(const std::string& name)
    {
        std::ofstream out{archive_dir_ / name, std::ios::binary | std::ios::trunc};
        out << "x";
        ASSERT_TRUE(out.good()) << name;
    }

    [[nodiscard]] bool exists(const std::string& name) const
    {
        std::error_code error;
        return std::filesystem::exists(archive_dir_ / name, error);
    }

    std::filesystem::path archive_dir_;
    FakeResponder responder_;
    FakeControl control_;
};

TEST_F(DeleteSegmentsSessionTest, awaitsReplaysAndDeletesOneFilePerPass)
{
    const std::string first = segment_file_name(7, 0);
    const std::string second = segment_file_name(7, 128 * 1024);
    create_file(first);
    create_file(second);

    control_.replays_in_progress = true;
    DeleteSegmentsSession session(7, 11, 22, archive_dir_.string(), {first, second}, responder_, control_, true);

    EXPECT_EQ(0, session.do_work());
    EXPECT_EQ(DeleteSegmentsSession::State::await_replays_stop, session.state());
    EXPECT_TRUE(exists(first));

    control_.replays_in_progress = false;
    EXPECT_EQ(1, session.do_work());
    EXPECT_EQ(DeleteSegmentsSession::State::delete_files, session.state());

    EXPECT_EQ(1, session.do_work());
    EXPECT_FALSE(exists(first));
    EXPECT_TRUE(exists(second));
    EXPECT_EQ(1u, session.files().size());

    EXPECT_EQ(1, session.do_work());
    EXPECT_FALSE(exists(second));
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(7, control_.last_has_replays_recording_id);

    session.close();
    ASSERT_EQ(1u, responder_.delete_signals.size());
    EXPECT_EQ(11, responder_.delete_signals[0].control_session_id);
    EXPECT_EQ(22, responder_.delete_signals[0].correlation_id);
    EXPECT_EQ(7, responder_.delete_signals[0].recording_id);
    ASSERT_EQ(1u, control_.removed_recordings.size());
    EXPECT_EQ(7, control_.removed_recordings[0]);
    EXPECT_TRUE(responder_.errors.empty());
}

TEST_F(DeleteSegmentsSessionTest, skipsTheAwaitWhenReplaysAreAlreadyStopped)
{
    const std::string name = segment_file_name(9, 0);
    create_file(name);

    DeleteSegmentsSession session(9, 1, 2, archive_dir_.string(), {name}, responder_, control_, false);

    EXPECT_EQ(1, session.do_work());
    EXPECT_EQ(DeleteSegmentsSession::State::delete_files, session.state());
    EXPECT_FALSE(exists(name));
    EXPECT_TRUE(session.is_done());
}

TEST_F(DeleteSegmentsSessionTest, fallsBackToTheDotDelNameAnInterruptedDeleteLeftBehind)
{
    const std::string name = segment_file_name(7, 0);
    const std::string renamed = name + ".del";
    create_file(renamed);

    DeleteSegmentsSession session(7, 1, 2, archive_dir_.string(), {name}, responder_, control_, false);
    EXPECT_EQ(1, session.do_work());

    EXPECT_FALSE(exists(renamed));
    EXPECT_TRUE(session.is_done());
    EXPECT_TRUE(responder_.errors.empty()) << "deleting the .del fallback is the reference's success path";
}

TEST_F(DeleteSegmentsSessionTest, doesNotRecurseOnANameThatAlreadyEndsWithDotDel)
{
    DeleteSegmentsSession session(7, 1, 2, archive_dir_.string(), {"7-0.rec.del"}, responder_, control_, false);
    EXPECT_EQ(1, session.do_work());

    EXPECT_TRUE(session.is_done());
    EXPECT_TRUE(responder_.errors.empty());
    EXPECT_TRUE(control_.logged_errors.empty());
}

TEST_F(DeleteSegmentsSessionTest, reportsAFileThatCannotBeDeleted)
{
    const std::string name = segment_file_name(7, 0);
    std::error_code error;
    std::filesystem::create_directories(archive_dir_ / name, error);
    ASSERT_FALSE(error);
    create_file(name + "/child");

    DeleteSegmentsSession session(7, 5, 6, archive_dir_.string(), {name}, responder_, control_, false);
    EXPECT_EQ(1, session.do_work());

    ASSERT_EQ(1u, responder_.errors.size());
    EXPECT_EQ(5, responder_.errors[0].control_session_id);
    EXPECT_EQ(6, responder_.errors[0].correlation_id);
    EXPECT_EQ("unable to delete segment file: " + name, responder_.errors[0].message);
    ASSERT_EQ(1u, control_.logged_errors.size());
    EXPECT_EQ(responder_.errors[0].message, control_.logged_errors[0]);
}

TEST_F(DeleteSegmentsSessionTest, maxDeletePositionIsTheLargestNamePosition)
{
    const std::vector<std::string> files{segment_file_name(7, 0), segment_file_name(7, 262144),
                                         segment_file_name(7, 131072) + ".del"};

    DeleteSegmentsSession session(7, 1, 2, archive_dir_.string(), files, responder_, control_, false);
    EXPECT_EQ(262144, session.max_delete_position());
}

TEST_F(DeleteSegmentsSessionTest, anEmptyListKeepsTheReferenceMinimum)
{
    DeleteSegmentsSession session(7, 1, 2, archive_dir_.string(), {}, responder_, control_, false);
    EXPECT_EQ(std::numeric_limits<std::int64_t>::min(), session.max_delete_position());
    EXPECT_TRUE(session.is_done());
}

}  // namespace
