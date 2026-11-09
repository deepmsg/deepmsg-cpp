// A3a-1 criteria: the paging cursor, the 64-candidate scan bound and the
// terminal response of `AbstractListRecordingsSession.java:79-142`
// (1.54.0 = 454da1e4e6). The C suite only reaches the happy paths.

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "archive/server/catalog.h"
#include "archive/server/list_recordings_session.h"

namespace
{

namespace archive_codecs = deepmsg::archive::server::archive_codecs;

using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOptions;
using deepmsg::archive::server::ListRecordingsConfig;
using deepmsg::archive::server::ListRecordingsResponses;
using deepmsg::archive::server::ListRecordingsSession;

constexpr std::int32_t SEGMENT_LENGTH = 128 * 1024;
constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t MTU_LENGTH = 1024;

class FakeResponses final : public ListRecordingsResponses
{
public:
    bool send_descriptor(std::int64_t control_session_id, std::int64_t correlation_id,
                         std::span<const std::uint8_t> frame) override
    {
        (void)control_session_id;
        (void)correlation_id;
        send_calls++;
        if (send_calls == fail_send_call)
        {
            return false;
        }

        frames.emplace_back(frame.begin(), frame.end());
        archive_codecs::RecordingDescriptor descriptor;
        descriptor.wrapForDecode(reinterpret_cast<char*>(frames.back().data()), Catalog::DESCRIPTOR_HEADER_LENGTH,
                                 archive_codecs::RecordingDescriptor::SBE_BLOCK_LENGTH,
                                 archive_codecs::RecordingDescriptor::SBE_SCHEMA_VERSION, frames.back().size());
        sent_recording_ids.push_back(descriptor.recordingId());
        return true;
    }

    void send_recording_unknown(std::int64_t control_session_id, std::int64_t correlation_id,
                                std::int64_t recording_id) override
    {
        (void)control_session_id;
        (void)correlation_id;
        unknown_ids.push_back(recording_id);
    }

    [[nodiscard]] bool control_session_is_done(std::int64_t control_session_id) const override
    {
        (void)control_session_id;
        return session_done;
    }

    void clear_active_listing(std::int64_t control_session_id) override
    {
        (void)control_session_id;
        clear_calls++;
    }

    int send_calls{0};
    int fail_send_call{-1};
    bool session_done{false};
    std::vector<std::vector<std::uint8_t>> frames;
    std::vector<std::int64_t> sent_recording_ids;
    std::vector<std::int64_t> unknown_ids;
    int clear_calls{0};
};

class ListRecordingsSessionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::error_code error;
        archive_dir_ = std::filesystem::temp_directory_path(error) / "axc-list-recordings-session-test";
        std::filesystem::remove_all(archive_dir_, error);
        std::filesystem::create_directories(archive_dir_, error);
        ASSERT_FALSE(error);

        CatalogOptions options;
        options.archive_dir = archive_dir_.string();
        auto created = Catalog::create(options);
        ASSERT_TRUE(created) << created.error().message.view();
        catalog_.emplace(std::move(*created));
    }

    void TearDown() override
    {
        if (catalog_)
        {
            (void)catalog_->close();
            catalog_.reset();
        }
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    [[nodiscard]] std::int64_t add_recording(std::int32_t stream_id = 33,
                                             const std::string& stripped = "aeron:udp?endpoint=localhost:3333",
                                             const std::string& original = "aeron:udp?endpoint=localhost:3333")
    {
        auto id = catalog_->add_new_recording(0, 0, 1, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 5, stream_id, stripped,
                                              original, "src");
        EXPECT_TRUE(id) << (id ? std::string{} : std::string{id.error().message.view()});
        return id.value_or(-1);
    }

    [[nodiscard]] ListRecordingsConfig config(std::int64_t from, std::int32_t count) const
    {
        ListRecordingsConfig config;
        config.control_session_id = 7;
        config.correlation_id = 9;
        config.from_recording_id = from;
        config.count = count;
        return config;
    }

    [[nodiscard]] ListRecordingsConfig uri_config(std::int64_t from, std::int32_t count, std::int32_t stream_id,
                                                  const std::string& fragment) const
    {
        ListRecordingsConfig config = this->config(from, count);
        config.filter_by_uri = true;
        config.stream_id = stream_id;
        config.channel_fragment = fragment;
        return config;
    }

    std::filesystem::path archive_dir_;
    std::optional<Catalog> catalog_;
    FakeResponses responses_;
};

TEST_F(ListRecordingsSessionTest, listsEveryRecordingWhenCountCoversThem)
{
    const std::int64_t first = add_recording();
    const std::int64_t second = add_recording();
    const std::int64_t third = add_recording();

    ListRecordingsSession session(*catalog_, responses_, config(INT64_MIN, 10));
    EXPECT_EQ(3, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(3, session.sent());
    EXPECT_EQ((std::vector<std::int64_t>{first, second, third}), responses_.sent_recording_ids);
    // The scan ran past the last entry, so the terminal response follows the
    // third descriptor (the client stops at `count`, but the server still sends
    // it because `sent != count`).
    ASSERT_EQ(1u, responses_.unknown_ids.size());
    EXPECT_EQ(third + 1, responses_.unknown_ids[0]);
}

TEST_F(ListRecordingsSessionTest, aFullPageEndsTheSessionWithoutATerminalResponse)
{
    (void)add_recording();
    (void)add_recording();

    ListRecordingsSession session(*catalog_, responses_, config(INT64_MIN, 1));
    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(1u, responses_.sent_recording_ids.size());
    EXPECT_TRUE(responses_.unknown_ids.empty()) << "the client collects by count, not by the terminal response";
}

TEST_F(ListRecordingsSessionTest, countZeroEndsWithoutSendingAnything)
{
    (void)add_recording();

    ListRecordingsSession session(*catalog_, responses_, config(INT64_MIN, 0));
    EXPECT_EQ(0, session.do_work());
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(0, session.sent());
    EXPECT_TRUE(responses_.frames.empty());
    EXPECT_TRUE(responses_.unknown_ids.empty());
}

TEST_F(ListRecordingsSessionTest, theCursorSkipsADeletedRecording)
{
    const std::int64_t first = add_recording();
    const std::int64_t second = add_recording();
    const std::int64_t third = add_recording();

    auto changed = catalog_->change_state(second, archive_codecs::RecordingState::DELETED);
    ASSERT_TRUE(changed.has_value() && *changed);

    ListRecordingsSession session(*catalog_, responses_, config(0, 10));
    session.do_work();
    EXPECT_EQ((std::vector<std::int64_t>{first, third}), responses_.sent_recording_ids);
}

TEST_F(ListRecordingsSessionTest, aFromIdPastTheEndReportsTheCursor)
{
    (void)add_recording();
    (void)add_recording();

    ListRecordingsSession session(*catalog_, responses_, config(100, 10));
    session.do_work();
    EXPECT_TRUE(session.is_done());
    EXPECT_TRUE(responses_.sent_recording_ids.empty());
    ASSERT_EQ(1u, responses_.unknown_ids.size());
    EXPECT_EQ(100, responses_.unknown_ids[0]);
}

TEST_F(ListRecordingsSessionTest, theUriFilterMatchesStreamAndOriginalChannel)
{
    const std::int64_t matching =
        add_recording(33, "aeron:udp?endpoint=localhost:3333", "aeron:udp?endpoint=localhost:3333");
    (void)add_recording(33, "aeron:udp?endpoint=localhost:3334", "aeron:udp?endpoint=localhost:3334");
    (void)add_recording(44, "aeron:udp?endpoint=localhost:3333", "aeron:udp?endpoint=localhost:3333");

    ListRecordingsSession session(*catalog_, responses_, uri_config(INT64_MIN, 10, 33, "3333"));
    session.do_work();
    EXPECT_EQ((std::vector<std::int64_t>{matching}), responses_.sent_recording_ids);
}

TEST_F(ListRecordingsSessionTest, theUriFilterReadsTheOriginalChannelNotTheStrippedOne)
{
    const std::int64_t matching =
        add_recording(33, "aeron:udp?endpoint=localhost:3333", "aeron:udp?endpoint=localhost:3333|term-length=64k");

    ListRecordingsSession session(*catalog_, responses_, uri_config(INT64_MIN, 10, 33, "term-length"));
    session.do_work();
    EXPECT_EQ((std::vector<std::int64_t>{matching}), responses_.sent_recording_ids);
}

TEST_F(ListRecordingsSessionTest, anEmptyFragmentMatchesEverything)
{
    (void)add_recording();
    (void)add_recording();

    ListRecordingsSession session(*catalog_, responses_, uri_config(INT64_MIN, 10, 33, ""));
    session.do_work();
    EXPECT_EQ(2u, responses_.sent_recording_ids.size());
}

TEST_F(ListRecordingsSessionTest, noMatchScansToTheEndAndReportsTheCursor)
{
    const std::int64_t first = add_recording();
    const std::int64_t second = add_recording();
    const std::int64_t third = add_recording();

    ListRecordingsSession session(*catalog_, responses_, uri_config(INT64_MIN, 10, 33, "no-match"));
    session.do_work();
    EXPECT_TRUE(session.is_done());
    EXPECT_TRUE(responses_.sent_recording_ids.empty());
    ASSERT_EQ(1u, responses_.unknown_ids.size());
    EXPECT_EQ(third + 1, responses_.unknown_ids[0]) << "the terminal relevantId is the cursor, not a recording";
    EXPECT_GT(third, first);
    EXPECT_GT(third, second);
}

TEST_F(ListRecordingsSessionTest, atMostSixtyFourCandidatesPerPass)
{
    for (int index = 0; index < 100; index++)
    {
        ASSERT_GE(add_recording(), 0);
    }

    ListRecordingsSession session(*catalog_, responses_, config(INT64_MIN, 1000));
    session.do_work();
    EXPECT_EQ(ListRecordingsSession::MAX_SCANS_PER_WORK_CYCLE, session.sent());
    EXPECT_FALSE(session.is_done()) << "the page is not full and the catalog is not exhausted";

    session.do_work();
    EXPECT_EQ(100, session.sent());
    EXPECT_TRUE(session.is_done());
    ASSERT_EQ(1u, responses_.unknown_ids.size());
    EXPECT_EQ(100, responses_.unknown_ids[0]);
}

TEST_F(ListRecordingsSessionTest, aBackPressuredSendKeepsTheCursor)
{
    const std::int64_t first = add_recording();
    (void)add_recording();

    responses_.fail_send_call = 1;
    ListRecordingsSession session(*catalog_, responses_, config(0, 10));
    session.do_work();
    EXPECT_FALSE(session.is_done());
    EXPECT_EQ(0, session.sent());
    EXPECT_EQ(first, session.cursor()) << "the failed record is retried, not skipped";

    responses_.fail_send_call = -1;
    session.do_work();
    EXPECT_TRUE(session.is_done());
    EXPECT_EQ(2, session.sent());
    EXPECT_EQ(first, responses_.sent_recording_ids[0]);
}

TEST_F(ListRecordingsSessionTest, aClosedControlSessionEndsTheSession)
{
    (void)add_recording();

    responses_.fail_send_call = 1;
    responses_.session_done = true;
    ListRecordingsSession session(*catalog_, responses_, config(0, 10));
    session.do_work();
    EXPECT_TRUE(session.is_done());
}

TEST_F(ListRecordingsSessionTest, closeReleasesTheActiveListing)
{
    (void)add_recording();
    ListRecordingsSession session(*catalog_, responses_, config(0, 10));
    session.close();
    EXPECT_EQ(1, responses_.clear_calls);
}

}  // namespace
