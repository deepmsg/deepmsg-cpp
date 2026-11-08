// A2b-7 criterion: the three recording events on the wire
// (`RecordingEventsProxy.java:30-152`, `aeron-archive-codecs.xml` 101-103).

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "archive/server/recording_events.h"
#include "io_aeron_archive_codecs/RecordingProgress.h"
#include "io_aeron_archive_codecs/RecordingStarted.h"
#include "io_aeron_archive_codecs/RecordingStopped.h"

namespace
{

using deepmsg::archive::server::RecordingEvents;
using deepmsg::archive::server::ResponsePublication;

class FakePublication final : public ResponsePublication
{
public:
    [[nodiscard]] OfferResult offer(std::span<const std::uint8_t> buffer) override
    {
        offered.assign(buffer.begin(), buffer.end());
        return 1;
    }

    [[nodiscard]] OfferResult try_claim(std::int32_t length, Claim& claim) override
    {
        ++claim_attempts;
        if (!allow_claims)
        {
            return -2;  // BACK_PRESSURED
        }
        claim_buffer.assign(static_cast<std::size_t>(length), 0);
        claim.buffer = claim_buffer;
        claim.commit_fn = nullptr;
        claim.context = nullptr;
        return 1;
    }

    [[nodiscard]] bool is_connected() const override
    {
        return true;
    }

    [[nodiscard]] std::int32_t max_payload_length() const override
    {
        return 1408;
    }

    std::vector<std::uint8_t> offered;
    std::vector<std::uint8_t> claim_buffer;
    bool allow_claims{true};
    int claim_attempts{0};
};

TEST(RecordingEventsTest, startedCarriesEveryReferenceField)
{
    FakePublication publication;
    RecordingEvents events(publication);

    events.started(7, 128, 11, 33, "aeron:udp?endpoint=localhost:3333", "aeron:ipc");

    ASSERT_FALSE(publication.offered.empty());
    const auto length = static_cast<std::uint64_t>(publication.offered.size());

    deepmsg::archive::server::archive_codecs::RecordingStarted decoder;
    decoder.wrapForDecode(reinterpret_cast<char*>(publication.offered.data()), 8,
                          deepmsg::archive::server::archive_codecs::RecordingStarted::sbeBlockLength(),
                          deepmsg::archive::server::archive_codecs::RecordingStarted::sbeSchemaVersion(), length);

    EXPECT_EQ(7, decoder.recordingId());
    EXPECT_EQ(128, decoder.startPosition());
    EXPECT_EQ(11, decoder.sessionId());
    EXPECT_EQ(33, decoder.streamId());

    const auto channel_length = decoder.channelLength();
    EXPECT_EQ("aeron:udp?endpoint=localhost:3333",
              std::string(decoder.channel(), static_cast<std::size_t>(channel_length)));
}

TEST(RecordingEventsTest, progressIsAClaimAndReportsFailure)
{
    FakePublication publication;
    RecordingEvents events(publication);

    EXPECT_TRUE(events.progress(7, 128, 640));

    deepmsg::archive::server::archive_codecs::RecordingProgress decoder;
    decoder.wrapForDecode(reinterpret_cast<char*>(publication.claim_buffer.data()), 8,
                          deepmsg::archive::server::archive_codecs::RecordingProgress::sbeBlockLength(),
                          deepmsg::archive::server::archive_codecs::RecordingProgress::sbeSchemaVersion(),
                          publication.claim_buffer.size());
    EXPECT_EQ(7, decoder.recordingId());
    EXPECT_EQ(128, decoder.startPosition());
    EXPECT_EQ(640, decoder.position());

    publication.allow_claims = false;
    EXPECT_FALSE(events.progress(7, 128, 704));
}

TEST(RecordingEventsTest, stoppedCarriesTheStopPosition)
{
    FakePublication publication;
    RecordingEvents events(publication);

    events.stopped(7, 128, 704);

    deepmsg::archive::server::archive_codecs::RecordingStopped decoder;
    decoder.wrapForDecode(reinterpret_cast<char*>(publication.claim_buffer.data()), 8,
                          deepmsg::archive::server::archive_codecs::RecordingStopped::sbeBlockLength(),
                          deepmsg::archive::server::archive_codecs::RecordingStopped::sbeSchemaVersion(),
                          publication.claim_buffer.size());
    EXPECT_EQ(7, decoder.recordingId());
    EXPECT_EQ(128, decoder.startPosition());
    EXPECT_EQ(704, decoder.stopPosition());
}

}  // namespace
