// A2b-1 criterion: the type-100 key and label bytes
// (`RecordingPos.java:33-132`, 1.54.0 = 454da1e4e6).
//
// The layout is the one the reference C client itself reads
// (`aeron_archive_recording_pos.c`): recordingId at 0, sessionId at 8, the
// source-identity length at 12, the identity at 16, archiveId after it.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "archive/server/recording_pos.h"

namespace
{

using deepmsg::archive::server::RecordingPos;

template <typename T>
T read(const RecordingPos::Key& key, std::size_t offset)
{
    T value{};
    std::memcpy(&value, key.bytes.data() + offset, sizeof(T));
    return value;
}

TEST(RecordingPosTest, keyLayoutPutsTheIdentityAndArchiveIdWhereTheReferenceReaderLooks)
{
    const auto key = RecordingPos::make_key(7, 3, 0x01020304, "aeron:ipc");

    ASSERT_EQ(16 + 9 + 8, key.length);
    EXPECT_EQ(3, read<std::int64_t>(key, RecordingPos::RECORDING_ID_OFFSET));
    EXPECT_EQ(0x01020304, read<std::int32_t>(key, RecordingPos::SESSION_ID_OFFSET));
    EXPECT_EQ(9, read<std::int32_t>(key, RecordingPos::SOURCE_IDENTITY_LENGTH_OFFSET));
    EXPECT_EQ("aeron:ipc", std::string(reinterpret_cast<const char*>(key.bytes.data() + 16), 9));
    EXPECT_EQ(7, read<std::int64_t>(key, 16 + 9));
}

TEST(RecordingPosTest, sourceIdentityIsTruncatedToTheReferenceLimit)
{
    const std::string identity(200, 'x');
    const auto key = RecordingPos::make_key(-1, 42, 5, identity);

    EXPECT_EQ(88, read<std::int32_t>(key, RecordingPos::SOURCE_IDENTITY_LENGTH_OFFSET));
    EXPECT_EQ(16 + 88 + 8, key.length);
    EXPECT_EQ(static_cast<std::int32_t>(RecordingPos::MAX_KEY_LENGTH), key.length);
}

TEST(RecordingPosTest, labelMatchesTheReferenceFormat)
{
    EXPECT_EQ("rec-pos: 3 16909060 33 aeron:udp?endpoint=localhost:3333 - archiveId=7",
              RecordingPos::make_label(7, 3, 0x01020304, 33, "aeron:udp?endpoint=localhost:3333"));
    EXPECT_EQ("rec-pos: 1 2 3 aeron:ipc - archiveId=-1", RecordingPos::make_label(-1, 1, 2, 3, "aeron:ipc"));
}

TEST(RecordingPosTest, labelIsTruncatedAfterLeavingRoomForTheArchiveIdSuffix)
{
    const std::string channel(2000, 'c');
    const std::string label = RecordingPos::make_label(12345, 1, 2, 3, channel);

    EXPECT_EQ(RecordingPos::MAX_LABEL_LENGTH, label.size());
    EXPECT_EQ(std::string::npos, label.find(channel));
    EXPECT_EQ(" - archiveId=12345", label.substr(label.size() - 18));
}

TEST(RecordingPosTest, parseKeyRoundTrips)
{
    const auto key = RecordingPos::make_key(9, 11, 12, "aeron:udp?endpoint=localhost:1");
    const auto view = RecordingPos::parse_key(key.view());

    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(11, view->recording_id);
    EXPECT_EQ(12, view->session_id);
    EXPECT_EQ("aeron:udp?endpoint=localhost:1", view->source_identity);
    EXPECT_EQ(9, view->archive_id);
}

TEST(RecordingPosTest, parseKeyRefusesMalformedKeys)
{
    std::uint8_t short_key[8]{};
    EXPECT_FALSE(RecordingPos::parse_key({short_key, sizeof(short_key)}).has_value());

    auto key = RecordingPos::make_key(1, 2, 3, "aeron:ipc");
    const std::int32_t bogus_length = 1000;
    std::memcpy(key.bytes.data() + RecordingPos::SOURCE_IDENTITY_LENGTH_OFFSET, &bogus_length, sizeof(bogus_length));
    EXPECT_FALSE(RecordingPos::parse_key(key.view()).has_value());
}

}  // namespace
