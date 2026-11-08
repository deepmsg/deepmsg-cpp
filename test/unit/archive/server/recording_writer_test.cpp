// Reference: `aeron-archive/src/test/java/io/aeron/archive/RecordingWriterTest.java`
// (Aeron 1.54.0, 454da1e4e6). Test names are the reference method names.
//
// Deviation, recorded in `records/a1b.md`: the reference's
// `onBlockThrowsArchiveExceptionIfCurrentThreadWasInterrupted` pins a Java thread
// interrupt closing the channel; C++ has no equivalent, so it is not ported.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "archive/server/frame_layout.h"
#include "archive/server/recording_writer.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::crc32;
using deepmsg::archive::server::DATA_HEADER_LENGTH;
using deepmsg::archive::server::NanoTimeSource;
using deepmsg::archive::server::RecordingMetrics;
using deepmsg::archive::server::RecordingWriter;
using deepmsg::archive::server::RecordingWriterContext;
using deepmsg::archive::server::segment_file_name;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;  // `LogBufferDescriptor.TERM_MIN_LENGTH`
constexpr std::int32_t SEGMENT_LENGTH = TERM_LENGTH * 4;

class ScriptedNanoTime final : public NanoTimeSource
{
public:
    explicit ScriptedNanoTime(std::vector<std::int64_t> values) : values_(std::move(values)) {}

    [[nodiscard]] std::int64_t nano_time() const noexcept override
    {
        const std::int64_t value = values_[index_ % values_.size()];
        index_++;
        return value;
    }

private:
    std::vector<std::int64_t> values_;
    mutable std::size_t index_{0};
};

class MonotonicNanoTime final : public NanoTimeSource
{
public:
    [[nodiscard]] std::int64_t nano_time() const noexcept override
    {
        value_ += 111;
        return value_;
    }

private:
    mutable std::int64_t value_{100};
};

class MetricsRecorder final : public RecordingMetrics
{
public:
    void bytes_written(std::int32_t length) noexcept override
    {
        last_bytes_written = length;
        blocks++;
    }

    void write_time_ns(std::int64_t duration) noexcept override
    {
        last_write_time_ns = duration;
    }

    std::int32_t last_bytes_written{-1};
    std::int64_t last_write_time_ns{-1};
    int blocks{0};
};

// Frame field accessors (`DataHeaderFlyweight` offsets, see frame_layout.h).
void set_frame_type(std::vector<std::uint8_t>& buffer, std::size_t offset, std::int32_t type)
{
    const auto value = static_cast<std::uint16_t>(type);
    std::memcpy(buffer.data() + offset + deepmsg::archive::server::FRAME_TYPE_OFFSET, &value, sizeof(value));
}

void set_frame_length(std::vector<std::uint8_t>& buffer, std::size_t offset, std::int32_t length)
{
    std::memcpy(buffer.data() + offset + deepmsg::archive::server::FRAME_LENGTH_OFFSET, &length, sizeof(length));
}

void set_frame_session_id(std::vector<std::uint8_t>& buffer, std::size_t offset, std::int32_t session_id)
{
    std::memcpy(buffer.data() + offset + deepmsg::archive::server::SESSION_ID_OFFSET, &session_id, sizeof(session_id));
}

void set_frame_term_id(std::vector<std::uint8_t>& buffer, std::size_t offset, std::int32_t term_id)
{
    std::memcpy(buffer.data() + offset + deepmsg::archive::server::TERM_ID_OFFSET, &term_id, sizeof(term_id));
}

template <typename T>
[[nodiscard]] T get(const std::vector<std::uint8_t>& buffer, std::size_t offset)
{
    T value{};
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    return value;
}

class RecordingWriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-recwriter-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        archive_dir_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    [[nodiscard]] RecordingWriterContext context() const
    {
        RecordingWriterContext ctx;
        ctx.archive_dir = archive_dir_;
        return ctx;
    }

    [[nodiscard]] std::string segment_path(std::int64_t recording_id, std::int64_t position) const
    {
        const std::int64_t segment_base_position = position - (position & (TERM_LENGTH - 1));
        return archive_dir_ + "/" + segment_file_name(recording_id, segment_base_position);
    }

    [[nodiscard]] static std::vector<std::uint8_t> read_all(const std::string& path)
    {
        std::ifstream input{path, std::ios::binary};
        return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    }

    std::string archive_dir_;
};

TEST_F(RecordingWriterTest, initShouldOpenAFileChannel)
{
    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, context(), nullptr};
    const std::string path = segment_path(1, 0);
    EXPECT_FALSE(std::filesystem::exists(path));

    auto initialized = writer.init();
    ASSERT_TRUE(initialized) << initialized.error().message.view();
    EXPECT_TRUE(std::filesystem::exists(path));

    ASSERT_TRUE(writer.close());
}

TEST_F(RecordingWriterTest, initFailsIfTheDirectoryIsNotADirectory)
{
    // `initThrowsIOExceptionIfItCannotOpenAFileChannel`.
    const std::string not_a_directory = archive_dir_ + "/dummy.txt";
    {
        std::ofstream{not_a_directory};
    }
    ASSERT_TRUE(std::filesystem::exists(not_a_directory));

    RecordingWriterContext ctx;
    ctx.archive_dir = not_a_directory;
    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, nullptr};

    auto initialized = writer.init();
    EXPECT_FALSE(initialized);
}

TEST_F(RecordingWriterTest, closeShouldCloseTheUnderlyingFile)
{
    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, context(), nullptr};
    ASSERT_TRUE(writer.init());
    ASSERT_TRUE(writer.close());

    const std::string path = segment_path(1, 0);
    ASSERT_TRUE(std::filesystem::exists(path));
    std::error_code error;
    EXPECT_TRUE(std::filesystem::remove(path, error)) << "the file must be closed";
}

TEST_F(RecordingWriterTest, onBlockFailsIfInitWasNotCalled)
{
    // Reference: `onBlockThrowsNullPointerExceptionIfInitWasNotCalled`.
    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, context(), nullptr};
    std::vector<std::uint8_t> term_buffer(32, 0);

    auto written = writer.on_block(term_buffer.data(), 0, 10, 5, 8);
    EXPECT_FALSE(written);
}

TEST_F(RecordingWriterTest, onBlockShouldWriteHeaderAndContentsOfTheNonPaddingFrame)
{
    ScriptedNanoTime clock{{3, 150, 200, 999}};
    MetricsRecorder metrics;
    RecordingWriterContext ctx = context();
    ctx.nano_clock = &clock;

    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, &metrics};
    ASSERT_TRUE(writer.init());

    std::vector<std::uint8_t> term_buffer(128, 0);
    set_frame_type(term_buffer, 0, deepmsg::archive::server::DATA_FRAME_TYPE);
    set_frame_length(term_buffer, 0, 128);
    std::memset(term_buffer.data() + DATA_HEADER_LENGTH, 7, 96);

    auto written = writer.on_block(term_buffer.data(), 0, 128, -1, -1);
    ASSERT_TRUE(written) << written.error().message.view();
    ASSERT_TRUE(writer.close());

    EXPECT_EQ(128, metrics.last_bytes_written);
    EXPECT_EQ(147, metrics.last_write_time_ns);

    const std::string path = segment_path(1, 0);
    ASSERT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(SEGMENT_LENGTH, std::filesystem::file_size(path));

    const std::vector<std::uint8_t> file = read_all(path);
    EXPECT_EQ(deepmsg::archive::server::DATA_FRAME_TYPE, get<std::int32_t>(file, 0 + 6) & 0xFFFF);
    EXPECT_EQ(128, get<std::int32_t>(file, 0));
    EXPECT_EQ(0, get<std::int32_t>(file, deepmsg::archive::server::SESSION_ID_OFFSET));
    for (std::size_t index = DATA_HEADER_LENGTH; index < 128; index++)
    {
        ASSERT_EQ(7, file[index]) << "index " << index;
    }
}

TEST_F(RecordingWriterTest, onBlockShouldWriteHeaderOfThePaddingFrameAndAdvanceFilePositionByThePaddingLength)
{
    constexpr std::int32_t segment_offset = 96;
    const std::int64_t start_position = 7LL * TERM_LENGTH + segment_offset;

    MonotonicNanoTime clock;
    RecordingWriterContext ctx = context();
    ctx.nano_clock = &clock;

    RecordingWriter writer{5, start_position, SEGMENT_LENGTH, TERM_LENGTH, start_position, ctx, nullptr};
    ASSERT_TRUE(writer.init());

    std::vector<std::uint8_t> term_buffer(1024, 0);
    set_frame_type(term_buffer, 0, deepmsg::archive::server::PADDING_FRAME_TYPE);
    set_frame_length(term_buffer, 0, 1024);
    set_frame_session_id(term_buffer, 0, 111);
    std::memset(term_buffer.data() + DATA_HEADER_LENGTH, 0xFF, 992);

    auto written = writer.on_block(term_buffer.data(), 0, 1024, -1, -1);
    ASSERT_TRUE(written) << written.error().message.view();
    ASSERT_TRUE(writer.close());

    const std::string path = segment_path(5, start_position);
    ASSERT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(SEGMENT_LENGTH, std::filesystem::file_size(path));

    const std::vector<std::uint8_t> file = read_all(path);
    for (std::size_t index = 0; index < static_cast<std::size_t>(segment_offset); index++)
    {
        ASSERT_EQ(0, file[index]) << "preamble index " << index;
    }
    EXPECT_EQ(deepmsg::archive::server::PADDING_FRAME_TYPE, get<std::int32_t>(file, segment_offset + 6) & 0xFFFF);
    EXPECT_EQ(1024, get<std::int32_t>(file, segment_offset));
    EXPECT_EQ(111, get<std::int32_t>(file, segment_offset + deepmsg::archive::server::SESSION_ID_OFFSET));
    for (std::size_t index = segment_offset + DATA_HEADER_LENGTH; index < segment_offset + 1024; index++)
    {
        ASSERT_EQ(0, file[index]) << "index " << index;
    }
}

TEST_F(RecordingWriterTest, onBlockShouldRollOverToTheNextSegmentFile)
{
    MonotonicNanoTime clock;
    RecordingWriterContext ctx = context();
    ctx.nano_clock = &clock;

    RecordingWriter writer{13, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, nullptr};
    ASSERT_TRUE(writer.init());

    std::vector<std::uint8_t> term_buffer(TERM_LENGTH, 0);
    set_frame_type(term_buffer, 0, deepmsg::archive::server::DATA_FRAME_TYPE);
    set_frame_length(term_buffer, 0, 1024);
    std::memset(term_buffer.data() + DATA_HEADER_LENGTH, 13, 992);

    for (std::int32_t index = 0; index < SEGMENT_LENGTH / 1024; index++)
    {
        auto written = writer.on_block(term_buffer.data(), 0, 1024, -1, -1);
        ASSERT_TRUE(written) << written.error().message.view();
    }

    set_frame_type(term_buffer, 0, deepmsg::archive::server::DATA_FRAME_TYPE);
    set_frame_length(term_buffer, 0, 192);
    std::memset(term_buffer.data() + DATA_HEADER_LENGTH, 22, 160);

    auto written = writer.on_block(term_buffer.data(), 0, 192, -1, -1);
    ASSERT_TRUE(written) << written.error().message.view();
    ASSERT_TRUE(writer.close());

    const std::string first = segment_path(13, 0);
    ASSERT_TRUE(std::filesystem::exists(first));
    EXPECT_EQ(SEGMENT_LENGTH, std::filesystem::file_size(first));
    const std::vector<std::uint8_t> first_file = read_all(first);
    EXPECT_EQ(deepmsg::archive::server::DATA_FRAME_TYPE, get<std::int32_t>(first_file, 6) & 0xFFFF);
    EXPECT_EQ(1024, get<std::int32_t>(first_file, 0));
    for (std::size_t index = DATA_HEADER_LENGTH; index < 1024; index++)
    {
        ASSERT_EQ(13, first_file[index]) << "index " << index;
    }

    const std::string second = segment_path(13, SEGMENT_LENGTH);
    ASSERT_TRUE(std::filesystem::exists(second));
    EXPECT_EQ(SEGMENT_LENGTH, std::filesystem::file_size(second));
    const std::vector<std::uint8_t> second_file = read_all(second);
    EXPECT_EQ(deepmsg::archive::server::DATA_FRAME_TYPE, get<std::int32_t>(second_file, 6) & 0xFFFF);
    EXPECT_EQ(192, get<std::int32_t>(second_file, 0));
    for (std::size_t index = DATA_HEADER_LENGTH; index < 192; index++)
    {
        ASSERT_EQ(22, second_file[index]) << "index " << index;
    }
}

TEST_F(RecordingWriterTest, onBlockShouldComputeCrcUsingTheChecksumBuffer)
{
    std::vector<std::uint8_t> checksum_buffer(512, 0);
    MonotonicNanoTime clock;
    RecordingWriterContext ctx = context();
    ctx.record_checksum = &crc32();
    ctx.record_checksum_buffer = checksum_buffer;
    ctx.nano_clock = &clock;

    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, nullptr};
    ASSERT_TRUE(writer.init());

    std::vector<std::uint8_t> term_buffer(512, 0);
    set_frame_type(term_buffer, 96, deepmsg::archive::server::DATA_FRAME_TYPE);
    set_frame_term_id(term_buffer, 96, 96);
    set_frame_length(term_buffer, 96, 64);
    set_frame_session_id(term_buffer, 96, 96);
    std::memset(term_buffer.data() + 96 + DATA_HEADER_LENGTH, 96, 32);

    set_frame_type(term_buffer, 160, deepmsg::archive::server::DATA_FRAME_TYPE);
    set_frame_term_id(term_buffer, 160, 160);
    set_frame_length(term_buffer, 160, 288);
    set_frame_session_id(term_buffer, 160, 160);
    std::memset(term_buffer.data() + 160 + DATA_HEADER_LENGTH, 160, 256);

    auto written = writer.on_block(term_buffer.data(), 96, 352, -1, -1);
    ASSERT_TRUE(written) << written.error().message.view();
    ASSERT_TRUE(writer.close());

    const std::vector<std::uint8_t> file = read_all(segment_path(1, 0));
    EXPECT_EQ(deepmsg::archive::server::DATA_FRAME_TYPE, get<std::int32_t>(file, 6) & 0xFFFF);
    EXPECT_EQ(96, get<std::int32_t>(file, deepmsg::archive::server::TERM_ID_OFFSET));
    EXPECT_EQ(64, get<std::int32_t>(file, 0));
    EXPECT_EQ(crc32().compute(term_buffer.data(), 96 + DATA_HEADER_LENGTH, 32),
              get<std::int32_t>(file, deepmsg::archive::server::SESSION_ID_OFFSET));

    EXPECT_EQ(deepmsg::archive::server::DATA_FRAME_TYPE, get<std::int32_t>(file, 64 + 6) & 0xFFFF);
    EXPECT_EQ(160, get<std::int32_t>(file, 64 + deepmsg::archive::server::TERM_ID_OFFSET));
    EXPECT_EQ(288, get<std::int32_t>(file, 64));
    EXPECT_EQ(crc32().compute(term_buffer.data(), 160 + DATA_HEADER_LENGTH, 256),
              get<std::int32_t>(file, 64 + deepmsg::archive::server::SESSION_ID_OFFSET));

    // The source buffer must not be modified.
    EXPECT_EQ(96, get<std::int32_t>(term_buffer, 96 + deepmsg::archive::server::SESSION_ID_OFFSET));
    EXPECT_EQ(160, get<std::int32_t>(term_buffer, 160 + deepmsg::archive::server::SESSION_ID_OFFSET));
}

TEST_F(RecordingWriterTest, onBlockShouldNotComputeCrcForThePaddingFrame)
{
    std::vector<std::uint8_t> checksum_buffer(512, 0);
    MonotonicNanoTime clock;
    RecordingWriterContext ctx = context();
    ctx.record_checksum = &crc32();
    ctx.record_checksum_buffer = checksum_buffer;
    ctx.nano_clock = &clock;

    RecordingWriter writer{1, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, nullptr};
    ASSERT_TRUE(writer.init());

    constexpr std::int32_t length = 128;
    constexpr std::int32_t session_id = 5;
    constexpr std::int32_t term_id = 18;

    std::vector<std::uint8_t> term_buffer(512, 0);
    set_frame_type(term_buffer, 0, deepmsg::archive::server::PADDING_FRAME_TYPE);
    set_frame_term_id(term_buffer, 0, term_id);
    set_frame_length(term_buffer, 0, length);
    set_frame_session_id(term_buffer, 0, session_id);
    std::memset(term_buffer.data() + DATA_HEADER_LENGTH, 99, length - DATA_HEADER_LENGTH);

    auto written = writer.on_block(term_buffer.data(), 0, DATA_HEADER_LENGTH, -1, -1);
    ASSERT_TRUE(written) << written.error().message.view();
    ASSERT_TRUE(writer.close());

    const std::vector<std::uint8_t> file = read_all(segment_path(1, 0));
    EXPECT_EQ(deepmsg::archive::server::PADDING_FRAME_TYPE, get<std::int32_t>(file, 6) & 0xFFFF);
    EXPECT_EQ(term_id, get<std::int32_t>(file, deepmsg::archive::server::TERM_ID_OFFSET));
    EXPECT_EQ(length, get<std::int32_t>(file, 0));
    EXPECT_EQ(session_id, get<std::int32_t>(file, deepmsg::archive::server::SESSION_ID_OFFSET));
}

}  // namespace
