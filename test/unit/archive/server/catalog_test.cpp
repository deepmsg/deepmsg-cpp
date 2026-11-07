// Reference: `aeron-archive/src/test/java/io/aeron/archive/CatalogTest.java`
// (Aeron 1.54.0, 454da1e4e6). Test names are the reference method names.
//
// Deviations from the reference test, recorded in `analysis/cpp/records/a1a.md`:
//   * `shouldCloseAndRemainReadableWhenTheClosingThreadIsInterrupted` and
//     `shouldCloseAfterAnInterruptClosedTheChannelDuringAnEarlierWrite` pin
//     Java's thread-interrupt-closes-a-channel behaviour, which has no C++
//     equivalent. The 1.54 retry they exercise (`fsyncInterruptSafe`) is covered
//     by `CatalogRetryForceTest` below.
//   * `new Random(seed)` in `getLastElementWhenIndexIsFull` is replaced by fixed
//     ids in the index test (same shape).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "archive/server/catalog.h"
#include "archive/server/catalog_index.h"

namespace
{

using axc::core::Error;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogEntry;
using deepmsg::archive::server::CatalogOpenOptions;
using deepmsg::archive::server::CatalogOptions;
using deepmsg::archive::server::crc32;
using deepmsg::archive::server::EpochClockSource;
using io::aeron::archive::codecs::CatalogHeader;
using io::aeron::archive::codecs::RecordingDescriptor;
using io::aeron::archive::codecs::RecordingDescriptorHeader;
using io::aeron::archive::codecs::RecordingState;

constexpr std::int64_t CAPACITY = 1024;
constexpr std::int32_t TERM_LENGTH = 2 * Catalog::PAGE_SIZE;
constexpr std::int32_t SEGMENT_LENGTH = 2 * TERM_LENGTH;
constexpr std::int32_t MTU_LENGTH = 1024;

class TestClock final : public EpochClockSource
{
public:
    std::int64_t now{1};

    [[nodiscard]] std::int64_t time_ms() const noexcept override
    {
        return now;
    }
};

[[nodiscard]] std::string make_temp_dir()
{
    std::array<char, 64> pattern{};
    std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-catalog-XXXXXX");
    const char* created = ::mkdtemp(pattern.data());
    return nullptr == created ? std::string{} : std::string{created};
}

/// `Archive.segmentFileName` (`Archive.java:4091`).
[[nodiscard]] std::string segment_file_name(std::int64_t recording_id, std::int64_t base_position)
{
    return std::to_string(recording_id) + "-" + std::to_string(base_position) + ".rec";
}

/// Writes a data frame header: frame length at offset 0, session id at 12
/// (`DataHeaderFlyweight.java:38`, `:88`).
void write_frame_header(int fd, std::int64_t position, std::int32_t frame_length, std::int32_t session_id)
{
    std::array<std::uint8_t, 32> header{};
    std::memcpy(header.data(), &frame_length, sizeof(frame_length));
    std::memcpy(header.data() + 12, &session_id, sizeof(session_id));
    EXPECT_EQ(static_cast<ssize_t>(header.size()), ::pwrite(fd, header.data(), header.size(), position));
}

void write_zero_byte(int fd, std::int64_t position)
{
    const std::uint8_t zero = 0;
    EXPECT_EQ(1, ::pwrite(fd, &zero, 1, position));
}

class CatalogTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        archive_dir_ = make_temp_dir();
        ASSERT_FALSE(archive_dir_.empty());
        add_three_recordings();
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    /// `CatalogTest.after()` + `ArchiveTests.makeTestDirectory()`: a fresh
    /// directory for the tests that need their own capacity.
    void reset_archive_dir()
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
        archive_dir_ = make_temp_dir();
        ASSERT_FALSE(archive_dir_.empty());
    }

    [[nodiscard]] std::expected<Catalog, Error> create_writer(
        const deepmsg::archive::server::Checksum* checksum = nullptr, std::int64_t capacity = CAPACITY,
        int file_sync_level = 0, std::span<std::uint8_t> recovery_buffer = {})
    {
        CatalogOptions options;
        options.archive_dir = archive_dir_;
        options.file_sync_level = file_sync_level;
        options.catalog_capacity = capacity;
        options.clock = &clock_;
        options.checksum = checksum;
        options.recovery_buffer = recovery_buffer;
        return Catalog::create(options);
    }

    [[nodiscard]] std::expected<Catalog, Error> open_catalog(
        bool writable = false, std::int64_t capacity = Catalog::MIN_CAPACITY,
        const deepmsg::archive::server::Checksum* checksum = nullptr,
        std::function<void(std::int32_t)> version_check = {})
    {
        CatalogOpenOptions options;
        options.archive_dir = archive_dir_;
        options.writable = writable;
        options.catalog_capacity = capacity;
        options.clock = &clock_;
        options.checksum = checksum;
        options.version_check = std::move(version_check);
        return Catalog::open(options);
    }

    void add_three_recordings()
    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        auto one = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                              "channelG?tag=f", "sourceA");
        ASSERT_TRUE(one) << one.error().message.view();
        recording_one_id_ = *one;

        auto two = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2, "channelH",
                                              "channelH?tag=f", "sourceV");
        ASSERT_TRUE(two) << two.error().message.view();
        recording_two_id_ = *two;

        auto three = catalog->add_new_recording(
            0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3, "channelThatIsVeryLongAndShouldNotBeTruncated",
            "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
            "source can also be a very very very long String and it will not be truncated even if gets very very long");
        ASSERT_TRUE(three) << three.error().message.view();
        recording_three_id_ = *three;

        auto closed = catalog->close();
        ASSERT_TRUE(closed) << closed.error().message.view();
    }

    /// `CatalogTest.newRecording` (`CatalogTest.java:622`).
    [[nodiscard]] std::int64_t new_recording()
    {
        auto catalog = create_writer();
        EXPECT_TRUE(catalog) << (catalog ? std::string{} : std::string{catalog.error().message.view()});
        auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                             "channelG?tag=f", "sourceA");
        EXPECT_TRUE(id) << (id ? std::string{} : std::string{id.error().message.view()});
        EXPECT_TRUE(catalog->close());
        return *id;
    }

    /// `CatalogTest.verifyRecordingForId` (`CatalogTest.java:1444`).
    void verify_recording_for_id(Catalog& catalog, std::int64_t id, std::int32_t length, std::int64_t start_position,
                                 std::int64_t stop_position, std::int64_t start_timestamp, std::int64_t stop_timestamp,
                                 std::int32_t initial_term_id, std::int32_t segment_file_length,
                                 std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id,
                                 std::int32_t stream_id, const std::string& stripped_channel,
                                 const std::string& original_channel, const std::string& source_identity)
    {
        auto frame = catalog.wrap_descriptor(id);
        ASSERT_FALSE(frame.empty());

        RecordingDescriptorHeader header;
        header.wrapForDecode(reinterpret_cast<char*>(frame.data()), 0, Catalog::DESCRIPTOR_HEADER_LENGTH,
                             RecordingDescriptorHeader::SBE_SCHEMA_VERSION, frame.size());
        EXPECT_EQ(RecordingState::VALID, header.state());
        EXPECT_EQ(length, header.length());

        RecordingDescriptor descriptor;
        descriptor.wrapForDecode(reinterpret_cast<char*>(frame.data()), Catalog::DESCRIPTOR_HEADER_LENGTH,
                                 RecordingDescriptor::SBE_BLOCK_LENGTH, RecordingDescriptor::SBE_SCHEMA_VERSION,
                                 frame.size());

        EXPECT_EQ(id, descriptor.recordingId());
        EXPECT_EQ(start_position, descriptor.startPosition());
        EXPECT_EQ(stop_position, descriptor.stopPosition());
        EXPECT_EQ(start_timestamp, descriptor.startTimestamp());
        EXPECT_EQ(stop_timestamp, descriptor.stopTimestamp());
        EXPECT_EQ(initial_term_id, descriptor.initialTermId());
        EXPECT_EQ(segment_file_length, descriptor.segmentFileLength());
        EXPECT_EQ(term_buffer_length, descriptor.termBufferLength());
        EXPECT_EQ(mtu_length, descriptor.mtuLength());
        EXPECT_EQ(session_id, descriptor.sessionId());
        EXPECT_EQ(stream_id, descriptor.streamId());
        EXPECT_EQ(stripped_channel, descriptor.getStrippedChannelAsString());
        EXPECT_EQ(original_channel, descriptor.getOriginalChannelAsString());
        EXPECT_EQ(source_identity, descriptor.getSourceIdentityAsString());
    }

    /// `CatalogTest.assertChecksum` (`CatalogTest.java:1400`).
    void assert_checksum(Catalog& catalog, std::int64_t id, std::int32_t aligned_checksum_length,
                         std::int32_t expected_checksum, const deepmsg::archive::server::Checksum* checksum)
    {
        const bool found =
            catalog.for_entry(id,
                              [&](CatalogEntry& entry)
                              {
                                  EXPECT_EQ(expected_checksum, entry.header.checksum());
                                  if (nullptr != checksum)
                                  {
                                      EXPECT_EQ(expected_checksum,
                                                checksum->compute(entry.frame.data(), Catalog::DESCRIPTOR_HEADER_LENGTH,
                                                                  aligned_checksum_length));
                                  }
                              });
        EXPECT_TRUE(found);
    }

    void test_change_state(std::int64_t recording_id, RecordingState::Value new_state)
    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog);
        const int entries = catalog->entry_count();
        const auto frame = catalog->wrap_descriptor(recording_id);
        ASSERT_FALSE(frame.empty());

        RecordingDescriptorHeader header;
        header.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(frame.data())), 0,
                             Catalog::DESCRIPTOR_HEADER_LENGTH, RecordingDescriptorHeader::SBE_SCHEMA_VERSION,
                             frame.size());

        auto changed = catalog->change_state(recording_id, new_state);
        ASSERT_TRUE(changed) << changed.error().message.view();
        EXPECT_TRUE(*changed);

        EXPECT_EQ(new_state, header.state());
        EXPECT_EQ(entries - 1, catalog->entry_count());
        EXPECT_FALSE(catalog->has_recording(recording_id));
    }

    void set_next_recording_id(std::int64_t next_recording_id)
    {
        const std::string path = archive_dir_ + "/archive.catalog";
        const int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        EXPECT_EQ(static_cast<ssize_t>(sizeof(next_recording_id)),
                  ::pwrite(fd, &next_recording_id, sizeof(next_recording_id),
                           static_cast<off_t>(CatalogHeader::nextRecordingIdEncodingOffset())));
        ::close(fd);
    }

    std::string archive_dir_;
    TestClock clock_;
    std::int64_t recording_one_id_{0};
    std::int64_t recording_two_id_{0};
    std::int64_t recording_three_id_{0};
};

TEST_F(CatalogTest, shouldUse1KBAlignmentWhenReadingFromOldCatalogFile)
{
    const std::int32_t old_record_length = 1024;

    const std::string catalog_file = archive_dir_ + "/archive.catalog";
    std::error_code error;
    std::filesystem::remove(catalog_file, error);
    {
        const int fd = ::open(catalog_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
        ASSERT_GE(fd, 0);
        std::vector<std::uint8_t> zeros(static_cast<std::size_t>(old_record_length), 0);
        ASSERT_EQ(static_cast<ssize_t>(zeros.size()), ::write(fd, zeros.data(), zeros.size()));
        ::close(fd);
    }

    auto catalog = open_catalog(true, Catalog::MIN_CAPACITY, nullptr, [](std::int32_t) {});
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(old_record_length, catalog->alignment());
}

TEST_F(CatalogTest, shouldComputeNextRecordingIdIfValueInHeaderIsZero)
{
    set_next_recording_id(0);

    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(recording_three_id_ + 1, catalog->next_recording_id());
}

TEST_F(CatalogTest, shouldThrowArchiveExceptionIfNextRecordingIdIsSmallerThanTheActualLastRecordInTheCatalog)
{
    set_next_recording_id(recording_two_id_);

    auto catalog = create_writer();
    ASSERT_FALSE(catalog);
    EXPECT_EQ("invalid nextRecordingId: expected value greater or equal to " + std::to_string(recording_three_id_ + 1) +
                  ", was " + std::to_string(recording_two_id_),
              catalog.error().message.view());
}

TEST_F(CatalogTest, shouldThrowArchiveExceptionIfNextRecordingIdIsInvalidWriteableCatalog)
{
    set_next_recording_id(-1);

    auto catalog = open_catalog(true, Catalog::MIN_CAPACITY);
    ASSERT_FALSE(catalog);
    EXPECT_EQ("invalid nextRecordingId: expected value greater or equal to " + std::to_string(recording_three_id_ + 1) +
                  ", was -1",
              catalog.error().message.view());
}

TEST_F(CatalogTest, shouldNotThrowArchiveExceptionWhenNextRecordingIdIsInvalidIfCatalogIsReadOnly)
{
    set_next_recording_id(recording_two_id_);

    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(recording_two_id_, catalog->next_recording_id());
}

TEST_F(CatalogTest, shouldReadNextRecordingIdFromCatalogHeader)
{
    constexpr std::int64_t next_recording_id = 10101010;
    set_next_recording_id(next_recording_id);

    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(next_recording_id, catalog->next_recording_id());
}

TEST_F(CatalogTest, shouldThrowIllegalArgumentExceptionIfCatalogCapacityIsLessThanMinimalCapacity)
{
    for (const std::int64_t capacity : {std::int64_t{-1}, std::int64_t{0}, Catalog::MIN_CAPACITY - 1})
    {
        auto catalog = create_writer(nullptr, capacity);
        ASSERT_FALSE(catalog);
        EXPECT_EQ("Invalid catalog capacity provided: expected value >= " + std::to_string(Catalog::MIN_CAPACITY) +
                      ", got " + std::to_string(capacity),
                  catalog.error().message.view());
    }
}

TEST_F(CatalogTest, shouldReloadExistingIndex)
{
    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    verify_recording_for_id(*catalog, recording_one_id_, 160, 0, Catalog::NULL_POSITION, 0, Catalog::NULL_TIMESTAMP, 0,
                            SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG", "channelG?tag=f", "sourceA");

    verify_recording_for_id(*catalog, recording_two_id_, 160, 0, Catalog::NULL_POSITION, 0, Catalog::NULL_TIMESTAMP, 0,
                            SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2, "channelH", "channelH?tag=f", "sourceV");

    verify_recording_for_id(
        *catalog, recording_three_id_, 352, 0, Catalog::NULL_POSITION, 0, Catalog::NULL_TIMESTAMP, 0, SEGMENT_LENGTH,
        TERM_LENGTH, MTU_LENGTH, 8, 3, "channelThatIsVeryLongAndShouldNotBeTruncated",
        "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
        "source can also be a very very very long String and it will not be truncated even if gets very very long");
}

TEST_F(CatalogTest, shouldAppendToExistingIndex)
{
    clock_.now = 3;

    std::int64_t new_recording_id = 0;
    {
        // The writer-only constructor refreshes and repairs, which fills in the
        // stop position/timestamp of the three existing entries.
        auto writer = create_writer();
        ASSERT_TRUE(writer) << writer.error().message.view();
        auto id = writer->add_new_recording(32, 128, 21, 42, 5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 4,
                                            "channelJ", "channelJ?tag=f", "sourceN");
        ASSERT_TRUE(id) << id.error().message.view();
        new_recording_id = *id;
        ASSERT_TRUE(writer->close());
    }

    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    verify_recording_for_id(*catalog, recording_one_id_, 160, 0,
                            0,  // updated from NULL_POSITION when the catalog was created for write
                            0,
                            3,  // updated from NULL_TIMESTAMP when the catalog was created for write
                            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG", "channelG?tag=f", "sourceA");

    verify_recording_for_id(*catalog, new_recording_id, 160, 32, 128, 21, 42, 5, SEGMENT_LENGTH, TERM_LENGTH,
                            MTU_LENGTH, 9, 4, "channelJ", "channelJ?tag=f", "sourceN");
}

TEST_F(CatalogTest, shouldAllowMultipleInstancesForSameStream)
{
    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(CAPACITY, catalog->capacity());

    const std::int64_t new_recording_id = new_recording();
    EXPECT_NE(recording_one_id_, new_recording_id);
}

TEST_F(CatalogTest, shouldIncreaseCapacity)
{
    const std::int64_t new_capacity = CAPACITY * 2;

    auto catalog = create_writer(nullptr, new_capacity);
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(new_capacity, catalog->capacity());
}

TEST_F(CatalogTest, shouldNotDecreaseCapacity)
{
    for (const std::int64_t new_capacity : {Catalog::MIN_CAPACITY, CAPACITY - 1, CAPACITY})
    {
        auto catalog = create_writer(nullptr, new_capacity);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        EXPECT_EQ(CAPACITY, catalog->capacity());
    }
}

TEST_F(CatalogTest, shouldFixTimestampForEmptyRecordingAfterFailure)
{
    const std::int64_t new_recording_id = new_recording();

    {
        auto catalog = open_catalog();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        bool checked = false;
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [&](CatalogEntry& entry)
                                       {
                                           checked = true;
                                           EXPECT_EQ(Catalog::NULL_TIMESTAMP, entry.descriptor.stopTimestamp());
                                       }));
        EXPECT_TRUE(checked);
    }

    clock_.now = 42;

    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        bool checked = false;
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [&](CatalogEntry& entry)
                                       {
                                           checked = true;
                                           EXPECT_EQ(42, entry.descriptor.stopTimestamp());
                                       }));
        EXPECT_TRUE(checked);
    }
}

TEST_F(CatalogTest, shouldFixTimestampAndPositionAfterFailureSamePage)
{
    const std::int64_t new_recording_id = new_recording();

    for (const std::int64_t base_position :
         {std::int64_t{0}, std::int64_t{SEGMENT_LENGTH}, 2 * std::int64_t{SEGMENT_LENGTH}})
    {
        const int fd = ::open((archive_dir_ + "/" + segment_file_name(new_recording_id, base_position)).c_str(),
                              O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        ::close(fd);
    }

    {
        const int fd =
            ::open((archive_dir_ + "/" + segment_file_name(new_recording_id, 3 * std::int64_t{SEGMENT_LENGTH})).c_str(),
                   O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, 1024, 0);
        write_frame_header(fd, 1024, 128, 0);
        write_frame_header(fd, 1024 + 128, 0, 0);
        ::close(fd);
    }

    {
        auto catalog = open_catalog();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [](CatalogEntry& entry)
                                       {
                                           EXPECT_EQ(Catalog::NULL_TIMESTAMP, entry.descriptor.stopTimestamp());
                                           EXPECT_EQ(Catalog::NULL_POSITION, entry.descriptor.stopPosition());
                                       }));
    }

    clock_.now = 42;

    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [](CatalogEntry& entry)
                                       {
                                           EXPECT_EQ(42, entry.descriptor.stopTimestamp());
                                           EXPECT_EQ(3 * std::int64_t{SEGMENT_LENGTH} + 1024 + 128,
                                                     entry.descriptor.stopPosition());
                                       }));
    }
}

TEST_F(CatalogTest, shouldThrowExceptionAfterFailureOnPageStraddle)
{
    const std::int64_t new_recording_id = new_recording();
    const std::string segment_file = archive_dir_ + "/" + segment_file_name(new_recording_id, 0);

    {
        const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, Catalog::PAGE_SIZE - 128, 0);
        write_frame_header(fd, Catalog::PAGE_SIZE - 128, 256, 0);
        write_zero_byte(fd, Catalog::PAGE_SIZE + 127);
        ::close(fd);
    }

    auto catalog = create_writer();
    ASSERT_FALSE(catalog);
    EXPECT_NE(std::string::npos,
              std::string{catalog.error().message.view()}.find(std::filesystem::absolute(segment_file).string()));
}

TEST_F(CatalogTest, shouldUseChecksumToVerifyLastFragmentAfterPageStraddle)
{
    const std::int64_t new_recording_id = new_recording();
    const std::string segment_file = archive_dir_ + "/" + segment_file_name(new_recording_id, 0);

    {
        const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, Catalog::PAGE_SIZE - 128, 0);
        write_frame_header(fd, Catalog::PAGE_SIZE - 128, 256, 1025596259);
        write_zero_byte(fd, Catalog::PAGE_SIZE + 127);
        ::close(fd);
    }

    clock_.now = 42;

    auto catalog = create_writer(&crc32());
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                   [](CatalogEntry& entry)
                                   {
                                       EXPECT_EQ(42, entry.descriptor.stopTimestamp());
                                       EXPECT_EQ(Catalog::PAGE_SIZE + 128, entry.descriptor.stopPosition());
                                   }));
}

TEST_F(CatalogTest, shouldVerifyLastFragmentWhenZeroLengthFrameIsReadIntoNextBuffer)
{
    const std::int64_t new_recording_id = new_recording();
    const std::string segment_file = archive_dir_ + "/" + segment_file_name(new_recording_id, 0);

    {
        const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, Catalog::PAGE_SIZE - 128, 0);
        write_frame_header(fd, Catalog::PAGE_SIZE - 128, 256, 1025596259);
        write_zero_byte(fd, SEGMENT_LENGTH - 1);
        ::close(fd);
    }

    clock_.now = 42;

    // Final fragment ends at the buffer limit, forcing the zero-length frame
    // into the next read.
    std::vector<std::uint8_t> recovery_buffer(static_cast<std::size_t>(Catalog::PAGE_SIZE + 128), 0);

    auto catalog = create_writer(&crc32(), CAPACITY, 0, std::span<std::uint8_t>(recovery_buffer));
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                   [](CatalogEntry& entry)
                                   {
                                       EXPECT_EQ(42, entry.descriptor.stopTimestamp());
                                       EXPECT_EQ(Catalog::PAGE_SIZE + 128, entry.descriptor.stopPosition());
                                   }));
}

TEST_F(CatalogTest, shouldNotComputeChecksumWhenFragmentExceedsRecoveryBuffer)
{
    const std::int64_t new_recording_id = new_recording();
    const std::string segment_file = archive_dir_ + "/" + segment_file_name(new_recording_id, 0);

    {
        const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, Catalog::PAGE_SIZE * 2, 0);
        write_zero_byte(fd, SEGMENT_LENGTH - 1);
        ::close(fd);
    }

    // A checksum that fires here would read outside the recovery buffer.
    class PoisonChecksum final : public deepmsg::archive::server::Checksum
    {
    public:
        [[nodiscard]] std::int32_t compute(const std::uint8_t*, std::int32_t, std::int32_t) const noexcept override
        {
            EXPECT_TRUE(false) << "checksum must not read outside the recovery buffer";
            return 0;
        }
    };
    const PoisonChecksum checksum;
    std::vector<std::uint8_t> recovery_buffer(static_cast<std::size_t>(Catalog::PAGE_SIZE), 0);

    auto catalog = create_writer(&checksum, CAPACITY, 0, std::span<std::uint8_t>(recovery_buffer));
    ASSERT_FALSE(catalog);
    EXPECT_NE(std::string::npos,
              std::string{catalog.error().message.view()}.find(std::filesystem::absolute(segment_file).string()));
}

TEST_F(CatalogTest, shouldFixTimestampAndPositionAfterFailureFullSegment)
{
    const std::int64_t new_recording_id = new_recording();
    const std::string segment_file = archive_dir_ + "/" + segment_file_name(new_recording_id, 0);

    {
        const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        write_frame_header(fd, 0, SEGMENT_LENGTH - 128, 0);
        write_frame_header(fd, SEGMENT_LENGTH - 128, 128, 0);
        EXPECT_EQ(0, ::ftruncate(fd, SEGMENT_LENGTH));
        ::close(fd);
    }

    {
        auto catalog = open_catalog();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [](CatalogEntry& entry)
                                       {
                                           EXPECT_EQ(Catalog::NULL_TIMESTAMP, entry.descriptor.stopTimestamp());
                                           EXPECT_EQ(Catalog::NULL_POSITION, entry.descriptor.stopPosition());
                                       }));
    }

    clock_.now = 42;

    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        EXPECT_TRUE(catalog->for_entry(new_recording_id,
                                       [](CatalogEntry& entry)
                                       {
                                           EXPECT_EQ(42, entry.descriptor.stopTimestamp());
                                           EXPECT_EQ(static_cast<std::int64_t>(SEGMENT_LENGTH),
                                                     entry.descriptor.stopPosition());
                                       }));
    }
}

TEST_F(CatalogTest, shouldNotGrowCatalogWhenReachingFullIfRecordingsFit)
{
    reset_archive_dir();
    const std::int64_t capacity = 384 + Catalog::MIN_CAPACITY;

    {
        auto catalog = create_writer(nullptr, capacity);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        for (int index = 0; index < 2; index++)
        {
            auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                                 "channelG?tag=f", "sourceA");
            ASSERT_TRUE(id) << id.error().message.view();
        }
    }

    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(2, catalog->entry_count());
    EXPECT_EQ(capacity, catalog->capacity());
}

TEST_F(CatalogTest, shouldGrowCatalogWhenMaxCapacityReached)
{
    reset_archive_dir();

    {
        auto catalog = create_writer(nullptr, Catalog::MIN_CAPACITY);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        for (int index = 0; index < 4; index++)
        {
            auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                                 "channelG?tag=f", "sourceA");
            ASSERT_TRUE(id) << id.error().message.view();
        }
    }

    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(4, catalog->entry_count());
    EXPECT_EQ(819, catalog->capacity());
}

TEST_F(CatalogTest, growCatalogThrowsArchiveExceptionIfCatalogIsFull)
{
    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto grown = catalog->grow_catalog(CAPACITY, static_cast<std::int32_t>(CAPACITY + 1));
    ASSERT_FALSE(grown);
    EXPECT_EQ("catalog is full, max capacity reached: " + std::to_string(CAPACITY), grown.error().message.view());
}

TEST_F(CatalogTest, growCatalogThrowsArchiveExceptionIfRecordingIsTooBig)
{
    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto grown = catalog->grow_catalog(CAPACITY * 2, std::numeric_limits<std::int32_t>::max());
    ASSERT_FALSE(grown);
    EXPECT_EQ("recording is too big: total recording length is " +
                  std::to_string(std::numeric_limits<std::int32_t>::max()) + " bytes, available space is " +
                  std::to_string((CAPACITY * 2) - 800) + " bytes",
              grown.error().message.view());
}

TEST_F(CatalogTest, growCatalogShouldNotExceedMaxCatalogCapacity)
{
    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    const std::int64_t max_catalog_capacity = CAPACITY * 1024;
    auto grown = catalog->grow_catalog(max_catalog_capacity, static_cast<std::int32_t>(max_catalog_capacity - 10000));
    ASSERT_TRUE(grown) << grown.error().message.view();
    EXPECT_EQ(max_catalog_capacity, catalog->capacity());
}

TEST_F(CatalogTest, shouldNotThrowWhenOldRecordingLogsAreDeleted)
{
    const std::string segment_file =
        archive_dir_ + "/" + segment_file_name(recording_three_id_, 2 * std::int64_t{SEGMENT_LENGTH});

    const int fd = ::open(segment_file.c_str(), O_RDWR | O_CREAT, 0644);
    ASSERT_GE(fd, 0);
    write_frame_header(fd, 0, 256, 0);
    ::close(fd);

    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_TRUE(catalog->close());
}

TEST_F(CatalogTest, shouldContainChannelFragment)
{
    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    const std::string original_channel = "aeron:udp?endpoint=localhost:7777|tags=777|alias=TestString";
    const std::string stripped_channel = "strippedChannelUri";
    auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, stripped_channel,
                                         original_channel, "sourceA");
    ASSERT_TRUE(id) << id.error().message.view();

    const auto frame = catalog->wrap_descriptor(*id);
    ASSERT_FALSE(frame.empty());

    const auto contains = [&](std::string_view fragment)
    {
        return Catalog::original_channel_contains(
            frame,
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(fragment.data()), fragment.size()));
    };

    EXPECT_TRUE(contains(""));
    EXPECT_TRUE(contains(original_channel));
    EXPECT_TRUE(contains("tags=777"));
    EXPECT_TRUE(contains("TestString"));
    EXPECT_FALSE(contains("wrong"));
}

TEST_F(CatalogTest, detectPageBoundaryStraddle)
{
    struct Case
    {
        std::int32_t fragment_offset;
        std::int32_t fragment_length;
        bool expected;
    };

    const Case cases[] = {
        {0, 64, false},
        {100, 300, false},
        {0, Catalog::PAGE_SIZE, false},
        {1, Catalog::PAGE_SIZE - 1, false},
        {Catalog::PAGE_SIZE - 1, 1, false},
        {Catalog::PAGE_SIZE * 7 + 32, 256, false},
        {Catalog::PAGE_SIZE * 3 + 111, Catalog::PAGE_SIZE - 111, false},
        {0, Catalog::PAGE_SIZE + 1, true},
        {Catalog::PAGE_SIZE - 1, 2, true},
        {Catalog::PAGE_SIZE * 4 + 11, Catalog::PAGE_SIZE - 10, true},
    };

    for (const Case& test_case : cases)
    {
        EXPECT_EQ(test_case.expected,
                  Catalog::fragment_straddles_page_boundary(test_case.fragment_offset, test_case.fragment_length))
            << "offset=" << test_case.fragment_offset << " length=" << test_case.fragment_length;
    }
}

TEST_F(CatalogTest, findLastReturnsNullRecordingIdIfMinRecordingIdIsOutOfRange)
{
    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    const std::string_view fragment = "channelG?tag=f";
    const auto bytes =
        std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(fragment.data()), fragment.size());

    for (const std::int64_t min_recording_id :
         {std::int64_t{-1}, std::int64_t{4}, std::numeric_limits<std::int64_t>::max()})
    {
        EXPECT_EQ(Catalog::NULL_RECORD_ID, catalog->find_last(min_recording_id, 6, 1, bytes));
    }
}

TEST_F(CatalogTest, findLastReturnsLastFoundRecordingMatchingGivenCriteria)
{
    const std::int32_t session_id = 6;
    const std::int32_t stream_id = 1;

    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto recording_f = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, session_id,
                                                  stream_id, "F", "channelG?tag=f", "sourceA");
    ASSERT_TRUE(recording_f) << recording_f.error().message.view();

    EXPECT_TRUE(catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, session_id, stream_id, "X",
                                           "channelG?tag=x", "sourceA"));
    EXPECT_TRUE(catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, session_id, stream_id + 1,
                                           "F", "channelG?tag=f", "sourceA"));
    EXPECT_TRUE(catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, session_id + 1, stream_id,
                                           "F", "channelG?tag=f", "sourceA"));

    const std::string_view fragment = "channelG?tag=f";
    const auto bytes =
        std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(fragment.data()), fragment.size());

    EXPECT_EQ(*recording_f, catalog->find_last(recording_one_id_, session_id, stream_id, bytes));
}

TEST_F(CatalogTest, findLastReturnsNullRecordingIdIfRecordingIsInTheInvalidState)
{
    auto catalog = create_writer();
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto changed = catalog->change_state(recording_one_id_, RecordingState::INVALID);
    ASSERT_TRUE(changed) << changed.error().message.view();
    EXPECT_TRUE(*changed);

    const std::string_view fragment = "channelG?tag=f";
    const auto bytes =
        std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(fragment.data()), fragment.size());
    EXPECT_EQ(Catalog::NULL_RECORD_ID, catalog->find_last(0, 6, 1, bytes));
}

TEST_F(CatalogTest, changeStateIsANoOpIfUnknownRecordingStateIdIsSpecified)
{
    for (const std::int64_t recording_id : {std::int64_t{-1}, std::numeric_limits<std::int64_t>::max()})
    {
        auto catalog = create_writer();
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        auto changed = catalog->change_state(recording_id, RecordingState::NULL_VALUE);
        ASSERT_TRUE(changed) << changed.error().message.view();
        EXPECT_FALSE(*changed);
    }
}

TEST_F(CatalogTest, changeStateOfTheIndexFirstEntry)
{
    test_change_state(recording_one_id_, RecordingState::DELETED);
}

TEST_F(CatalogTest, changeStateOfTheIndexMiddleEntry)
{
    test_change_state(recording_two_id_, RecordingState::NULL_VALUE);
}

TEST_F(CatalogTest, changeStateOfTheIndexLastEntry)
{
    test_change_state(recording_three_id_, RecordingState::INVALID);
}

TEST_F(CatalogTest, shouldComputeChecksumOfTheRecordingDescriptorUponAddingToTheCatalog)
{
    auto catalog = create_writer(&crc32());
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto recording_id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelNew",
                                                   "channelNew?tag=X", "sourceX");
    ASSERT_TRUE(recording_id) << recording_id.error().message.view();

    auto recording_id2 = catalog->add_new_recording(1, 100, 2, 222, 111, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 16,
                                                    12, "channelNew2", "channelNew?tag=X2", "sourceX2");
    ASSERT_TRUE(recording_id2) << recording_id2.error().message.view();

    catalog->for_each(
        [&](CatalogEntry& entry)
        {
            if (*recording_id == entry.descriptor.recordingId())
            {
                EXPECT_EQ(1691549102, entry.header.checksum());
            }
            else if (*recording_id2 == entry.descriptor.recordingId())
            {
                EXPECT_EQ(1452384985, entry.header.checksum());
            }
            else
            {
                EXPECT_EQ(0, entry.header.checksum());
            }
        });
}

TEST_F(CatalogTest, recordingStoppedShouldUpdateChecksum)
{
    const auto* checksum = &crc32();
    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    assert_checksum(*catalog, recording_one_id_, 160, 0, nullptr);

    ASSERT_TRUE(catalog->recording_stopped(recording_one_id_, 140, 231723682323LL));

    assert_checksum(*catalog, recording_one_id_, 160, 1656993099, checksum);
}

TEST_F(CatalogTest, stopPositionShouldUpdateChecksum)
{
    const auto* checksum = &crc32();
    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    assert_checksum(*catalog, recording_two_id_, 160, 0, nullptr);

    ASSERT_TRUE(catalog->stop_position(recording_two_id_, 7777));

    assert_checksum(*catalog, recording_two_id_, 160, -1985007076, checksum);
}

TEST_F(CatalogTest, startPositionShouldUpdateChecksum)
{
    const auto* checksum = &crc32();
    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    assert_checksum(*catalog, recording_three_id_, 352, 0, nullptr);

    ASSERT_TRUE(catalog->start_position(recording_three_id_, 123));

    assert_checksum(*catalog, recording_three_id_, 352, -160510802, checksum);
}

TEST_F(CatalogTest, extendRecordingShouldUpdateChecksum)
{
    const auto* checksum = &crc32();
    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto recording_id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelNew",
                                                   "channelNew?tag=X", "sourceX");
    ASSERT_TRUE(recording_id) << recording_id.error().message.view();
    assert_checksum(*catalog, *recording_id, 160, 1691549102, checksum);

    ASSERT_TRUE(catalog->extend_recording(*recording_id, 555, 13, 31));

    assert_checksum(*catalog, *recording_id, 160, -1694749833, checksum);
}

TEST_F(CatalogTest, replaceThrowsArchiveExceptionIfRecordingIdIsUnknown)
{
    const auto* checksum = &crc32();
    {
        auto catalog = create_writer(checksum);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        ASSERT_TRUE(catalog->start_position(recording_one_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_two_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_three_id_, 0));
        assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);
        assert_checksum(*catalog, recording_two_id_, 160, -1947831311, checksum);
        assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
    }

    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    constexpr std::int64_t unknown_recording_id = 1000000;
    auto replaced = catalog->replace_recording(unknown_recording_id, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, "11", "12", "13");
    ASSERT_FALSE(replaced);
    EXPECT_EQ("unknown recording id: " + std::to_string(unknown_recording_id), replaced.error().message.view());

    EXPECT_EQ(1024, catalog->capacity());
    EXPECT_EQ(3, catalog->index().size());
    EXPECT_EQ(recording_three_id_ + 1, catalog->next_recording_id());
    assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);
    assert_checksum(*catalog, recording_two_id_, 160, -1947831311, checksum);
    assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
}

TEST_F(CatalogTest, replaceRecordingUpdateCatalogFileWhenNewMetadataDoesNotFit)
{
    const auto* checksum = &crc32();
    {
        auto catalog = create_writer(checksum);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        ASSERT_TRUE(catalog->start_position(recording_one_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_two_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_three_id_, 0));
        assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);
        assert_checksum(*catalog, recording_two_id_, 160, -1947831311, checksum);
        assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
    }

    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    const std::int64_t old_capacity = catalog->capacity();

    std::string new_source_identity =
        "and the source identity changes as well and is also quite a long one funny thing: ";
    for (int index = 0; index < 2000; index++)
    {
        new_source_identity.push_back('!');
    }

    auto replaced = catalog->replace_recording(
        recording_two_id_, 1024, 16 * 1024, 3252535612LL, 6238423648LL, 777, SEGMENT_LENGTH * 4, TERM_LENGTH * 2, 1344,
        -19, 42,
        "suppose to be a short description of the channel but can be whatever and surprising. Veni, vidi, vici",
        "aeron:ipc?tag=15|alias=that is very very very long and will overflow the originally assigned length for sure "
        "and then some",
        new_source_identity);
    ASSERT_TRUE(replaced) << replaced.error().message.view();

    EXPECT_GT(catalog->capacity(), old_capacity);
    EXPECT_EQ(recording_three_id_ + 1, catalog->next_recording_id());
    verify_recording_for_id(*catalog, recording_one_id_, 160, 0, 0, 0, 1, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6,
                            1, "channelG", "channelG?tag=f", "sourceA");
    assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);

    verify_recording_for_id(
        *catalog, recording_two_id_, 2400, 1024, 16 * 1024, 3252535612LL, 6238423648LL, 777, SEGMENT_LENGTH * 4,
        TERM_LENGTH * 2, 1344, -19, 42,
        "suppose to be a short description of the channel but can be whatever and surprising. Veni, vidi, vici",
        "aeron:ipc?tag=15|alias=that is very very very long and will overflow the originally assigned length for sure "
        "and then some",
        new_source_identity);
    assert_checksum(*catalog, recording_two_id_, 2400, -2076878182, checksum);

    verify_recording_for_id(
        *catalog, recording_three_id_, 352, 0, 0, 0, 1, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3,
        "channelThatIsVeryLongAndShouldNotBeTruncated", "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
        "source can also be a very very very long String and it will not be truncated even if gets very very long");
    assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
}

TEST_F(CatalogTest, replaceRecordingUpdateCatalogFileWhenNewMetadataDoesNotFitAndDifferenceInSizeIsSmall)
{
    const auto* checksum = &crc32();
    {
        auto catalog = create_writer(checksum);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        ASSERT_TRUE(catalog->start_position(recording_one_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_two_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_three_id_, 0));
        assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);
        assert_checksum(*catalog, recording_two_id_, 160, -1947831311, checksum);
        assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
    }

    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    auto replaced = catalog->replace_recording(recording_two_id_, 1024, 16 * 1024, 3252535612LL, 6238423648LL, 777,
                                               SEGMENT_LENGTH * 4, TERM_LENGTH * 2, 1344, -19, 42, "channelH",
                                               "channelH?tag=f", "to source or not to source that is the question");
    ASSERT_TRUE(replaced) << replaced.error().message.view();

    EXPECT_EQ(recording_three_id_ + 1, catalog->next_recording_id());
    verify_recording_for_id(*catalog, recording_one_id_, 160, 0, 0, 0, 1, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6,
                            1, "channelG", "channelG?tag=f", "sourceA");
    assert_checksum(*catalog, recording_one_id_, 160, -866186973, checksum);

    verify_recording_for_id(*catalog, recording_two_id_, 224, 1024, 16 * 1024, 3252535612LL, 6238423648LL, 777,
                            SEGMENT_LENGTH * 4, TERM_LENGTH * 2, 1344, -19, 42, "channelH", "channelH?tag=f",
                            "to source or not to source that is the question");
    assert_checksum(*catalog, recording_two_id_, 224, -1059517110, checksum);

    verify_recording_for_id(
        *catalog, recording_three_id_, 352, 0, 0, 0, 1, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3,
        "channelThatIsVeryLongAndShouldNotBeTruncated", "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
        "source can also be a very very very long String and it will not be truncated even if gets very very long");
    assert_checksum(*catalog, recording_three_id_, 352, -529628341, checksum);
}

TEST_F(CatalogTest, replaceRecordingShouldUpdateMetadataInPlaceWhenShorterThanTheOldOne)
{
    constexpr std::int64_t update_time = 555555;
    const auto* checksum = &crc32();

    {
        TestClock update_clock;
        update_clock.now = update_time;

        CatalogOptions options;
        options.archive_dir = archive_dir_;
        options.catalog_capacity = CAPACITY;
        options.clock = &update_clock;
        options.checksum = checksum;
        auto catalog = Catalog::create(options);
        ASSERT_TRUE(catalog) << catalog.error().message.view();

        ASSERT_TRUE(catalog->start_position(recording_one_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_two_id_, 0));
        ASSERT_TRUE(catalog->start_position(recording_three_id_, 0));
        assert_checksum(*catalog, recording_one_id_, 160, 729050496, checksum);
        assert_checksum(*catalog, recording_two_id_, 160, 1825380178, checksum);
        assert_checksum(*catalog, recording_three_id_, 352, -1553583482, checksum);
    }

    auto catalog = create_writer(checksum);
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    const std::int64_t original_capacity = catalog->capacity();

    ASSERT_TRUE(catalog->replace_recording(recording_one_id_, 128, 512, 111, 222, -19091, SEGMENT_LENGTH * 16,
                                           TERM_LENGTH * 4, 1372, 21, 8, "A", "B", "C"));

    EXPECT_EQ(recording_three_id_ + 1, catalog->next_recording_id());
    EXPECT_EQ(original_capacity, catalog->capacity());

    constexpr std::int32_t old_frame_length = 160;
    verify_recording_for_id(*catalog, recording_one_id_, old_frame_length, 128, 512, 111, 222, -19091,
                            SEGMENT_LENGTH * 16, TERM_LENGTH * 4, 1372, 21, 8, "A", "B", "C");

    constexpr std::int32_t new_frame_length = 96;
    const auto frame = catalog->wrap_descriptor(recording_one_id_);
    ASSERT_FALSE(frame.empty());
    ASSERT_GT(old_frame_length, new_frame_length);
    for (std::int32_t index = new_frame_length; index < old_frame_length; index++)
    {
        EXPECT_EQ(0, frame[static_cast<std::size_t>(Catalog::DESCRIPTOR_HEADER_LENGTH + index)]);
    }
    assert_checksum(*catalog, recording_one_id_, new_frame_length, 1488471744, checksum);

    verify_recording_for_id(*catalog, recording_two_id_, 160, 0, 0, 0, update_time, 0, SEGMENT_LENGTH, TERM_LENGTH,
                            MTU_LENGTH, 7, 2, "channelH", "channelH?tag=f", "sourceV");
    assert_checksum(*catalog, recording_two_id_, 160, 1825380178, checksum);

    verify_recording_for_id(
        *catalog, recording_three_id_, 352, 0, 0, 0, update_time, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3,
        "channelThatIsVeryLongAndShouldNotBeTruncated", "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
        "source can also be a very very very long String and it will not be truncated even if gets very very long");
    assert_checksum(*catalog, recording_three_id_, 352, -1553583482, checksum);
}

// ---------------------------------------------------------------------------
// Criteria that the reference test does not pin (1.54 `076bd5f8b4`).

TEST_F(CatalogTest, levelZeroForcesNothingUntilItCloses)
{
    auto catalog = create_writer(nullptr, CAPACITY, 0);
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(0u, catalog->force_count());

    ASSERT_TRUE(catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                           "channelG?tag=f", "sourceA"));
    ASSERT_TRUE(catalog->recording_stopped(recording_one_id_, 128, 42));
    EXPECT_EQ(0u, catalog->force_count());

    ASSERT_TRUE(catalog->close());
    // The closing force uses both arguments as literals, so it happens at every
    // level (`Catalog.java:1257`, `:1296`).
    EXPECT_EQ(1u, catalog->force_count());
}

TEST_F(CatalogTest, everyChangeThatLandsIsForced)
{
    auto catalog = create_writer(&crc32(), CAPACITY, 2);
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(0u, catalog->force_count());

    const auto step = [&](const char* what, auto&& action)
    {
        const std::uint64_t before = catalog->force_count();
        ASSERT_TRUE(action()) << what;
        EXPECT_EQ(before + 1, catalog->force_count()) << what;
    };

    std::int64_t added_id = 0;
    {
        const std::uint64_t before = catalog->force_count();
        auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                             "channelG?tag=f", "sourceA");
        ASSERT_TRUE(id) << id.error().message.view();
        added_id = *id;
        EXPECT_EQ(before + 1, catalog->force_count()) << "addNewRecording";
    }

    step("recordingStopped", [&] { return catalog->recording_stopped(added_id, 128, 42); });
    step("stopPosition", [&] { return catalog->stop_position(added_id, 256); });
    step("startPosition", [&] { return catalog->start_position(added_id, 512); });
    step("extendRecording", [&] { return catalog->extend_recording(added_id, 1, 2, 3); });
    step("replaceRecording",
         [&] { return catalog->replace_recording(added_id, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, "a", "b", "c"); });
    step("changeState",
         [&]
         {
             auto changed = catalog->change_state(added_id, RecordingState::INVALID);
             return std::expected<void, Error>(changed ? std::expected<void, Error>{}
                                                       : std::unexpected(changed.error()));
         });
}

TEST_F(CatalogTest, aChangeThatMatchedNothingIsNotForced)
{
    auto catalog = create_writer(nullptr, CAPACITY, 2);
    ASSERT_TRUE(catalog) << catalog.error().message.view();

    const std::uint64_t before = catalog->force_count();
    auto changed = catalog->change_state(1'000'000, RecordingState::INVALID);
    ASSERT_TRUE(changed) << changed.error().message.view();
    EXPECT_FALSE(*changed);
    EXPECT_EQ(before, catalog->force_count());
}

TEST_F(CatalogTest, creatingTheCatalogForcesTheArchiveDirectoryAtLevelAboveZero)
{
    reset_archive_dir();

    auto level_zero = create_writer(nullptr, CAPACITY, 0);
    ASSERT_TRUE(level_zero) << level_zero.error().message.view();
    EXPECT_EQ(0u, level_zero->force_count());
    ASSERT_TRUE(level_zero->close());

    std::error_code error;
    std::filesystem::remove_all(archive_dir_, error);
    archive_dir_ = make_temp_dir();
    ASSERT_FALSE(archive_dir_.empty());

    const int directory_fd = ::open(archive_dir_.c_str(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(directory_fd, 0);

    CatalogOptions options;
    options.archive_dir = archive_dir_;
    options.archive_dir_fd = directory_fd;
    options.file_sync_level = 2;
    options.catalog_capacity = CAPACITY;
    options.clock = &clock_;
    auto level_two = Catalog::create(options);
    ASSERT_TRUE(level_two) << level_two.error().message.view();
    EXPECT_EQ(1u, level_two->force_count()) << "the new catalog forces the archive directory once";

    ASSERT_TRUE(level_two->close());
    ::close(directory_fd);
}

TEST(CatalogRetryForceTest, anInterruptedForceReopensTheChannelAndRetries)
{
    int attempts = 0;
    int reopened = 0;
    int fd = 7;

    const auto result = deepmsg::archive::server::detail::retry_force(
        [&]
        {
            attempts++;
            return attempts < 3 ? EINTR : 0;
        },
        [&]
        {
            reopened++;
            fd = 8;
            return fd;
        },
        Catalog::MAX_CLOSE_FSYNC_ATTEMPTS);

    EXPECT_TRUE(result) << result.error().message.view();
    EXPECT_EQ(3, attempts);
    EXPECT_EQ(2, reopened);
}

TEST(CatalogRetryForceTest, aNonInterruptedFailureIsNotRetried)
{
    int attempts = 0;

    const auto result = deepmsg::archive::server::detail::retry_force(
        [&]
        {
            attempts++;
            return EIO;
        },
        []
        {
            EXPECT_TRUE(false) << "reopen must not be called for a non-EINTR failure";
            return -1;
        },
        Catalog::MAX_CLOSE_FSYNC_ATTEMPTS);

    EXPECT_FALSE(result);
    EXPECT_EQ(1, attempts);
}

TEST(CatalogRetryForceTest, theRetryGivesUpAfterMaxAttempts)
{
    int attempts = 0;

    const auto result = deepmsg::archive::server::detail::retry_force(
        [&]
        {
            attempts++;
            return EINTR;
        },
        [] { return 3; }, Catalog::MAX_CLOSE_FSYNC_ATTEMPTS);

    EXPECT_FALSE(result);
    EXPECT_EQ(Catalog::MAX_CLOSE_FSYNC_ATTEMPTS, attempts);
}

}  // namespace
