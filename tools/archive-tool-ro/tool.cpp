// T1: the read-only subset of the reference `io.aeron.archive.ArchiveTool`.
//
// Every command here is a transcription of the reference method named beside
// it; the output text is the contract (the interop test runs the reference tool
// on the same archive directory and compares). Commands that are not part of
// the read-only subset are refused by the reference's own fallback
// (`ERR: Invalid command`), and the write commands (`compact`, `migrate`,
// `mark-*`, `delete-orphaned-segments`) are deliberately absent (Q7).

#include "tool.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "archive/server/archive_mark_file.h"
#include "archive/server/catalog.h"
#include "archive/server/checksum.h"
#include "archive/server/frame_layout.h"
#include "archive/server/recording_reader.h"
#include "archive/server/segment_files.h"
#include "core/io/mapped_file.h"
#include "io_aeron_archive_codecs/RecordingDescriptor.h"
#include "io_aeron_archive_codecs/RecordingState.h"
#include "io_aeron_archive_codecs_mark/MarkFileHeader.h"
#include "io_aeron_archive_codecs_mark/MessageHeader.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace deepmsg::tools
{
namespace
{

using axc::core::Error;
using deepmsg::archive::server::ArchiveMarkFile;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogEntry;
using deepmsg::archive::server::CatalogOpenOptions;
using deepmsg::archive::server::Checksum;
using deepmsg::archive::server::FragmentHandler;
using deepmsg::archive::server::RecordingReader;

namespace archive_codecs = ::io::aeron::archive::codecs;
namespace archive_mark_codecs = ::io::aeron::archive::codecs::mark;

/// `ArchiveTool.printHelp` (`ArchiveTool.java:1753-1787`), byte for byte. The
/// text still lists the write commands the reference has, because the help is
/// part of the tool's output contract even when this binary refuses them.
constexpr std::string_view HELP_TEXT =
    "Usage: <archive-dir> <command> (items in square brackets are optional)\n\n"
    "  capacity [capacity in bytes]: gets or increases catalog capacity.\n\n"
    "  checksum className [recordingId] [-a]: computes and persists checksums.\n"
    "     checksums are computed using the specified Checksum implementation\n"
    "     (e.g. io.aeron.archive.checksum.Crc32).\n"
    "     Only the last segment file of each recording is processed by default,\n"
    "     unless flag '-a' is specified in which case all of the segment files are processed.\n\n"
    "  compact: compacts Catalog file by removing entries in non-valid state and deleting the\n"
    "     corresponding segment files.\n\n"
    "  count-entries: queries the number of `VALID` recording entries in the catalog.\n\n"
    "  delete-orphaned-segments [recordingId]: deletes orphaned recording segments that have been detached,\n"
    "     If recordingId is specified, only delete orphaned segments for that recording.\n"
    "     i.e. outside the start and stop recording range, but are not deleted.\n\n"
    "  describe: prints out descriptors for all valid recordings in the catalog.\n\n"
    "  describe recordingId: prints out descriptor for the specified recording entry in the catalog.\n\n"
    "  describe-all: prints out descriptors for all recordings in the catalog.\n\n"
    "  dump [data fragment limit per recording]: prints descriptor(s)\n"
    "     in the catalog and associated recorded data.\n\n"
    "  errors: prints errors for the archive and media driver.\n\n"
    "  max-entries [number of entries]: *** DEPRECATED: use `capacity` instead. ***\n\n"
    "  migrate: migrates archive MarkFile, Catalog, and recordings to the latest version.\n\n"
    "  pid: prints just PID of archive.\n\n"
    "  verify [recordingId] [-a] [-checksum className]: verifies descriptor(s) in the catalog\n"
    "     checking recording files availability and contents. Only the last segment file is\n"
    "     verified unless flag '-a' is specified, i.e. meaning verify all segment files.\n"
    "     To perform checksum for each data frame specify the '-checksum' flag together with\n"
    "     the Checksum implementation class name (e.g. io.aeron.archive.checksum.Crc32).\n"
    "     Faulty entries are marked as `INVALID`.\n\n"
    "  mark-invalid <recordingId>: marks a recording as invalid. \n\n"
    "  mark-valid <recordingId>: marks a previously invalidated recording as valid. \n\n";

/// `ArchiveMarkFile.LINK_FILENAME`: the ASCII path of the directory holding the
/// mark file, written next to the archive directory when the two differ.
constexpr const char* MARK_FILE_LINK_NAME = "archive-mark.lnk";

void print_help()
{
    std::fwrite(HELP_TEXT.data(), 1, HELP_TEXT.size(), stdout);
    std::fflush(stdout);
}

void print_error(const Error& error)
{
    std::fprintf(stderr, "ERR: %.*s\n", static_cast<int>(error.message.view().size()), error.message.view().data());
}

std::string trim(std::string value)
{
    const auto first = value.find_first_not_of(" \t\r\n");
    if (std::string::npos == first)
    {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

/// `ArchiveTool.resolveMarkFileDir` (`ArchiveTool.java:1151-1174`): `archive-mark.lnk`
/// names the directory that holds `archive-mark.dat`; without it the mark file
/// lives in the archive directory.
std::filesystem::path resolve_mark_file_dir(const std::filesystem::path& archive_dir, std::string& error)
{
    const std::filesystem::path link_file = archive_dir / MARK_FILE_LINK_NAME;
    std::error_code exists_error;
    if (!std::filesystem::exists(link_file, exists_error))
    {
        return archive_dir;
    }

    std::ifstream input(link_file, std::ios::binary);
    if (!input)
    {
        error = "failed to read link file=" + link_file.string();
        return {};
    }
    std::string contents((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    return std::filesystem::path(trim(std::move(contents)));
}

/// `ArchiveTool.openMarkFile` (`ArchiveTool.java:1134-1138`) with the reference's
/// 5 s liveness wait not needed for a read-only inspection: the file is opened
/// through `ArchiveMarkFile::open_existing`, which validates the major version.
std::expected<ArchiveMarkFile, Error> open_mark_file(const std::filesystem::path& archive_dir, std::string& error,
                                                     bool log_exists)
{
    const std::filesystem::path mark_dir = resolve_mark_file_dir(archive_dir, error);
    if (mark_dir.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, error));
    }

    ArchiveMarkFile::Options options;
    options.directory = mark_dir.string();
    // `refresh_header` replaces this with the header's `errorBufferLength`.
    options.error_buffer_length = 0;
    auto mark_file = ArchiveMarkFile::open_existing(options);
    if (mark_file && log_exists)
    {
        // Agrona `MarkFile`'s logger (`org.agrona.MarkFile`, the constructor the
        // reference passes `out::println` to): describe/describe-all/dump print
        // this before anything else; pid/errors pass a null logger.
        const std::filesystem::path mark_path = mark_dir / ArchiveMarkFile::FILENAME;
        std::printf("INFO: mark file exists: %s\n", mark_path.string().c_str());
    }
    return mark_file;
}

std::expected<Catalog, Error> open_catalog_read_only(const std::filesystem::path& archive_dir)
{
    CatalogOpenOptions options;
    options.archive_dir = archive_dir.string();
    options.catalog_capacity = Catalog::MIN_CAPACITY;
    options.writable = false;
    return Catalog::open(options);
}

/// `RecordingDescriptorDecoder.appendTo` (`RecordingDescriptorDecoder.java`, the
/// SBE-generated `toString`): the field order, the `|` separator and the
/// acting/current version and block-length forms are the output contract.
std::string descriptor_to_string(archive_codecs::RecordingDescriptor& descriptor)
{
    descriptor.sbeRewind();
    std::string out = "[RecordingDescriptor](sbeTemplateId=22|sbeSchemaId=101|sbeSchemaVersion=";
    if (descriptor.actingVersion() != archive_codecs::RecordingDescriptor::SBE_SCHEMA_VERSION)
    {
        out += std::to_string(descriptor.actingVersion());
        out += '/';
    }
    out += "14|sbeBlockLength=80):";
    out += "controlSessionId=" + std::to_string(descriptor.controlSessionId());
    out += "|correlationId=" + std::to_string(descriptor.correlationId());
    out += "|recordingId=" + std::to_string(descriptor.recordingId());
    out += "|startTimestamp=" + std::to_string(descriptor.startTimestamp());
    out += "|stopTimestamp=" + std::to_string(descriptor.stopTimestamp());
    out += "|startPosition=" + std::to_string(descriptor.startPosition());
    out += "|stopPosition=" + std::to_string(descriptor.stopPosition());
    out += "|initialTermId=" + std::to_string(descriptor.initialTermId());
    out += "|segmentFileLength=" + std::to_string(descriptor.segmentFileLength());
    out += "|termBufferLength=" + std::to_string(descriptor.termBufferLength());
    out += "|mtuLength=" + std::to_string(descriptor.mtuLength());
    out += "|sessionId=" + std::to_string(descriptor.sessionId());
    out += "|streamId=" + std::to_string(descriptor.streamId());
    out += "|strippedChannel='" + descriptor.getStrippedChannelAsString() + '\'';
    out += "|originalChannel='" + descriptor.getOriginalChannelAsString() + '\'';
    out += "|sourceIdentity='" + descriptor.getSourceIdentityAsString() + '\'';
    return out;
}

/// `MarkFileHeaderDecoder.appendTo` (the SBE-generated `toString` used by
/// `markFile.decoder().toString()`).
std::string mark_header_to_string(ArchiveMarkFile& mark_file)
{
    const std::span<std::uint8_t> bytes = mark_file.buffer();
    auto* buffer = reinterpret_cast<char*>(bytes.data());

    archive_mark_codecs::MessageHeader message_header;
    message_header.wrap(buffer, 0, archive_mark_codecs::MessageHeader::sbeSchemaVersion(), bytes.size());

    archive_mark_codecs::MarkFileHeader header;
    header.wrapForDecode(buffer, static_cast<std::uint64_t>(mark_file.header_offset()),
                         archive_mark_codecs::MarkFileHeader::SBE_BLOCK_LENGTH, message_header.version(), bytes.size());

    header.sbeRewind();
    std::string out = "[MarkFileHeader](sbeTemplateId=200|sbeSchemaId=100|sbeSchemaVersion=";
    if (header.actingVersion() != archive_mark_codecs::MarkFileHeader::SBE_SCHEMA_VERSION)
    {
        out += std::to_string(header.actingVersion());
        out += '/';
    }
    out += "2|sbeBlockLength=128):";
    out += "version=" + std::to_string(header.version());
    out += "|activityTimestamp=" + std::to_string(header.activityTimestamp());
    out += "|startTimestamp=" + std::to_string(header.startTimestamp());
    out += "|pid=" + std::to_string(header.pid());
    out += "|controlStreamId=" + std::to_string(header.controlStreamId());
    out += "|localControlStreamId=" + std::to_string(header.localControlStreamId());
    out += "|eventsStreamId=" + std::to_string(header.eventsStreamId());
    out += "|headerLength=" + std::to_string(header.headerLength());
    out += "|errorBufferLength=" + std::to_string(header.errorBufferLength());
    out += "|archiveId=" + std::to_string(header.archiveId());
    out += "|controlChannel='" + header.getControlChannelAsString() + '\'';
    out += "|localControlChannel='" + header.getLocalControlChannelAsString() + '\'';
    out += "|eventsChannel='" + header.getEventsChannelAsString() + '\'';
    out += "|aeronDirectory='" + header.getAeronDirectoryAsString() + '\'';
    return out;
}

std::string format_time(std::int64_t epoch_millis, const char* format)
{
    const std::time_t seconds = static_cast<std::time_t>(epoch_millis / 1000);
    std::tm broken_down{};
    localtime_r(&seconds, &broken_down);
    char buffer[64];
    std::strftime(buffer, sizeof(buffer), format, &broken_down);
    return buffer;
}

/// `ArchiveTool.printMarkInformation` (`ArchiveTool.java:1237-1243`): the
/// first time is "now", then the mark file's start and activity timestamps.
void print_mark_information(ArchiveMarkFile& mark_file, std::string& out)
{
    const auto now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    out += format_time(now, "%H:%M:%S");
    out += " (start: " + format_time(mark_file.start_timestamp(), "%Y-%m-%d %H:%M:%S");
    out += ", activity: " + format_time(mark_file.activity_timestamp_volatile(), "%Y-%m-%d %H:%M:%S");
    out += ")\n";
    out += mark_header_to_string(mark_file);
    out += '\n';
}

int command_count_entries(const std::filesystem::path& archive_dir)
{
    auto catalog = open_catalog_read_only(archive_dir);
    if (!catalog)
    {
        print_error(catalog.error());
        return 1;
    }

    std::printf("%d\n", catalog->entry_count());
    return 0;
}

/// `ArchiveTool.max-entries` (`ArchiveTool.java:275-288`): both argument forms
/// print the deprecation text; there is no `capacity` branch in the parser.
int command_max_entries()
{
    std::printf("max-entries is deprecated please use capacity.\n");
    return 0;
}

int command_pid(const std::filesystem::path& archive_dir)
{
    std::string error;
    auto mark_file = open_mark_file(archive_dir, error, false);
    if (!mark_file)
    {
        print_error(mark_file.error());
        return 1;
    }

    std::printf("%d\n", mark_file->pid());
    return 0;
}

/// `ArchiveTool.describe` / `describeAll` / `describeRecording`
/// (`ArchiveTool.java:421-489`).
int command_describe(const std::filesystem::path& archive_dir, bool all_valid, bool one_recording, long recording_id)
{
    auto catalog = open_catalog_read_only(archive_dir);
    if (!catalog)
    {
        print_error(catalog.error());
        return 1;
    }

    if (one_recording)
    {
        bool found = false;
        catalog->for_each(
            [&](CatalogEntry& entry)
            {
                if (entry.descriptor.recordingId() == recording_id)
                {
                    found = true;
                    std::printf("%s\n", descriptor_to_string(entry.descriptor).c_str());
                }
            });
        if (!found)
        {
            std::fprintf(stderr, "io.aeron.exceptions.AeronException: no recording found with recordingId: %ld\n",
                         recording_id);
            return 1;
        }
        return 0;
    }

    std::string error;
    auto mark_file = open_mark_file(archive_dir, error, true);
    if (!mark_file)
    {
        print_error(mark_file.error());
        return 1;
    }

    std::string out;
    print_mark_information(*mark_file, out);
    out += "Catalog capacity in bytes: " + std::to_string(catalog->capacity()) + '\n';
    catalog->for_each(
        [&](CatalogEntry& entry)
        {
            if (all_valid || archive_codecs::RecordingState::VALID == entry.header.state())
            {
                out += descriptor_to_string(entry.descriptor);
                if (all_valid)
                {
                    out += '|';
                    out += archive_codecs::RecordingState::c_str(entry.header.state());
                }
                out += '\n';
            }
        });
    std::fwrite(out.data(), 1, out.size(), stdout);
    return 0;
}

std::int32_t read_int32_le(const std::uint8_t* bytes) noexcept
{
    return static_cast<std::int32_t>(
        static_cast<std::uint32_t>(bytes[0]) | (static_cast<std::uint32_t>(bytes[1]) << 8) |
        (static_cast<std::uint32_t>(bytes[2]) << 16) | (static_cast<std::uint32_t>(bytes[3]) << 24));
}

std::int16_t read_int16_le(const std::uint8_t* bytes) noexcept
{
    return static_cast<std::int16_t>(static_cast<std::uint16_t>(bytes[0]) |
                                     (static_cast<std::uint16_t>(bytes[1]) << 8));
}

/// `ArchiveTool.checksumSegmentFile` (`ArchiveTool.java:1603-1672`): every DATA
/// frame's payload is CRC'd and the result is written into the frame's
/// session-id field (`SESSION_ID_FIELD_OFFSET`), which is what `verify
/// -checksum` reads back. PAD frames are skipped.
void checksum_segment_file(const std::filesystem::path& archive_dir, const Checksum& checksum,
                           std::int64_t recording_id, const std::string& file_name, std::int64_t start_position,
                           std::int32_t term_length, std::int32_t mtu_length)
{
    const std::filesystem::path file = archive_dir / file_name;
    const std::string file_text = file.string();
    const auto fail = [&](const std::string& reason)
    {
        std::printf("(recordingId=%lld, file=%s) ERR: %s\n", static_cast<long long>(recording_id), file_text.c_str(),
                    reason.c_str());
    };

    const std::int64_t start_term_offset = start_position & (term_length - 1);
    const std::int64_t start_term_base_position = start_position - start_term_offset;
    auto segment_base_position = deepmsg::archive::server::parse_segment_file_position(file_name);
    if (!segment_base_position)
    {
        fail("failed to checksum");
        return;
    }

    const int fd = ::open(file.c_str(), O_RDWR);
    if (fd < 0)
    {
        fail("failed to checksum");
        return;
    }

    struct stat status{};
    (void)::fstat(fd, &status);
    const std::int64_t size = status.st_size;
    std::int64_t file_offset = *segment_base_position == start_term_base_position ? start_term_offset : 0;
    std::vector<std::uint8_t> buffer(static_cast<std::size_t>(deepmsg::archive::server::frame_align(mtu_length, 64)));

    while (file_offset < size)
    {
        std::array<std::uint8_t, deepmsg::archive::server::DATA_HEADER_LENGTH> header{};
        if (::pread(fd, header.data(), header.size(), file_offset) != static_cast<ssize_t>(header.size()))
        {
            fail("failed to read fragment header");
            (void)::close(fd);
            return;
        }

        const std::int32_t frame_length = read_int32_le(header.data());
        if (0 == frame_length)
        {
            break;
        }

        const std::int32_t aligned_length =
            deepmsg::archive::server::frame_align(frame_length, deepmsg::archive::server::FRAME_ALIGNMENT);
        const std::int16_t frame_type = read_int16_le(header.data() + deepmsg::archive::server::FRAME_TYPE_OFFSET);
        if (deepmsg::archive::server::DATA_FRAME_TYPE == frame_type)
        {
            const std::int32_t data_length = aligned_length - deepmsg::archive::server::DATA_HEADER_LENGTH;
            if (data_length < 0 || static_cast<std::size_t>(data_length) > buffer.size() ||
                ::pread(fd, buffer.data(), data_length, file_offset + deepmsg::archive::server::DATA_HEADER_LENGTH) !=
                    data_length)
            {
                fail("failed to read " + std::to_string(data_length) + " byte(s) of data at offset " +
                     std::to_string(file_offset + deepmsg::archive::server::DATA_HEADER_LENGTH));
                (void)::close(fd);
                return;
            }

            const std::int32_t result = checksum.compute(buffer.data(), 0, data_length);
            const std::uint8_t bytes[4] = {
                static_cast<std::uint8_t>(result & 0xFF), static_cast<std::uint8_t>((result >> 8) & 0xFF),
                static_cast<std::uint8_t>((result >> 16) & 0xFF), static_cast<std::uint8_t>((result >> 24) & 0xFF)};
            if (::pwrite(fd, bytes, sizeof(bytes), file_offset + deepmsg::archive::server::SESSION_ID_OFFSET) !=
                static_cast<ssize_t>(sizeof(bytes)))
            {
                fail("failed to checksum");
                (void)::close(fd);
                return;
            }
        }

        file_offset += aligned_length;
    }

    (void)::close(fd);
}

/// `ArchiveTool.checksum` / `checksumRecording` (`ArchiveTool.java:1545-1705`):
/// the catalog is opened read-write because `updateChecksum` rewrites each
/// descriptor's checksum; the segment files are rewritten in place.
int command_checksum(const std::filesystem::path& archive_dir, bool all_files, const std::string& checksum_name,
                     std::optional<std::int64_t> recording_id)
{
    auto checksum = deepmsg::archive::server::checksum_by_name(checksum_name);
    if (!checksum)
    {
        print_error(checksum.error());
        return 1;
    }

    CatalogOpenOptions options;
    options.archive_dir = archive_dir.string();
    options.catalog_capacity = Catalog::MIN_CAPACITY;
    options.writable = true;
    options.checksum = *checksum;
    auto catalog = Catalog::open(options);
    if (!catalog)
    {
        print_error(catalog.error());
        return 1;
    }

    auto index = deepmsg::archive::server::index_segment_files(archive_dir.string());
    if (!index)
    {
        print_error(index.error());
        return 1;
    }

    const auto process = [&](CatalogEntry& entry)
    {
        catalog->update_checksum(entry.recording_descriptor_offset);
        const auto found = index->find(entry.descriptor.recordingId());
        static const std::vector<std::string> no_files;
        const std::vector<std::string>& files = found != index->end() ? found->second : no_files;
        if (all_files)
        {
            for (const std::string& file : files)
            {
                checksum_segment_file(archive_dir, **checksum, entry.descriptor.recordingId(), file,
                                      entry.descriptor.startPosition(), entry.descriptor.termBufferLength(),
                                      entry.descriptor.mtuLength());
            }
        }
        else
        {
            auto last = deepmsg::archive::server::find_segment_file_with_highest_position(files);
            if (!last)
            {
                return;
            }
            if (!last->empty())
            {
                checksum_segment_file(archive_dir, **checksum, entry.descriptor.recordingId(), *last,
                                      entry.descriptor.startPosition(), entry.descriptor.termBufferLength(),
                                      entry.descriptor.mtuLength());
            }
        }
    };

    if (recording_id)
    {
        if (!catalog->for_entry(*recording_id, process))
        {
            std::fprintf(stderr, "io.aeron.exceptions.AeronException: no recording found with recordingId: %lld\n",
                         static_cast<long long>(*recording_id));
            return 1;
        }
    }
    else
    {
        catalog->for_each(process);
    }

    (void)catalog->close();
    return 0;
}

constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int32_t FILE_IO_MAX_LENGTH_DEFAULT = 1024 * 1024;

bool is_not_frame_aligned(std::int64_t position) noexcept
{
    return 0 != (position & (deepmsg::archive::server::FRAME_ALIGNMENT - 1));
}

std::int32_t compute_term_id_from_position(std::int64_t position, std::int32_t position_bits_to_shift,
                                           std::int32_t initial_term_id) noexcept
{
    return static_cast<std::int32_t>(position >> position_bits_to_shift) + initial_term_id;
}

/// `ReplaySession.isInvalidHeader` (`ReplaySession.java:654-661`).
bool is_invalid_header(const std::uint8_t* header, std::int32_t stream_id, std::int32_t term_id,
                       std::int32_t term_offset) noexcept
{
    return read_int32_le(header + deepmsg::archive::server::TERM_OFFSET_FIELD_OFFSET) != term_offset ||
           read_int32_le(header + deepmsg::archive::server::TERM_ID_OFFSET) != term_id ||
           read_int32_le(header + deepmsg::archive::server::STREAM_ID_OFFSET) != stream_id;
}

std::string yes_no_prompt(const std::string& message)
{
    std::printf("\n%s: ", message.c_str());
    std::fflush(stdout);

    // `readContinueAnswer`: one line from stdin, empty means "yes". `fgets`
    // rather than <iostream>: the gate forbids iostream in product code.
    char buffer[256];
    if (nullptr == std::fgets(buffer, sizeof(buffer), stdin))
    {
        return {};
    }
    return trim(std::string{buffer});
}

bool is_affirmative(const std::string& answer)
{
    return answer.empty() || answer == "y" || answer == "Y" || answer == "yes" || answer == "YeS" || answer == "yEs" ||
           answer == "yeS" || answer == "Yes" || answer == "YES";
}

/// `ArchiveTool.isInvalidSegmentFile` (`ArchiveTool.java:1433-1524`).
bool is_invalid_segment_file(const std::filesystem::path& archive_dir, std::int64_t recording_id,
                             const std::string& file_name, std::int64_t start_position, std::int32_t term_length,
                             std::int32_t segment_length, std::int32_t stream_id, std::int32_t initial_term_id,
                             bool apply_checksum, const Checksum* checksum)
{
    const std::filesystem::path file = archive_dir / file_name;
    const std::string file_text = file.string();
    const auto fail = [&](const std::string& reason)
    {
        std::printf("(recordingId=%lld, file=%s) ERR: %s\n", static_cast<long long>(recording_id), file_text.c_str(),
                    reason.c_str());
    };

    const int fd = ::open(file.c_str(), O_RDONLY);
    if (fd < 0)
    {
        fail("failed to verify file");
        return true;
    }

    struct stat status{};
    (void)::fstat(fd, &status);
    const std::int64_t offset_limit =
        std::min(static_cast<std::int64_t>(segment_length), static_cast<std::int64_t>(status.st_size));
    const std::int32_t position_bits_to_shift = deepmsg::archive::server::position_bits_to_shift(term_length);
    const std::int64_t start_term_offset = start_position & (term_length - 1);
    const std::int64_t start_term_base_position = start_position - start_term_offset;
    auto segment_file_base_position = deepmsg::archive::server::parse_segment_file_position(file_name);
    if (!segment_file_base_position)
    {
        fail("failed to verify file");
        (void)::close(fd);
        return true;
    }

    std::int64_t file_offset = *segment_file_base_position == start_term_base_position ? start_term_offset : 0;
    std::int64_t position = *segment_file_base_position + file_offset;
    std::vector<std::uint8_t> buffer(FILE_IO_MAX_LENGTH_DEFAULT);

    do
    {
        std::array<std::uint8_t, deepmsg::archive::server::DATA_HEADER_LENGTH> header{};
        if (::pread(fd, header.data(), header.size(), file_offset) != static_cast<ssize_t>(header.size()))
        {
            fail("failed to read fragment header");
            (void)::close(fd);
            return true;
        }

        const std::int32_t frame_length = read_int32_le(header.data());
        if (0 == frame_length)
        {
            break;
        }

        const std::int32_t term_id = compute_term_id_from_position(position, position_bits_to_shift, initial_term_id);
        const std::int32_t term_offset = static_cast<std::int32_t>(position & (term_length - 1));
        if (is_invalid_header(header.data(), stream_id, term_id, term_offset))
        {
            std::printf(
                "(recordingId=%lld, file=%s) ERR: fragment termOffset=%d (expected=%d), termId=%d "
                "(expected=%d), streamId=%d (expected=%d)\n",
                static_cast<long long>(recording_id), file_text.c_str(),
                read_int32_le(header.data() + deepmsg::archive::server::TERM_OFFSET_FIELD_OFFSET), term_offset,
                read_int32_le(header.data() + deepmsg::archive::server::TERM_ID_OFFSET), term_id,
                read_int32_le(header.data() + deepmsg::archive::server::STREAM_ID_OFFSET), stream_id);
            (void)::close(fd);
            return true;
        }

        const std::int16_t frame_type = read_int16_le(header.data() + deepmsg::archive::server::FRAME_TYPE_OFFSET);
        const std::int32_t session_id = read_int32_le(header.data() + deepmsg::archive::server::SESSION_ID_OFFSET);
        const std::int32_t aligned_frame_length =
            deepmsg::archive::server::frame_align(frame_length, deepmsg::archive::server::FRAME_ALIGNMENT);
        const std::int32_t data_length = aligned_frame_length - deepmsg::archive::server::DATA_HEADER_LENGTH;
        if (data_length < 0 || static_cast<std::size_t>(data_length) > buffer.size() ||
            ::pread(fd, buffer.data(), data_length, file_offset + deepmsg::archive::server::DATA_HEADER_LENGTH) !=
                data_length)
        {
            fail("failed to read " + std::to_string(data_length) + " byte(s) of data at offset " +
                 std::to_string(file_offset + deepmsg::archive::server::DATA_HEADER_LENGTH));
            (void)::close(fd);
            return true;
        }

        if (apply_checksum && deepmsg::archive::server::DATA_FRAME_TYPE == frame_type)
        {
            const std::int32_t computed = checksum->compute(buffer.data(), 0, data_length);
            if (computed != session_id)
            {
                fail("checksum failed recorded=" + std::to_string(session_id) +
                     " (expected=" + std::to_string(computed) + ")");
                (void)::close(fd);
                return true;
            }
        }

        file_offset += aligned_frame_length;
        position += aligned_frame_length;
    } while (file_offset < offset_limit);

    (void)::close(fd);
    return false;
}

/// `ArchiveTool.verify` / `verifyRecording` (`ArchiveTool.java:645-730`, `:947-996`,
/// `:1246-1395`): writes the catalog back, as the reference does — INVALID on a
/// fault, stopPosition/stopTimestamp on a repair, and VALID again when an INVALID
/// recording's segment files are present and sound.
int command_verify(const std::filesystem::path& archive_dir, bool all_segment_files, const Checksum* checksum,
                   bool apply_checksum, std::optional<std::int64_t> recording_id)
{
    CatalogOpenOptions options;
    options.archive_dir = archive_dir.string();
    options.catalog_capacity = Catalog::MIN_CAPACITY;
    options.writable = true;
    options.checksum = checksum;
    auto catalog = Catalog::open(options);
    if (!catalog)
    {
        print_error(catalog.error());
        return 1;
    }

    auto segment_files = deepmsg::archive::server::index_segment_files(archive_dir.string());
    if (!segment_files)
    {
        print_error(segment_files.error());
        return 1;
    }

    std::vector<std::uint8_t> buffer(FILE_IO_MAX_LENGTH_DEFAULT);
    int error_count = 0;

    const Catalog::TruncateOnPageStraddle truncate_on_page_straddle = [](const std::string& path)
    {
        return is_affirmative(yes_no_prompt("Last fragment in segment file: " + path +
                                            " straddles a page boundary,\n"
                                            "i.e. it is not possible to verify if it was written correctly.\n\n"
                                            "Please choose the corrective action: (y) to truncate the file or (n) to "
                                            "do nothing"));
    };

    const auto process = [&](CatalogEntry& entry)
    {
        const std::int64_t recording_id_value = entry.descriptor.recordingId();
        const archive_codecs::RecordingState::Value state = entry.header.state();
        if (archive_codecs::RecordingState::VALID != state && archive_codecs::RecordingState::INVALID != state)
        {
            std::printf("(recordingId=%lld) skipping: %s\n", static_cast<long long>(recording_id_value),
                        archive_codecs::RecordingState::c_str(state));
            return;
        }

        const std::int64_t start_position = entry.descriptor.startPosition();
        const std::int64_t stop_position = entry.descriptor.stopPosition();
        const auto fail = [&]()
        {
            ++error_count;
            entry.header.state(archive_codecs::RecordingState::INVALID);
        };

        if (start_position < 0)
        {
            std::printf("(recordingId=%lld) ERR: Negative startPosition=%lld\n",
                        static_cast<long long>(recording_id_value), static_cast<long long>(start_position));
            fail();
            return;
        }
        if (is_not_frame_aligned(start_position))
        {
            std::printf("(recordingId=%lld) ERR: Non-aligned startPosition=%lld\n",
                        static_cast<long long>(recording_id_value), static_cast<long long>(start_position));
            fail();
            return;
        }
        if (NULL_POSITION != stop_position)
        {
            if (stop_position < start_position)
            {
                std::printf(
                    "(recordingId=%lld) ERR: Invariant violation stopPosition=%lld is before "
                    "startPosition=%lld\n",
                    static_cast<long long>(recording_id_value), static_cast<long long>(stop_position),
                    static_cast<long long>(start_position));
                fail();
                return;
            }
            if (is_not_frame_aligned(stop_position))
            {
                std::printf("(recordingId=%lld) ERR: Non-aligned stopPosition=%lld\n",
                            static_cast<long long>(recording_id_value), static_cast<long long>(stop_position));
                fail();
                return;
            }
        }

        const std::int32_t segment_length = entry.descriptor.segmentFileLength();
        const std::int32_t term_length = entry.descriptor.termBufferLength();
        static const std::vector<std::string> no_files;
        const auto found = segment_files->find(recording_id_value);
        const std::vector<std::string>& files = found != segment_files->end() ? found->second : no_files;
        auto max_segment_file = deepmsg::archive::server::find_segment_file_with_highest_position(files);
        if (!max_segment_file)
        {
            std::printf("(recordingId=%lld) ERR: %.*s\n", static_cast<long long>(recording_id_value),
                        static_cast<int>(max_segment_file.error().message.view().size()),
                        max_segment_file.error().message.view().data());
            fail();
            return;
        }

        if (!max_segment_file->empty())
        {
            auto max_position = deepmsg::archive::server::parse_segment_file_position(*max_segment_file);
            if (!max_position)
            {
                std::printf("(recordingId=%lld) ERR: %.*s\n", static_cast<long long>(recording_id_value),
                            static_cast<int>(max_position.error().message.view().size()),
                            max_position.error().message.view().data());
                fail();
                return;
            }
            const std::int64_t max_segment_position = *max_position + (segment_length - 1);
            if (start_position > max_segment_position || stop_position > max_segment_position)
            {
                std::printf(
                    "(recordingId=%lld) ERR: Invariant violation: startPosition=%lld and/or "
                    "stopPosition=%lld exceed max segment file position=%lld\n",
                    static_cast<long long>(recording_id_value), static_cast<long long>(start_position),
                    static_cast<long long>(stop_position), static_cast<long long>(max_segment_position));
                fail();
                return;
            }
        }

        auto computed_stop_position =
            Catalog::compute_stop_position(archive_dir.string(), *max_segment_file, start_position, term_length,
                                           segment_length, checksum, buffer, truncate_on_page_straddle);
        if (!computed_stop_position)
        {
            std::printf("(recordingId=%lld) ERR: %.*s\n", static_cast<long long>(recording_id_value),
                        static_cast<int>(computed_stop_position.error().message.view().size()),
                        computed_stop_position.error().message.view().data());
            fail();
            return;
        }

        if (apply_checksum)
        {
            const std::int32_t expected = catalog->compute_recording_descriptor_checksum(
                entry.recording_descriptor_offset, entry.header.length());
            if (expected != entry.header.checksum())
            {
                std::printf("(recordingId=%lld) ERR: invalid Catalog checksum: expected=%d, actual=%d\n",
                            static_cast<long long>(recording_id_value), expected, entry.header.checksum());
                fail();
                return;
            }
        }

        if (!max_segment_file->empty())
        {
            const std::int32_t stream_id = entry.descriptor.streamId();
            if (all_segment_files)
            {
                for (const std::string& file : files)
                {
                    if (is_invalid_segment_file(archive_dir, recording_id_value, file, start_position, term_length,
                                                segment_length, stream_id, entry.descriptor.initialTermId(),
                                                apply_checksum, checksum))
                    {
                        fail();
                        return;
                    }
                }
            }
            else if (is_invalid_segment_file(archive_dir, recording_id_value, *max_segment_file, start_position,
                                             term_length, segment_length, stream_id, entry.descriptor.initialTermId(),
                                             apply_checksum, checksum))
            {
                fail();
                return;
            }
        }

        if (*computed_stop_position != stop_position)
        {
            entry.descriptor.stopPosition(*computed_stop_position);
            const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            entry.descriptor.stopTimestamp(now);
        }

        if (archive_codecs::RecordingState::INVALID == state && !files.empty())
        {
            entry.header.state(archive_codecs::RecordingState::VALID);
        }

        std::printf("(recordingId=%lld) OK\n", static_cast<long long>(recording_id_value));
    };

    if (recording_id)
    {
        if (!catalog->for_entry(*recording_id, process))
        {
            std::fprintf(stderr, "io.aeron.exceptions.AeronException: no recording found with recordingId: %lld\n",
                         static_cast<long long>(*recording_id));
            return 1;
        }
    }
    else
    {
        catalog->for_each(process);
    }

    (void)catalog->close();
    return 0 == error_count ? 0 : -1;
}

/// `RecordingDescriptorHeaderDecoder.appendTo` (the SBE-generated `toString`).
std::string descriptor_header_to_string(archive_codecs::RecordingDescriptorHeader& header)
{
    header.sbeRewind();
    std::string out = "[RecordingDescriptorHeader](sbeTemplateId=21|sbeSchemaId=101|sbeSchemaVersion=";
    if (header.actingVersion() != archive_codecs::RecordingDescriptorHeader::SBE_SCHEMA_VERSION)
    {
        out += std::to_string(header.actingVersion());
        out += '/';
    }
    out += "14|sbeBlockLength=32):";
    out += "length=" + std::to_string(header.length());
    out += "|state=";
    out += archive_codecs::RecordingState::c_str(header.state());
    out += "|checksum=" + std::to_string(header.checksum());
    out += "|reserved=" + std::to_string(header.reserved());
    return out;
}

/// `org.agrona.PrintBufferUtil.prettyHexDump` (the format `dump` prints for a
/// DATA frame): a 16-byte row, offset and ASCII columns, and a trailing footer
/// with **no** newline (the caller's `println` supplies it).
std::string pretty_hex_dump(const std::uint8_t* data, std::int32_t length)
{
    if (length <= 0)
    {
        return {};
    }

    static constexpr const char* HEADER =
        "         +-------------------------------------------------+\n"
        "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\n"
        "+--------+-------------------------------------------------+----------------+\n";
    static constexpr const char* FOOTER =
        "+--------+-------------------------------------------------+----------------+";

    std::string out = HEADER;
    for (std::int32_t base = 0; base < length; base += 16)
    {
        char prefix[16];
        std::snprintf(prefix, sizeof(prefix), "|%08x|", static_cast<unsigned>(base));
        out += prefix;

        int i = 0;
        for (; i < 16 && base + i < length; ++i)
        {
            char byte_text[8];
            std::snprintf(byte_text, sizeof(byte_text), " %02x", data[base + i]);
            out += byte_text;
        }
        for (; i < 16; ++i)
        {
            out += "   ";
        }
        out += " |";
        for (i = 0; i < 16; ++i)
        {
            if (base + i < length)
            {
                const std::uint8_t value = data[base + i];
                out += value >= 0x20 && value <= 0x7E ? static_cast<char>(value) : '.';
            }
            else
            {
                out += ' ';
            }
        }
        out += "|\n";
    }
    out += FOOTER;
    return out;
}

bool continue_on_frame_limit(std::int64_t frame_limit)
{
    return is_affirmative(
        yes_no_prompt("Specified frame limit " + std::to_string(frame_limit) + " reached. Continue? (y/n)"));
}

/// `ArchiveTool.dump`'s `SimpleFragmentHandler`
/// (`ArchiveTool.java:548-566`).
class DumpHandler final : public FragmentHandler
{
public:
    void on_fragment(const std::uint8_t* buffer, std::int32_t offset, std::int32_t length, std::int32_t frame_type,
                     std::uint8_t flags, std::int64_t /*reserved_value*/) override
    {
        std::printf("data at offset [%d] with length = %d\n", offset, length);
        if (deepmsg::archive::server::PADDING_FRAME_TYPE == frame_type)
        {
            std::printf("PADDING FRAME\n");
        }
        else if (deepmsg::archive::server::DATA_FRAME_TYPE == frame_type)
        {
            constexpr std::uint8_t begins_fragment = 0x80;
            constexpr std::uint8_t ends_fragment = 0x40;
            if ((flags & (begins_fragment | ends_fragment)) != (begins_fragment | ends_fragment))
            {
                std::string suffix;
                if ((flags & begins_fragment) == begins_fragment)
                {
                    suffix = "BEGIN_FRAGMENT";
                }
                if ((flags & ends_fragment) == ends_fragment)
                {
                    suffix += "END_FRAGMENT";
                }
                std::printf("Fragmented frame. %s\n", suffix.c_str());
            }
            const std::string hex = pretty_hex_dump(buffer + offset, length);
            std::fwrite(hex.data(), 1, hex.size(), stdout);
            std::printf("\n");
        }
        else
        {
            std::printf("Unexpected frame type %d\n", frame_type);
        }
    }
};

/// `ArchiveTool.dump` (`ArchiveTool.java:535-589`, `:1176-1235`).
int command_dump(const std::filesystem::path& archive_dir, std::int64_t fragment_count_limit)
{
    if (fragment_count_limit <= 0)
    {
        std::fprintf(stderr, "ERR: Invalid fragmentCountLimit=%lld\n", static_cast<long long>(fragment_count_limit));
        return 1;
    }

    auto catalog = open_catalog_read_only(archive_dir);
    if (!catalog)
    {
        print_error(catalog.error());
        return 1;
    }

    std::string error;
    auto mark_file = open_mark_file(archive_dir, error, true);
    if (!mark_file)
    {
        print_error(mark_file.error());
        return 1;
    }

    std::string out;
    print_mark_information(*mark_file, out);
    out += "Catalog capacity in bytes: " + std::to_string(catalog->capacity()) + "\n\n";
    out += "Dumping up to " + std::to_string(fragment_count_limit) + " fragments per recording\n";
    std::fwrite(out.data(), 1, out.size(), stdout);

    DumpHandler handler;
    catalog->for_each(
        [&](CatalogEntry& entry)
        {
            const std::int64_t recording_id = entry.descriptor.recordingId();
            const std::int64_t start_position = entry.descriptor.startPosition();
            const std::int64_t stop_position = entry.descriptor.stopPosition();
            const std::int64_t stream_length = stop_position - start_position;

            std::string text;
            text += "\n\nRecording " + std::to_string(recording_id) + "\n";
            text += "  channel: " + entry.descriptor.getStrippedChannelAsString() + "\n";
            text += "  streamId: " + std::to_string(entry.descriptor.streamId()) + "\n";
            text += "  stream length: " +
                    (NULL_POSITION == stop_position ? std::to_string(NULL_POSITION) : std::to_string(stream_length)) +
                    "\n";
            text += descriptor_header_to_string(entry.header) + "\n";
            text += descriptor_to_string(entry.descriptor) + "\n";
            std::fwrite(text.data(), 1, text.size(), stdout);

            if (0 == stream_length)
            {
                std::printf("Recording is empty\n");
                return;
            }

            auto reader = RecordingReader::create(catalog->recording_summary(recording_id), archive_dir.string(),
                                                  start_position, RecordingReader::NULL_POSITION);
            if (!reader)
            {
                print_error(reader.error());
                return;
            }

            bool active = true;
            std::int64_t fragment_count = fragment_count_limit;
            do
            {
                std::printf("\nFrame at position [%lld] ", static_cast<long long>(reader->replay_position()));
                auto polled = reader->poll(handler, 1);
                if (!polled)
                {
                    print_error(polled.error());
                    return;
                }
                if (*polled > 0 && --fragment_count == 0)
                {
                    fragment_count = fragment_count_limit;
                    if (NULL_POSITION != stop_position)
                    {
                        std::printf("%lld bytes of (from %lld) remaining in recording id %lld\n",
                                    static_cast<long long>(stream_length - reader->replay_position()),
                                    static_cast<long long>(stream_length), static_cast<long long>(recording_id));
                    }
                    active = continue_on_frame_limit(fragment_count_limit);
                }
            } while (active && !reader->is_done());
        });
    return 0;
}

std::int64_t read_int64_le(const std::uint8_t* bytes) noexcept
{
    std::uint64_t value = 0;
    for (int i = 7; i >= 0; --i)
    {
        value = (value << 8) | bytes[i];
    }
    return static_cast<std::int64_t>(value);
}

/// `CommonContext.DATE_TIME_FORMATTER` (`CommonContext.java:572`):
/// `uuuu-MM-dd HH:mm:ss.SSSSSSZ`, i.e. microseconds and a numeric offset.
std::string format_error_time(std::int64_t epoch_millis)
{
    const std::time_t seconds = static_cast<std::time_t>(epoch_millis / 1000);
    std::tm local{};
    localtime_r(&seconds, &local);
    char date[64];
    std::strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", &local);

    const std::int64_t micros = ((epoch_millis % 1000) + 1000) % 1000 * 1000;
    const long offset_seconds = local.tm_gmtoff;
    const char sign = offset_seconds < 0 ? '-' : '+';
    const long offset = offset_seconds < 0 ? -offset_seconds : offset_seconds;

    char buffer[96];
    std::snprintf(buffer, sizeof(buffer), "%s.%06lld%c%02ld%02ld", date, static_cast<long long>(micros), sign,
                  offset / 3600, (offset % 3600) / 60);
    return buffer;
}

/// `CommonContext.printErrorLog` (`CommonContext.java:1274-1297`) over an
/// Aeron distinct-error-log region (`aeron_distinct_error_log.h:27-40`): a
/// 24-byte header per entry (length, observation count, last/first timestamps)
/// followed by the description, entries 8-byte aligned, the length stored last.
int print_error_log(const std::uint8_t* buffer, std::size_t size)
{
    if (size < 24 || 0 == read_int32_le(buffer))
    {
        // `CommonContext.printErrorLog`'s else branch (`:1297-1301`): no
        // errors still prints the summary, without the trailing period.
        std::printf("\n0 distinct errors observed\n");
        return 0;
    }

    int distinct_error_count = 0;
    std::size_t offset = 0;
    while (offset + 24 <= size)
    {
        const std::int32_t length = read_int32_le(buffer + offset);
        if (0 == length)
        {
            break;
        }
        if (length < 24 || offset + static_cast<std::size_t>(length) > size)
        {
            break;
        }

        const std::int32_t observations = read_int32_le(buffer + offset + 4);
        const std::int64_t last_timestamp = read_int64_le(buffer + offset + 8);
        const std::int64_t first_timestamp = read_int64_le(buffer + offset + 16);
        const auto payload = static_cast<std::size_t>(length - 24);
        std::printf("\n%d observations from %s to %s for:\n", observations, format_error_time(first_timestamp).c_str(),
                    format_error_time(last_timestamp).c_str());
        std::fwrite(buffer + offset + 24, 1, payload, stdout);
        std::printf("\n");

        ++distinct_error_count;
        offset += (static_cast<std::size_t>(length) + 7) & ~static_cast<std::size_t>(7);
    }

    std::printf("\n%d distinct errors observed.\n", distinct_error_count);
    return distinct_error_count;
}

/// `ArchiveTool.printErrors` (`ArchiveTool.java:1526-1543`): the archive's own
/// error log, then the media driver's from `<aeronDirectory>/cnc.dat`.
int command_errors(const std::filesystem::path& archive_dir)
{
    std::string error;
    auto mark_file = open_mark_file(archive_dir, error, false);
    if (!mark_file)
    {
        print_error(mark_file.error());
        return 1;
    }

    std::printf("Archive error log:\n");
    const std::span<std::uint8_t> archive_errors = mark_file->error_buffer();
    (void)print_error_log(archive_errors.data(), archive_errors.size());

    const std::string aeron_directory = mark_file->aeron_directory();
    std::printf("\nAeron driver error log (directory: %s):\n", aeron_directory.c_str());

    const std::filesystem::path cnc_path = std::filesystem::path(aeron_directory) / "cnc.dat";
    auto cnc = axc::core::MappedFile::open(cnc_path.string(), false);
    if (!cnc)
    {
        print_error(cnc.error());
        return 1;
    }

    const std::span<const std::uint8_t> bytes = cnc->mapping();
    if (bytes.size() < 128)
    {
        std::fprintf(stderr, "ERR: cnc file too short: %s\n", cnc_path.string().c_str());
        return 1;
    }
    const std::int64_t to_driver = read_int32_le(bytes.data() + 4);
    const std::int64_t to_clients = read_int32_le(bytes.data() + 8);
    const std::int64_t counters_metadata = read_int32_le(bytes.data() + 12);
    const std::int64_t counters_values = read_int32_le(bytes.data() + 16);
    const std::int64_t error_log_length = read_int32_le(bytes.data() + 20);
    const std::int64_t error_log_start = 128 + to_driver + to_clients + counters_metadata + counters_values;
    if (error_log_length <= 0 || error_log_start < 0 ||
        static_cast<std::uint64_t>(error_log_start + error_log_length) > bytes.size())
    {
        std::fprintf(stderr, "ERR: cnc error log region out of range: %s\n", cnc_path.string().c_str());
        return 1;
    }

    (void)print_error_log(bytes.data() + error_log_start, static_cast<std::size_t>(error_log_length));
    return 0;
}

}  // namespace

int run_archive_tool_ro(int argc, char* argv[])
{
    // `args.length == 0 || args.length > 6` (`ArchiveTool.java:114-118`).
    if (argc < 2 || argc > 7)
    {
        print_help();
        return -1;
    }

    const std::filesystem::path archive_dir = argv[1];
    std::error_code exists_error;
    if (!std::filesystem::exists(archive_dir, exists_error))
    {
        std::fprintf(stderr, "ERR: Archive folder not found: %s\n",
                     std::filesystem::absolute(archive_dir, exists_error).c_str());
        print_help();
        return -1;
    }

    const std::string_view command = argv[2];
    const int arguments = argc - 2;  // the reference's args.length - 2

    if (command == "count-entries" && arguments == 1)
    {
        return command_count_entries(archive_dir);
    }
    if (command == "max-entries" && (arguments == 1 || arguments == 2))
    {
        return command_max_entries();
    }
    if (command == "pid" && arguments == 1)
    {
        return command_pid(archive_dir);
    }
    // `args.length > 1 && "describe-all"` (`ArchiveTool.java:129-131`): extra
    // arguments are ignored by the reference.
    if (command == "describe-all" && arguments >= 1)
    {
        return command_describe(archive_dir, true, false, 0);
    }
    if (command == "describe" && arguments == 1)
    {
        return command_describe(archive_dir, false, false, 0);
    }
    if (command == "describe" && arguments == 2)
    {
        return command_describe(archive_dir, false, true, std::strtol(argv[3], nullptr, 10));
    }

    if (command == "checksum" && arguments >= 2)
    {
        const std::string checksum_name = argv[3];
        if (arguments == 2)
        {
            return command_checksum(archive_dir, false, checksum_name, std::nullopt);
        }
        if (std::string_view(argv[4]) == "-a")
        {
            return command_checksum(archive_dir, true, checksum_name, std::nullopt);
        }
        const std::int64_t id = std::strtoll(argv[4], nullptr, 10);
        const bool all_files = arguments >= 4 && std::string_view(argv[5]) == "-a";
        return command_checksum(archive_dir, all_files, checksum_name, id);
    }

    if (command == "verify" && arguments >= 1)
    {
        const int option_count = arguments - 1;  // arguments after the command
        bool all_segments = false;
        bool apply_checksum = false;
        const Checksum* checksum = nullptr;
        std::optional<std::int64_t> recording_id;
        const auto checksum_option = [&](const char* name)
        {
            auto resolved = deepmsg::archive::server::checksum_by_name(name);
            if (!resolved)
            {
                print_error(resolved.error());
                return false;
            }
            checksum = *resolved;
            apply_checksum = true;
            return true;
        };

        if (option_count == 0)
        {
            // defaults
        }
        else if (option_count == 1)
        {
            if (std::string_view(argv[3]) == "-a")
            {
                all_segments = true;
            }
            else
            {
                recording_id = std::strtoll(argv[3], nullptr, 10);
            }
        }
        else if (option_count == 2)
        {
            if (std::string_view(argv[3]) == "-checksum")
            {
                if (!checksum_option(argv[4]))
                {
                    return 1;
                }
            }
            else
            {
                recording_id = std::strtoll(argv[3], nullptr, 10);
                all_segments = std::string_view(argv[4]) == "-a";
            }
        }
        else if (option_count == 3)
        {
            if (std::string_view(argv[3]) == "-a")
            {
                all_segments = true;
                if (!checksum_option(argv[5]))
                {
                    return 1;
                }
            }
            else
            {
                recording_id = std::strtoll(argv[3], nullptr, 10);
                if (!checksum_option(argv[5]))
                {
                    return 1;
                }
            }
        }
        else if (option_count == 4)
        {
            recording_id = std::strtoll(argv[3], nullptr, 10);
            all_segments = true;
            if (!checksum_option(argv[6]))
            {
                return 1;
            }
        }
        else
        {
            std::fprintf(stderr, "ERR: Invalid command\n");
            print_help();
            return -1;
        }

        return command_verify(archive_dir, all_segments, checksum, apply_checksum, recording_id);
    }

    if (command == "dump" && arguments >= 1 && arguments <= 2)
    {
        const std::int64_t limit =
            arguments == 2 ? std::strtoll(argv[3], nullptr, 10) : std::numeric_limits<std::int64_t>::max();
        return command_dump(archive_dir, limit);
    }

    if (command == "errors" && arguments == 1)
    {
        return command_errors(archive_dir);
    }

    if (command == "dump" || command == "errors")
    {
        std::fprintf(stderr, "ERR: %.*s is not implemented yet\n", static_cast<int>(command.size()), command.data());
        return 2;
    }

    std::fprintf(stderr, "ERR: Invalid command\n");
    print_help();
    return -1;
}

}  // namespace deepmsg::tools
