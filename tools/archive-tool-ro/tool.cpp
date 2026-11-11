// T1: the read-only subset of the reference `io.aeron.archive.ArchiveTool`.
//
// Every command here is a transcription of the reference method named beside
// it; the output text is the contract (the interop test runs the reference tool
// on the same archive directory and compares). Commands that are not part of
// the read-only subset are refused by the reference's own fallback
// (`ERR: Invalid command`), and the write commands (`compact`, `migrate`,
// `mark-*`, `delete-orphaned-segments`) are deliberately absent (Q7).

#include "tool.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "archive/server/archive_mark_file.h"
#include "archive/server/catalog.h"

namespace deepmsg::tools
{
namespace
{

using axc::core::Error;
using deepmsg::archive::server::ArchiveMarkFile;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOpenOptions;

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
std::expected<ArchiveMarkFile, Error> open_mark_file(const std::filesystem::path& archive_dir, std::string& error)
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
    return ArchiveMarkFile::open_existing(options);
}

std::expected<Catalog, Error> open_catalog_read_only(const std::filesystem::path& archive_dir)
{
    CatalogOpenOptions options;
    options.archive_dir = archive_dir.string();
    options.catalog_capacity = Catalog::MIN_CAPACITY;
    options.writable = false;
    return Catalog::open(options);
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
    auto mark_file = open_mark_file(archive_dir, error);
    if (!mark_file)
    {
        print_error(mark_file.error());
        return 1;
    }

    std::printf("%d\n", mark_file->pid());
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

    if (command == "describe" || command == "describe-all" || command == "dump" || command == "errors" ||
        command == "verify" || command == "checksum")
    {
        std::fprintf(stderr, "ERR: %.*s is not implemented yet\n", static_cast<int>(command.size()), command.data());
        return 2;
    }

    std::fprintf(stderr, "ERR: Invalid command\n");
    print_help();
    return -1;
}

}  // namespace deepmsg::tools
