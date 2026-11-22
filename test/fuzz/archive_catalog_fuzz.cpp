// Archive disk state: archive.catalog and archive-mark.dat.
//
// The catalog is an append-only file with a header, aligned descriptor frames
// and CRCs; the mark file is a mapping whose error ring buffer is printed at
// startup. Both are parsed before the conductor serves anything.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <span>
#include <vector>

#include "archive/server/archive_mark_file.h"
#include "archive/server/catalog.h"
#include "archive/server/checksum.h"
#include "core/io/distinct_error_log.h"
#include "fuzz_common.h"
#include "io_aeron_archive_codecs_mark/MarkFileHeader.h"

namespace
{

using axc::fuzz::ShmSandbox;
using deepmsg::archive::server::ArchiveMarkFile;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOpenOptions;

constexpr std::size_t MAX_CATALOG_BYTES = 512U * 1024U;

/// The golden files start with the 8-byte SBE message header of the mark
/// template (`detect_header_offset` answers 8 for them).
constexpr std::size_t MARK_MESSAGE_HEADER_LENGTH = 8;

std::vector<std::uint8_t> read_seed(const std::filesystem::path& path)
{
    std::ifstream stream{path, std::ios::binary};
    return std::vector<std::uint8_t>{std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

void write_seed(const std::filesystem::path& dir, const char* name, const std::vector<std::uint8_t>& bytes)
{
    std::FILE* file = std::fopen((dir / name).c_str(), "wb");
    if (nullptr != file)
    {
        (void)std::fwrite(bytes.data(), 1, bytes.size(), file);
        (void)std::fclose(file);
    }
}

/// A golden mark file whose 32-bit field at `offset` says INT32_MAX: the
/// var-ascii length and the error-buffer length are the two that used to be
/// trusted by the generated getters.
void emit_corrupt_mark(const std::filesystem::path& dir, const char* name, std::size_t offset)
{
    std::vector<std::uint8_t> bytes =
        read_seed(std::filesystem::path{AXC_PROJECT_SOURCE_DIR} / "test/golden/archive/mark-file/archive-mark.dat");
    if (bytes.size() < offset + sizeof(std::uint32_t))
    {
        return;
    }
    const std::uint32_t huge = 0x7FFFFFFFU;
    std::memcpy(bytes.data() + offset, &huge, sizeof(huge));
    write_seed(dir, name, bytes);
}

void exercise_catalog(const ShmSandbox& sandbox, std::span<const std::uint8_t> bytes,
                      const deepmsg::archive::server::Checksum* checksum)
{
    (void)sandbox.write_file("archive.catalog", bytes);

    CatalogOpenOptions options;
    options.archive_dir = sandbox.dir().string();
    options.catalog_capacity = 32;
    options.writable = false;
    options.checksum = checksum;

    auto catalog = Catalog::open(options);
    if (!catalog)
    {
        return;
    }
    (void)catalog->entry_count();
    (void)catalog->capacity();
    (void)catalog->for_each([](auto& entry) { (void)entry; });
    (void)catalog->wrap_descriptor(0);
    (void)catalog->recording_summary(0);
    (void)catalog->close();
}

void exercise_archive_mark(const ShmSandbox& sandbox, std::span<const std::uint8_t> bytes)
{
    if (bytes.size() < static_cast<std::size_t>(ArchiveMarkFile::HEADER_LENGTH))
    {
        return;
    }
    (void)sandbox.write_file("archive-mark.dat", bytes);

    ArchiveMarkFile::Options options;
    options.directory = sandbox.dir().string();
    const std::size_t error_length = bytes.size() - static_cast<std::size_t>(ArchiveMarkFile::HEADER_LENGTH);
    options.error_buffer_length = static_cast<std::int32_t>(std::min<std::size_t>(error_length, INT32_MAX));

    auto mark = ArchiveMarkFile::open_existing(options);
    if (!mark)
    {
        return;
    }
    (void)mark->pid();
    (void)mark->control_channel();
    (void)mark->local_control_channel();
    (void)mark->events_channel();
    (void)mark->aeron_directory();
    (void)axc::core::render_distinct_error_log(mark->error_buffer());
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    if (size == 0 || size > MAX_CATALOG_BYTES)
    {
        return 0;
    }
    static ShmSandbox sandbox{};
    const std::span<const std::uint8_t> bytes{data, size};

    exercise_catalog(sandbox, bytes, nullptr);
    exercise_catalog(sandbox, bytes, &deepmsg::archive::server::crc32());
    exercise_archive_mark(sandbox, bytes);
    return 0;
}

namespace axc::fuzz
{

void emit_seeds(const std::filesystem::path& dir)
{
    // The two inputs the first libFuzzer runs found (a short descriptor and a
    // corrupt mark-file var-ascii length) stay in the PR regression, and so do
    // two golden mark files with one 32-bit length forced to INT32_MAX.
    const std::filesystem::path artifacts = std::filesystem::path{AXC_PROJECT_SOURCE_DIR} / "test/fuzz/artifacts";
    axc::fuzz::copy_seed_file(artifacts / "archive-catalog-short-descriptor.bin", dir);
    axc::fuzz::copy_seed_file(artifacts / "archive-catalog-mark-var-string.bin", dir);

    emit_corrupt_mark(
        dir, "archive-mark-huge-var-string.bin",
        MARK_MESSAGE_HEADER_LENGTH + deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::SBE_BLOCK_LENGTH);
    emit_corrupt_mark(
        dir, "archive-mark-huge-error-length.bin",
        MARK_MESSAGE_HEADER_LENGTH +
            deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::errorBufferLengthEncodingOffset());
}

}  // namespace axc::fuzz

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
{
    return axc::fuzz::seed_emitter_initialize(argc, argv);
}
