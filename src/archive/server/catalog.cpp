#include "archive/server/catalog.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <system_error>
#include <utility>

#include "archive/server/codecs.h"
#include "archive/server/segment_files.h"
#include "core/io/fs.h"
#include "core/time/clock.h"
#include "core/trace/trace.h"

namespace deepmsg::archive::server
{
namespace
{

using archive_codecs::CatalogHeader;
using archive_codecs::RecordingDescriptor;
using archive_codecs::RecordingDescriptorHeader;
using archive_codecs::RecordingState;

// `FrameDescriptor.FRAME_ALIGNMENT` (`FrameDescriptor.java:66`) and the
// `DataHeaderFlyweight` constants the catalog's recovery scan uses
// (`DataHeaderFlyweight.java:38`, `:88`).
constexpr std::int32_t FRAME_ALIGNMENT = 32;
constexpr std::int32_t DATA_HEADER_LENGTH = 32;
constexpr std::int32_t SESSION_ID_OFFSET = 12;

// `Archive.Configuration.FILE_IO_MAX_LENGTH_DEFAULT` (`Archive.java:301`).
constexpr std::int64_t FILE_IO_MAX_LENGTH_DEFAULT = 1024LL * 1024;

/// `BitUtil.align(value, alignment)` (`Catalog.java:490`, `:1016`).
[[nodiscard]] std::int32_t align_up(std::int32_t value, std::int32_t alignment) noexcept
{
    if (alignment <= 0)
    {
        return value;
    }
    return value + ((alignment - (value % alignment)) % alignment);
}

template <typename T>
[[nodiscard]] T read_value(const std::uint8_t* address) noexcept
{
    T value{};
    std::memcpy(&value, address, sizeof(T));
    return value;
}

template <typename T>
void write_value(std::uint8_t* address, T value) noexcept
{
    std::memcpy(address, &value, sizeof(T));
}

/// `UnsafeBuffer.putLongVolatile`/`getLongVolatile`. On the little-endian
/// platforms the reference supports (`nativeOrder() == BYTE_ORDER`) these are a
/// plain release store and acquire load.
void write_long_volatile(std::uint8_t* address, std::int64_t value) noexcept
{
    std::atomic_ref<std::int64_t>(*reinterpret_cast<std::int64_t*>(address)).store(value, std::memory_order_release);
}

[[nodiscard]] std::int64_t read_long_volatile(std::uint8_t* address) noexcept
{
    return std::atomic_ref<std::int64_t>(*reinterpret_cast<std::int64_t*>(address)).load(std::memory_order_acquire);
}

[[nodiscard]] std::string path_in(std::string_view archive_dir, const std::string& name)
{
    std::filesystem::path path{archive_dir};
    path /= name;
    return path.string();
}

// `Catalog.FRAME_ALIGNMENT` stepping and frame accessors
// (`FrameDescriptor.frameLength`, `FrameDescriptor.java:261`).
[[nodiscard]] std::int32_t frame_length_at(const std::uint8_t* buffer, std::int32_t offset) noexcept
{
    return read_value<std::int32_t>(buffer + offset);
}

/// `Catalog.isValidFragment` (`Catalog.java:1434`).
[[nodiscard]] bool has_valid_checksum(const std::uint8_t* buffer, std::int32_t fragment_offset,
                                      std::int32_t aligned_fragment_length, const Checksum& checksum) noexcept
{
    const std::int32_t computed =
        checksum.compute(buffer, fragment_offset + DATA_HEADER_LENGTH, aligned_fragment_length - DATA_HEADER_LENGTH);
    const std::int32_t recorded = read_value<std::int32_t>(buffer + fragment_offset + SESSION_ID_OFFSET);
    return recorded == computed;
}

/// `Catalog.isEmptyPage` (`Catalog.java:1476`).
[[nodiscard]] bool is_empty_page(const std::uint8_t* buffer, std::int32_t page_start, std::int32_t end_offset) noexcept
{
    const std::int32_t page_end = std::min(page_start + Catalog::PAGE_SIZE, end_offset);
    for (std::int32_t index = page_start; index < page_end; index += 8)
    {
        if (0 != read_value<std::int64_t>(buffer + index))
        {
            return false;
        }
    }
    return true;
}

/// `Catalog.hasDataInAllPagesAfterStraddle` (`Catalog.java:1459`).
[[nodiscard]] bool has_data_in_all_pages_after_straddle(const std::uint8_t* buffer, std::int32_t fragment_offset,
                                                        std::int32_t aligned_fragment_length) noexcept
{
    std::int32_t straddle_offset = ((fragment_offset / Catalog::PAGE_SIZE) + 1) * Catalog::PAGE_SIZE;
    const std::int32_t end_offset = fragment_offset + aligned_fragment_length;
    while (straddle_offset < end_offset)
    {
        if (is_empty_page(buffer, straddle_offset, end_offset))
        {
            return false;
        }
        straddle_offset += Catalog::PAGE_SIZE;
    }
    return true;
}

/// `Catalog.isValidFragment` (`Catalog.java:1434`).
[[nodiscard]] bool is_valid_fragment(std::span<std::uint8_t> buffer, std::int32_t fragment_offset,
                                     std::int32_t aligned_fragment_length, const Checksum* checksum) noexcept
{
    const auto capacity = static_cast<std::int32_t>(buffer.size());
    if (fragment_offset < 0 || aligned_fragment_length < DATA_HEADER_LENGTH ||
        aligned_fragment_length > capacity - fragment_offset)
    {
        return false;
    }

    return (nullptr != checksum &&
            has_valid_checksum(buffer.data(), fragment_offset, aligned_fragment_length, *checksum)) ||
           has_data_in_all_pages_after_straddle(buffer.data(), fragment_offset, aligned_fragment_length);
}

/// `Catalog.readNextChunk` (`Catalog.java:1415`), `pread` in place of
/// `FileChannel.read(ByteBuffer, position)`.
[[nodiscard]] std::int32_t read_next_chunk(int fd, std::span<std::uint8_t> buffer, std::int32_t offset,
                                           std::int32_t limit) noexcept
{
    const std::int32_t capacity = static_cast<std::int32_t>(buffer.size());
    const std::int32_t length = std::max(0, std::min(capacity, limit - offset));
    std::int32_t position = offset;

    while (position - offset < length)
    {
        const ssize_t bytes_read = ::pread(fd, buffer.data() + (position - offset),
                                           static_cast<std::size_t>(length - (position - offset)), position);
        if (bytes_read <= 0)
        {
            break;
        }
        position += static_cast<std::int32_t>(bytes_read);
    }

    return position - offset;
}

/// `SemanticVersion.toString` (`org.agrona.SemanticVersion`).
[[nodiscard]] std::string semantic_version_to_string(std::int32_t version)
{
    return std::to_string((version >> 16) & 0xFFFF) + "." + std::to_string((version >> 8) & 0xFF) + "." +
           std::to_string(version & 0xFF);
}

/// Reference `"ERROR - " + message` is the control-protocol envelope (A2a); the
/// catalog keeps the message itself, which is what `CatalogTest` compares.
[[nodiscard]] axc::core::Error message_error(axc::core::ErrorCode code, std::string_view message)
{
    return axc::core::make_error(code, message);
}

/// Production clock, shared by every catalog that was not given one.
[[nodiscard]] const SystemEpochClock& system_clock() noexcept
{
    static const SystemEpochClock clock;
    return clock;
}

/// `Catalog.recoverStopOffset` (`Catalog.java:1333`): walk the segment file
/// frame by frame and return the offset just past the last complete frame.
[[nodiscard]] std::expected<std::int32_t, axc::core::Error> recover_stop_offset(
    std::string_view archive_dir, const std::string& segment_file, std::int32_t offset,
    std::int32_t segment_file_length, const Catalog::TruncateOnPageStraddle& truncate_on_page_straddle,
    const Checksum* checksum, std::span<std::uint8_t> buffer)
{
    const std::string path = path_in(archive_dir, segment_file);
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "open segment file failed", errno));
    }

    struct stat info{};
    if (0 != ::fstat(fd, &info))
    {
        const int error = errno;
        ::close(fd);
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "fstat segment file failed", error));
    }

    const auto offset_limit = static_cast<std::int32_t>(std::min<std::int64_t>(segment_file_length, info.st_size));

    std::array<std::uint8_t, 4> frame_length_buffer{};

    std::int32_t next_fragment_offset = offset;
    std::int32_t last_fragment_length = 0;
    std::int32_t buffer_offset = 0;
    bool stopped = false;

    while (next_fragment_offset < offset_limit)
    {
        const std::int32_t bytes_read = read_next_chunk(fd, buffer, next_fragment_offset, offset_limit);
        buffer_offset = 0;

        while (buffer_offset < bytes_read)
        {
            const std::int32_t frame_length = frame_length_at(buffer.data(), buffer_offset);
            if (frame_length <= 0)
            {
                stopped = true;
                break;
            }

            last_fragment_length = align_up(frame_length, FRAME_ALIGNMENT);
            next_fragment_offset += last_fragment_length;
            buffer_offset += last_fragment_length;
        }

        if (stopped)
        {
            break;
        }

        const std::int32_t last_fragment_offset = next_fragment_offset - last_fragment_length;
        // Peek at the next frame length, preserving a page-straddling candidate
        // in the recovery buffer for validation (`Catalog.java:1372-1385`).
        if (next_fragment_offset < offset_limit && last_fragment_length > 0 &&
            Catalog::fragment_straddles_page_boundary(last_fragment_offset, last_fragment_length) &&
            4 == read_next_chunk(fd, frame_length_buffer, next_fragment_offset, next_fragment_offset + 4) &&
            read_value<std::int32_t>(frame_length_buffer.data()) <= 0)
        {
            break;
        }
    }

    const std::int32_t last_fragment_offset = next_fragment_offset - last_fragment_length;
    if (Catalog::fragment_straddles_page_boundary(last_fragment_offset, last_fragment_length) &&
        !is_valid_fragment(buffer, buffer_offset - last_fragment_length, last_fragment_length, checksum))
    {
        if (!truncate_on_page_straddle(path))
        {
            ::close(fd);
            return std::unexpected(message_error(
                axc::core::ErrorCode::protocol_error,
                "Found potentially incomplete last fragment straddling page boundary in file: " +
                    std::filesystem::absolute(path).string() + "\nRun `ArchiveTool verify` for corrective action!"));
        }

        if (0 != ::ftruncate(fd, last_fragment_offset))
        {
            const int error = errno;
            ::close(fd);
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "segment truncate failed", error));
        }

        const std::uint8_t zero = 0;
        if (1 != ::pwrite(fd, &zero, 1, static_cast<off_t>(segment_file_length) - 1))
        {
            const int error = errno;
            ::close(fd);
            return std::unexpected(axc::core::make_error(
                axc::core::ErrorCode::io_error, "Failed to write single byte to set segment file length", error));
        }

        ::close(fd);
        return last_fragment_offset;
    }

    ::close(fd);
    return next_fragment_offset;
}

}  // namespace

namespace detail
{

std::expected<void, axc::core::Error> retry_force(const ForceFn& force, const ReopenFn& reopen, int max_attempts)
{
    // `Catalog.fsyncInterruptSafe` (`Catalog.java:1292-1313`): on a channel
    // closed by an interrupt, reopen it and try again, up to
    // `MAX_CLOSE_FSYNC_ATTEMPTS`; anything else is rethrown.
    for (int attempt = 1;; attempt++)
    {
        const int error = force();
        if (0 == error)
        {
            return {};
        }
        if (EINTR != error)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "force failed", error));
        }
        if (attempt >= max_attempts)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "force failed", error));
        }
        if (-1 == reopen())
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "reopen for force failed", errno));
        }
    }
}

}  // namespace detail

Catalog::Catalog(std::string archive_dir)
    : archive_dir_(std::move(archive_dir)), catalog_file_(path_in(archive_dir_, CATALOG_FILE_NAME))
{
}

Catalog::Catalog(Catalog&& other) noexcept
    : archive_dir_(std::move(other.archive_dir_)),
      catalog_file_(std::move(other.catalog_file_)),
      mapping_(std::move(other.mapping_)),
      sync_fd_(other.sync_fd_),
      capacity_(other.capacity_),
      alignment_(other.alignment_),
      first_recording_descriptor_offset_(other.first_recording_descriptor_offset_),
      next_recording_id_(other.next_recording_id_),
      next_recording_descriptor_offset_(other.next_recording_descriptor_offset_),
      version_(other.version_),
      force_writes_(other.force_writes_),
      force_metadata_(other.force_metadata_),
      is_closed_(other.is_closed_),
      writable_(other.writable_),
      archive_dir_fd_(other.archive_dir_fd_),
      clock_(other.clock_),
      checksum_(other.checksum_),
      index_(std::move(other.index_)),
      owned_recovery_buffer_(std::move(other.owned_recovery_buffer_)),
      external_recovery_buffer_(other.external_recovery_buffer_),
      force_count_(other.force_count_)
{
    // The moved-from catalog must not close the file descriptor the new one now
    // owns, so it is left looking closed.
    other.sync_fd_ = -1;
    other.is_closed_ = true;
}

Catalog& Catalog::operator=(Catalog&& other) noexcept
{
    if (this != &other)
    {
        close_quietly();
        archive_dir_ = std::move(other.archive_dir_);
        catalog_file_ = std::move(other.catalog_file_);
        mapping_ = std::move(other.mapping_);
        sync_fd_ = other.sync_fd_;
        capacity_ = other.capacity_;
        alignment_ = other.alignment_;
        first_recording_descriptor_offset_ = other.first_recording_descriptor_offset_;
        next_recording_id_ = other.next_recording_id_;
        next_recording_descriptor_offset_ = other.next_recording_descriptor_offset_;
        version_ = other.version_;
        force_writes_ = other.force_writes_;
        force_metadata_ = other.force_metadata_;
        is_closed_ = other.is_closed_;
        writable_ = other.writable_;
        archive_dir_fd_ = other.archive_dir_fd_;
        clock_ = other.clock_;
        checksum_ = other.checksum_;
        index_ = std::move(other.index_);
        owned_recovery_buffer_ = std::move(other.owned_recovery_buffer_);
        external_recovery_buffer_ = other.external_recovery_buffer_;
        force_count_ = other.force_count_;

        other.sync_fd_ = -1;
        other.is_closed_ = true;
    }
    return *this;
}

Catalog::~Catalog()
{
    close_quietly();
}

void Catalog::close_quietly() noexcept
{
    if (!is_closed_)
    {
        is_closed_ = true;
        if (mapping_.valid())
        {
            mapping_ = axc::core::MappedFile{};
        }
        if (sync_fd_ >= 0)
        {
            ::close(sync_fd_);
            sync_fd_ = -1;
        }
    }
}

std::expected<Catalog, axc::core::Error> Catalog::create(const CatalogOptions& options)
{
    Catalog catalog{options.archive_dir};
    catalog.archive_dir_fd_ = options.archive_dir_fd;
    catalog.force_writes_ = options.file_sync_level > 0;
    catalog.force_metadata_ = options.file_sync_level > 1;
    catalog.clock_ = nullptr != options.clock ? options.clock : &system_clock();
    catalog.checksum_ = options.checksum;
    catalog.writable_ = true;
    catalog.external_recovery_buffer_ = options.recovery_buffer;

    if (auto result = catalog.init_create(options); !result)
    {
        catalog.close_quietly();
        return std::unexpected(result.error());
    }
    return catalog;
}

std::expected<Catalog, axc::core::Error> Catalog::open(const CatalogOpenOptions& options)
{
    Catalog catalog{options.archive_dir};
    catalog.force_writes_ = false;
    catalog.force_metadata_ = false;
    catalog.clock_ = nullptr != options.clock ? options.clock : &system_clock();
    catalog.checksum_ = options.checksum;
    catalog.writable_ = options.writable;

    if (auto result = catalog.init_open(options); !result)
    {
        catalog.close_quietly();
        return std::unexpected(result.error());
    }
    return catalog;
}

std::expected<void, axc::core::Error> Catalog::init_create(const CatalogOptions& options)
{
    if (options.catalog_capacity < MIN_CAPACITY || options.catalog_capacity > MAX_CATALOG_LENGTH)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::invalid_argument,
                          "Invalid catalog capacity provided: expected value >= " + std::to_string(MIN_CAPACITY) +
                              ", got " + std::to_string(options.catalog_capacity)));
    }

    const bool catalog_exists = axc::core::file_exists(catalog_file_);
    std::int64_t file_capacity = 0;
    if (catalog_exists)
    {
        auto size = axc::core::file_size(catalog_file_);
        if (!size)
        {
            return std::unexpected(size.error());
        }
        file_capacity = static_cast<std::int64_t>(*size);
    }

    capacity_ = std::max(file_capacity, options.catalog_capacity);

    auto mapping = axc::core::MappedFile::create(catalog_file_, static_cast<std::size_t>(capacity_));
    if (!mapping)
    {
        return std::unexpected(mapping.error());
    }
    mapping_ = std::move(*mapping);

    sync_fd_ = ::open(catalog_file_.c_str(), O_RDWR);
    if (sync_fd_ < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "open archive.catalog failed", errno));
    }

    CatalogHeader header;
    header.wrapForDecode(reinterpret_cast<char*>(base()), 0, CatalogHeader::SBE_BLOCK_LENGTH,
                         CatalogHeader::SBE_SCHEMA_VERSION, static_cast<std::uint64_t>(capacity_));

    if (catalog_exists)
    {
        const std::int32_t version = header.version();
        version_ = version;
        if (((version >> 16) & 0xFFFF) != MAJOR_VERSION)
        {
            return std::unexpected(message_error(axc::core::ErrorCode::protocol_error,
                                                 "incompatible catalog file version " +
                                                     semantic_version_to_string(version) + ", archive software is " +
                                                     semantic_version_to_string(SEMANTIC_VERSION)));
        }
        alignment_ = header.alignment();
        next_recording_id_ = header.nextRecordingId();
    }
    else
    {
        alignment_ = CACHE_LINE_LENGTH;
        version_ = SEMANTIC_VERSION;
        header.version(SEMANTIC_VERSION)
            .length(CatalogHeader::SBE_BLOCK_LENGTH)
            .nextRecordingId(next_recording_id_)
            .alignment(alignment_);
        if (auto forced = force_directory(); !forced)
        {
            return std::unexpected(forced.error());
        }
    }

    first_recording_descriptor_offset_ = CatalogHeader::SBE_BLOCK_LENGTH;

    if (auto indexed = build_index(true); !indexed)
    {
        return std::unexpected(indexed.error());
    }
    return refresh_catalog(true, checksum_, recovery_buffer());
}

std::expected<void, axc::core::Error> Catalog::init_open(const CatalogOpenOptions& options)
{
    if (options.catalog_capacity < MIN_CAPACITY || options.catalog_capacity > MAX_CATALOG_LENGTH)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::invalid_argument,
                          "Invalid catalog capacity provided: expected value >= " + std::to_string(MIN_CAPACITY) +
                              ", got " + std::to_string(options.catalog_capacity)));
    }
    if (!axc::core::file_exists(catalog_file_))
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::not_found, "no archive.catalog in archive directory"));
    }

    auto size = axc::core::file_size(catalog_file_);
    if (!size)
    {
        return std::unexpected(size.error());
    }
    capacity_ = std::max(static_cast<std::int64_t>(*size), options.catalog_capacity);

    if (options.writable)
    {
        auto mapping = axc::core::MappedFile::create(catalog_file_, static_cast<std::size_t>(capacity_));
        if (!mapping)
        {
            return std::unexpected(mapping.error());
        }
        mapping_ = std::move(*mapping);
    }
    else
    {
        auto mapping = axc::core::MappedFile::open(catalog_file_, false);
        if (!mapping)
        {
            return std::unexpected(mapping.error());
        }
        mapping_ = std::move(*mapping);
        // A read-only open cannot extend the file, so the mapping is the
        // capacity (`Catalog.java:275-277` maps `max(size, requested)` and would
        // fail on a file shorter than that).
        capacity_ = static_cast<std::int64_t>(mapping_.length());
        if (capacity_ < options.catalog_capacity)
        {
            return std::unexpected(
                message_error(axc::core::ErrorCode::io_error,
                              "catalog file is shorter than the requested capacity: " + std::to_string(capacity_)));
        }
    }

    if (options.writable)
    {
        sync_fd_ = ::open(catalog_file_.c_str(), O_RDWR);
        if (sync_fd_ < 0)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::io_error, "open archive.catalog failed", errno));
        }
    }

    CatalogHeader header;
    header.wrapForDecode(reinterpret_cast<char*>(base()), 0, CatalogHeader::SBE_BLOCK_LENGTH,
                         CatalogHeader::SBE_SCHEMA_VERSION, static_cast<std::uint64_t>(capacity_));

    const std::int32_t version = header.version();
    version_ = version;
    if (!options.version_check)
    {
        if (((version >> 16) & 0xFFFF) != MAJOR_VERSION)
        {
            return std::unexpected(message_error(axc::core::ErrorCode::protocol_error,
                                                 "incompatible catalog file version " +
                                                     semantic_version_to_string(version) + ", archive software is " +
                                                     semantic_version_to_string(SEMANTIC_VERSION)));
        }
    }
    else
    {
        options.version_check(version);
    }

    const std::int32_t alignment = header.alignment();
    if (0 != alignment)
    {
        alignment_ = alignment;
        first_recording_descriptor_offset_ = CatalogHeader::SBE_BLOCK_LENGTH;
        next_recording_id_ = header.nextRecordingId();
    }
    else
    {
        alignment_ = DEFAULT_ALIGNMENT;
        first_recording_descriptor_offset_ = DEFAULT_ALIGNMENT;
    }

    if (auto indexed = build_index(options.writable); !indexed)
    {
        return std::unexpected(indexed.error());
    }
    return refresh_catalog(false, nullptr, {});
}

std::expected<void, axc::core::Error> Catalog::close()
{
    if (is_closed_)
    {
        return {};
    }
    is_closed_ = true;
    return unmap_and_close();
}

std::expected<void, axc::core::Error> Catalog::unmap_and_close()
{
    std::expected<void, axc::core::Error> sync_result{};
    if (mapping_.valid())
    {
        // `MappedByteBuffer.force()` (`Catalog.java:1263`).
        sync_result = mapping_.flush();
        mapping_ = axc::core::MappedFile{};
    }

    if (sync_fd_ < 0)
    {
        return sync_result;
    }

    // `fsyncInterruptSafe(catalogChannel)` is reached for *every* sync level:
    // the catalog channel is the mapping channel and is always open, and both
    // `force` arguments are literals there (`Catalog.java:1257`, `:1296`).
    ++force_count_;
    int fd = sync_fd_;
    auto force_result = detail::retry_force([&fd] { return 0 == ::fsync(fd) ? 0 : errno; },
                                            [&fd, path = catalog_file_]
                                            {
                                                fd = ::open(path.c_str(), O_WRONLY);
                                                return fd;
                                            },
                                            MAX_CLOSE_FSYNC_ATTEMPTS);

    ::close(fd);
    sync_fd_ = -1;

    if (!sync_result)
    {
        return sync_result;
    }
    return force_result;
}

void Catalog::update_version(std::int32_t version) noexcept
{
    version_ = version;
    if (!is_closed_ && mapping_.valid())
    {
        write_value<std::int32_t>(base() + CatalogHeader::versionEncodingOffset(), version);
    }
}

std::expected<void, axc::core::Error> Catalog::build_index(bool writable)
{
    const auto end_offset = static_cast<std::int32_t>(capacity_);
    std::int32_t offset = first_recording_descriptor_offset_;
    std::int64_t recording_id = -1;

    while (offset < end_offset)
    {
        std::span<std::uint8_t> frame;
        const std::int32_t frame_length = wrap_descriptor_at_offset(frame, offset);
        if (frame_length < 0)
        {
            break;
        }

        recording_id = read_value<std::int64_t>(frame.data() + DESCRIPTOR_HEADER_LENGTH +
                                                RecordingDescriptor::recordingIdEncodingOffset());
        const std::int32_t state =
            read_value<std::int32_t>(frame.data() + RecordingDescriptorHeader::stateEncodingOffset());
        if (RecordingState::VALID == state)
        {
            index_.add(recording_id, offset);
        }

        offset += frame_length;
    }

    next_recording_descriptor_offset_ = offset;

    if (0 == next_recording_id_)
    {
        next_recording_id_ = recording_id + 1;
    }
    else if (writable && next_recording_id_ < recording_id + 1)
    {
        return std::unexpected(message_error(axc::core::ErrorCode::protocol_error,
                                             "invalid nextRecordingId: expected value greater or equal to " +
                                                 std::to_string(recording_id + 1) + ", was " +
                                                 std::to_string(next_recording_id_)));
    }

    return {};
}

std::expected<void, axc::core::Error> Catalog::refresh_catalog(bool fix_on_refresh, const Checksum* checksum,
                                                               std::span<std::uint8_t> buffer)
{
    if (!fix_on_refresh)
    {
        return {};
    }

    std::span<std::uint8_t> scratch = buffer;
    if (scratch.empty())
    {
        owned_recovery_buffer_.assign(static_cast<std::size_t>(FILE_IO_MAX_LENGTH_DEFAULT), 0);
        scratch = std::span<std::uint8_t>(owned_recovery_buffer_);
    }

    auto segment_files = server::index_segment_files(archive_dir_);
    if (!segment_files)
    {
        return std::unexpected(segment_files.error());
    }

    std::expected<void, axc::core::Error> result{};
    for_each(
        [&](CatalogEntry& entry)
        {
            if (!result)
            {
                return;
            }
            result = refresh_and_fix_descriptor(entry, *segment_files, checksum, scratch);
        });
    return result;
}

std::expected<void, axc::core::Error> Catalog::refresh_and_fix_descriptor(
    CatalogEntry& entry, const std::unordered_map<std::int64_t, std::vector<std::string>>& segment_files,
    const Checksum* checksum, std::span<std::uint8_t> buffer)
{
    const std::int64_t recording_id = entry.descriptor.recordingId();

    // `Catalog.refreshAndFixDescriptor` (`Catalog.java:1069`).
    const std::int32_t state =
        read_value<std::int32_t>(entry.frame.data() + RecordingDescriptorHeader::stateEncodingOffset());
    if (RecordingState::VALID == state && NULL_POSITION == entry.descriptor.stopPosition())
    {
        static const std::vector<std::string> EMPTY;
        const auto found = segment_files.find(recording_id);
        const std::vector<std::string>& files = found != segment_files.end() ? found->second : EMPTY;

        auto max_segment_file = server::find_segment_file_with_highest_position(files);
        if (!max_segment_file)
        {
            return std::unexpected(max_segment_file.error());
        }

        auto stop_position = compute_stop_position(
            archive_dir_, *max_segment_file, entry.descriptor.startPosition(), entry.descriptor.termBufferLength(),
            entry.descriptor.segmentFileLength(), checksum, buffer, [](const std::string&) { return false; });
        if (!stop_position)
        {
            return std::unexpected(stop_position.error());
        }

        entry.descriptor.stopPosition(*stop_position);
        entry.descriptor.stopTimestamp(clock_->time_ms());
    }

    return {};
}

void Catalog::invoke_entry_processor(const CatalogEntryProcessor& processor, std::int32_t offset)
{
    std::span<std::uint8_t> frame;
    if (wrap_descriptor_at_offset(frame, offset) < 0)
    {
        return;
    }

    CatalogEntry entry;
    entry.recording_descriptor_offset = offset;
    entry.frame = frame;
    entry.header.wrapForDecode(reinterpret_cast<char*>(frame.data()), 0, DESCRIPTOR_HEADER_LENGTH,
                               RecordingDescriptorHeader::SBE_SCHEMA_VERSION, frame.size());
    entry.descriptor.wrapForDecode(reinterpret_cast<char*>(frame.data()), DESCRIPTOR_HEADER_LENGTH,
                                   RecordingDescriptor::SBE_BLOCK_LENGTH, RecordingDescriptor::SBE_SCHEMA_VERSION,
                                   frame.size());

    processor(entry);
}

std::int32_t Catalog::recording_descriptor_frame_length(std::string_view stripped_channel,
                                                        std::string_view original_channel,
                                                        std::string_view source_identity) const
{
    // `Catalog.recordingDescriptorFrameLength` (`Catalog.java:1007`): 7 longs and
    // 6 ints in the fixed block, then three length-prefixed strings.
    const auto descriptor_length = static_cast<std::int32_t>(7 * 8 + 6 * 4 + 4 + stripped_channel.size() + 4 +
                                                             original_channel.size() + 4 + source_identity.size());
    return align_up(DESCRIPTOR_HEADER_LENGTH + descriptor_length, alignment_);
}

std::expected<void, axc::core::Error> Catalog::force_writes()
{
    if (sync_fd_ < 0 || !force_writes_)
    {
        return {};
    }
    ++force_count_;
    const int result = force_metadata_ ? ::fsync(sync_fd_) : ::fdatasync(sync_fd_);
    if (0 != result)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "catalog force failed", errno));
    }
    return {};
}

std::expected<void, axc::core::Error> Catalog::force_directory()
{
    if (archive_dir_fd_ < 0 || !force_writes_)
    {
        return {};
    }
    ++force_count_;
    const int result = force_metadata_ ? ::fsync(archive_dir_fd_) : ::fdatasync(archive_dir_fd_);
    if (0 != result)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "archive directory force failed", errno));
    }
    return {};
}

std::expected<std::int64_t, axc::core::Error> Catalog::add_new_recording(
    std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp, std::int64_t stop_timestamp,
    std::int32_t image_initial_term_id, std::int32_t segment_file_length, std::int32_t term_buffer_length,
    std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
    std::string_view original_channel, std::string_view source_identity)
{
    const std::int32_t frame_length =
        recording_descriptor_frame_length(stripped_channel, original_channel, source_identity);
    const std::int32_t recording_descriptor_offset = next_recording_descriptor_offset_;

    if (recording_descriptor_offset + frame_length > capacity_)
    {
        if (auto grown = grow_catalog(MAX_CATALOG_LENGTH, frame_length); !grown)
        {
            return std::unexpected(grown.error());
        }
    }

    const std::int64_t recording_id = next_recording_id_;

    auto* frame_base = base() + recording_descriptor_offset;
    RecordingDescriptor descriptor;
    descriptor.wrapForEncode(reinterpret_cast<char*>(frame_base), DESCRIPTOR_HEADER_LENGTH,
                             static_cast<std::uint64_t>(frame_length));
    descriptor.recordingId(recording_id)
        .startTimestamp(start_timestamp)
        .stopTimestamp(stop_timestamp)
        .startPosition(start_position)
        .stopPosition(stop_position)
        .initialTermId(image_initial_term_id)
        .segmentFileLength(segment_file_length)
        .termBufferLength(term_buffer_length)
        .mtuLength(mtu_length)
        .sessionId(session_id)
        .streamId(stream_id)
        .putStrippedChannel(stripped_channel.data(), static_cast<std::uint32_t>(stripped_channel.size()))
        .putOriginalChannel(original_channel.data(), static_cast<std::uint32_t>(original_channel.size()))
        .putSourceIdentity(source_identity.data(), static_cast<std::uint32_t>(source_identity.size()));

    const std::int32_t recording_length = frame_length - DESCRIPTOR_HEADER_LENGTH;
    RecordingDescriptorHeader header;
    header.wrapForEncode(reinterpret_cast<char*>(frame_base), 0, static_cast<std::uint64_t>(frame_length));
    header.length(recording_length)
        .checksum(compute_recording_descriptor_checksum(recording_descriptor_offset, recording_length))
        .state(RecordingState::VALID);

    CatalogHeader catalog_header;
    catalog_header.wrapForEncode(reinterpret_cast<char*>(base()), 0, static_cast<std::uint64_t>(capacity_));
    catalog_header.nextRecordingId(recording_id + 1);

    if (auto forced = force_writes(); !forced)
    {
        return std::unexpected(forced.error());
    }

    next_recording_id_ = recording_id + 1;
    next_recording_descriptor_offset_ = recording_descriptor_offset + frame_length;
    index_.add(recording_id, recording_descriptor_offset);

    return recording_id;
}

std::expected<std::int64_t, axc::core::Error> Catalog::add_new_recording(
    std::int64_t start_position, std::int64_t start_timestamp, std::int32_t image_initial_term_id,
    std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id,
    std::int32_t stream_id, std::string_view stripped_channel, std::string_view original_channel,
    std::string_view source_identity)
{
    return add_new_recording(start_position, NULL_POSITION, start_timestamp, NULL_TIMESTAMP, image_initial_term_id,
                             segment_file_length, term_buffer_length, mtu_length, session_id, stream_id,
                             stripped_channel, original_channel, source_identity);
}

std::span<std::uint8_t> Catalog::wrap_descriptor(std::int64_t recording_id) const
{
    if (recording_id < 0)
    {
        return {};
    }

    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    if (recording_descriptor_offset < 0)
    {
        return {};
    }

    std::span<std::uint8_t> frame;
    return wrap_descriptor_at_offset(frame, recording_descriptor_offset) > 0 ? frame : std::span<std::uint8_t>{};
}

std::int32_t Catalog::wrap_descriptor_at_offset(std::span<std::uint8_t>& out,
                                                std::int32_t recording_descriptor_offset) const noexcept
{
    const std::int32_t recording_length = read_value<std::int32_t>(base() + recording_descriptor_offset +
                                                                   RecordingDescriptorHeader::lengthEncodingOffset());

    if (recording_length > 0)
    {
        const std::int32_t frame_length = align_up(recording_length + DESCRIPTOR_HEADER_LENGTH, alignment_);
        out = std::span<std::uint8_t>(base() + recording_descriptor_offset, static_cast<std::size_t>(frame_length));
        return frame_length;
    }

    return -1;
}

bool Catalog::has_recording(std::int64_t recording_id) const
{
    return recording_id >= 0 && recording_descriptor_offset(recording_id) > 0;
}

int Catalog::for_each(const CatalogEntryProcessor& processor)
{
    int count = 0;
    std::int32_t offset = first_recording_descriptor_offset_;
    while (offset < next_recording_descriptor_offset_)
    {
        std::span<std::uint8_t> frame;
        const std::int32_t frame_length = wrap_descriptor_at_offset(frame, offset);
        if (frame_length < 0)
        {
            break;
        }

        invoke_entry_processor(processor, offset);

        offset += frame_length;
        count++;
    }
    return count;
}

bool Catalog::for_entry(std::int64_t recording_id, const CatalogEntryProcessor& processor)
{
    if (recording_id < 0)
    {
        return false;
    }

    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    if (recording_descriptor_offset < 0)
    {
        return false;
    }

    std::span<std::uint8_t> frame;
    if (wrap_descriptor_at_offset(frame, recording_descriptor_offset) > 0)
    {
        invoke_entry_processor(processor, recording_descriptor_offset);
        return true;
    }

    return false;
}

std::int64_t Catalog::find_last(std::int64_t min_recording_id, std::int32_t session_id, std::int32_t stream_id,
                                std::span<const std::uint8_t> channel_fragment)
{
    if (min_recording_id < 0)
    {
        return NULL_RECORD_ID;
    }

    const std::span<const std::int64_t> index = index_.index();
    const int last_position = index_.last_position();
    for (int slot = last_position; slot >= 0; slot -= 2)
    {
        const std::int64_t recording_id = index[static_cast<std::size_t>(slot)];
        if (recording_id < min_recording_id)
        {
            break;
        }

        std::span<std::uint8_t> frame;
        if (wrap_descriptor_at_offset(frame, static_cast<std::int32_t>(index[static_cast<std::size_t>(slot) + 1])) < 0)
        {
            continue;
        }

        RecordingDescriptor descriptor;
        descriptor.wrapForDecode(reinterpret_cast<char*>(frame.data()), DESCRIPTOR_HEADER_LENGTH,
                                 RecordingDescriptor::SBE_BLOCK_LENGTH, RecordingDescriptor::SBE_SCHEMA_VERSION,
                                 frame.size());

        if (session_id == descriptor.sessionId() && stream_id == descriptor.streamId() &&
            original_channel_contains(frame, channel_fragment))
        {
            return recording_id;
        }
    }

    return NULL_RECORD_ID;
}

bool Catalog::original_channel_contains(std::span<const std::uint8_t> frame,
                                        std::span<const std::uint8_t> channel_fragment)
{
    const auto fragment_length = static_cast<std::int32_t>(channel_fragment.size());
    if (0 == fragment_length)
    {
        return true;
    }

    const std::uint8_t* buffer = frame.data();
    const std::int32_t stripped_channel_length_offset =
        DESCRIPTOR_HEADER_LENGTH + RecordingDescriptor::SBE_BLOCK_LENGTH;
    const std::int32_t stripped_channel_length = read_value<std::int32_t>(buffer + stripped_channel_length_offset);
    const std::int32_t original_channel_length_offset = stripped_channel_length_offset + 4 + stripped_channel_length;
    const std::int32_t original_channel_length = read_value<std::int32_t>(buffer + original_channel_length_offset);
    const std::int32_t channel_offset = original_channel_length_offset + 4;

    for (std::int32_t end = channel_offset + (original_channel_length - fragment_length), offset = channel_offset;
         offset <= end; offset++)
    {
        bool matches = true;
        for (std::int32_t index = 0; index < fragment_length; index++)
        {
            if (buffer[offset + index] != channel_fragment[static_cast<std::size_t>(index)])
            {
                matches = false;
                break;
            }
        }
        if (matches)
        {
            return true;
        }
    }

    return false;
}

std::expected<void, axc::core::Error> Catalog::recording_stopped(std::int64_t recording_id, std::int64_t position,
                                                                 std::int64_t timestamp_ms)
{
    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    const std::int32_t offset = recording_descriptor_offset + DESCRIPTOR_HEADER_LENGTH;

    write_value<std::int64_t>(base() + offset + RecordingDescriptor::stopTimestampEncodingOffset(), timestamp_ms);
    write_long_volatile(base() + offset + RecordingDescriptor::stopPositionEncodingOffset(), position);
    update_checksum(recording_descriptor_offset);

    return force_writes();
}

std::int64_t Catalog::stop_position(std::int64_t recording_id) const
{
    const std::int32_t offset = recording_descriptor_offset(recording_id) + DESCRIPTOR_HEADER_LENGTH +
                                static_cast<std::int32_t>(RecordingDescriptor::stopPositionEncodingOffset());
    return read_long_volatile(base() + offset);
}

std::expected<void, axc::core::Error> Catalog::stop_position(std::int64_t recording_id, std::int64_t position)
{
    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    const std::int32_t offset = recording_descriptor_offset + DESCRIPTOR_HEADER_LENGTH;

    write_long_volatile(base() + offset + RecordingDescriptor::stopPositionEncodingOffset(), position);
    update_checksum(recording_descriptor_offset);

    return force_writes();
}

std::expected<void, axc::core::Error> Catalog::extend_recording(std::int64_t recording_id,
                                                                std::int64_t control_session_id,
                                                                std::int64_t correlation_id, std::int32_t session_id)
{
    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    const std::int32_t offset = recording_descriptor_offset + DESCRIPTOR_HEADER_LENGTH;

    write_value<std::int64_t>(base() + offset + RecordingDescriptor::controlSessionIdEncodingOffset(),
                              control_session_id);
    write_value<std::int64_t>(base() + offset + RecordingDescriptor::correlationIdEncodingOffset(), correlation_id);
    write_value<std::int64_t>(base() + offset + RecordingDescriptor::stopTimestampEncodingOffset(), NULL_TIMESTAMP);
    write_value<std::int32_t>(base() + offset + RecordingDescriptor::sessionIdEncodingOffset(), session_id);
    write_long_volatile(base() + offset + RecordingDescriptor::stopPositionEncodingOffset(), NULL_POSITION);
    update_checksum(recording_descriptor_offset);

    return force_writes();
}

std::expected<void, axc::core::Error> Catalog::replace_recording(
    std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position_value,
    std::int64_t start_timestamp, std::int64_t stop_timestamp, std::int32_t image_initial_term_id,
    std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id,
    std::int32_t stream_id, std::string_view stripped_channel, std::string_view original_channel,
    std::string_view source_identity)
{
    const std::int32_t recording_offset = recording_descriptor_offset(recording_id);
    if (-1 == recording_offset)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::not_found, "unknown recording id: " + std::to_string(recording_id)));
    }

    const std::int32_t recording_length =
        read_value<std::int32_t>(base() + recording_offset + RecordingDescriptorHeader::lengthEncodingOffset());
    const std::int32_t old_frame_length = align_up(recording_length + DESCRIPTOR_HEADER_LENGTH, alignment_);
    const std::int32_t new_frame_length =
        recording_descriptor_frame_length(stripped_channel, original_channel, source_identity);
    std::int32_t length = 0;
    std::int32_t checksum_length = 0;

    if (new_frame_length > old_frame_length)
    {
        length = new_frame_length - DESCRIPTOR_HEADER_LENGTH;
        checksum_length = length;

        const std::int32_t shift_bytes = new_frame_length - old_frame_length;
        const std::int32_t end_of_last_recording = next_recording_descriptor_offset_;
        if (end_of_last_recording + shift_bytes > capacity_)
        {
            if (auto grown = grow_catalog(MAX_CATALOG_LENGTH, shift_bytes); !grown)
            {
                return std::unexpected(grown.error());
            }
        }

        next_recording_descriptor_offset_ += shift_bytes;

        const int last_position = index_.last_position();
        if (recording_id != index_.index()[static_cast<std::size_t>(last_position)])
        {
            // `shiftDataToTheRight` (`Catalog.java:1019`).
            std::memmove(base() + recording_offset + new_frame_length, base() + recording_offset + old_frame_length,
                         static_cast<std::size_t>(end_of_last_recording - (recording_offset + old_frame_length)));

            // `fixupIndexForShifterRecordings` (`Catalog.java:1029`).
            const std::span<std::int64_t> pairs = index_.mutable_index();
            bool update_offset = false;
            for (int slot = 0; slot <= last_position; slot += 2)
            {
                if (update_offset)
                {
                    pairs[static_cast<std::size_t>(slot) + 1] += shift_bytes;
                }
                else if (recording_id == pairs[static_cast<std::size_t>(slot)])
                {
                    update_offset = true;
                }
            }
        }
    }
    else
    {
        length = old_frame_length - DESCRIPTOR_HEADER_LENGTH;
        checksum_length = new_frame_length - DESCRIPTOR_HEADER_LENGTH;
        std::memset(base() + recording_offset + new_frame_length, 0,
                    static_cast<std::size_t>(old_frame_length - new_frame_length));
    }

    auto* frame_base = base() + recording_offset + DESCRIPTOR_HEADER_LENGTH;
    const auto frame_length = static_cast<std::uint32_t>(length + DESCRIPTOR_HEADER_LENGTH);
    RecordingDescriptor descriptor;
    descriptor.wrapForEncode(reinterpret_cast<char*>(frame_base), 0, frame_length);
    descriptor.recordingId(recording_id)
        .startTimestamp(start_timestamp)
        .stopTimestamp(stop_timestamp)
        .startPosition(start_position)
        .stopPosition(stop_position_value)
        .initialTermId(image_initial_term_id)
        .segmentFileLength(segment_file_length)
        .termBufferLength(term_buffer_length)
        .mtuLength(mtu_length)
        .sessionId(session_id)
        .streamId(stream_id)
        .putStrippedChannel(stripped_channel.data(), static_cast<std::uint32_t>(stripped_channel.size()))
        .putOriginalChannel(original_channel.data(), static_cast<std::uint32_t>(original_channel.size()))
        .putSourceIdentity(source_identity.data(), static_cast<std::uint32_t>(source_identity.size()));

    RecordingDescriptorHeader header;
    header.wrapForEncode(reinterpret_cast<char*>(base() + recording_offset), 0,
                         static_cast<std::uint64_t>(old_frame_length));
    header.length(length)
        .checksum(compute_recording_descriptor_checksum(recording_offset, checksum_length))
        .state(RecordingState::VALID);

    return force_writes();
}

std::expected<void, axc::core::Error> Catalog::replace_recording_channels(std::int64_t recording_id,
                                                                          std::string_view stripped_channel,
                                                                          std::string_view original_channel)
{
    // `UpdateChannelSession.doWork` (`UpdateChannelSession.java:84-127`): every
    // field is read back out of the descriptor and only the two channel values
    // change. `sourceIdentity` is copied first because the write below reuses
    // the same catalog memory.
    std::int64_t start_position_value = 0;
    std::int64_t stop_position_value = 0;
    std::int64_t start_timestamp = 0;
    std::int64_t stop_timestamp = 0;
    std::int32_t initial_term_id = 0;
    std::int32_t segment_file_length = 0;
    std::int32_t term_buffer_length = 0;
    std::int32_t mtu_length = 0;
    std::int32_t session_id = 0;
    std::int32_t stream_id = 0;
    std::string source_identity;

    const bool found = for_entry(recording_id,
                                 [&](CatalogEntry& entry)
                                 {
                                     RecordingDescriptor& descriptor = entry.descriptor;
                                     start_position_value = descriptor.startPosition();
                                     stop_position_value = descriptor.stopPosition();
                                     start_timestamp = descriptor.startTimestamp();
                                     stop_timestamp = descriptor.stopTimestamp();
                                     initial_term_id = descriptor.initialTermId();
                                     segment_file_length = descriptor.segmentFileLength();
                                     term_buffer_length = descriptor.termBufferLength();
                                     mtu_length = descriptor.mtuLength();
                                     session_id = descriptor.sessionId();
                                     stream_id = descriptor.streamId();
                                     // SBE's variable-length getters advance the
                                     // decoder position, so they must be read in
                                     // schema order (`UpdateChannelSession`'s
                                     // `skipStrippedChannel`/`skipOriginalChannel`
                                     // does the same).
                                     (void)descriptor.getStrippedChannelAsString();
                                     (void)descriptor.getOriginalChannelAsString();
                                     source_identity = descriptor.getSourceIdentityAsString();
                                 });
    if (!found)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::not_found, "unknown recording id: " + std::to_string(recording_id)));
    }

    return replace_recording(recording_id, start_position_value, stop_position_value, start_timestamp, stop_timestamp,
                             initial_term_id, segment_file_length, term_buffer_length, mtu_length, session_id,
                             stream_id, stripped_channel, original_channel, source_identity);
}

std::int64_t Catalog::start_position(std::int64_t recording_id) const
{
    const std::int32_t offset = recording_descriptor_offset(recording_id) + DESCRIPTOR_HEADER_LENGTH +
                                static_cast<std::int32_t>(RecordingDescriptor::startPositionEncodingOffset());
    return read_long_volatile(base() + offset);
}

std::expected<void, axc::core::Error> Catalog::start_position(std::int64_t recording_id, std::int64_t position)
{
    const std::int32_t recording_descriptor_offset = this->recording_descriptor_offset(recording_id);
    const std::int32_t offset = recording_descriptor_offset + DESCRIPTOR_HEADER_LENGTH;

    write_value<std::int64_t>(base() + offset + RecordingDescriptor::startPositionEncodingOffset(), position);
    update_checksum(recording_descriptor_offset);

    return force_writes();
}

std::expected<bool, axc::core::Error> Catalog::change_state(std::int64_t recording_id, RecordingState::Value new_state)
{
    if (recording_id >= 0)
    {
        const std::int64_t offset = index_.remove(recording_id);
        if (CatalogIndex::NULL_VALUE != offset)
        {
            write_value<std::int32_t>(
                base() + offset + static_cast<std::int32_t>(RecordingDescriptorHeader::stateEncodingOffset()),
                static_cast<std::int32_t>(new_state));

            if (auto forced = force_writes(); !forced)
            {
                return std::unexpected(forced.error());
            }
            return true;
        }
    }

    return false;
}

RecordingSummary Catalog::recording_summary(std::int64_t recording_id) const
{
    const std::int32_t offset = recording_descriptor_offset(recording_id) + DESCRIPTOR_HEADER_LENGTH;

    RecordingSummary summary;
    summary.recording_id = recording_id;
    summary.start_position =
        read_value<std::int64_t>(base() + offset + RecordingDescriptor::startPositionEncodingOffset());
    summary.stop_position =
        read_value<std::int64_t>(base() + offset + RecordingDescriptor::stopPositionEncodingOffset());
    summary.initial_term_id =
        read_value<std::int32_t>(base() + offset + RecordingDescriptor::initialTermIdEncodingOffset());
    summary.segment_file_length =
        read_value<std::int32_t>(base() + offset + RecordingDescriptor::segmentFileLengthEncodingOffset());
    summary.term_buffer_length =
        read_value<std::int32_t>(base() + offset + RecordingDescriptor::termBufferLengthEncodingOffset());
    summary.mtu_length = read_value<std::int32_t>(base() + offset + RecordingDescriptor::mtuLengthEncodingOffset());
    summary.stream_id = read_value<std::int32_t>(base() + offset + RecordingDescriptor::streamIdEncodingOffset());
    summary.session_id = read_value<std::int32_t>(base() + offset + RecordingDescriptor::sessionIdEncodingOffset());
    return summary;
}

std::int32_t Catalog::descriptor_length(std::span<const std::uint8_t> frame) noexcept
{
    return read_value<std::int32_t>(frame.data() + RecordingDescriptorHeader::lengthEncodingOffset());
}

bool Catalog::is_valid_descriptor(std::span<const std::uint8_t> frame) noexcept
{
    return RecordingState::VALID ==
           read_value<std::int32_t>(frame.data() + RecordingDescriptorHeader::stateEncodingOffset());
}

std::int64_t Catalog::recording_id(std::span<const std::uint8_t> frame) noexcept
{
    return read_value<std::int64_t>(frame.data() + DESCRIPTOR_HEADER_LENGTH +
                                    RecordingDescriptor::recordingIdEncodingOffset());
}

std::int32_t Catalog::recording_descriptor_offset(std::int64_t recording_id) const
{
    const std::int64_t offset = index_.recording_offset(recording_id);
    if (CatalogIndex::NULL_VALUE == offset)
    {
        return -1;
    }
    return static_cast<std::int32_t>(offset);
}

std::expected<void, axc::core::Error> Catalog::grow_catalog(std::int64_t max_catalog_capacity,
                                                            std::int32_t frame_length)
{
    const std::int64_t old_capacity = capacity_;
    const std::int64_t recording_offset = next_recording_descriptor_offset_;
    const std::int64_t target_capacity = recording_offset + frame_length;

    if (target_capacity > max_catalog_capacity)
    {
        if (max_catalog_capacity == old_capacity)
        {
            return std::unexpected(
                message_error(axc::core::ErrorCode::no_capacity,
                              "catalog is full, max capacity reached: " + std::to_string(max_catalog_capacity)));
        }
        return std::unexpected(message_error(axc::core::ErrorCode::no_capacity,
                                             "recording is too big: total recording length is " +
                                                 std::to_string(frame_length) + " bytes, available space is " +
                                                 std::to_string(max_catalog_capacity - recording_offset) + " bytes"));
    }

    std::int64_t new_capacity = old_capacity;
    while (new_capacity < target_capacity)
    {
        new_capacity = std::min(new_capacity + (new_capacity >> 1), max_catalog_capacity);
    }

    // The reference unmaps and remaps (`Catalog.java:874-891`); `MappedFile::grow`
    // ftruncates and `mremap`s, which keeps the same file and the same contents.
    if (auto grown = mapping_.grow(static_cast<std::size_t>(new_capacity)); !grown)
    {
        return std::unexpected(grown.error());
    }

    capacity_ = new_capacity;
    catalog_resized(old_capacity, new_capacity);

    return {};
}

void Catalog::catalog_resized(std::int64_t old_capacity, std::int64_t new_capacity) noexcept
{
    // `ArchiveTracing.traceCatalogResize` (`Catalog.java:925`).
    AXC_TRACE(EVENT_CATALOG_RESIZE, old_capacity, new_capacity);
}

void Catalog::update_checksum(std::int32_t recording_descriptor_offset)
{
    if (nullptr != checksum_)
    {
        const std::int32_t recording_length = read_value<std::int32_t>(
            base() + recording_descriptor_offset + RecordingDescriptorHeader::lengthEncodingOffset());
        const std::int32_t checksum_value =
            checksum_->compute(base(), DESCRIPTOR_HEADER_LENGTH + recording_descriptor_offset, recording_length);
        write_value<std::int32_t>(
            base() + recording_descriptor_offset + RecordingDescriptorHeader::checksumEncodingOffset(), checksum_value);
    }
}

std::int32_t Catalog::compute_recording_descriptor_checksum(std::int32_t recording_descriptor_offset,
                                                            std::int32_t recording_length) const
{
    if (nullptr != checksum_)
    {
        return checksum_->compute(base(), DESCRIPTOR_HEADER_LENGTH + recording_descriptor_offset, recording_length);
    }
    return 0;
}

bool Catalog::fragment_straddles_page_boundary(std::int32_t fragment_offset, std::int32_t fragment_length) noexcept
{
    return (fragment_offset / PAGE_SIZE) != ((fragment_offset + (fragment_length - 1)) / PAGE_SIZE);
}

std::expected<std::int64_t, axc::core::Error> Catalog::compute_stop_position(
    std::string_view archive_dir, const std::string& max_segment_file, std::int64_t start_position,
    std::int32_t term_length, std::int32_t segment_length, const Checksum* checksum, std::span<std::uint8_t> buffer,
    const TruncateOnPageStraddle& truncate_on_page_straddle)
{
    if (max_segment_file.empty())
    {
        return start_position;
    }

    const auto start_term_offset = static_cast<std::int32_t>(start_position & (term_length - 1));
    const std::int64_t start_term_base_position = start_position - start_term_offset;
    auto segment_file_base_position = server::parse_segment_file_position(max_segment_file);
    if (!segment_file_base_position)
    {
        return std::unexpected(segment_file_base_position.error());
    }

    const std::int32_t file_offset = *segment_file_base_position == start_term_base_position ? start_term_offset : 0;
    auto segment_stop_offset = recover_stop_offset(archive_dir, max_segment_file, file_offset, segment_length,
                                                   truncate_on_page_straddle, checksum, buffer);
    if (!segment_stop_offset)
    {
        return std::unexpected(segment_stop_offset.error());
    }

    return std::max(*segment_file_base_position + *segment_stop_offset, start_position);
}

}  // namespace deepmsg::archive::server
