#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "archive/server/catalog_index.h"
#include "archive/server/checksum.h"
#include "archive/server/codecs.h"
#include "archive/server/epoch_clock_source.h"
#include "core/error/error.h"
#include "core/io/mapped_file.h"
#include "io_aeron_archive_codecs/CatalogHeader.h"
#include "io_aeron_archive_codecs/RecordingDescriptor.h"
#include "io_aeron_archive_codecs/RecordingDescriptorHeader.h"
#include "io_aeron_archive_codecs/RecordingState.h"

namespace deepmsg::archive::server
{

/// One descriptor as handed to a `CatalogEntryProcessor`, equivalent to the
/// reference's `(offset, headerEncoder, headerDecoder, descriptorEncoder,
/// descriptorDecoder)` argument list (`Catalog.java:104-109`). The generated
/// codecs expose encode and decode on one class, so header/descriptor can be
/// both read and, on the repair path, written.
struct CatalogEntry
{
    std::int32_t recording_descriptor_offset{0};
    std::span<std::uint8_t> frame{};
    archive_codecs::RecordingDescriptorHeader header{};
    archive_codecs::RecordingDescriptor descriptor{};
};

using CatalogEntryProcessor = std::function<void(CatalogEntry& entry)>;

struct RecordingSummary
{
    std::int64_t recording_id{0};
    std::int64_t start_position{0};
    std::int64_t stop_position{0};
    std::int32_t initial_term_id{0};
    std::int32_t segment_file_length{0};
    std::int32_t term_buffer_length{0};
    std::int32_t mtu_length{0};
    std::int32_t stream_id{0};
    std::int32_t session_id{0};
};

/// `Catalog`'s first constructor (`Catalog.java:154`): create-or-open for
/// writing, with the crash-repair pass.
struct CatalogOptions
{
    /// Directory holding `archive.catalog` (`Archive.Configuration.CATALOG_FILE_NAME`).
    std::string archive_dir;
    /// Directory channel used for the single force when the catalog file is
    /// created (`Catalog.java:229`). -1 means "no channel", which is what the
    /// reference passes when `file.sync.level` is 0.
    int archive_dir_fd{-1};
    int file_sync_level{0};
    std::int64_t catalog_capacity{1024 * 1024};
    /// nullptr ⇒ `SystemEpochClock`.
    const EpochClockSource* clock{nullptr};
    /// nullptr ⇒ descriptor checksums are not computed, as in the reference's
    /// tests that pass a null `Checksum`.
    const Checksum* checksum{nullptr};
    /// Scratch buffer for the crash-recovery scan (`Catalog.refreshCatalog`).
    /// Empty ⇒ a 1 MiB buffer is allocated, the reference default
    /// (`FILE_IO_MAX_LENGTH_DEFAULT`, `Archive.java:301`).
    std::span<std::uint8_t> recovery_buffer{};
};

/// `Catalog`'s second constructor (`Catalog.java:248`): open an existing
/// catalog for reading, or for writing when `writable`, without the repair pass.
struct CatalogOpenOptions
{
    std::string archive_dir;
    std::int64_t catalog_capacity{32};
    bool writable{false};
    /// nullptr ⇒ `SystemEpochClock`.
    const EpochClockSource* clock{nullptr};
    const Checksum* checksum{nullptr};
    /// Reference `IntConsumer versionCheck` (`Catalog.java:254`). Empty ⇒ the
    /// major version is compared against `MAJOR_VERSION`.
    std::function<void(std::int32_t)> version_check{};
};

namespace detail
{

/// `Catalog.fsyncInterruptSafe` (`Catalog.java:1281`, added to 1.54 by
/// `076bd5f8b4`): force the file; when the force fails because the channel was
/// interrupted, reopen it and retry, at most `max_attempts` times.
///
/// `force` returns 0 on success or an errno value; `reopen` returns a new fd or
/// -1. Exposed so the retry criterion can be exercised without fault injection
/// into the kernel.
using ForceFn = std::function<int()>;
using ReopenFn = std::function<int()>;

[[nodiscard]] std::expected<void, axc::core::Error> retry_force(const ForceFn& force, const ReopenFn& reopen,
                                                                int max_attempts);

}  // namespace detail

/// `Catalog` (`Catalog.java:99-1517`): the append-only `archive.catalog` file
/// plus its in-memory id → offset index.
///
/// Disk layout (pinned in `docs/compat.md`):
///   * `CatalogHeader` (32 bytes: version, length, nextRecordingId, alignment),
///   * then variable-length descriptors, each a `RecordingDescriptorHeader`
///     (32 bytes) plus a `RecordingDescriptor`, padded to `alignment` — 64 for
///     files this code creates, 1024 for pre-1.54 files.
///
/// Concurrency: conductor thread only (`impl-archive` §4.3). No synchronisation
/// is performed here; every pointer into the mapping is invalidated by growth.
class Catalog
{
public:
    // `Catalog.java:112-122`.
    static constexpr std::int32_t PAGE_SIZE = 4096;
    static constexpr std::int64_t NULL_RECORD_ID = -1;
    static constexpr std::int32_t DESCRIPTOR_HEADER_LENGTH =
        archive_codecs::RecordingDescriptorHeader::SBE_BLOCK_LENGTH;
    static constexpr std::int32_t DEFAULT_ALIGNMENT = 1024;
    static constexpr std::int64_t MAX_CATALOG_LENGTH = 2147483647;
    static constexpr std::int64_t DEFAULT_CAPACITY = 1024 * 1024;
    static constexpr std::int64_t MIN_CAPACITY = archive_codecs::CatalogHeader::SBE_BLOCK_LENGTH;
    static constexpr int MAX_CLOSE_FSYNC_ATTEMPTS = 3;

    // `AeronArchive.NULL_VALUE` and friends (`AeronArchive.java:96-101`).
    static constexpr std::int64_t NULL_POSITION = -1;
    static constexpr std::int64_t NULL_TIMESTAMP = -1;

    // `ArchiveMarkFile.MAJOR_VERSION` / `SEMANTIC_VERSION` (`ArchiveMarkFile.java:55-72`),
    // composed with Agrona `SemanticVersion.compose`.
    static constexpr std::int32_t MAJOR_VERSION = 3;
    static constexpr std::int32_t SEMANTIC_VERSION = (3 << 16) | (1 << 8) | 0;

    // `Archive.Configuration.CATALOG_FILE_NAME` (`Archive.java:290`).
    static constexpr const char* CATALOG_FILE_NAME = "archive.catalog";

    /// `ArchiveEventCode.CATALOG_RESIZE` (`ArchiveEventCode.java:193`).
    static constexpr std::int32_t EVENT_CATALOG_RESIZE = 37;

    /// `CACHE_LINE_LENGTH`, the alignment of a newly created catalog
    /// (`Catalog.java:221`, Agrona `BitUtil.CACHE_LINE_LENGTH`).
    static constexpr std::int32_t CACHE_LINE_LENGTH = 64;

    [[nodiscard]] static std::expected<Catalog, axc::core::Error> create(const CatalogOptions& options);
    [[nodiscard]] static std::expected<Catalog, axc::core::Error> open(const CatalogOpenOptions& options);

    Catalog() = default;
    Catalog(Catalog&& other) noexcept;
    Catalog& operator=(Catalog&& other) noexcept;
    ~Catalog();

    Catalog(const Catalog&) = delete;
    Catalog& operator=(const Catalog&) = delete;

    /// `Catalog.close` (`Catalog.java:328`): msync, unmap, then force the
    /// catalog file **regardless of the sync level** with the reference's
    /// interrupt-safe retry. Idempotent.
    [[nodiscard]] std::expected<void, axc::core::Error> close();

    [[nodiscard]] bool is_closed() const noexcept
    {
        return is_closed_;
    }

    [[nodiscard]] std::int64_t capacity() const noexcept
    {
        return capacity_;
    }

    [[nodiscard]] std::int32_t alignment() const noexcept
    {
        return alignment_;
    }

    [[nodiscard]] int entry_count() const noexcept
    {
        return index_.size();
    }

    [[nodiscard]] std::int64_t next_recording_id() const noexcept
    {
        return next_recording_id_;
    }

    [[nodiscard]] const CatalogIndex& index() const noexcept
    {
        return index_;
    }

    [[nodiscard]] std::int32_t version() const noexcept
    {
        return version_;
    }

    void update_version(std::int32_t version) noexcept;

    /// Number of forces this object issued (test observable). Counts our own
    /// calls, not the kernel's; see `analysis/cpp/records/a1a.md`.
    [[nodiscard]] std::uint64_t force_count() const noexcept
    {
        return force_count_;
    }

    // `Catalog.java:377-465`.
    [[nodiscard]] std::expected<std::int64_t, axc::core::Error> add_new_recording(
        std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp,
        std::int64_t stop_timestamp, std::int32_t image_initial_term_id, std::int32_t segment_file_length,
        std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id,
        std::string_view stripped_channel, std::string_view original_channel, std::string_view source_identity);

    [[nodiscard]] std::expected<std::int64_t, axc::core::Error> add_new_recording(
        std::int64_t start_position, std::int64_t start_timestamp, std::int32_t image_initial_term_id,
        std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length,
        std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
        std::string_view original_channel, std::string_view source_identity);

    // `Catalog.java:467-501`.
    [[nodiscard]] std::span<std::uint8_t> wrap_descriptor(std::int64_t recording_id) const;
    [[nodiscard]] std::int32_t wrap_descriptor_at_offset(std::span<std::uint8_t>& out,
                                                         std::int32_t offset) const noexcept;
    [[nodiscard]] bool has_recording(std::int64_t recording_id) const;

    // `Catalog.java:503-545`.
    int for_each(const CatalogEntryProcessor& processor);
    bool for_entry(std::int64_t recording_id, const CatalogEntryProcessor& processor);

    // `Catalog.java:547`.
    [[nodiscard]] std::int64_t find_last(std::int64_t min_recording_id, std::int32_t session_id, std::int32_t stream_id,
                                         std::span<const std::uint8_t> channel_fragment);

    /// `Catalog.originalChannelContains` (`Catalog.java:588`). `frame` is the
    /// whole descriptor frame, i.e. the header starts at offset 0, exactly like
    /// the buffer the reference decoder is wrapped around.
    [[nodiscard]] static bool original_channel_contains(std::span<const std::uint8_t> frame,
                                                        std::span<const std::uint8_t> channel_fragment);

    // `Catalog.java:628-801`.
    [[nodiscard]] std::expected<void, axc::core::Error> recording_stopped(std::int64_t recording_id,
                                                                          std::int64_t position,
                                                                          std::int64_t timestamp_ms);

    [[nodiscard]] std::int64_t stop_position(std::int64_t recording_id) const;
    [[nodiscard]] std::expected<void, axc::core::Error> stop_position(std::int64_t recording_id, std::int64_t position);

    [[nodiscard]] std::expected<void, axc::core::Error> extend_recording(std::int64_t recording_id,
                                                                         std::int64_t control_session_id,
                                                                         std::int64_t correlation_id,
                                                                         std::int32_t session_id);

    [[nodiscard]] std::expected<void, axc::core::Error> replace_recording(
        std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position,
        std::int64_t start_timestamp, std::int64_t stop_timestamp, std::int32_t image_initial_term_id,
        std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length,
        std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
        std::string_view original_channel, std::string_view source_identity);

    // `Catalog.java:751-818`.
    [[nodiscard]] std::int64_t start_position(std::int64_t recording_id) const;
    [[nodiscard]] std::expected<void, axc::core::Error> start_position(std::int64_t recording_id,
                                                                       std::int64_t position);

    [[nodiscard]] std::expected<bool, axc::core::Error> change_state(std::int64_t recording_id,
                                                                     archive_codecs::RecordingState::Value new_state);

    [[nodiscard]] RecordingSummary recording_summary(std::int64_t recording_id) const;

    /// `Catalog.descriptorLength` / `isValidDescriptor` / `recordingId`
    /// (`Catalog.java:820-834`). The reference reads these through a wrapped
    /// `UnsafeBuffer`; here the caller hands over the frame it got from
    /// `wrap_descriptor`.
    [[nodiscard]] static std::int32_t descriptor_length(std::span<const std::uint8_t> frame) noexcept;
    [[nodiscard]] static bool is_valid_descriptor(std::span<const std::uint8_t> frame) noexcept;
    [[nodiscard]] static std::int64_t recording_id(std::span<const std::uint8_t> frame) noexcept;

    [[nodiscard]] std::int32_t recording_descriptor_offset(std::int64_t recording_id) const;

    // `Catalog.java:846-928`.
    [[nodiscard]] std::expected<void, axc::core::Error> grow_catalog(std::int64_t max_catalog_capacity,
                                                                     std::int32_t frame_length);
    void update_checksum(std::int32_t recording_descriptor_offset);
    [[nodiscard]] std::int32_t compute_recording_descriptor_checksum(std::int32_t recording_descriptor_offset,
                                                                     std::int32_t recording_length) const;

    // The segment-file naming and parsing helpers live in `segment_files.h`
    // (`Catalog.java:1123-1221`); the repair pass uses them from there.
    [[nodiscard]] static bool fragment_straddles_page_boundary(std::int32_t fragment_offset,
                                                               std::int32_t fragment_length) noexcept;

    /// `Catalog.computeStopPosition` (`Catalog.java:1223`). `truncate_on_page_straddle`
    /// returns false to refuse the truncation; the reference's predicate throws
    /// from there, so the same message comes back as an error here.
    using TruncateOnPageStraddle = std::function<bool(const std::string& path)>;

    [[nodiscard]] static std::expected<std::int64_t, axc::core::Error> compute_stop_position(
        std::string_view archive_dir, const std::string& max_segment_file, std::int64_t start_position,
        std::int32_t term_length, std::int32_t segment_length, const Checksum* checksum, std::span<std::uint8_t> buffer,
        const TruncateOnPageStraddle& truncate_on_page_straddle);

private:
    explicit Catalog(std::string archive_dir);

    [[nodiscard]] std::expected<void, axc::core::Error> init_create(const CatalogOptions& options);
    [[nodiscard]] std::expected<void, axc::core::Error> init_open(const CatalogOpenOptions& options);

    [[nodiscard]] std::expected<void, axc::core::Error> build_index(bool writable);
    [[nodiscard]] std::expected<void, axc::core::Error> refresh_catalog(bool fix_on_refresh, const Checksum* checksum,
                                                                        std::span<std::uint8_t> buffer);
    [[nodiscard]] std::expected<void, axc::core::Error> refresh_and_fix_descriptor(
        CatalogEntry& entry, const std::unordered_map<std::int64_t, std::vector<std::string>>& segment_files,
        const Checksum* checksum, std::span<std::uint8_t> buffer);
    void invoke_entry_processor(const CatalogEntryProcessor& processor, std::int32_t offset);
    [[nodiscard]] std::int32_t recording_descriptor_frame_length(std::string_view stripped_channel,
                                                                 std::string_view original_channel,
                                                                 std::string_view source_identity) const;
    [[nodiscard]] std::expected<void, axc::core::Error> force_writes();
    [[nodiscard]] std::expected<void, axc::core::Error> force_directory();
    void catalog_resized(std::int64_t old_capacity, std::int64_t new_capacity) noexcept;
    void close_quietly() noexcept;
    [[nodiscard]] std::expected<void, axc::core::Error> unmap_and_close();

    [[nodiscard]] std::uint8_t* base() const noexcept
    {
        // The mapping is owned by this object and is always writable unless the
        // catalog was opened read-only, in which case no caller writes through it.
        return const_cast<std::uint8_t*>(mapping_.mapping().data());
    }

    [[nodiscard]] std::span<std::uint8_t> recovery_buffer() noexcept
    {
        return external_recovery_buffer_.empty() ? std::span<std::uint8_t>(owned_recovery_buffer_)
                                                 : external_recovery_buffer_;
    }

    std::string archive_dir_;
    std::string catalog_file_;
    axc::core::MappedFile mapping_;
    int sync_fd_{-1};
    std::int64_t capacity_{0};
    std::int32_t alignment_{0};
    std::int32_t first_recording_descriptor_offset_{0};
    std::int64_t next_recording_id_{0};
    std::int32_t next_recording_descriptor_offset_{0};
    std::int32_t version_{0};
    bool force_writes_{false};
    bool force_metadata_{false};
    bool is_closed_{false};
    bool writable_{false};
    int archive_dir_fd_{-1};
    const EpochClockSource* clock_{nullptr};
    const Checksum* checksum_{nullptr};
    CatalogIndex index_;
    std::vector<std::uint8_t> owned_recovery_buffer_;
    std::span<std::uint8_t> external_recovery_buffer_{};
    std::uint64_t force_count_{0};
};

}  // namespace deepmsg::archive::server
