#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/error/error.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

/// The `recording.log` of a cluster node: the only non-SBE persistent file.
///
/// Transcribed from `RecordingLog.java` at Aeron 1.54.0 (`454da1e4e6`). Record
/// layout and ordering are a byte contract; see `docs/compat.md`.
/// The file has no header, no magic and no version (`RecordingLog.java:47-93`).
class RecordingLog
{
public:
    static constexpr std::string_view FILE_NAME = "recording.log";

    /// `Aeron.NULL_VALUE` and `AeronArchive.NULL_POSITION` are both -1.
    static constexpr std::int64_t NULL_VALUE = -1;
    static constexpr std::int64_t NULL_POSITION = -1;
    static constexpr std::int32_t SERVICE_ID = -1;  // ConsensusModule.Configuration.SERVICE_ID

    static constexpr std::int32_t ENTRY_TYPE_TERM = 0;
    static constexpr std::int32_t ENTRY_TYPE_SNAPSHOT = 1;
    static constexpr std::int32_t ENTRY_TYPE_STANDBY_SNAPSHOT = 2;
    static constexpr std::int32_t ENTRY_TYPE_INVALID_FLAG = 1 << 31;

    static constexpr std::size_t RECORDING_ID_OFFSET = 0;
    static constexpr std::size_t LEADERSHIP_TERM_ID_OFFSET = 8;
    static constexpr std::size_t TERM_BASE_LOG_POSITION_OFFSET = 16;
    static constexpr std::size_t LOG_POSITION_OFFSET = 24;
    static constexpr std::size_t TIMESTAMP_OFFSET = 32;
    static constexpr std::size_t SERVICE_ID_OFFSET = 40;
    static constexpr std::size_t ENTRY_TYPE_OFFSET = 44;
    static constexpr std::size_t ENDPOINT_OFFSET = 48;

    static constexpr std::int32_t MAX_ENTRY_LENGTH = 4096;
    static constexpr std::size_t MAX_ENDPOINT_LENGTH = MAX_ENTRY_LENGTH - ENDPOINT_OFFSET - sizeof(std::int32_t);
    static constexpr std::size_t RECORD_ALIGNMENT = 64;

    /// One entry (`RecordingLog.Entry`). `position` is the byte offset in the
    /// file; `entry_index` is the order in which the entry was first written.
    ///
    /// Reference equality (`Entry.equals`) ignores `archive_endpoint` and
    /// `position`; `operator==` follows it.
    struct Entry
    {
        std::int64_t recording_id{NULL_VALUE};
        std::int64_t leadership_term_id{NULL_VALUE};
        std::int64_t term_base_log_position{NULL_VALUE};
        std::int64_t log_position{NULL_VALUE};
        std::int64_t timestamp{NULL_VALUE};
        std::int32_t service_id{static_cast<std::int32_t>(NULL_VALUE)};
        std::int32_t type{ENTRY_TYPE_TERM};
        std::int32_t entry_index{0};
        bool is_valid{true};
        std::string archive_endpoint{};
        std::int64_t position{NULL_VALUE};

        /// `Entry.length()`: `align(48 [+ 4 + endpoint length], 64)`.
        [[nodiscard]] std::size_t length() const noexcept;

        /// `Entry.toString()` (`RecordingLog.java:337-354`) -- the shape
        /// `ClusterTool recording-log` prints.
        [[nodiscard]] std::string to_string() const;

        [[nodiscard]] bool is_term() const noexcept
        {
            return ENTRY_TYPE_TERM == type;
        }

        [[nodiscard]] bool is_snapshot() const noexcept
        {
            return ENTRY_TYPE_SNAPSHOT == type;
        }

        [[nodiscard]] bool is_standby_snapshot() const noexcept
        {
            return ENTRY_TYPE_STANDBY_SNAPSHOT == type;
        }
    };

    [[nodiscard]] static bool entry_equal(const Entry& lhs, const Entry& rhs) noexcept;

    friend bool operator==(const Entry& lhs, const Entry& rhs) noexcept
    {
        return entry_equal(lhs, rhs);
    }

    struct Snapshot
    {
        std::int64_t recording_id{NULL_VALUE};
        std::int64_t leadership_term_id{NULL_VALUE};
        std::int64_t term_base_log_position{NULL_VALUE};
        std::int64_t log_position{NULL_VALUE};
        std::int64_t timestamp{NULL_VALUE};
        std::int32_t service_id{static_cast<std::int32_t>(NULL_VALUE)};
    };

    struct Log
    {
        std::int64_t recording_id{NULL_VALUE};
        std::int64_t leadership_term_id{NULL_VALUE};
        std::int64_t term_base_log_position{NULL_VALUE};
        std::int64_t log_position{NULL_VALUE};
        std::int64_t start_position{NULL_VALUE};
        std::int64_t stop_position{NULL_VALUE};
        std::int32_t initial_term_id{0};
        std::int32_t term_buffer_length{0};
        std::int32_t mtu_length{0};
        std::int32_t session_id{0};
    };

    struct RecoveryPlan
    {
        std::int64_t last_leadership_term_id{NULL_VALUE};
        std::int64_t last_term_base_log_position{0};
        std::int64_t appended_log_position{0};
        std::int64_t committed_log_position{0};
        std::vector<Snapshot> snapshots{};
        std::optional<Log> log{};
    };

    /// The subset of `AeronArchive.RecordingExtent` that the recovery plan
    /// reads. The lookup returns `std::nullopt` when the archive does not know
    /// the recording id (`AeronArchive.listRecording` returns 0).
    struct RecordingExtent
    {
        std::int64_t start_position{0};
        std::int64_t stop_position{0};
        std::int32_t initial_term_id{0};
        std::int32_t term_buffer_length{0};
        std::int32_t mtu_length{0};
        std::int32_t session_id{0};
    };

    /// Startup/recovery path only; not a hot-path callback.
    using RecordingExtentSource = std::function<std::optional<RecordingExtent>(std::int64_t recording_id)>;

    /// Opens (and optionally creates) `<dir>/recording.log`. A newly created
    /// file syncs the directory; an existing one is reloaded
    /// (`RecordingLog.java:566-594`).
    [[nodiscard]] static std::expected<RecordingLog, Error> open(std::string_view cluster_dir, bool create_new);

    RecordingLog() = default;
    RecordingLog(const RecordingLog&) = delete;
    RecordingLog& operator=(const RecordingLog&) = delete;
    RecordingLog(RecordingLog&& other) noexcept;
    RecordingLog& operator=(RecordingLog&& other) noexcept;
    ~RecordingLog();

    /// `FileChannel.force(metaData = fileSyncLevel > 1)` when level > 0.
    [[nodiscard]] std::expected<void, Error> force(std::int32_t file_sync_level) noexcept;

    [[nodiscard]] const std::vector<Entry>& entries() const noexcept
    {
        return entries_;
    }

    [[nodiscard]] std::int32_t next_entry_index() const noexcept
    {
        return next_entry_index_;
    }

    /// Re-reads the file, sorts the entries and rebuilds the caches
    /// (`RecordingLog.java:648-700`).
    [[nodiscard]] std::expected<void, Error> reload();

    [[nodiscard]] std::int64_t find_last_term_recording_id() const noexcept;
    [[nodiscard]] const Entry* find_last_term() const noexcept;

    /// Throws `ClusterException("unknown leadershipTermId=N")` in the reference.
    [[nodiscard]] std::expected<const Entry*, Error> get_term_entry(std::int64_t leadership_term_id) const noexcept;
    [[nodiscard]] const Entry* find_term_entry(std::int64_t leadership_term_id) const noexcept;

    [[nodiscard]] const Entry* get_latest_snapshot(std::int32_t service_id) const noexcept;

    [[nodiscard]] std::expected<bool, Error> invalidate_latest_snapshot();

    /// Returns `NULL_VALUE` when the term is unknown (`RecordingLog.java:870-879`).
    [[nodiscard]] std::int64_t get_term_timestamp(std::int64_t leadership_term_id) const noexcept;

    [[nodiscard]] std::expected<RecoveryPlan, Error> create_recovery_plan(const RecordingExtentSource& extent_source,
                                                                          std::int32_t service_count,
                                                                          std::int64_t replicated_recording_id) const;

    [[nodiscard]] bool is_unknown(std::int64_t leadership_term_id) const noexcept;

    [[nodiscard]] std::expected<void, Error> append_term(std::int64_t recording_id, std::int64_t leadership_term_id,
                                                         std::int64_t term_base_log_position, std::int64_t timestamp);

    [[nodiscard]] std::expected<void, Error> append_snapshot(std::int64_t recording_id, std::int64_t leadership_term_id,
                                                             std::int64_t term_base_log_position,
                                                             std::int64_t log_position, std::int64_t timestamp,
                                                             std::int32_t service_id);

    [[nodiscard]] std::expected<void, Error> append_standby_snapshot(
        std::int64_t recording_id, std::int64_t leadership_term_id, std::int64_t term_base_log_position,
        std::int64_t log_position, std::int64_t timestamp, std::int32_t service_id, std::string_view archive_endpoint);

    [[nodiscard]] std::expected<void, Error> commit_log_position(std::int64_t leadership_term_id,
                                                                 std::int64_t log_position);

    /// Package-private in the reference; public here for the ported unit tests
    /// and for `invalidate_latest_snapshot`.
    [[nodiscard]] std::expected<void, Error> invalidate_entry(std::int32_t index);

    /// Package-private in the reference; public here for the ported unit tests.
    [[nodiscard]] std::expected<void, Error> remove_entry(std::int64_t leadership_term_id, std::int32_t entry_index);

    /// Most recent complete standby-snapshot set per archive endpoint
    /// (`RecordingLog.java:1159-1195`). Returned pointers stay valid until the
    /// next mutation/reload.
    [[nodiscard]] std::map<std::string, std::vector<const Entry*>, std::less<>> latest_standby_snapshots(
        std::int32_t service_count) const;

    /// Standby snapshots that are newer than the latest local snapshot for the
    /// same service (`StandbySnapshotReplicator.java:309-332`). Q6 uses this for
    /// the startup warning; it does not replicate anything.
    [[nodiscard]] std::map<std::string, std::vector<const Entry*>, std::less<>> newer_standby_snapshots(
        std::int32_t service_count) const;

    /// Package-private in the reference; public here for the ported unit tests.
    [[nodiscard]] std::optional<std::vector<Snapshot>> find_snapshot_at_or_before_or_lowest(std::int64_t log_position,
                                                                                            std::int32_t service_count);

    [[nodiscard]] std::expected<void, Error> ensure_coherent(std::int64_t recording_id,
                                                             std::int64_t initial_log_leadership_term_id,
                                                             std::int64_t initial_term_base_log_position,
                                                             std::int64_t leadership_term_id,
                                                             std::int64_t term_base_log_position,
                                                             std::int64_t log_position, std::int64_t now_ns,
                                                             std::int64_t timestamp, std::int32_t file_sync_level);

    /// Package-private in the reference; public here for the ported unit tests.
    [[nodiscard]] std::expected<std::int32_t, Error> append(std::int32_t entry_type, std::int64_t recording_id,
                                                            std::int64_t leadership_term_id,
                                                            std::int64_t term_base_log_position,
                                                            std::int64_t log_position, std::int64_t timestamp,
                                                            std::int32_t service_id, std::string_view endpoint);

    [[nodiscard]] static std::string_view type_as_string(std::int32_t type) noexcept;
    [[nodiscard]] static bool is_valid_snapshot(const Entry& entry) noexcept;
    [[nodiscard]] static bool is_invalid_snapshot(const Entry& entry) noexcept;
    [[nodiscard]] static bool is_valid_any_snapshot(const Entry& entry) noexcept;

    /// `RecordingLog.addSnapshots` (`RecordingLog.java:1300-1337`); public for
    /// the ported unit test `shouldCorrectlyOrderSnapshots`.
    [[nodiscard]] static std::expected<void, Error> add_snapshots(std::vector<Snapshot>& snapshots,
                                                                  const std::vector<Entry>& entries,
                                                                  std::int32_t service_count,
                                                                  std::int32_t snapshot_index);

    /// `RecordingLog.convertEntryListToSnapshotList` (`:1233-1250`).
    [[nodiscard]] static std::vector<Snapshot> convert_entry_list_to_snapshot_list(std::span<const Entry> entries);

private:
    [[nodiscard]] static bool entry_less(const Entry& lhs, const Entry& rhs) noexcept;
    [[nodiscard]] static constexpr std::size_t align(std::size_t value, std::size_t alignment) noexcept
    {
        return (value + (alignment - 1)) & ~(alignment - 1);
    }

    [[nodiscard]] std::expected<int, Error> capture_entries_from_buffer(std::span<const std::uint8_t> buffer);
    [[nodiscard]] std::expected<Entry, Error> write_entry(Entry entry);
    [[nodiscard]] std::expected<void, Error> persist(std::int64_t position, std::span<const std::uint8_t> bytes);
    [[nodiscard]] std::expected<void, Error> write_field(std::int64_t entry_position, std::size_t offset,
                                                         std::span<const std::uint8_t> bytes);

    [[nodiscard]] std::expected<bool, Error> restore_invalid_snapshot(std::int32_t snapshot_entry_type,
                                                                      std::int64_t recording_id,
                                                                      std::int64_t leadership_term_id,
                                                                      std::int64_t term_base_log_position,
                                                                      std::int64_t log_position, std::int64_t timestamp,
                                                                      std::int32_t service_id);

    [[nodiscard]] std::expected<void, Error> validate_term_recording_id(std::int64_t recording_id);
    [[nodiscard]] static bool matches_entry(const Entry& entry, std::int64_t leadership_term_id,
                                            std::int64_t term_base_log_position, std::int64_t log_position,
                                            std::int32_t service_id) noexcept;

    [[nodiscard]] static bool validate_snapshot_entries(const std::vector<Entry>& entries, std::int32_t service_count,
                                                        std::int32_t snapshot_index,
                                                        const Entry& consensus_module_entry) noexcept;

    // `Long2LongHashMap` layout is not needed here (recording.log is not a byte
    // contract for hash iteration order), but the Agrona map keeps the port
    // honest and matches the reference's O(1) term lookup. A sorted vector is
    // used instead: the term id set is small and append already inserts in
    // order. `cache_index_by_leadership_term_id_` maps term id -> cache index.
    std::map<std::int64_t, std::int32_t> cache_index_by_leadership_term_id_{};

    std::vector<Entry> entries_{};
    std::vector<std::int32_t> invalid_snapshots_{};
    std::int64_t term_recording_id_{NULL_VALUE};
    std::int32_t next_entry_index_{0};
    int fd_{-1};
};

}  // namespace deepmsg::cluster::common
