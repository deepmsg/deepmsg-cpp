#include "cluster/common/recording_log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include "core/io/fs.h"

namespace deepmsg::cluster::common
{

using axc::core::ErrorCode;
using axc::core::make_error;

namespace
{

[[nodiscard]] std::string join_path(std::string_view dir, std::string_view name)
{
    std::string path{dir};
    if (!path.empty() && '/' != path.back())
    {
        path.push_back('/');
    }
    path.append(name);
    return path;
}

/// The reference writes little endian explicitly (`RecordingLog.java:1341-1351`).
[[nodiscard]] std::int32_t get_le32(const std::uint8_t* data) noexcept
{
    return static_cast<std::int32_t>(static_cast<std::uint32_t>(data[0]) | (static_cast<std::uint32_t>(data[1]) << 8) |
                                     (static_cast<std::uint32_t>(data[2]) << 16) |
                                     (static_cast<std::uint32_t>(data[3]) << 24));
}

[[nodiscard]] std::int64_t get_le64(const std::uint8_t* data) noexcept
{
    return static_cast<std::int64_t>(
        static_cast<std::uint64_t>(static_cast<std::uint32_t>(get_le32(data))) |
        (static_cast<std::uint64_t>(static_cast<std::uint32_t>(get_le32(data + 4))) << 32));
}

void put_le32(std::uint8_t* data, std::int32_t value) noexcept
{
    const auto value_u = static_cast<std::uint32_t>(value);
    data[0] = static_cast<std::uint8_t>(value_u);
    data[1] = static_cast<std::uint8_t>(value_u >> 8);
    data[2] = static_cast<std::uint8_t>(value_u >> 16);
    data[3] = static_cast<std::uint8_t>(value_u >> 24);
}

void put_le64(std::uint8_t* data, std::int64_t value) noexcept
{
    const auto value_u = static_cast<std::uint64_t>(value);
    (void)put_le32(data, static_cast<std::int32_t>(value_u));
    (void)put_le32(data + 4, static_cast<std::int32_t>(value_u >> 32));
}

/// `FileChannel.write`/`read` loops until the whole region is transferred.
[[nodiscard]] std::expected<void, Error> pread_full(int fd, std::span<std::uint8_t> bytes, std::int64_t offset) noexcept
{
    std::size_t done = 0;
    while (done < bytes.size())
    {
        const ssize_t read_bytes = ::pread(fd, bytes.data() + done, bytes.size() - done,
                                           static_cast<off_t>(offset + static_cast<std::int64_t>(done)));
        if (read_bytes < 0)
        {
            if (EINTR == errno)
            {
                continue;
            }
            return std::unexpected(make_error(ErrorCode::io_error, "read recording.log failed", errno));
        }
        if (0 == read_bytes)
        {
            return std::unexpected(make_error(ErrorCode::io_error, "unexpected end of recording.log"));
        }
        done += static_cast<std::size_t>(read_bytes);
    }
    return {};
}

[[nodiscard]] std::expected<void, Error> pwrite_full(int fd, std::span<const std::uint8_t> bytes,
                                                     std::int64_t offset) noexcept
{
    std::size_t done = 0;
    while (done < bytes.size())
    {
        const ssize_t written = ::pwrite(fd, bytes.data() + done, bytes.size() - done,
                                         static_cast<off_t>(offset + static_cast<std::int64_t>(done)));
        if (written < 0)
        {
            if (EINTR == errno)
            {
                continue;
            }
            return std::unexpected(make_error(ErrorCode::io_error, "write recording.log failed", errno));
        }
        if (0 == written)
        {
            return std::unexpected(make_error(ErrorCode::io_error, "short write of recording.log"));
        }
        done += static_cast<std::size_t>(written);
    }
    return {};
}

/// `RecordingLog.syncDirectory` ignores failures (`:1683-1692`).
void sync_directory(std::string_view dir) noexcept
{
    const std::string path{dir};
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd >= 0)
    {
        (void)::fsync(fd);
        (void)::close(fd);
    }
}

[[nodiscard]] std::expected<std::int64_t, Error> file_length(int fd) noexcept
{
    struct stat status{};
    if (0 != ::fstat(fd, &status))
    {
        return std::unexpected(make_error(ErrorCode::io_error, "fstat recording.log failed", errno));
    }
    return static_cast<std::int64_t>(status.st_size);
}

}  // namespace

std::size_t RecordingLog::Entry::length() const noexcept
{
    const std::size_t unaligned_length = ENTRY_TYPE_STANDBY_SNAPSHOT == type
                                             ? (ENDPOINT_OFFSET + sizeof(std::int32_t) + archive_endpoint.size())
                                             : ENDPOINT_OFFSET;

    return RecordingLog::align(unaligned_length, RECORD_ALIGNMENT);
}

std::string RecordingLog::Entry::to_string() const
{
    const std::string archive_endpoint_entry =
        ENTRY_TYPE_STANDBY_SNAPSHOT == type ? ", archiveEndpoint=" + archive_endpoint : std::string{};

    return "Entry{recordingId=" + std::to_string(recording_id) +
           ", leadershipTermId=" + std::to_string(leadership_term_id) +
           ", termBaseLogPosition=" + std::to_string(term_base_log_position) +
           ", logPosition=" + std::to_string(log_position) + ", timestamp=" + std::to_string(timestamp) +
           ", serviceId=" + std::to_string(service_id) + ", type=" + std::string(RecordingLog::type_as_string(type)) +
           ", isValid=" + (is_valid ? "true" : "false") + ", entryIndex=" + std::to_string(entry_index) +
           archive_endpoint_entry + '}';
}

bool RecordingLog::entry_equal(const Entry& lhs, const Entry& rhs) noexcept
{
    return lhs.recording_id == rhs.recording_id && lhs.leadership_term_id == rhs.leadership_term_id &&
           lhs.term_base_log_position == rhs.term_base_log_position && lhs.log_position == rhs.log_position &&
           lhs.timestamp == rhs.timestamp && lhs.service_id == rhs.service_id && lhs.type == rhs.type &&
           lhs.entry_index == rhs.entry_index && lhs.is_valid == rhs.is_valid;
}

RecordingLog::RecordingLog(RecordingLog&& other) noexcept
    : cache_index_by_leadership_term_id_(std::move(other.cache_index_by_leadership_term_id_)),
      entries_(std::move(other.entries_)),
      invalid_snapshots_(std::move(other.invalid_snapshots_)),
      term_recording_id_(other.term_recording_id_),
      next_entry_index_(other.next_entry_index_),
      fd_(other.fd_)
{
    other.fd_ = -1;
    other.term_recording_id_ = NULL_VALUE;
    other.next_entry_index_ = 0;
}

RecordingLog& RecordingLog::operator=(RecordingLog&& other) noexcept
{
    if (this != &other)
    {
        if (fd_ >= 0)
        {
            (void)::close(fd_);
        }
        cache_index_by_leadership_term_id_ = std::move(other.cache_index_by_leadership_term_id_);
        entries_ = std::move(other.entries_);
        invalid_snapshots_ = std::move(other.invalid_snapshots_);
        term_recording_id_ = other.term_recording_id_;
        next_entry_index_ = other.next_entry_index_;
        fd_ = other.fd_;

        other.fd_ = -1;
        other.term_recording_id_ = NULL_VALUE;
        other.next_entry_index_ = 0;
    }
    return *this;
}

RecordingLog::~RecordingLog()
{
    if (fd_ >= 0)
    {
        (void)::close(fd_);
    }
}

std::expected<RecordingLog, Error> RecordingLog::open(std::string_view cluster_dir, bool create_new)
{
    const std::string path = join_path(cluster_dir, FILE_NAME);
    const bool is_new_file = !axc::core::file_exists(path);

    const int flags = O_RDWR | (create_new ? O_CREAT : 0) | O_CLOEXEC;
    const int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0)
    {
        return std::unexpected(make_error(
            ErrorCode::io_error, create_new ? "open/create recording.log failed" : "open recording.log failed", errno));
    }

    RecordingLog log;
    log.fd_ = fd;

    if (is_new_file)
    {
        sync_directory(cluster_dir);
    }
    else
    {
        auto reloaded = log.reload();
        if (!reloaded)
        {
            return std::unexpected(reloaded.error());
        }
    }

    return log;
}

std::expected<void, Error> RecordingLog::force(std::int32_t file_sync_level) noexcept
{
    if (file_sync_level > 0)
    {
        const int result = file_sync_level > 1 ? ::fsync(fd_) : ::fdatasync(fd_);
        if (0 != result)
        {
            return std::unexpected(make_error(ErrorCode::io_error, "force recording.log failed", errno));
        }
    }
    return {};
}

std::expected<void, Error> RecordingLog::reload()
{
    entries_.clear();
    cache_index_by_leadership_term_id_.clear();
    invalid_snapshots_.clear();
    next_entry_index_ = 0;

    auto length = file_length(fd_);
    if (!length)
    {
        return std::unexpected(length.error());
    }

    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(*length));
    if (!bytes.empty())
    {
        auto read = pread_full(fd_, bytes, 0);
        if (!read)
        {
            return std::unexpected(read.error());
        }
    }

    auto captured = capture_entries_from_buffer(bytes);
    if (!captured)
    {
        return std::unexpected(captured.error());
    }

    std::stable_sort(entries_.begin(), entries_.end(), &RecordingLog::entry_less);

    for (std::int32_t i = 0; i < static_cast<std::int32_t>(entries_.size()); ++i)
    {
        const Entry& entry = entries_[static_cast<std::size_t>(i)];
        if (entry.is_term() && entry.is_valid)
        {
            cache_index_by_leadership_term_id_[entry.leadership_term_id] = i;
        }

        if (!entry.is_valid && (entry.is_snapshot() || entry.is_standby_snapshot()))
        {
            invalid_snapshots_.push_back(i);
        }
    }

    return {};
}

std::expected<int, Error> RecordingLog::capture_entries_from_buffer(std::span<const std::uint8_t> buffer)
{
    std::size_t consumed = 0;
    const std::size_t length = buffer.size();

    while (consumed + ENDPOINT_OFFSET < length)
    {
        const std::int32_t entry_type = get_le32(buffer.data() + consumed + ENTRY_TYPE_OFFSET);
        const std::int32_t type = entry_type & ~ENTRY_TYPE_INVALID_FLAG;
        const bool is_valid = 0 == (entry_type & ENTRY_TYPE_INVALID_FLAG);
        const std::size_t endpoint_offset = consumed + ENDPOINT_OFFSET;

        std::string endpoint{};
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == type)
        {
            if (endpoint_offset + sizeof(std::int32_t) > length)
            {
                break;
            }

            const std::int32_t endpoint_length = get_le32(buffer.data() + endpoint_offset);
            if (endpoint_length < 0 ||
                endpoint_offset + sizeof(std::int32_t) + static_cast<std::size_t>(endpoint_length) > length)
            {
                break;
            }

            endpoint.assign(reinterpret_cast<const char*>(buffer.data() + endpoint_offset + sizeof(std::int32_t)),
                            static_cast<std::size_t>(endpoint_length));
        }

        Entry entry;
        entry.recording_id = get_le64(buffer.data() + consumed + RECORDING_ID_OFFSET);
        entry.leadership_term_id = get_le64(buffer.data() + consumed + LEADERSHIP_TERM_ID_OFFSET);
        entry.term_base_log_position = get_le64(buffer.data() + consumed + TERM_BASE_LOG_POSITION_OFFSET);
        entry.log_position = get_le64(buffer.data() + consumed + LOG_POSITION_OFFSET);
        entry.timestamp = get_le64(buffer.data() + consumed + TIMESTAMP_OFFSET);
        entry.service_id = get_le32(buffer.data() + consumed + SERVICE_ID_OFFSET);
        entry.type = type;
        entry.archive_endpoint = std::move(endpoint);
        entry.is_valid = is_valid;
        entry.position = static_cast<std::int64_t>(consumed);
        entry.entry_index = next_entry_index_;
        const std::size_t entry_length = entry.length();

        if (NULL_VALUE != entry_type)
        {
            if (ENTRY_TYPE_TERM == type)
            {
                auto validated = validate_term_recording_id(entry.recording_id);
                if (!validated)
                {
                    return std::unexpected(validated.error());
                }
            }

            entries_.push_back(std::move(entry));
        }

        consumed += entry_length;
        ++next_entry_index_;
    }

    return static_cast<int>(consumed);
}

bool RecordingLog::entry_less(const Entry& lhs, const Entry& rhs) noexcept
{
    if (lhs.leadership_term_id != rhs.leadership_term_id)
    {
        return lhs.leadership_term_id < rhs.leadership_term_id;
    }

    if (lhs.term_base_log_position != rhs.term_base_log_position)
    {
        return lhs.term_base_log_position < rhs.term_base_log_position;
    }

    if (lhs.type != rhs.type)
    {
        if (ENTRY_TYPE_TERM == lhs.type)
        {
            return true;
        }
        if (ENTRY_TYPE_TERM == rhs.type)
        {
            return false;
        }
    }
    else if (ENTRY_TYPE_TERM == lhs.type)
    {
        return lhs.entry_index < rhs.entry_index;
    }

    if (lhs.log_position != rhs.log_position)
    {
        return lhs.log_position < rhs.log_position;
    }

    if (lhs.type != rhs.type)
    {
        return rhs.type < lhs.type;  // reverse entry type order
    }

    if (lhs.service_id != rhs.service_id)
    {
        return rhs.service_id < lhs.service_id;  // reverse serviceId order
    }

    return false;
}

std::int64_t RecordingLog::find_last_term_recording_id() const noexcept
{
    const Entry* last_term = find_last_term();
    return nullptr != last_term ? last_term->recording_id : NULL_VALUE;
}

const RecordingLog::Entry* RecordingLog::find_last_term() const noexcept
{
    for (std::size_t i = entries_.size(); i-- > 0;)
    {
        const Entry& entry = entries_[i];
        if (entry.is_term() && entry.is_valid)
        {
            return &entry;
        }
    }

    return nullptr;
}

std::expected<const RecordingLog::Entry*, Error> RecordingLog::get_term_entry(
    std::int64_t leadership_term_id) const noexcept
{
    const Entry* entry = find_term_entry(leadership_term_id);
    if (nullptr == entry)
    {
        return std::unexpected(
            make_error(ErrorCode::not_found, "unknown leadershipTermId=" + std::to_string(leadership_term_id)));
    }
    return entry;
}

const RecordingLog::Entry* RecordingLog::find_term_entry(std::int64_t leadership_term_id) const noexcept
{
    const auto found = cache_index_by_leadership_term_id_.find(leadership_term_id);
    if (cache_index_by_leadership_term_id_.end() == found)
    {
        return nullptr;
    }

    return &entries_[static_cast<std::size_t>(found->second)];
}

const RecordingLog::Entry* RecordingLog::get_latest_snapshot(std::int32_t service_id) const noexcept
{
    for (std::size_t i = entries_.size(); i-- > 0;)
    {
        const Entry& entry = entries_[i];
        if (is_valid_snapshot(entry) && SERVICE_ID == entry.service_id)
        {
            if (SERVICE_ID == service_id)
            {
                return &entry;
            }

            const auto service_snapshot_index = static_cast<std::int32_t>(i) - (service_id + 1);
            if (service_snapshot_index >= 0)
            {
                const Entry& snapshot = entries_[static_cast<std::size_t>(service_snapshot_index)];
                if (is_valid_snapshot(snapshot) && service_id == snapshot.service_id)
                {
                    return &snapshot;
                }
            }
        }
    }

    return nullptr;
}

std::expected<bool, Error> RecordingLog::invalidate_latest_snapshot()
{
    std::vector<std::int32_t> indices{};
    std::int64_t high_log_position = NULL_POSITION;

    for (std::int32_t idx = static_cast<std::int32_t>(entries_.size()) - 1; idx >= 0; --idx)
    {
        const Entry& entry = entries_[static_cast<std::size_t>(idx)];
        if (is_valid_any_snapshot(entry) && SERVICE_ID == entry.service_id)
        {
            if (entry.log_position >= high_log_position)
            {
                if (cache_index_by_leadership_term_id_.end() ==
                    cache_index_by_leadership_term_id_.find(entry.leadership_term_id))
                {
                    return std::unexpected(make_error(
                        ErrorCode::not_found,
                        "no matching term for snapshot: leadershipTermId=" + std::to_string(entry.leadership_term_id)));
                }

                if (entry.log_position > high_log_position)
                {
                    indices.clear();
                    high_log_position = entry.log_position;
                }

                indices.push_back(idx);
            }
            else
            {
                // found an earlier snapshot, so stop searching through the cache
                break;
            }
        }
    }

    const bool found = !indices.empty();

    while (!indices.empty())
    {
        const std::int32_t starting_index = indices.back();
        indices.pop_back();

        std::int32_t service_id = SERVICE_ID;

        for (std::int32_t idx = starting_index; idx >= 0; --idx)
        {
            const Entry& entry = entries_[static_cast<std::size_t>(idx)];
            if (is_valid_any_snapshot(entry) && entry.service_id == service_id)
            {
                auto invalidated = invalidate_entry(idx);
                if (!invalidated)
                {
                    return std::unexpected(invalidated.error());
                }
                ++service_id;
            }
            else
            {
                break;
            }
        }
    }

    return found;
}

std::int64_t RecordingLog::get_term_timestamp(std::int64_t leadership_term_id) const noexcept
{
    const auto found = cache_index_by_leadership_term_id_.find(leadership_term_id);
    if (cache_index_by_leadership_term_id_.end() != found)
    {
        return entries_[static_cast<std::size_t>(found->second)].timestamp;
    }

    return NULL_VALUE;
}

std::expected<RecordingLog::RecoveryPlan, Error> RecordingLog::create_recovery_plan(
    const RecordingExtentSource& extent_source, std::int32_t service_count, std::int64_t replicated_recording_id) const
{
    RecoveryPlan plan{};
    std::optional<Log> log{};

    if (entries_.empty())
    {
        if (NULL_VALUE != replicated_recording_id)
        {
            std::optional<RecordingExtent> extent = extent_source(replicated_recording_id);
            if (!extent.has_value())
            {
                return std::unexpected(make_error(ErrorCode::not_found,
                                                  "unknown recording id: " + std::to_string(replicated_recording_id)));
            }

            log = Log{replicated_recording_id,
                      NULL_VALUE,
                      0,
                      0,
                      extent->start_position,
                      extent->stop_position,
                      extent->initial_term_id,
                      extent->term_buffer_length,
                      extent->mtu_length,
                      extent->session_id};
        }
    }
    else
    {
        std::int32_t log_index = -1;
        std::int32_t snapshot_index = -1;

        for (std::int32_t i = static_cast<std::int32_t>(entries_.size()) - 1; i >= 0; --i)
        {
            const Entry& entry = entries_[static_cast<std::size_t>(i)];
            if (-1 == snapshot_index && is_valid_snapshot(entry) && entry.service_id == SERVICE_ID)
            {
                snapshot_index = i;
            }
            else if (-1 == log_index && entry.is_term() && entry.is_valid && NULL_VALUE != entry.recording_id)
            {
                log_index = i;
            }
            else if (-1 != snapshot_index && -1 != log_index)
            {
                break;
            }
        }

        if (-1 != snapshot_index)
        {
            auto added = add_snapshots(plan.snapshots, entries_, service_count, snapshot_index);
            if (!added)
            {
                return std::unexpected(added.error());
            }
        }

        if (-1 != log_index)
        {
            const Entry& entry = entries_[static_cast<std::size_t>(log_index)];
            std::optional<RecordingExtent> extent = extent_source(entry.recording_id);
            if (!extent.has_value())
            {
                return std::unexpected(
                    make_error(ErrorCode::not_found, "unknown recording id: " + std::to_string(entry.recording_id)));
            }

            const std::int64_t start_position =
                -1 == snapshot_index ? extent->start_position : plan.snapshots[0].log_position;

            log = Log{
                entry.recording_id, entry.leadership_term_id, entry.term_base_log_position, entry.log_position,
                start_position,     extent->stop_position,    extent->initial_term_id,      extent->term_buffer_length,
                extent->mtu_length, extent->session_id};
        }
    }

    plan.last_leadership_term_id = NULL_VALUE;
    plan.last_term_base_log_position = 0;
    plan.committed_log_position = 0;
    plan.appended_log_position = 0;

    if (!plan.snapshots.empty())
    {
        const Snapshot& snapshot = plan.snapshots[0];
        plan.last_leadership_term_id = snapshot.leadership_term_id;
        plan.last_term_base_log_position = snapshot.term_base_log_position;
        plan.appended_log_position = snapshot.log_position;
        plan.committed_log_position = snapshot.log_position;
    }

    if (log.has_value())
    {
        plan.last_leadership_term_id = log->leadership_term_id;
        plan.last_term_base_log_position = log->term_base_log_position;
        plan.appended_log_position = log->stop_position;
        plan.committed_log_position =
            NULL_POSITION != log->log_position ? log->log_position : plan.committed_log_position;
    }

    plan.log = log;
    return plan;
}

bool RecordingLog::is_unknown(std::int64_t leadership_term_id) const noexcept
{
    return cache_index_by_leadership_term_id_.end() == cache_index_by_leadership_term_id_.find(leadership_term_id);
}

std::expected<void, Error> RecordingLog::validate_term_recording_id(std::int64_t recording_id)
{
    if (NULL_VALUE == recording_id)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "invalid recordingId=" + std::to_string(recording_id)));
    }

    if (recording_id != term_recording_id_)
    {
        if (NULL_VALUE == term_recording_id_)
        {
            term_recording_id_ = recording_id;
        }
        else
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument,
                                              "invalid TERM recordingId=" + std::to_string(recording_id) +
                                                  ", expected recordingId=" + std::to_string(term_recording_id_)));
        }
    }

    return {};
}

std::expected<void, Error> RecordingLog::append_term(std::int64_t recording_id, std::int64_t leadership_term_id,
                                                     std::int64_t term_base_log_position, std::int64_t timestamp)
{
    auto validated = validate_term_recording_id(recording_id);
    if (!validated)
    {
        return std::unexpected(validated.error());
    }

    std::int64_t log_position = NULL_POSITION;

    if (!entries_.empty())
    {
        if (cache_index_by_leadership_term_id_.end() != cache_index_by_leadership_term_id_.find(leadership_term_id))
        {
            return std::unexpected(
                make_error(ErrorCode::invalid_argument,
                           "duplicate TERM entry for leadershipTermId=" + std::to_string(leadership_term_id)));
        }

        const std::int64_t previous_leadership_term_id = leadership_term_id - 1;
        if (cache_index_by_leadership_term_id_.end() !=
            cache_index_by_leadership_term_id_.find(previous_leadership_term_id))
        {
            auto committed = commit_log_position(previous_leadership_term_id, term_base_log_position);
            if (!committed)
            {
                return std::unexpected(committed.error());
            }
        }

        const Entry* next_term_entry = find_term_entry(leadership_term_id + 1);
        if (nullptr != next_term_entry)
        {
            log_position = next_term_entry->term_base_log_position;
        }
    }

    auto appended = append(ENTRY_TYPE_TERM, recording_id, leadership_term_id, term_base_log_position, log_position,
                           timestamp, static_cast<std::int32_t>(NULL_VALUE), {});
    if (!appended)
    {
        return std::unexpected(appended.error());
    }

    cache_index_by_leadership_term_id_[leadership_term_id] = *appended;
    return {};
}

std::expected<void, Error> RecordingLog::append_snapshot(std::int64_t recording_id, std::int64_t leadership_term_id,
                                                         std::int64_t term_base_log_position, std::int64_t log_position,
                                                         std::int64_t timestamp, std::int32_t service_id)
{
    if (NULL_VALUE == recording_id)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "invalid recordingId=" + std::to_string(recording_id)));
    }

    auto restored = restore_invalid_snapshot(ENTRY_TYPE_SNAPSHOT, recording_id, leadership_term_id,
                                             term_base_log_position, log_position, timestamp, service_id);
    if (!restored)
    {
        return std::unexpected(restored.error());
    }

    if (!*restored)
    {
        auto appended = append(ENTRY_TYPE_SNAPSHOT, recording_id, leadership_term_id, term_base_log_position,
                               log_position, timestamp, service_id, {});
        if (!appended)
        {
            return std::unexpected(appended.error());
        }
    }

    return {};
}

std::expected<void, Error> RecordingLog::append_standby_snapshot(
    std::int64_t recording_id, std::int64_t leadership_term_id, std::int64_t term_base_log_position,
    std::int64_t log_position, std::int64_t timestamp, std::int32_t service_id, std::string_view archive_endpoint)
{
    if (NULL_VALUE == recording_id)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "invalid recordingId=" + std::to_string(recording_id)));
    }

    if (archive_endpoint.empty())
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "Remote snapshots must have a valid endpoint"));
    }

    if (archive_endpoint.size() > MAX_ENDPOINT_LENGTH)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "Endpoint is too long: " + std::to_string(archive_endpoint.size()) +
                                                        " vs " + std::to_string(MAX_ENDPOINT_LENGTH)));
    }

    auto restored = restore_invalid_snapshot(ENTRY_TYPE_STANDBY_SNAPSHOT, recording_id, leadership_term_id,
                                             term_base_log_position, log_position, timestamp, service_id);
    if (!restored)
    {
        return std::unexpected(restored.error());
    }

    if (!*restored)
    {
        auto appended = append(ENTRY_TYPE_STANDBY_SNAPSHOT, recording_id, leadership_term_id, term_base_log_position,
                               log_position, timestamp, service_id, archive_endpoint);
        if (!appended)
        {
            return std::unexpected(appended.error());
        }
    }

    return {};
}

std::expected<void, Error> RecordingLog::commit_log_position(std::int64_t leadership_term_id, std::int64_t log_position)
{
    const auto found = cache_index_by_leadership_term_id_.find(leadership_term_id);
    if (cache_index_by_leadership_term_id_.end() == found)
    {
        return std::unexpected(
            make_error(ErrorCode::not_found, "unknown leadershipTermId=" + std::to_string(leadership_term_id)));
    }

    const auto index = static_cast<std::size_t>(found->second);
    Entry& entry = entries_[index];
    if (entry.log_position != log_position)
    {
        std::array<std::uint8_t, sizeof(std::int64_t)> bytes{};
        put_le64(bytes.data(), log_position);

        auto written = write_field(entry.position, LOG_POSITION_OFFSET, bytes);
        if (!written)
        {
            return std::unexpected(written.error());
        }

        entry.log_position = log_position;
    }

    return {};
}

std::expected<void, Error> RecordingLog::invalidate_entry(std::int32_t index)
{
    if (index < 0 || static_cast<std::size_t>(index) >= entries_.size())
    {
        return std::unexpected(make_error(ErrorCode::not_found, "unknown entry index: " + std::to_string(index)));
    }

    Entry& entry = entries_[static_cast<std::size_t>(index)];
    entry.is_valid = false;

    if (ENTRY_TYPE_TERM == entry.type)
    {
        cache_index_by_leadership_term_id_.erase(entry.leadership_term_id);
    }
    else if (ENTRY_TYPE_SNAPSHOT == entry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type)
    {
        invalid_snapshots_.push_back(index);
    }

    const std::int32_t invalid_entry_type = ENTRY_TYPE_INVALID_FLAG | entry.type;
    std::array<std::uint8_t, sizeof(std::int32_t)> bytes{};
    put_le32(bytes.data(), invalid_entry_type);

    return write_field(entry.position, ENTRY_TYPE_OFFSET, bytes);
}

std::expected<void, Error> RecordingLog::remove_entry(std::int64_t leadership_term_id, std::int32_t entry_index)
{
    const Entry* entry_to_remove = nullptr;

    for (std::size_t i = entries_.size(); i-- > 0;)
    {
        const Entry& entry = entries_[i];
        if (entry.leadership_term_id == leadership_term_id && entry.entry_index == entry_index)
        {
            entry_to_remove = &entry;
            break;
        }
    }

    if (nullptr == entry_to_remove)
    {
        return std::unexpected(make_error(ErrorCode::not_found, "unknown entry index: " + std::to_string(entry_index)));
    }

    std::array<std::uint8_t, sizeof(std::int32_t)> bytes{};
    put_le32(bytes.data(), static_cast<std::int32_t>(NULL_VALUE));

    auto written = write_field(entry_to_remove->position, ENTRY_TYPE_OFFSET, bytes);
    if (!written)
    {
        return std::unexpected(written.error());
    }

    return reload();
}

std::map<std::string, std::vector<const RecordingLog::Entry*>, std::less<>> RecordingLog::latest_standby_snapshots(
    std::int32_t service_count) const
{
    std::map<std::string, std::map<std::int64_t, std::vector<const Entry*>>, std::less<>> standby_snapshots{};

    for (std::size_t i = entries_.size(); i-- > 0;)
    {
        const Entry& entry = entries_[i];
        if (ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type && entry.is_valid)
        {
            standby_snapshots[entry.archive_endpoint][entry.log_position].push_back(&entry);
        }
    }

    std::map<std::string, std::vector<const Entry*>, std::less<>> latest{};

    for (auto& [endpoint, by_position] : standby_snapshots)
    {
        while (!by_position.empty())
        {
            auto last_entry = by_position.end();
            --last_entry;

            const std::int32_t snapshot_count = service_count + 1;
            if (static_cast<std::int32_t>(last_entry->second.size()) == snapshot_count)
            {
                latest.emplace(endpoint, last_entry->second);
                break;
            }

            by_position.erase(last_entry);
        }
    }

    return latest;
}

std::map<std::string, std::vector<const RecordingLog::Entry*>, std::less<>> RecordingLog::newer_standby_snapshots(
    std::int32_t service_count) const
{
    auto snapshots_by_endpoint = latest_standby_snapshots(service_count);

    for (auto endpoint_it = snapshots_by_endpoint.begin(); endpoint_it != snapshots_by_endpoint.end();)
    {
        auto& snapshots = endpoint_it->second;

        for (std::size_t i = snapshots.size(); i-- > 0;)
        {
            const Entry* standby_snapshot = snapshots[i];
            const Entry* snapshot = get_latest_snapshot(standby_snapshot->service_id);
            if (nullptr != snapshot && standby_snapshot->log_position <= snapshot->log_position)
            {
                snapshots.erase(snapshots.begin() + static_cast<std::ptrdiff_t>(i));
            }
        }

        if (snapshots.empty())
        {
            endpoint_it = snapshots_by_endpoint.erase(endpoint_it);
        }
        else
        {
            ++endpoint_it;
        }
    }

    return snapshots_by_endpoint;
}

std::optional<std::vector<RecordingLog::Snapshot>> RecordingLog::find_snapshot_at_or_before_or_lowest(
    std::int64_t log_position, std::int32_t service_count)
{
    std::stable_sort(entries_.begin(), entries_.end(), &RecordingLog::entry_less);

    for (std::int32_t i = static_cast<std::int32_t>(entries_.size()) - 1; i >= 0; --i)
    {
        const Entry& entry = entries_[static_cast<std::size_t>(i)];
        if (is_valid_snapshot(entry) && SERVICE_ID == entry.service_id)
        {
            if (entry.log_position > log_position)
            {
                continue;
            }

            if (validate_snapshot_entries(entries_, service_count, i, entry))
            {
                const auto begin = entries_.begin() + static_cast<std::ptrdiff_t>(i - service_count);
                const auto end = entries_.begin() + (static_cast<std::ptrdiff_t>(i) + 1);
                return convert_entry_list_to_snapshot_list(std::span<const Entry>(begin, end));
            }
        }
    }

    for (std::int32_t i = 0; i < static_cast<std::int32_t>(entries_.size()); ++i)
    {
        const Entry& entry = entries_[static_cast<std::size_t>(i)];
        if (is_valid_snapshot(entry) && SERVICE_ID == entry.service_id)
        {
            if (validate_snapshot_entries(entries_, service_count, i, entry))
            {
                const auto begin = entries_.begin() + static_cast<std::ptrdiff_t>(i - service_count);
                const auto end = entries_.begin() + (static_cast<std::ptrdiff_t>(i) + 1);
                return convert_entry_list_to_snapshot_list(std::span<const Entry>(begin, end));
            }
        }
    }

    return std::nullopt;
}

std::vector<RecordingLog::Snapshot> RecordingLog::convert_entry_list_to_snapshot_list(std::span<const Entry> entries)
{
    std::vector<Snapshot> snapshots;
    snapshots.reserve(entries.size());

    for (const Entry& entry : entries)
    {
        snapshots.push_back(Snapshot{entry.recording_id, entry.leadership_term_id, entry.term_base_log_position,
                                     entry.log_position, entry.timestamp, entry.service_id});
    }

    return snapshots;
}

bool RecordingLog::validate_snapshot_entries(const std::vector<Entry>& entries, std::int32_t service_count,
                                             std::int32_t snapshot_index, const Entry& consensus_module_entry) noexcept
{
    if (snapshot_index - service_count < 0)
    {
        return false;
    }

    for (std::int32_t i = 1; i <= service_count; ++i)
    {
        const Entry& entry = entries[static_cast<std::size_t>(snapshot_index - i)];

        if (ENTRY_TYPE_SNAPSHOT != entry.type ||
            entry.leadership_term_id != consensus_module_entry.leadership_term_id ||
            entry.log_position != consensus_module_entry.log_position)
        {
            return false;
        }
    }

    return true;
}

std::expected<void, Error> RecordingLog::ensure_coherent(
    std::int64_t recording_id, std::int64_t initial_log_leadership_term_id, std::int64_t initial_term_base_log_position,
    std::int64_t leadership_term_id, std::int64_t term_base_log_position, std::int64_t log_position,
    std::int64_t now_ns, std::int64_t timestamp, std::int32_t file_sync_level)
{
    (void)now_ns;
    const Entry* last_term = find_last_term();

    if (nullptr == last_term)
    {
        for (std::int64_t term_id = std::max<std::int64_t>(0, initial_log_leadership_term_id);
             term_id < leadership_term_id; ++term_id)
        {
            auto appended = append_term(recording_id, term_id, initial_term_base_log_position, timestamp);
            if (!appended)
            {
                return std::unexpected(appended.error());
            }
        }

        auto appended = append_term(recording_id, leadership_term_id, term_base_log_position, timestamp);
        if (!appended)
        {
            return std::unexpected(appended.error());
        }
        if (NULL_VALUE != log_position)
        {
            auto committed = commit_log_position(leadership_term_id, log_position);
            if (!committed)
            {
                return std::unexpected(committed.error());
            }
        }
    }
    else if (last_term->leadership_term_id < leadership_term_id)
    {
        if (NULL_VALUE == last_term->log_position)
        {
            if (NULL_VALUE == term_base_log_position)
            {
                return std::unexpected(make_error(
                    ErrorCode::invalid_argument,
                    "Prior term was not committed: leadershipTermId=" + std::to_string(last_term->leadership_term_id) +
                        " and logTermBasePosition was not specified: leadershipTermId = " +
                        std::to_string(leadership_term_id) +
                        ", logTermBasePosition = " + std::to_string(term_base_log_position) +
                        ", logPosition = " + std::to_string(log_position) + ", nowNs = " + std::to_string(now_ns)));
            }

            auto committed = commit_log_position(last_term->leadership_term_id, term_base_log_position);
            if (!committed)
            {
                return std::unexpected(committed.error());
            }
            last_term = find_last_term();
            if (nullptr == last_term)
            {
                return std::unexpected(make_error(ErrorCode::internal_error, "no last term after commit"));
            }
        }

        for (std::int64_t term_id = last_term->leadership_term_id + 1; term_id < leadership_term_id; ++term_id)
        {
            auto appended = append_term(recording_id, term_id, last_term->log_position, timestamp);
            if (!appended)
            {
                return std::unexpected(appended.error());
            }
        }

        auto appended = append_term(recording_id, leadership_term_id, last_term->log_position, timestamp);
        if (!appended)
        {
            return std::unexpected(appended.error());
        }
        if (NULL_VALUE != log_position)
        {
            auto committed = commit_log_position(leadership_term_id, log_position);
            if (!committed)
            {
                return std::unexpected(committed.error());
            }
        }
    }
    else
    {
        if (NULL_VALUE != log_position)
        {
            auto committed = commit_log_position(leadership_term_id, log_position);
            if (!committed)
            {
                return std::unexpected(committed.error());
            }
        }
    }

    return force(file_sync_level);
}

std::expected<std::int32_t, Error> RecordingLog::append(std::int32_t entry_type, std::int64_t recording_id,
                                                        std::int64_t leadership_term_id,
                                                        std::int64_t term_base_log_position, std::int64_t log_position,
                                                        std::int64_t timestamp, std::int32_t service_id,
                                                        std::string_view endpoint)
{
    Entry entry;
    entry.recording_id = recording_id;
    entry.leadership_term_id = leadership_term_id;
    entry.term_base_log_position = term_base_log_position;
    entry.log_position = log_position;
    entry.timestamp = timestamp;
    entry.service_id = service_id;
    entry.type = entry_type;
    entry.archive_endpoint = endpoint;
    entry.is_valid = true;
    entry.position = NULL_VALUE;
    entry.entry_index = next_entry_index_;

    auto written = write_entry(std::move(entry));
    if (!written)
    {
        return std::unexpected(written.error());
    }

    entry = std::move(*written);
    ++next_entry_index_;

    std::int32_t index = static_cast<std::int32_t>(entries_.size());

    for (std::int32_t i = static_cast<std::int32_t>(entries_.size()) - 1; i >= 0; --i)
    {
        if (entry_less(entry, entries_[static_cast<std::size_t>(i)]))
        {
            --index;
        }
        else
        {
            break;
        }
    }

    if (index < static_cast<std::int32_t>(entries_.size()))
    {
        entries_.insert(entries_.begin() + index, entry);

        for (auto& [term_id, cache_index] : cache_index_by_leadership_term_id_)
        {
            if (term_id > leadership_term_id)
            {
                ++cache_index;
            }
        }

        for (std::size_t i = invalid_snapshots_.size(); i-- > 0;)
        {
            if (invalid_snapshots_[i] >= index)
            {
                ++invalid_snapshots_[i];
            }
            else
            {
                break;
            }
        }
    }
    else
    {
        entries_.push_back(std::move(entry));
    }

    return index;
}

std::expected<RecordingLog::Entry, Error> RecordingLog::write_entry(Entry entry)
{
    std::array<std::uint8_t, MAX_ENTRY_LENGTH> buffer{};

    put_le64(buffer.data() + RECORDING_ID_OFFSET, entry.recording_id);
    put_le64(buffer.data() + LEADERSHIP_TERM_ID_OFFSET, entry.leadership_term_id);
    put_le64(buffer.data() + TERM_BASE_LOG_POSITION_OFFSET, entry.term_base_log_position);
    put_le64(buffer.data() + LOG_POSITION_OFFSET, entry.log_position);
    put_le64(buffer.data() + TIMESTAMP_OFFSET, entry.timestamp);
    put_le32(buffer.data() + SERVICE_ID_OFFSET, entry.service_id);
    put_le32(buffer.data() + ENTRY_TYPE_OFFSET, entry.type);

    if (!entry.archive_endpoint.empty())
    {
        if (entry.archive_endpoint.size() > MAX_ENDPOINT_LENGTH)
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument,
                                              "Endpoint is too long: " + std::to_string(entry.archive_endpoint.size()) +
                                                  " vs " + std::to_string(MAX_ENDPOINT_LENGTH)));
        }

        put_le32(buffer.data() + ENDPOINT_OFFSET, static_cast<std::int32_t>(entry.archive_endpoint.size()));
        std::memcpy(buffer.data() + ENDPOINT_OFFSET + sizeof(std::int32_t), entry.archive_endpoint.data(),
                    entry.archive_endpoint.size());
    }

    if (NULL_VALUE == entry.position)
    {
        auto length = file_length(fd_);
        if (!length)
        {
            return std::unexpected(length.error());
        }
        entry.position = static_cast<std::int64_t>(align(static_cast<std::size_t>(*length), RECORD_ALIGNMENT));
    }

    auto written = persist(entry.position, std::span<const std::uint8_t>(buffer.data(), entry.length()));
    if (!written)
    {
        return std::unexpected(written.error());
    }

    return entry;
}

std::expected<void, Error> RecordingLog::persist(std::int64_t position, std::span<const std::uint8_t> bytes)
{
    auto written = pwrite_full(fd_, bytes, position);
    if (!written)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "failed to write field atomically"));
    }
    return {};
}

std::expected<void, Error> RecordingLog::write_field(std::int64_t entry_position, std::size_t offset,
                                                     std::span<const std::uint8_t> bytes)
{
    return persist(entry_position + static_cast<std::int64_t>(offset), bytes);
}

std::expected<bool, Error> RecordingLog::restore_invalid_snapshot(
    std::int32_t snapshot_entry_type, std::int64_t recording_id, std::int64_t leadership_term_id,
    std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t timestamp, std::int32_t service_id)
{
    for (std::size_t i = invalid_snapshots_.size(); i-- > 0;)
    {
        const std::int32_t entry_cache_index = invalid_snapshots_[i];
        const Entry& entry = entries_[static_cast<std::size_t>(entry_cache_index)];

        if (snapshot_entry_type == entry.type &&
            matches_entry(entry, leadership_term_id, term_base_log_position, log_position, service_id))
        {
            Entry validated;
            validated.recording_id = recording_id;
            validated.leadership_term_id = leadership_term_id;
            validated.term_base_log_position = term_base_log_position;
            validated.log_position = log_position;
            validated.timestamp = timestamp;
            validated.service_id = service_id;
            validated.type = snapshot_entry_type;
            validated.entry_index = entry.entry_index;
            validated.is_valid = true;
            validated.position = entry.position;

            auto written = write_entry(std::move(validated));
            if (!written)
            {
                return std::unexpected(written.error());
            }

            entries_[static_cast<std::size_t>(entry_cache_index)] = std::move(*written);
            invalid_snapshots_[i] = invalid_snapshots_.back();
            invalid_snapshots_.pop_back();

            return true;
        }
    }

    return false;
}

bool RecordingLog::matches_entry(const Entry& entry, std::int64_t leadership_term_id,
                                 std::int64_t term_base_log_position, std::int64_t log_position,
                                 std::int32_t service_id) noexcept
{
    return entry.leadership_term_id == leadership_term_id && entry.term_base_log_position == term_base_log_position &&
           entry.log_position == log_position && entry.service_id == service_id;
}

std::expected<void, Error> RecordingLog::add_snapshots(std::vector<Snapshot>& snapshots,
                                                       const std::vector<Entry>& entries, std::int32_t service_count,
                                                       std::int32_t snapshot_index)
{
    const Entry& snapshot = entries[static_cast<std::size_t>(snapshot_index)];
    snapshots.push_back(Snapshot{snapshot.recording_id, snapshot.leadership_term_id, snapshot.term_base_log_position,
                                 snapshot.log_position, snapshot.timestamp, snapshot.service_id});

    for (std::int32_t i = 1; i <= service_count; ++i)
    {
        if ((snapshot_index - i) < 0)
        {
            return std::unexpected(
                make_error(ErrorCode::invalid_argument, "snapshot missing for service at index " + std::to_string(i)));
        }

        const Entry& entry = entries[static_cast<std::size_t>(snapshot_index - i)];

        if (ENTRY_TYPE_SNAPSHOT == entry.type && entry.leadership_term_id == snapshot.leadership_term_id &&
            entry.log_position == snapshot.log_position)
        {
            const std::size_t insert_at = static_cast<std::size_t>(entry.service_id) + 1;
            if (insert_at > snapshots.size())
            {
                return std::unexpected(make_error(ErrorCode::invalid_argument,
                                                  "snapshot missing for service at index " + std::to_string(i)));
            }

            snapshots.insert(snapshots.begin() + static_cast<std::ptrdiff_t>(insert_at),
                             Snapshot{entry.recording_id, entry.leadership_term_id, entry.term_base_log_position,
                                      entry.log_position, entry.timestamp, entry.service_id});
        }
    }

    return {};
}

std::string_view RecordingLog::type_as_string(std::int32_t type) noexcept
{
    switch (type)
    {
        case ENTRY_TYPE_TERM:
            return "TERM";
        case ENTRY_TYPE_SNAPSHOT:
            return "SNAPSHOT";
        case ENTRY_TYPE_STANDBY_SNAPSHOT:
            return "STANDBY_SNAPSHOT";
        default:
            return "UNKNOWN";
    }
}

bool RecordingLog::is_valid_snapshot(const Entry& entry) noexcept
{
    return entry.is_valid && ENTRY_TYPE_SNAPSHOT == entry.type;
}

bool RecordingLog::is_invalid_snapshot(const Entry& entry) noexcept
{
    return !entry.is_valid && ENTRY_TYPE_SNAPSHOT == entry.type;
}

bool RecordingLog::is_valid_any_snapshot(const Entry& entry) noexcept
{
    return entry.is_valid && (ENTRY_TYPE_SNAPSHOT == entry.type || ENTRY_TYPE_STANDBY_SNAPSHOT == entry.type);
}

}  // namespace deepmsg::cluster::common
