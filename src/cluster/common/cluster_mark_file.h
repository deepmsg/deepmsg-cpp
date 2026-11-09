#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>

#include "core/error/error.h"
#include "core/io/mapped_file.h"

namespace deepmsg::cluster::common
{

using axc::core::Error;

enum class ClusterComponentType : std::int32_t
{
    unknown = 0,
    consensus_module = 1,
    container = 2,
    backup = 3,
    standby = 4
};

[[nodiscard]] std::string_view cluster_component_type_name(ClusterComponentType type) noexcept;

struct ClusterNodeControlProperties
{
    std::int32_t member_id{-1};
    std::int32_t service_stream_id{0};
    std::int32_t consensus_module_stream_id{0};
    std::string aeron_directory{};
    std::string control_channel{};
};

/// `cluster-mark.dat` and `cluster-mark-service-<id>.dat`.
///
/// Transcribed from `ClusterMarkFile.java` (Aeron 1.54.0) and the Agrona
/// `MarkFile` activation protocol it relies on:
///   * layout: 8-byte SBE message header (`MarkFileHeader`, schema 110) at
///     offset 8, 128-byte body, var-ascii fields from offset 136, error buffer
///     at 8 KiB, total length page-aligned;
///   * an existing file is claimed by checking `now - activityTimestamp >=
///     timeoutMs`, then CAS-ing the timestamp to `INT64_MAX`
///     (`MarkFile.mapNewOrExistingMarkFile`);
///   * `signal_ready` writes the activity timestamp (release), then the
///     semantic version (release), then msync;
///   * a `BACKUP` file may be taken over by a `CONSENSUS_MODULE`, no other
///     type mismatch is tolerated.
class ClusterMarkFile
{
public:
    static constexpr std::string_view FILE_NAME = "cluster-mark.dat";
    static constexpr std::string_view LINK_FILE_NAME = "cluster-mark.lnk";
    static constexpr std::string_view SERVICE_FILE_NAME_PREFIX = "cluster-mark-service-";
    static constexpr std::string_view FILE_EXTENSION = ".dat";
    static constexpr std::string_view LINK_FILE_EXTENSION = ".lnk";

    static constexpr std::int32_t HEADER_OFFSET = 8;
    static constexpr std::int32_t HEADER_LENGTH = 8 * 1024;
    static constexpr std::int32_t ERROR_BUFFER_MIN_LENGTH = 1024 * 1024;
    static constexpr std::int32_t ERROR_BUFFER_MAX_LENGTH = INT32_MAX - HEADER_LENGTH;
    static constexpr std::int32_t VERSION_FAILED = -1;
    /// `Agrona.MarkFile.ACTIVATION_IN_PROGRESS_TIMESTAMP`.
    static constexpr std::int64_t ACTIVATION_IN_PROGRESS_TIMESTAMP = INT64_MAX;

    struct Options
    {
        std::string path{};
        ClusterComponentType component_type{ClusterComponentType::consensus_module};
        std::int32_t error_buffer_length{ERROR_BUFFER_MIN_LENGTH};
        std::int64_t timeout_ms{10'000};
        std::int32_t file_page_size{4 * 1024};
        std::int64_t now_ms{0};
    };

    /// Create, or take over an inactive, component mark file.
    [[nodiscard]] static std::expected<ClusterMarkFile, Error> create_or_open(const Options& options);

    /// Open an existing mark file for inspection (tools); no activation.
    [[nodiscard]] static std::expected<ClusterMarkFile, Error> open_existing(std::string_view path,
                                                                             std::int64_t now_ms);

    ClusterMarkFile() = default;
    ClusterMarkFile(const ClusterMarkFile&) = delete;
    ClusterMarkFile& operator=(const ClusterMarkFile&) = delete;
    ClusterMarkFile(ClusterMarkFile&&) noexcept = default;
    ClusterMarkFile& operator=(ClusterMarkFile&&) noexcept = default;
    ~ClusterMarkFile() = default;

    [[nodiscard]] ClusterComponentType component_type() const noexcept;
    [[nodiscard]] std::int32_t semantic_version() const noexcept;
    [[nodiscard]] std::int64_t pid() const noexcept;
    [[nodiscard]] std::int64_t start_timestamp() const noexcept;
    [[nodiscard]] std::int64_t candidate_term_id() const noexcept;
    [[nodiscard]] std::int32_t member_id() const noexcept;
    [[nodiscard]] std::int32_t service_id() const noexcept;
    [[nodiscard]] std::int32_t cluster_id() const noexcept;
    [[nodiscard]] std::int32_t archive_stream_id() const noexcept;
    [[nodiscard]] std::int32_t service_stream_id() const noexcept;
    [[nodiscard]] std::int32_t consensus_module_stream_id() const noexcept;
    [[nodiscard]] std::int32_t ingress_stream_id() const noexcept;
    [[nodiscard]] std::int32_t header_length() const noexcept;
    [[nodiscard]] std::int32_t error_buffer_length() const noexcept;

    [[nodiscard]] std::string aeron_directory() const;
    [[nodiscard]] std::string control_channel() const;
    [[nodiscard]] std::string ingress_channel() const;
    [[nodiscard]] std::string service_name() const;
    [[nodiscard]] std::string authenticator() const;
    [[nodiscard]] std::string services_cluster_dir() const;

    void set_candidate_term_id(std::int64_t candidate_term_id) noexcept;
    void set_member_id(std::int32_t member_id) noexcept;
    void set_service_id(std::int32_t service_id) noexcept;
    void set_cluster_id(std::int32_t cluster_id) noexcept;
    void set_archive_stream_id(std::int32_t stream_id) noexcept;
    void set_service_stream_id(std::int32_t stream_id) noexcept;
    void set_consensus_module_stream_id(std::int32_t stream_id) noexcept;
    void set_ingress_stream_id(std::int32_t stream_id) noexcept;

    /// Writes the six var-ascii fields in schema order.
    void set_channels(std::string_view aeron_directory, std::string_view control_channel,
                      std::string_view ingress_channel, std::string_view service_name, std::string_view authenticator,
                      std::string_view services_cluster_dir);

    /// `ClusterMarkFile.signalReady(activityTimestamp)`.
    [[nodiscard]] std::expected<void, Error> signal_ready(std::int64_t activity_timestamp) noexcept;
    [[nodiscard]] std::expected<void, Error> signal_failed_start() noexcept;
    [[nodiscard]] std::expected<void, Error> signal_terminated() noexcept;

    void update_activity_timestamp(std::int64_t now_ms) noexcept;
    [[nodiscard]] std::int64_t activity_timestamp_volatile() const noexcept;

    [[nodiscard]] std::expected<void, Error> force() noexcept;

    [[nodiscard]] std::span<std::uint8_t> error_buffer() noexcept;
    [[nodiscard]] std::span<const std::uint8_t> error_buffer() const noexcept;

    [[nodiscard]] ClusterNodeControlProperties load_control_properties() const;

    /// Errors preserved from the previous (inactive) process during takeover,
    /// already rendered by `render_error_log`; empty when there were none.
    [[nodiscard]] const std::string& existing_errors() const noexcept
    {
        return existing_errors_;
    }

    /// True when the previous process left `VERSION_FAILED` in the version field.
    [[nodiscard]] bool previous_startup_failed() const noexcept
    {
        return previous_startup_failed_;
    }

    [[nodiscard]] static std::string mark_filename_for_service(std::int32_t service_id);
    [[nodiscard]] static std::string link_filename_for_service(std::int32_t service_id);

    /// `CommonContext.printErrorLog` over an Agrona distinct-error-log buffer.
    [[nodiscard]] static std::string render_error_log(std::span<const std::uint8_t> buffer);

    /// Resolve `cluster-mark.lnk` (a file containing the real mark-file path)
    /// into the mark file's directory; the input directory when there is no
    /// link file.
    [[nodiscard]] static std::string resolve_mark_file_dir(std::string_view directory);

private:
    [[nodiscard]] std::int32_t header_offset() const noexcept
    {
        return header_offset_;
    }

    [[nodiscard]] std::int32_t header_body_offset() const noexcept;
    [[nodiscard]] std::int32_t body_field_offset(std::size_t encoding_offset) const noexcept;
    [[nodiscard]] std::span<std::uint8_t> mapped() noexcept;
    [[nodiscard]] std::span<const std::uint8_t> mapped() const noexcept;

    [[nodiscard]] std::expected<void, Error> validate_existing(const Options& options);
    [[nodiscard]] std::expected<void, Error> write_header(const Options& options, std::int64_t candidate_term_id);
    void save_existing_errors(const Options& options);
    void reset_activity_on_failure() noexcept;

    axc::core::MappedFile file_{};
    std::int32_t header_offset_{HEADER_OFFSET};
    std::int32_t error_buffer_length_{0};
    std::int32_t semantic_version_{0};
    std::int64_t inherited_candidate_term_id_{-1};
    bool previous_startup_failed_{false};
    std::string existing_errors_{};
};

}  // namespace deepmsg::cluster::common
