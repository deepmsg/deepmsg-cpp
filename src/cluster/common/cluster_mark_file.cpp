#include "cluster/common/cluster_mark_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <string>

#include "cluster/common/cluster_versions.h"
#include "core/io/fs.h"
#include "io_aeron_cluster_codecs_mark/MarkFileHeader.h"
#include "io_aeron_cluster_codecs_mark/MessageHeader.h"

namespace deepmsg::cluster::common
{

using axc::core::ErrorCode;
using axc::core::make_error;

namespace
{

namespace mark = ::io::aeron::cluster::codecs::mark;

constexpr std::int32_t NULL_VALUE = -1;
constexpr std::size_t ERROR_LOG_ENTRY_HEADER_LENGTH = 24;
constexpr std::size_t ERROR_LOG_ALIGNMENT = 8;

[[nodiscard]] constexpr std::size_t align(std::size_t value, std::size_t alignment) noexcept
{
    return (value + (alignment - 1)) & ~(alignment - 1);
}

[[nodiscard]] bool is_power_of_two(std::int32_t value) noexcept
{
    return value > 0 && 0 == (value & (value - 1));
}

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

[[nodiscard]] std::int32_t get_le32(const std::uint8_t* data) noexcept
{
    std::int32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

[[nodiscard]] std::int64_t get_le64(const std::uint8_t* data) noexcept
{
    std::int64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

[[nodiscard]] std::string format_date_time(std::int64_t epoch_ms)
{
    const auto seconds = static_cast<time_t>(epoch_ms / 1000);
    const auto millis = static_cast<int>((epoch_ms % 1000 + 1000) % 1000);

    std::tm broken_down{};
    (void)::localtime_r(&seconds, &broken_down);

    std::array<char, 64> buffer{};
    const std::size_t written = std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S", &broken_down);

    std::array<char, 16> zone{};
    (void)std::strftime(zone.data(), zone.size(), "%z", &broken_down);

    char fraction[8]{};
    std::snprintf(fraction, sizeof(fraction), "%06d", millis * 1000);

    std::string result{buffer.data(), written};
    result.push_back('.');
    result.append(fraction);
    result.append(zone.data());
    return result;
}

[[nodiscard]] std::string format_file_name_time(std::int64_t epoch_ms)
{
    const auto seconds = static_cast<time_t>(epoch_ms / 1000);
    const auto millis = static_cast<int>((epoch_ms % 1000 + 1000) % 1000);

    std::tm broken_down{};
    (void)::localtime_r(&seconds, &broken_down);

    std::array<char, 64> buffer{};
    const std::size_t written = std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d-%H-%M-%S", &broken_down);

    std::array<char, 16> zone{};
    (void)std::strftime(zone.data(), zone.size(), "%z", &broken_down);

    char fraction[8]{};
    std::snprintf(fraction, sizeof(fraction), "%06d", millis * 1000);

    std::string result{buffer.data(), written};
    result.push_back('-');
    result.append(fraction);
    result.append(zone.data());
    return result;
}

}  // namespace

std::string_view cluster_component_type_name(ClusterComponentType type) noexcept
{
    switch (type)
    {
        case ClusterComponentType::unknown:
            return "UNKNOWN";
        case ClusterComponentType::consensus_module:
            return "CONSENSUS_MODULE";
        case ClusterComponentType::container:
            return "CONTAINER";
        case ClusterComponentType::backup:
            return "BACKUP";
        case ClusterComponentType::standby:
            return "STANDBY";
    }
    return "UNKNOWN";
}

std::span<std::uint8_t> ClusterMarkFile::mapped() noexcept
{
    return file_.mapping();
}

std::span<const std::uint8_t> ClusterMarkFile::mapped() const noexcept
{
    return file_.mapping();
}

std::int32_t ClusterMarkFile::header_body_offset() const noexcept
{
    return header_offset_;
}

std::int32_t ClusterMarkFile::body_field_offset(std::size_t encoding_offset) const noexcept
{
    return header_offset_ + static_cast<std::int32_t>(encoding_offset);
}

ClusterComponentType ClusterMarkFile::component_type() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    const auto length = static_cast<std::uint64_t>(data.size());

    try
    {
        mark::MessageHeader message_header(buffer, 0, length, mark::MessageHeader::sbeSchemaVersion());
        mark::MarkFileHeader header;
        header.wrapForDecode(buffer, static_cast<std::uint64_t>(header_offset_), message_header.blockLength(),
                             message_header.version(), length);
        return static_cast<ClusterComponentType>(header.componentType());
    }
    catch (const std::exception&)
    {
        // A corrupt or foreign header is reported as UNKNOWN to the caller.
        return ClusterComponentType::unknown;
    }
}

std::int32_t ClusterMarkFile::semantic_version() const noexcept
{
    return semantic_version_;
}

std::int64_t ClusterMarkFile::pid() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le64(data.data() + body_field_offset(mark::MarkFileHeader::pidEncodingOffset()));
}

std::int64_t ClusterMarkFile::start_timestamp() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le64(data.data() + body_field_offset(mark::MarkFileHeader::startTimestampEncodingOffset()));
}

std::int64_t ClusterMarkFile::candidate_term_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    const auto* address = data.data() + body_field_offset(mark::MarkFileHeader::candidateTermIdEncodingOffset());
    return __atomic_load_n(reinterpret_cast<const std::int64_t*>(address), __ATOMIC_ACQUIRE);
}

std::int32_t ClusterMarkFile::member_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::memberIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::service_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::serviceIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::cluster_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::clusterIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::archive_stream_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::archiveStreamIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::service_stream_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::serviceStreamIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::consensus_module_stream_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::consensusModuleStreamIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::ingress_stream_id() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::ingressStreamIdEncodingOffset()));
}

std::int32_t ClusterMarkFile::header_length() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    return get_le32(data.data() + body_field_offset(mark::MarkFileHeader::headerLengthEncodingOffset()));
}

std::int32_t ClusterMarkFile::error_buffer_length() const noexcept
{
    return error_buffer_length_;
}

namespace
{

[[nodiscard]] mark::MarkFileHeader decode_mark_header(char* buffer, std::int32_t body_offset, std::uint64_t length)
{
    mark::MessageHeader message_header(buffer, 0, length, mark::MessageHeader::sbeSchemaVersion());
    mark::MarkFileHeader header;
    header.wrapForDecode(buffer, static_cast<std::uint64_t>(body_offset), message_header.blockLength(),
                         message_header.version(), length);
    return header;
}

}  // namespace

std::string ClusterMarkFile::aeron_directory() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    return header.getAeronDirectoryAsString();
}

std::string ClusterMarkFile::control_channel() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    header.skipAeronDirectory();
    return header.getControlChannelAsString();
}

std::string ClusterMarkFile::ingress_channel() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    header.skipAeronDirectory();
    header.skipControlChannel();
    return header.getIngressChannelAsString();
}

std::string ClusterMarkFile::service_name() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    header.skipAeronDirectory();
    header.skipControlChannel();
    header.skipIngressChannel();
    return header.getServiceNameAsString();
}

std::string ClusterMarkFile::authenticator() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    header.skipAeronDirectory();
    header.skipControlChannel();
    header.skipIngressChannel();
    header.skipServiceName();
    return header.getAuthenticatorAsString();
}

std::string ClusterMarkFile::services_cluster_dir() const
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    auto header = decode_mark_header(buffer, header_offset_, static_cast<std::uint64_t>(data.size()));
    header.skipAeronDirectory();
    header.skipControlChannel();
    header.skipIngressChannel();
    header.skipServiceName();
    header.skipAuthenticator();
    return header.getServicesClusterDirAsString();
}

namespace
{

void put_int32_field(std::span<std::uint8_t> data, std::int32_t offset, std::int32_t value) noexcept
{
    std::memcpy(data.data() + offset, &value, sizeof(value));
}

}  // namespace

void ClusterMarkFile::set_candidate_term_id(std::int64_t candidate_term_id) noexcept
{
    auto data = mapped();
    auto* address = data.data() + body_field_offset(mark::MarkFileHeader::candidateTermIdEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(address), candidate_term_id, __ATOMIC_RELEASE);
}

void ClusterMarkFile::set_member_id(std::int32_t member_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::memberIdEncodingOffset()), member_id);
}

void ClusterMarkFile::set_service_id(std::int32_t service_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::serviceIdEncodingOffset()), service_id);
}

void ClusterMarkFile::set_cluster_id(std::int32_t cluster_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::clusterIdEncodingOffset()), cluster_id);
}

void ClusterMarkFile::set_archive_stream_id(std::int32_t stream_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::archiveStreamIdEncodingOffset()), stream_id);
}

void ClusterMarkFile::set_service_stream_id(std::int32_t stream_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::serviceStreamIdEncodingOffset()), stream_id);
}

void ClusterMarkFile::set_consensus_module_stream_id(std::int32_t stream_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::consensusModuleStreamIdEncodingOffset()),
                    stream_id);
}

void ClusterMarkFile::set_ingress_stream_id(std::int32_t stream_id) noexcept
{
    put_int32_field(mapped(), body_field_offset(mark::MarkFileHeader::ingressStreamIdEncodingOffset()), stream_id);
}

void ClusterMarkFile::set_channels(std::string_view aeron_directory, std::string_view control_channel,
                                   std::string_view ingress_channel, std::string_view service_name,
                                   std::string_view authenticator, std::string_view services_cluster_dir)
{
    auto data = mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    const auto length = static_cast<std::uint64_t>(data.size());

    mark::MarkFileHeader header;
    header.wrapAndApplyHeader(buffer, 0, length);
    header.putAeronDirectory(aeron_directory)
        .putControlChannel(control_channel)
        .putIngressChannel(ingress_channel)
        .putServiceName(service_name)
        .putAuthenticator(authenticator)
        .putServicesClusterDir(services_cluster_dir);
}

std::expected<void, Error> ClusterMarkFile::signal_ready(std::int64_t activity_timestamp) noexcept
{
    auto data = mapped();
    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(timestamp_address), activity_timestamp, __ATOMIC_RELEASE);

    auto* version_address = data.data() + body_field_offset(mark::MarkFileHeader::versionEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int32_t*>(version_address), CLUSTER_MARK_SEMANTIC_VERSION, __ATOMIC_RELEASE);
    semantic_version_ = CLUSTER_MARK_SEMANTIC_VERSION;

    return force();
}

std::expected<void, Error> ClusterMarkFile::signal_failed_start() noexcept
{
    auto data = mapped();
    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(timestamp_address), static_cast<std::int64_t>(NULL_VALUE),
                     __ATOMIC_RELEASE);

    auto* version_address = data.data() + body_field_offset(mark::MarkFileHeader::versionEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int32_t*>(version_address), VERSION_FAILED, __ATOMIC_RELEASE);
    semantic_version_ = VERSION_FAILED;

    return force();
}

std::expected<void, Error> ClusterMarkFile::signal_terminated() noexcept
{
    auto data = mapped();
    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(timestamp_address), static_cast<std::int64_t>(NULL_VALUE),
                     __ATOMIC_RELEASE);
    return force();
}

void ClusterMarkFile::update_activity_timestamp(std::int64_t now_ms) noexcept
{
    auto data = mapped();
    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(timestamp_address), now_ms, __ATOMIC_RELEASE);
}

std::int64_t ClusterMarkFile::activity_timestamp_volatile() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    const auto* timestamp_address =
        data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    return __atomic_load_n(reinterpret_cast<const std::int64_t*>(timestamp_address), __ATOMIC_ACQUIRE);
}

std::expected<void, Error> ClusterMarkFile::force() noexcept
{
    return file_.flush();
}

std::span<std::uint8_t> ClusterMarkFile::error_buffer() noexcept
{
    auto data = mapped();
    const auto offset = static_cast<std::size_t>(HEADER_LENGTH);
    const auto length = static_cast<std::size_t>(error_buffer_length_);
    return data.subspan(offset, length);
}

std::span<const std::uint8_t> ClusterMarkFile::error_buffer() const noexcept
{
    auto data = const_cast<ClusterMarkFile*>(this)->mapped();
    const auto offset = static_cast<std::size_t>(HEADER_LENGTH);
    const auto length = static_cast<std::size_t>(error_buffer_length_);
    return data.subspan(offset, length);
}

ClusterNodeControlProperties ClusterMarkFile::load_control_properties() const
{
    ClusterNodeControlProperties properties;
    properties.member_id = member_id();
    properties.service_stream_id = service_stream_id();
    properties.consensus_module_stream_id = consensus_module_stream_id();
    properties.aeron_directory = aeron_directory();
    properties.control_channel = control_channel();
    return properties;
}

namespace
{

[[nodiscard]] std::int32_t detect_header_offset(std::span<const std::uint8_t> data) noexcept
{
    if (data.size() < mark::MessageHeader::encodedLength())
    {
        return 0;
    }

    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(data.data()));
    try
    {
        mark::MessageHeader header(buffer, 0, data.size(), mark::MessageHeader::sbeSchemaVersion());
        if (mark::MarkFileHeader::SBE_TEMPLATE_ID == header.templateId() &&
            mark::MarkFileHeader::SBE_SCHEMA_ID == header.schemaId())
        {
            return static_cast<std::int32_t>(mark::MessageHeader::encodedLength());
        }
    }
    catch (const std::exception&)
    {
        // Too short to hold a message header: treat it as the legacy layout.
        return 0;
    }

    return 0;
}

}  // namespace

void ClusterMarkFile::reset_activity_on_failure() noexcept
{
    auto data = mapped();
    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    __atomic_store_n(reinterpret_cast<std::int64_t*>(timestamp_address), static_cast<std::int64_t>(NULL_VALUE),
                     __ATOMIC_RELEASE);
    auto forced = force();
    if (!forced)
    {
        // Best effort: the caller is about to fail anyway.
    }
}

std::expected<void, Error> ClusterMarkFile::validate_existing(const Options& options)
{
    auto data = mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    const auto length = static_cast<std::uint64_t>(data.size());

    if (data.size() < static_cast<std::size_t>(header_offset_))
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "mark file too short"));
    }

    mark::MarkFileHeader header;
    if (0 != header_offset_)
    {
        header = decode_mark_header(buffer, header_offset_, length);
    }
    else
    {
        header.wrapForDecode(buffer, 0, mark::MarkFileHeader::SBE_BLOCK_LENGTH,
                             mark::MarkFileHeader::SBE_SCHEMA_VERSION, length);
    }

    const std::int32_t version = header.version();
    if (VERSION_FAILED == version)
    {
        previous_startup_failed_ = true;
    }
    else if (semantic_major(version) != CLUSTER_MARK_MAJOR_VERSION)
    {
        reset_activity_on_failure();
        return std::unexpected(make_error(
            ErrorCode::protocol_error, "mark file major version " + std::to_string(semantic_major(version)) +
                                           " does not match software: " + std::to_string(CLUSTER_MARK_MAJOR_VERSION)));
    }

    const auto existing_type = static_cast<ClusterComponentType>(header.componentType());
    if (ClusterComponentType::unknown != existing_type && existing_type != options.component_type)
    {
        if (ClusterComponentType::backup != existing_type ||
            ClusterComponentType::consensus_module != options.component_type)
        {
            reset_activity_on_failure();
            return std::unexpected(make_error(
                ErrorCode::protocol_error,
                "existing Mark file type " + std::string{cluster_component_type_name(existing_type)} +
                    " not same as required type " + std::string{cluster_component_type_name(options.component_type)}));
        }
    }

    inherited_candidate_term_id_ = header.candidateTermId();

    auto* timestamp_address = data.data() + body_field_offset(mark::MarkFileHeader::activityTimestampEncodingOffset());
    auto& timestamp_ref = *reinterpret_cast<std::int64_t*>(timestamp_address);
    std::int64_t existing_timestamp = __atomic_load_n(&timestamp_ref, __ATOMIC_ACQUIRE);
    const std::int64_t timestamp_diff = options.now_ms - existing_timestamp;

    if (timestamp_diff < options.timeout_ms)
    {
        return std::unexpected(
            make_error(ErrorCode::protocol_error,
                       "active mark file: " + options.path + " timestamp=" + std::to_string(existing_timestamp)));
    }

    if (!__atomic_compare_exchange_n(&timestamp_ref, &existing_timestamp, ACTIVATION_IN_PROGRESS_TIMESTAMP, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    {
        return std::unexpected(
            make_error(ErrorCode::protocol_error, "failed to claim active mark file: " + options.path));
    }

    const std::int32_t existing_error_buffer_length = header.errorBufferLength();
    const std::int32_t existing_header_length = header.headerLength();
    if (existing_error_buffer_length > 0 && existing_header_length > 0 &&
        static_cast<std::uint64_t>(existing_header_length) + static_cast<std::uint64_t>(existing_error_buffer_length) <=
            data.size())
    {
        error_buffer_length_ = existing_error_buffer_length;
        save_existing_errors(options);
        auto errors = error_buffer();
        std::memset(errors.data(), 0, errors.size());
    }

    if (0 == header_offset_)
    {
        std::memset(data.data(), 0, data.size());
        header_offset_ = HEADER_OFFSET;
    }

    return {};
}

std::expected<void, Error> ClusterMarkFile::write_header(const Options& options, std::int64_t candidate_term_id)
{
    auto data = mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    const auto length = static_cast<std::uint64_t>(data.size());

    mark::MarkFileHeader header;
    header.wrapAndApplyHeader(buffer, 0, length);
    header.componentType(static_cast<mark::ClusterComponentType::Value>(options.component_type))
        .startTimestamp(options.now_ms)
        .pid(static_cast<std::int64_t>(::getpid()))
        .candidateTermId(candidate_term_id)
        .headerLength(HEADER_LENGTH)
        .errorBufferLength(options.error_buffer_length);

    error_buffer_length_ = options.error_buffer_length;
    return {};
}

std::expected<ClusterMarkFile, Error> ClusterMarkFile::create_or_open(const Options& options)
{
    if (options.error_buffer_length < ERROR_BUFFER_MIN_LENGTH || options.error_buffer_length > ERROR_BUFFER_MAX_LENGTH)
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument,
                                          "Invalid errorBufferLength: " + std::to_string(options.error_buffer_length)));
    }

    if (options.file_page_size < 4096)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument,
                       "Page size less than min size of 4096: page size=" + std::to_string(options.file_page_size)));
    }
    if (options.file_page_size > (1 << 30))
    {
        return std::unexpected(make_error(
            ErrorCode::invalid_argument,
            "Page size more than max size of 1073741824: page size=" + std::to_string(options.file_page_size)));
    }
    if (!is_power_of_two(options.file_page_size))
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "Page size not a power of 2: page size=" +
                                                                           std::to_string(options.file_page_size)));
    }

    const std::size_t total_file_length =
        align(static_cast<std::size_t>(HEADER_LENGTH) + static_cast<std::size_t>(options.error_buffer_length),
              static_cast<std::size_t>(options.file_page_size));

    ClusterMarkFile mark_file;
    std::int64_t candidate_term_id = NULL_VALUE;

    if (axc::core::file_exists(options.path))
    {
        auto file = axc::core::MappedFile::open(options.path, true);
        if (!file)
        {
            return std::unexpected(file.error());
        }
        mark_file.file_ = std::move(*file);

        if (mark_file.file_.length() < total_file_length)
        {
            auto grown = mark_file.file_.grow(total_file_length);
            if (!grown)
            {
                return std::unexpected(grown.error());
            }
        }

        mark_file.header_offset_ = detect_header_offset(mark_file.mapped());

        auto validated = mark_file.validate_existing(options);
        if (!validated)
        {
            return std::unexpected(validated.error());
        }

        candidate_term_id = mark_file.inherited_candidate_term_id_;
    }
    else
    {
        auto file = axc::core::MappedFile::create(options.path, total_file_length);
        if (!file)
        {
            return std::unexpected(file.error());
        }
        mark_file.file_ = std::move(*file);
        mark_file.header_offset_ = HEADER_OFFSET;
    }

    auto written = mark_file.write_header(options, candidate_term_id);
    if (!written)
    {
        return std::unexpected(written.error());
    }

    return mark_file;
}

std::expected<ClusterMarkFile, Error> ClusterMarkFile::open_existing(std::string_view path, std::int64_t now_ms)
{
    (void)now_ms;
    auto file = axc::core::MappedFile::open(path, true);
    if (!file)
    {
        return std::unexpected(file.error());
    }

    ClusterMarkFile mark_file;
    mark_file.file_ = std::move(*file);
    mark_file.header_offset_ = detect_header_offset(mark_file.mapped());

    auto data = mark_file.mapped();
    auto* buffer = reinterpret_cast<char*>(data.data());
    const auto length = static_cast<std::uint64_t>(data.size());

    mark::MarkFileHeader header;
    if (0 != mark_file.header_offset_)
    {
        header = decode_mark_header(buffer, mark_file.header_offset_, length);
    }
    else
    {
        header.wrapForDecode(buffer, 0, mark::MarkFileHeader::SBE_BLOCK_LENGTH,
                             mark::MarkFileHeader::SBE_SCHEMA_VERSION, length);
    }

    const std::int32_t version = header.version();
    if (VERSION_FAILED == version)
    {
        mark_file.previous_startup_failed_ = true;
    }
    else if (semantic_major(version) != CLUSTER_MARK_MAJOR_VERSION)
    {
        return std::unexpected(make_error(
            ErrorCode::protocol_error, "mark file major version " + std::to_string(semantic_major(version)) +
                                           " does not match software: " + std::to_string(CLUSTER_MARK_MAJOR_VERSION)));
    }

    mark_file.semantic_version_ = version;
    mark_file.error_buffer_length_ = header.errorBufferLength();
    return mark_file;
}

void ClusterMarkFile::save_existing_errors(const Options& options)
{
    auto errors = error_buffer();
    if (!std::any_of(errors.begin(), errors.end(), [](std::uint8_t value) { return 0 != value; }))
    {
        return;
    }

    existing_errors_ = render_error_log(errors);

    std::error_code error_code;
    const std::filesystem::path path{options.path};
    const std::string file_name = std::string{cluster_component_type_name(options.component_type)} + '-' +
                                  format_file_name_time(options.now_ms) + "-error.log";
    const std::filesystem::path report = path.parent_path() / file_name;
    std::ofstream out{report};
    if (out)
    {
        out << existing_errors_;
    }
}

std::string ClusterMarkFile::render_error_log(std::span<const std::uint8_t> buffer)
{
    std::string rendered;
    std::size_t offset = 0;
    std::size_t entries = 0;

    while (offset + ERROR_LOG_ENTRY_HEADER_LENGTH <= buffer.size())
    {
        const std::int32_t length = get_le32(buffer.data() + offset);
        if (0 == length)
        {
            break;
        }

        const std::int32_t observation_count = get_le32(buffer.data() + offset + 4);
        const std::int64_t last_observation_timestamp = get_le64(buffer.data() + offset + 8);
        const std::int64_t first_observation_timestamp = get_le64(buffer.data() + offset + 16);

        if (static_cast<std::size_t>(length) < ERROR_LOG_ENTRY_HEADER_LENGTH)
        {
            break;
        }
        const std::size_t description_length = static_cast<std::size_t>(length) - ERROR_LOG_ENTRY_HEADER_LENGTH;
        const std::string description{
            reinterpret_cast<const char*>(buffer.data() + offset + ERROR_LOG_ENTRY_HEADER_LENGTH), description_length};

        rendered.push_back('\n');
        rendered.append(std::to_string(observation_count));
        rendered.append(" observations from ");
        rendered.append(format_date_time(first_observation_timestamp));
        rendered.append(" to ");
        rendered.append(format_date_time(last_observation_timestamp));
        rendered.append(" for:\n");
        rendered.append(description);
        rendered.push_back('\n');

        ++entries;
        offset += align(static_cast<std::size_t>(length), ERROR_LOG_ALIGNMENT);
    }

    rendered.push_back('\n');
    if (0 == entries)
    {
        rendered.append("0 distinct errors observed\n");
    }
    else
    {
        rendered.append(std::to_string(entries));
        rendered.append(" distinct errors observed.\n");
    }

    return rendered;
}

std::string ClusterMarkFile::mark_filename_for_service(std::int32_t service_id)
{
    return std::string{SERVICE_FILE_NAME_PREFIX} + std::to_string(service_id) + std::string{FILE_EXTENSION};
}

std::string ClusterMarkFile::link_filename_for_service(std::int32_t service_id)
{
    return std::string{SERVICE_FILE_NAME_PREFIX} + std::to_string(service_id) + std::string{LINK_FILE_EXTENSION};
}

std::string ClusterMarkFile::resolve_mark_file_dir(std::string_view directory)
{
    const std::string link_path = join_path(directory, LINK_FILE_NAME);
    if (!axc::core::file_exists(link_path))
    {
        return std::string{directory};
    }

    std::ifstream input{link_path};
    std::string target;
    std::getline(input, target);
    while (!target.empty() && ('\n' == target.back() || '\r' == target.back() || ' ' == target.back()))
    {
        target.pop_back();
    }

    if (target.empty())
    {
        return std::string{directory};
    }

    // `ClusterToolOperator.resolveDirectoryFromLinkFile`: the link contains the
    // mark file's **directory**, not the file.
    std::filesystem::path target_path{target};
    if (target_path.is_relative())
    {
        target_path = std::filesystem::path{std::string{directory}} / target_path;
    }

    return target_path.string();
}

}  // namespace deepmsg::cluster::common
