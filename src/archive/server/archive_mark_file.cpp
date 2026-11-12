#include "archive/server/archive_mark_file.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <utility>

#include "core/io/distinct_error_log.h"
#include "core/io/fs.h"

namespace deepmsg::archive::server
{
namespace
{

constexpr std::int32_t NULL_VALUE = -1;

[[nodiscard]] std::string mark_file_path(const std::string& directory)
{
    return directory + "/" + ArchiveMarkFile::FILENAME;
}

/// `ArchiveMarkFile.headerOffset(File)` (`ArchiveMarkFile.java:528-549`): a
/// modern file starts with a message header carrying the mark template id; an
/// older one starts with the block itself.
[[nodiscard]] std::int32_t detect_header_offset(const std::uint8_t* bytes) noexcept
{
    archive_mark_codecs::MessageHeader message_header;
    message_header.wrap(const_cast<char*>(reinterpret_cast<const char*>(bytes)), 0,
                        archive_mark_codecs::MessageHeader::sbeSchemaVersion(),
                        archive_mark_codecs::MessageHeader::encodedLength());

    return archive_mark_codecs::MarkFileHeader::SBE_TEMPLATE_ID == message_header.templateId() &&
                   archive_mark_codecs::MarkFileHeader::SBE_SCHEMA_ID == message_header.schemaId()
               ? ArchiveMarkFile::HEADER_OFFSET
               : 0;
}

[[nodiscard]] axc::core::Error version_error(const std::string& path, std::int32_t version)
{
    const std::int32_t major = (version >> 16) & 0xFFFF;
    return axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                 "mark file (" + path + ") major version " + std::to_string(major) +
                                     " does not match software: " + std::to_string(ArchiveMarkFile::MAJOR_VERSION));
}

/// The SBE header block has to fit the mapping before it is decoded; the
/// reference's `headerDecoder.wrap` throws otherwise (inside the try block that
/// resets the activity timestamp).
[[nodiscard]] bool header_fits(const std::span<const std::uint8_t> bytes, std::int32_t header_offset) noexcept
{
    const auto start = static_cast<std::size_t>(header_offset);
    return bytes.size() >= start && archive_mark_codecs::MarkFileHeader::SBE_BLOCK_LENGTH <= bytes.size() - start;
}

}  // namespace

ArchiveMarkFile::ArchiveMarkFile(ArchiveMarkFile&& other) noexcept = default;

ArchiveMarkFile& ArchiveMarkFile::operator=(ArchiveMarkFile&& other) noexcept = default;

void ArchiveMarkFile::close()
{
    mark_file_ = axc::core::MarkFile{};
}

std::expected<ArchiveMarkFile, axc::core::Error> ArchiveMarkFile::create_or_open(const Options& options,
                                                                                 const HeaderValues& values)
{
    const std::string path = mark_file_path(options.directory);
    const bool exists = axc::core::file_exists(path);
    const std::int64_t now_ms =
        nullptr != options.epoch_clock ? options.epoch_clock->time_ms() : axc::core::EpochClock::now_ms();

    ArchiveMarkFile archive_mark;
    archive_mark.error_buffer_length_ = options.error_buffer_length;

    std::int32_t header_offset = HEADER_OFFSET;
    if (exists)
    {
        // Read the first bytes to tell a modern layout from a legacy one.
        auto probe = axc::core::MappedFile::open(path, false);
        if (!probe)
        {
            return std::unexpected(probe.error());
        }
        const std::span<const std::uint8_t> bytes = probe->mapping();
        header_offset = bytes.size() < static_cast<std::size_t>(HEADER_OFFSET) ? 0 : detect_header_offset(bytes.data());
    }
    archive_mark.header_offset_ = header_offset;

    if (exists)
    {
        // `ArchiveMarkFile.java:121-131`: open the existing file. Agrona
        // checks the version before it touches the activity timestamp.
        auto mark_file = axc::core::MarkFile::open_existing(
            path,
            static_cast<std::size_t>(header_offset + archive_mark_codecs::MarkFileHeader::versionEncodingOffset()),
            static_cast<std::size_t>(header_offset +
                                     archive_mark_codecs::MarkFileHeader::activityTimestampEncodingOffset()));
        if (!mark_file)
        {
            return std::unexpected(mark_file.error());
        }
        archive_mark.mark_file_ = std::move(*mark_file);

        const std::int32_t version = archive_mark.mark_file_.version();
        if (((version >> 16) & 0xFFFF) != MAJOR_VERSION)
        {
            return std::unexpected(version_error(path, version));
        }

        // `MarkFile.mapNewOrExistingMarkFile`: one liveness decision, then the
        // CAS claim. Not a polling wait.
        auto activated = archive_mark.mark_file_.activate(now_ms, LIVENESS_TIMEOUT_MS, path);
        if (!activated)
        {
            return std::unexpected(activated.error());
        }

        // `ArchiveMarkFile.java:190-196`: a start that fails after the claim
        // must not leave an "always active" timestamp behind.
        const auto fail = [&archive_mark](axc::core::Error error) -> std::expected<ArchiveMarkFile, axc::core::Error>
        {
            archive_mark.mark_file_.update_activity(NULL_VALUE);
            const auto flushed = archive_mark.mark_file_.flush();
            (void)flushed;
            return std::unexpected(error);
        };

        const std::span<std::uint8_t> existing = archive_mark.mark_file_.mapped();
        if (!header_fits(existing, header_offset))
        {
            return fail(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                              "mark file too short to hold its header: " + path));
        }

        archive_mark.refresh_header();
        // `ArchiveMarkFile.java:146-160`: before the header is overwritten,
        // save the previous process's errors and zero their buffer.
        archive_mark.save_existing_errors(path);

        if (HEADER_OFFSET != header_offset)
        {
            // `ArchiveMarkFile.java:166-203`: rebuild a legacy file in the
            // current layout (the old mapping dies with the old file).
            archive_mark.mark_file_ = axc::core::MarkFile{};
            auto recreated = axc::core::MarkFile::create(
                path, static_cast<std::size_t>(options.total_file_length),
                static_cast<std::size_t>(HEADER_OFFSET + archive_mark_codecs::MarkFileHeader::versionEncodingOffset()),
                static_cast<std::size_t>(HEADER_OFFSET +
                                         archive_mark_codecs::MarkFileHeader::activityTimestampEncodingOffset()));
            if (!recreated)
            {
                return std::unexpected(recreated.error());
            }
            archive_mark.mark_file_ = std::move(*recreated);
            archive_mark.header_offset_ = HEADER_OFFSET;
            std::memset(archive_mark.mark_file_.mapped().data(), 0, archive_mark.mark_file_.mapped().size());
            archive_mark.refresh_header();
        }
    }
    else
    {
        // New file (`ArchiveMarkFile.java:205-218`).
        auto mark_file = axc::core::MarkFile::create(
            path, static_cast<std::size_t>(options.total_file_length),
            static_cast<std::size_t>(header_offset + archive_mark_codecs::MarkFileHeader::versionEncodingOffset()),
            static_cast<std::size_t>(header_offset +
                                     archive_mark_codecs::MarkFileHeader::activityTimestampEncodingOffset()));
        if (!mark_file)
        {
            return std::unexpected(mark_file.error());
        }
        archive_mark.mark_file_ = std::move(*mark_file);
        archive_mark.header_offset_ = HEADER_OFFSET;
        std::memset(archive_mark.mark_file_.mapped().data(), 0, archive_mark.mark_file_.mapped().size());
        archive_mark.refresh_header();
    }

    archive_mark.header_.pid(static_cast<std::int64_t>(::getpid()));
    archive_mark.apply(values);
    return archive_mark;
}

void ArchiveMarkFile::save_existing_errors(const std::string& mark_file_path_value)
{
    const std::int32_t old_header_length = header_.headerLength();
    const std::int32_t old_error_buffer_length = header_.errorBufferLength();
    const std::span<std::uint8_t> bytes = mark_file_.mapped();
    if (old_error_buffer_length <= 0 || old_header_length <= 0 ||
        static_cast<std::size_t>(old_header_length) + static_cast<std::size_t>(old_error_buffer_length) > bytes.size())
    {
        // A v0/v1 layout or a truncated file: no readable error region.
        return;
    }

    const auto error_buffer = std::span<const std::uint8_t>(bytes.data() + old_header_length,
                                                            static_cast<std::size_t>(old_error_buffer_length));
    const axc::core::RenderedErrorLog rendered = axc::core::render_distinct_error_log(error_buffer);

    if (rendered.distinct_error_count > 0)
    {
        // `CommonContext.saveExistingErrors`: system clock, system time zone,
        // six fractional digits and a numeric offset.
        const std::filesystem::path directory = std::filesystem::path{mark_file_path_value}.parent_path();
        const std::string error_log_path =
            (directory /
             ("archive-" + axc::core::format_file_name_time(axc::core::EpochClock::now_ms()) + "-error.log"))
                .string();

        const int fd = ::open(error_log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        bool saved = false;
        if (fd >= 0)
        {
            const ssize_t written = ::write(fd, rendered.text.data(), rendered.text.size());
            saved = written == static_cast<ssize_t>(rendered.text.size());
            ::close(fd);
        }

        if (saved)
        {
            std::fprintf(stderr, "WARNING: existing errors saved to: %s\n", error_log_path.c_str());
        }
        else
        {
            // `CommonContext.java:1344-1352`: log the failure and dump the
            // rendered errors to the logger.
            std::fprintf(stderr, "ERROR: Failed to save existing errors to: %s\n", error_log_path.c_str());
            std::fprintf(stderr, "\nDumping errors here:\n\n");
            std::fwrite(rendered.text.data(), 1, rendered.text.size(), stderr);
        }
    }

    // The buffer is cleared whether or not the file could be written.
    std::memset(bytes.data() + old_header_length, 0, static_cast<std::size_t>(old_error_buffer_length));
}

std::expected<ArchiveMarkFile, axc::core::Error> ArchiveMarkFile::open_existing(const Options& options)
{
    return open_existing(options,
                         [path = mark_file_path(options.directory)](std::int32_t version)
                         {
                             if (((version >> 16) & 0xFFFF) != MAJOR_VERSION)
                             {
                                 throw axc::core::ConfigError(version_error(path, version).message.c_str());
                             }
                         });
}

std::expected<ArchiveMarkFile, axc::core::Error> ArchiveMarkFile::open_existing(
    const Options& options, const std::function<void(std::int32_t)>& version_check)
{
    const std::string path = mark_file_path(options.directory);
    if (!axc::core::file_exists(path))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::not_found, "no mark file: " + path));
    }

    auto probe = axc::core::MappedFile::open(path, false);
    if (!probe)
    {
        return std::unexpected(probe.error());
    }
    const std::span<const std::uint8_t> bytes = probe->mapping();
    const std::int32_t header_offset =
        bytes.size() < static_cast<std::size_t>(HEADER_OFFSET) ? 0 : detect_header_offset(bytes.data());

    ArchiveMarkFile archive_mark;
    archive_mark.header_offset_ = header_offset;
    archive_mark.error_buffer_length_ = options.error_buffer_length;

    auto mark_file = axc::core::MarkFile::open_existing(
        path, static_cast<std::size_t>(header_offset + archive_mark_codecs::MarkFileHeader::versionEncodingOffset()),
        static_cast<std::size_t>(header_offset +
                                 archive_mark_codecs::MarkFileHeader::activityTimestampEncodingOffset()));
    if (!mark_file)
    {
        return std::unexpected(mark_file.error());
    }
    archive_mark.mark_file_ = std::move(*mark_file);
    archive_mark.refresh_header();

    if (version_check)
    {
        try
        {
            version_check(archive_mark.mark_file_.version());
        }
        catch (const std::exception& error)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error, error.what()));
        }
    }

    archive_mark.error_buffer_length_ = archive_mark.header_.errorBufferLength();
    return archive_mark;
}

void ArchiveMarkFile::refresh_header() noexcept
{
    const std::span<std::uint8_t> bytes = mark_file_.mapped();
    auto* buffer = reinterpret_cast<char*>(bytes.data());

    if (HEADER_OFFSET == header_offset_)
    {
        archive_mark_codecs::MessageHeader message_header;
        message_header.wrap(buffer, 0, archive_mark_codecs::MessageHeader::sbeSchemaVersion(), bytes.size());
        header_.wrapForDecode(buffer, static_cast<std::uint64_t>(HEADER_OFFSET),
                              archive_mark_codecs::MarkFileHeader::SBE_BLOCK_LENGTH, message_header.version(),
                              bytes.size());
    }
    else
    {
        // Legacy layout (`ArchiveMarkFile.java:273-280`): the block sits at 0.
        // Decode with the current schema version first, then re-wrap with the
        // version the presence of `headerLength` implies.
        constexpr std::uint64_t acting_block_length = 128;
        header_.wrapForDecode(buffer, 0, acting_block_length, archive_mark_codecs::MarkFileHeader::SBE_SCHEMA_VERSION,
                              bytes.size());

        const std::uint64_t acting_version = header_.headerLength() > 0 ? 1 : 0;
        header_.wrapForDecode(buffer, 0, acting_block_length, acting_version, bytes.size());
    }
}

void ArchiveMarkFile::apply(const HeaderValues& values)
{
    // `ArchiveMarkFile.encode` (`ArchiveMarkFile.java:502-516`). The encoder has
    // to start from the message header every time: the var-data fields are
    // written in order after the block.
    const std::span<std::uint8_t> bytes = mark_file_.mapped();
    archive_mark_codecs::MarkFileHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(bytes.data()), 0, bytes.size());

    encoder.pid(static_cast<std::int64_t>(::getpid()));
    encoder.startTimestamp(values.start_timestamp);
    encoder.controlStreamId(values.control_stream_id);
    encoder.localControlStreamId(values.local_control_stream_id);
    encoder.eventsStreamId(values.events_stream_id);
    encoder.headerLength(HEADER_LENGTH);
    encoder.errorBufferLength(error_buffer_length_);
    encoder.archiveId(values.archive_id);
    encoder.putControlChannel(values.control_channel);
    encoder.putLocalControlChannel(values.local_control_channel);
    encoder.putEventsChannel(values.events_channel);
    encoder.putAeronDirectory(values.aeron_directory);

    refresh_header();
}

void ArchiveMarkFile::signal_ready(std::int64_t activity_timestamp)
{
    // `ArchiveMarkFile.signalReady(long)` (`ArchiveMarkFile.java:342-350`).
    if (is_closed())
    {
        return;
    }
    mark_file_.update_activity(activity_timestamp);
    mark_file_.set_version(SEMANTIC_VERSION);
}

void ArchiveMarkFile::signal_terminated()
{
    signal_ready(NULL_VALUE);
}

void ArchiveMarkFile::update_activity_timestamp(std::int64_t now_ms)
{
    if (!is_closed())
    {
        mark_file_.update_activity(now_ms);
    }
}

std::int64_t ArchiveMarkFile::activity_timestamp_volatile() const
{
    return is_closed() ? NULL_VALUE : mark_file_.activity_timestamp();
}

std::int32_t ArchiveMarkFile::version() const
{
    return is_closed() ? 0 : mark_file_.version();
}

std::expected<void, axc::core::Error> ArchiveMarkFile::force()
{
    if (is_closed())
    {
        return {};
    }
    return mark_file_.flush();
}

std::span<std::uint8_t> ArchiveMarkFile::buffer() noexcept
{
    return mark_file_.mapped();
}

std::span<std::uint8_t> ArchiveMarkFile::error_buffer() noexcept
{
    const std::span<std::uint8_t> bytes = mark_file_.mapped();
    if (error_buffer_length_ <= 0 || static_cast<std::size_t>(HEADER_LENGTH + error_buffer_length_) > bytes.size())
    {
        return {};
    }
    return bytes.subspan(static_cast<std::size_t>(HEADER_LENGTH), static_cast<std::size_t>(error_buffer_length_));
}

std::int64_t ArchiveMarkFile::archive_id() const
{
    return is_closed() ? static_cast<std::int64_t>(NULL_VALUE) : header_.archiveId();
}

std::int32_t ArchiveMarkFile::pid() const
{
    return static_cast<std::int32_t>(header_.pid());
}

std::int64_t ArchiveMarkFile::start_timestamp() const
{
    return header_.startTimestamp();
}

std::int32_t ArchiveMarkFile::control_stream_id() const
{
    return header_.controlStreamId();
}

std::int32_t ArchiveMarkFile::local_control_stream_id() const
{
    return header_.localControlStreamId();
}

std::int32_t ArchiveMarkFile::events_stream_id() const
{
    return header_.eventsStreamId();
}

std::int32_t ArchiveMarkFile::header_length() const
{
    return header_.headerLength();
}

std::int32_t ArchiveMarkFile::error_buffer_length() const
{
    return header_.errorBufferLength();
}

std::string ArchiveMarkFile::control_channel() const
{
    auto header = header_;
    header.sbeRewind();
    return header.getControlChannelAsString();
}

std::string ArchiveMarkFile::local_control_channel() const
{
    auto header = header_;
    header.sbeRewind();
    header.skipControlChannel();
    return header.getLocalControlChannelAsString();
}

std::string ArchiveMarkFile::events_channel() const
{
    auto header = header_;
    header.sbeRewind();
    header.skipControlChannel();
    header.skipLocalControlChannel();
    return header.getEventsChannelAsString();
}

std::string ArchiveMarkFile::aeron_directory() const
{
    // `ArchiveMarkFile.aeronDirectory` (`ArchiveMarkFile.java:488-495`).
    auto header = header_;
    header.sbeRewind();
    header.skipControlChannel();
    header.skipLocalControlChannel();
    header.skipEventsChannel();
    return header.getAeronDirectoryAsString();
}

}  // namespace deepmsg::archive::server
