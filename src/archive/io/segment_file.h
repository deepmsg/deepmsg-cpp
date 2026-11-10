#pragma once

#include <cstdint>
#include <expected>

#include "archive/io/segment_writer.h"
#include "core/error/error.h"

namespace deepmsg::archive::server::io
{

/// A segment file opened and sized the way every write backend needs it.
struct OpenedSegment
{
    int fd{-1};
    /// Whether the archive directory was forced while creating the file
    /// (`RecordingWriter.java:221`).
    bool directory_forced{false};
};

/// Opens `<archive_dir>/<recordingId>-<basePosition>.rec`, sizes it to
/// `config.segment_length`, and (when the sync level asks for it) forces the
/// archive directory entry, like `RandomAccessFile` + `setLength` followed by
/// `RecordingWriter.forceDirectory` (`RecordingWriter.java:218-221`).
///
/// `fail_if_exists` is the roll-over case: the reference refuses to overwrite
/// the next segment (`RecordingWriter.java:242`).
[[nodiscard]] std::expected<OpenedSegment, axc::core::Error> open_segment_file(const WriterConfig& config,
                                                                               std::int64_t base_position,
                                                                               bool fail_if_exists) noexcept;

}  // namespace deepmsg::archive::server::io
