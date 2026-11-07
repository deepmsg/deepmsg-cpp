#pragma once

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `Archive.Configuration.RECORDING_SEGMENT_SUFFIX` (`Archive.java:295`).
inline constexpr std::string_view RECORDING_SEGMENT_SUFFIX = ".rec";

/// `Archive.segmentFileName` (`Archive.java:4091`): `<recordingId>-<segmentBasePosition>.rec`.
[[nodiscard]] std::string segment_file_name(std::int64_t recording_id, std::int64_t segment_base_position);

/// `max(ctx.segmentFileLength(), termBufferLength)` (`ArchiveConductor.java:2028`):
/// a segment can never be shorter than one term, otherwise a term would span
/// segment files.
[[nodiscard]] std::int32_t segment_file_length(std::int32_t term_length, std::int32_t segment_length) noexcept;

/// `AeronArchive.segmentFileBasePosition` (`AeronArchive.java:195`): the base
/// position of the segment file holding `position`, measured from the term base
/// of `start_position`.
[[nodiscard]] std::int64_t segment_file_base_position(std::int64_t start_position, std::int64_t position,
                                                      std::int32_t term_length, std::int32_t segment_length) noexcept;

// The helpers below are `Catalog`'s (`Catalog.java:1123-1221`) and are shared
// with the archive tool and the repair pass.

/// `Catalog.listSegmentFiles` (`Catalog.java:1123`).
[[nodiscard]] std::vector<std::string> list_segment_files(std::string_view archive_dir, std::int64_t recording_id);

/// `Catalog.indexSegmentFiles` (`Catalog.java:1144`). A file whose name has no
/// usable `<id>-` prefix is skipped, exactly like the reference's
/// `InvalidRecordingNameException`; an id that is not a number aborts, also like
/// the reference.
[[nodiscard]] std::expected<std::unordered_map<std::int64_t, std::vector<std::string>>, axc::core::Error>
index_segment_files(std::string_view archive_dir);

/// `Catalog.findSegmentFileWithHighestPosition` (`Catalog.java:1171`). An empty
/// name means "none" (the reference returns null).
[[nodiscard]] std::expected<std::string, axc::core::Error> find_segment_file_with_highest_position(
    const std::vector<std::string>& segment_files);

/// `Catalog.parseSegmentFilePosition` (`Catalog.java:1194`).
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> parse_segment_file_position(const std::string& filename);

/// `Catalog.parseSegmentFileRecordingId` (`Catalog.java:1212`).
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> parse_segment_file_recording_id(
    const std::string& filename);

}  // namespace deepmsg::archive::server
