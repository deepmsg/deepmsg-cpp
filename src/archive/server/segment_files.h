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

/// `ArchiveConductor.DELETE_SUFFIX` (`ArchiveConductor.java:130`): a segment
/// file left behind by an interrupted delete is renamed to `<name>.del` on the
/// next attempt (`DeleteSegmentsSession.doDeleteFiles`).
inline constexpr std::string_view DELETE_SUFFIX = ".del";

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

/// `Catalog.listSegmentFiles` (`Catalog.java:1123`). Only `.rec`: the crash
/// repair pass must not see half-deleted files.
[[nodiscard]] std::vector<std::string> list_segment_files(std::string_view archive_dir, std::int64_t recording_id);

/// `ArchiveConductor.listSegmentFiles` (`ArchiveConductor.java:1981-2005`):
/// `.rec` **and** `.del`. The delete paths use this one, so an interrupted
/// delete can be retried. (The two methods really are different in the
/// reference; do not merge them.)
[[nodiscard]] std::vector<std::string> list_segment_files_with_delete_suffix(std::string_view archive_dir,
                                                                             std::int64_t recording_id);

/// `ArchiveConductor.findDetachedSegments` (`ArchiveConductor.java:1722-1745`):
/// the file names between `prev_start_position` and the current
/// `start_position`, one segment apart, ascending.
[[nodiscard]] std::vector<std::string> find_detached_segments(std::string_view archive_dir, std::int64_t recording_id,
                                                              std::int64_t start_position,
                                                              std::int64_t prev_start_position,
                                                              std::int32_t term_length, std::int32_t segment_length);

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

/// The digits between `<recordingId>-` and the first `.`, i.e. what
/// `ArchiveConductor.deleteDetachedSegments` (`:1526-1535`) and
/// `DeleteSegmentsSession`'s constructor (`:76-86`) read. Also accepts the
/// `.del` names an interrupted delete leaves behind.
[[nodiscard]] bool parse_segment_file_position_for_recording(std::string_view file_name, std::int64_t recording_id,
                                                             std::int64_t& position) noexcept;

/// `ReplaySession.notHeaderAligned` / `ArchiveConductor.eraseRemainingSegment`:
/// read the 32-byte frame header at `offset` and compare it with the expected
/// term offset, term id and stream id.
enum class FrameHeaderCheck
{
    ok,
    unaligned,
    read_error
};

[[nodiscard]] FrameHeaderCheck check_first_frame(int fd, std::int64_t offset, std::int32_t term_offset,
                                                 std::int32_t term_id, std::int32_t stream_id) noexcept;

/// `ArchiveConductor.eraseRemainingSegment` (`ArchiveConductor.java:2570-2623`):
/// verify the frame header at `segment_offset`, truncate the file there and put
/// one zero byte at `segment_length - 1` so the file still occupies a whole
/// segment. An error is the reference's `sendErrorResponse` message.
[[nodiscard]] std::expected<void, std::string> erase_remaining_segment(
    std::string_view archive_dir, std::string_view file_name, std::int64_t position, std::int32_t segment_length,
    std::int32_t segment_offset, std::int32_t term_length, std::int32_t term_id, std::int32_t stream_id) noexcept;

/// `ArchiveConductor.findTermOffsetForStart` (`ArchiveConductor.java:1789-1871`).
/// Returns the offset of the first valid frame in the file's first term, or the
/// reference's `sendErrorResponse` message.
[[nodiscard]] std::expected<std::int32_t, std::string> find_term_offset_for_start(std::string_view archive_dir,
                                                                                  std::string_view file_name,
                                                                                  std::int32_t stream_id,
                                                                                  std::int32_t term_id,
                                                                                  std::int32_t term_length);

}  // namespace deepmsg::archive::server
