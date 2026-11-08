#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "archive/server/control_response_proxy.h"

namespace deepmsg::archive::server
{

/// `RecordingEventsProxy` (`RecordingEventsProxy.java:30-152`): the three events
/// an archive publishes on `aeron.archive.recording.events.channel`.
///
/// One publication is shared by every recording session; the conductor owns it
/// and hands this object to the sessions.
class RecordingEvents
{
public:
    /// `RecordingEventsProxy.SEND_ATTEMPTS`.
    static constexpr int SEND_ATTEMPTS = 3;

    explicit RecordingEvents(ResponsePublication& publication);

    RecordingEvents(const RecordingEvents&) = delete;
    RecordingEvents& operator=(const RecordingEvents&) = delete;

    /// `RecordingStarted` (template 101): offer, 3 attempts, stop on
    /// `NOT_CONNECTED`.
    void started(std::int64_t recording_id, std::int64_t start_position, std::int32_t session_id,
                 std::int32_t stream_id, std::string_view channel, std::string_view source_identity) noexcept;

    /// `RecordingProgress` (template 102): try-claim; false when the claim did
    /// not happen, which is what keeps `progressEventPosition` from advancing.
    [[nodiscard]] bool progress(std::int64_t recording_id, std::int64_t start_position, std::int64_t position) noexcept;

    /// `RecordingStopped` (template 103): try-claim, 3 attempts, stop on
    /// `NOT_CONNECTED`.
    void stopped(std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position) noexcept;

private:
    ResponsePublication& publication_;
    /// `ExpandableArrayBuffer(1024)`, one buffer per proxy.
    std::vector<std::uint8_t> buffer_;
};

}  // namespace deepmsg::archive::server
