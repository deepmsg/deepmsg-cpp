#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <string>
#include <string_view>

#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// A recording descriptor as the source archive reports it
/// (`RecordingDescriptor` template 22).
struct SourceRecordingDescriptor
{
    std::int64_t control_session_id{0};
    std::int64_t correlation_id{0};
    std::int64_t recording_id{0};
    std::int64_t start_timestamp{0};
    std::int64_t stop_timestamp{0};
    std::int64_t start_position{0};
    std::int64_t stop_position{0};
    std::int32_t initial_term_id{0};
    std::int32_t segment_file_length{0};
    std::int32_t term_buffer_length{0};
    std::int32_t mtu_length{0};
    std::int32_t session_id{0};
    std::int32_t stream_id{0};
    std::string stripped_channel;
    std::string original_channel;
    std::string source_identity;
};

/// The source-side client a `ReplicationSession` drives
/// (`ReplicationSession.java`'s embedded `AeronArchive`): the operations the
/// state machine uses, and nothing else. `SourceArchiveClient` is the product
/// implementation; a unit test substitutes a fake so the states can be driven
/// without a second archive.
class ReplicationSource
{
public:
    /// `ControlResponse` / `Challenge` from the source
    /// (`ControlResponsePoller.poll()`).
    struct Response
    {
        enum class Kind
        {
            none,
            ok,
            error,
            challenged
        };

        Kind kind{Kind::none};
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::int64_t relevant_id{0};
        std::int32_t code{0};
        std::string error_message;
        std::string encoded_challenge;
    };

    ReplicationSource() = default;
    ReplicationSource(const ReplicationSource&) = delete;
    ReplicationSource& operator=(const ReplicationSource&) = delete;
    virtual ~ReplicationSource() = default;

    /// Advances the connect state machine (`CONNECT`).
    virtual int do_work() = 0;

    [[nodiscard]] virtual bool is_connected() const noexcept = 0;
    [[nodiscard]] virtual bool is_failed() const noexcept = 0;
    [[nodiscard]] virtual std::string_view error_message() const noexcept = 0;
    [[nodiscard]] virtual std::int64_t control_session_id() const = 0;
    [[nodiscard]] virtual std::int64_t next_correlation_id() = 0;

    // Offers (`AeronArchive.archiveProxy()`), all non-blocking.
    virtual bool list_recording(std::int64_t correlation_id, std::int64_t recording_id) = 0;
    virtual bool replay(std::int64_t correlation_id, std::int64_t recording_id, std::string_view replay_channel,
                        std::int32_t replay_stream_id, std::int64_t position, std::int64_t length,
                        std::int32_t file_io_max_length) = 0;
    virtual bool stop_replay(std::int64_t correlation_id, std::int64_t replay_session_id) = 0;
    virtual bool get_recording_position(std::int64_t correlation_id, std::int64_t recording_id) = 0;

    /// One pass of the control response poller.
    [[nodiscard]] virtual std::expected<Response, axc::core::Error> poll_response() = 0;

    /// Arms the descriptor poller for one `list_recording`
    /// (`RecordingDescriptorPoller.reset(correlationId, 1, consumer)`); the
    /// consumer is called from `poll_descriptors`.
    virtual void reset_descriptor_poller(std::int64_t correlation_id, std::int32_t record_count,
                                         std::function<void(SourceRecordingDescriptor&)> consumer) = 0;

    /// One pass of the descriptor poller.
    [[nodiscard]] virtual std::expected<int, axc::core::Error> poll_descriptors() = 0;

    [[nodiscard]] virtual bool descriptor_dispatch_complete() const noexcept = 0;
    [[nodiscard]] virtual std::int32_t descriptor_remaining_record_count() const noexcept = 0;
};

}  // namespace deepmsg::archive::server
