#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <aeronc.h>

#include "core/error/error.h"

struct aeron_archive_context_stct;
struct aeron_archive_async_connect_stct;
struct aeron_archive_stct;
struct aeron_archive_recording_descriptor_stct;

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

/// The source-side client a replication runs against
/// (`ReplicationSession.java`'s embedded `AeronArchive`).
///
/// It is the **official C archive client's non-blocking pieces** — async
/// connect, the archive proxy's offers and the two pollers — driven from the
/// conductor's own `aeron_t` (the probe in
/// `test/interop/archive/replication_client_probe_test.cpp` measures the
/// shared-client shape). Nothing here blocks: `do_work` advances one step and
/// every request sender only offers.
class SourceArchiveClient
{
public:
    struct Config
    {
        /// The source archive's control channel/stream (from the request).
        std::string control_request_channel;
        std::int32_t control_request_stream_id{0};
        /// Derived by the conductor from its own control channel
        /// (`Archive.java:1587-1610`), or the request's `srcResponseChannel`.
        std::string control_response_channel;
        std::int32_t control_response_stream_id{20};
        std::string client_name;
        /// The replication request's `encodedCredentials`; empty ⇒ the client's
        /// null credentials supplier.
        std::string encoded_credentials;
    };

    /// Holds the replication request's encoded credentials and the C view the
    /// supplier callbacks hand out (the client only borrows them).
    struct Credentials;

    /// `aeron` is the conductor's client; the archive client shares it
    /// (`Archive.java:1620`).
    static std::expected<std::unique_ptr<SourceArchiveClient>, axc::core::Error> create(aeron_t* aeron,
                                                                                        const Config& config);

    ~SourceArchiveClient();

    SourceArchiveClient(const SourceArchiveClient&) = delete;
    SourceArchiveClient& operator=(const SourceArchiveClient&) = delete;

    /// Advances the connect state machine and the response subscription.
    /// Returns the number of things that changed this pass.
    int do_work();

    [[nodiscard]] bool is_connected() const noexcept
    {
        return nullptr != archive_;
    }

    [[nodiscard]] bool is_failed() const noexcept
    {
        return failed_;
    }

    [[nodiscard]] std::string_view error_message() const noexcept
    {
        return error_;
    }

    [[nodiscard]] std::int64_t control_session_id() const;

    [[nodiscard]] std::int64_t next_correlation_id();

    // Offers (`AeronArchive.archiveProxy()`), all non-blocking.
    [[nodiscard]] bool list_recording(std::int64_t correlation_id, std::int64_t recording_id);
    [[nodiscard]] bool replay(std::int64_t correlation_id, std::int64_t recording_id, std::string_view replay_channel,
                              std::int32_t replay_stream_id, std::int64_t position, std::int64_t length,
                              std::int32_t file_io_max_length);
    [[nodiscard]] bool stop_replay(std::int64_t correlation_id, std::int64_t replay_session_id);
    [[nodiscard]] bool get_recording_position(std::int64_t correlation_id, std::int64_t recording_id);

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

    /// One pass of the control response poller.
    [[nodiscard]] std::expected<Response, axc::core::Error> poll_response();

    /// Arms the descriptor poller for one `list_recording`
    /// (`RecordingDescriptorPoller.reset(correlationId, 1, consumer)`); the
    /// consumer is called from `poll_descriptors`.
    void reset_descriptor_poller(std::int64_t correlation_id, std::int32_t record_count,
                                 std::function<void(SourceRecordingDescriptor&)> consumer);

    /// One pass of the descriptor poller.
    [[nodiscard]] std::expected<int, axc::core::Error> poll_descriptors();

    [[nodiscard]] bool descriptor_dispatch_complete() const noexcept;
    [[nodiscard]] std::int32_t descriptor_remaining_record_count() const noexcept;

private:
    SourceArchiveClient(aeron_t* archive_aeron, const Config& config);

    /// The C descriptor poller's callback (the C type is only forward declared
    /// here; the bridge lives in the .cpp).
    static void on_descriptor(aeron_archive_recording_descriptor_stct* descriptor, void* clientd);

    aeron_archive_context_stct* context_{nullptr};
    aeron_archive_async_connect_stct* async_{nullptr};
    aeron_archive_stct* archive_{nullptr};
    std::string error_;
    bool failed_{false};
    std::function<void(SourceRecordingDescriptor&)> descriptor_consumer_;
    std::vector<SourceRecordingDescriptor> descriptors_;

    std::unique_ptr<Credentials> credentials_;
};

}  // namespace deepmsg::archive::server
