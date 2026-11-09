#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace deepmsg::archive::server
{

/// Everything `ArchiveConductor.replicate` passes to a session
/// (`ReplicationSession.java:80-120`).
struct ReplicationConfig
{
    std::int64_t control_session_id{0};
    /// `replicationId` (`:82`): the id the destination's stop finds it by and
    /// the correlation id every signal/error goes out with.
    std::int64_t replication_id{0};
    std::int64_t src_recording_id{0};
    /// `Aeron.NULL_VALUE` ⇒ the session creates the destination recording row.
    std::int64_t dst_recording_id{-1};
    std::int64_t stop_position{-1};
    std::int64_t channel_tag_id{-1};
    std::int64_t subscription_tag_id{-1};
    std::int32_t src_control_stream_id{0};
    std::int32_t file_io_max_length{-1};
    std::int32_t replication_session_id{-1};
    std::string src_control_channel;
    std::string live_destination;
    std::string replication_channel;
    std::string src_response_channel;
    std::string encoded_credentials;
};

/// `ReplicationSession` (`ReplicationSession.java:60-75`): one archive-to-archive
/// copy, driven a state at a time from the conductor's session worker.
///
/// The state bodies land in A4b-4..A4b-7: CONNECT uses the `SourceArchiveClient`,
/// REPLICATE_DESCRIPTOR builds the destination catalog row, EXTEND asks the
/// conductor for the destination recording subscription, REPLAY/AWAIT_IMAGE
/// drive the source replay, REPLICATE watches the image, and `close()` sends
/// `REPLICATE_END`. Until then `do_work` is a no-op that keeps the session
/// alive, which is exactly what the reference does before CONNECT finishes.
class ReplicationSession
{
public:
    enum class State
    {
        connect,
        replicate_descriptor,
        src_recording_position,
        extend,
        replay_token,
        get_archive_proxy,
        replay,
        await_image,
        replicate,
        catchup,
        attempt_live_join,
        done
    };

    explicit ReplicationSession(const ReplicationConfig& config);

    ReplicationSession(const ReplicationSession&) = delete;
    ReplicationSession& operator=(const ReplicationSession&) = delete;

    /// `ReplicationSession.doWork` (`:273-280`).
    int do_work();

    [[nodiscard]] bool is_done() const noexcept
    {
        return is_done_;
    }

    /// `:200-215`: a stop ends the session; its `close()` sends `REPLICATE_END`.
    void abort(std::string_view reason);

    /// `:196-213`: the destination's subscription and the source replay are
    /// stopped and `REPLICATE_END` goes out **once**.
    void close();

    [[nodiscard]] std::int64_t session_id() const noexcept
    {
        return config_.replication_id;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] const ReplicationConfig& config() const noexcept
    {
        return config_;
    }

private:
    ReplicationConfig config_;
    State state_{State::connect};
    bool is_done_{false};
};

}  // namespace deepmsg::archive::server
