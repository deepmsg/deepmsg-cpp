#pragma once

#include <cstdint>
#include <deque>
#include <span>
#include <string>
#include <string_view>

#include "archive/server/authenticator.h"
#include "archive/server/control_response_proxy.h"
#include "archive/server/epoch_clock_source.h"
#include "core/collections/fixed_string.h"

namespace deepmsg::archive::server
{

/// `ControlSession` (`ControlSession.java:52-1104`): the state machine of one
/// control connection, driven by the conductor's `do_work` pass.
///
/// State order (`ControlSession.java:64-67`):
///   INIT → CONNECTING → CONNECTED → (CHALLENGED →) AUTHENTICATED → ACTIVE,
///   with REJECTED for a failed authentication and DONE as the terminal state.
///
/// ★ `attempt_to_activate()` is the first statement of 29 of the 30 request
///   handlers in the reference; a session that never leaves AUTHENTICATED
///   re-sends its connect OK forever and the client sees stale responses
///   (`memory/deepmsg-archive-session-activation.md`).
///
/// Not in this card: the async response queue and the recording/replay handlers
/// (A2b+); `send_descriptor`/`send_subscription_descriptor` land with the list
/// and replay sessions.
class ControlSession final : public SessionAbort, public SessionProxy
{
public:
    static constexpr std::int64_t NULL_VALUE = -1;
    static constexpr std::int64_t RESEND_INTERVAL_MS = 200;
    static constexpr std::string_view SESSION_CLOSED_MSG = "session closed";
    static constexpr std::string_view RESPONSE_NOT_CONNECTED_MSG = "control response publication is not connected";
    static constexpr std::string_view SESSION_REJECTED_MSG = "authentication rejected";

    enum class State
    {
        init,
        connecting,
        connected,
        challenged,
        authenticated,
        active,
        rejected,
        done
    };

    struct Config
    {
        std::int64_t control_session_id{0};
        std::int64_t correlation_id{0};
        std::int64_t connect_timeout_ms{3000};
        std::int64_t session_liveness_check_interval_ms{1000};
        /// Non-empty ⇒ the connect response is an ERROR with this message
        /// instead of an authentication decision (`ControlSession.java:933-941`).
        std::string invalid_version_message;
    };

    ControlSession(const Config& config, ControlResponseProxy& proxy, Authenticator& authenticator,
                   const EpochClockSource& clock);

    ControlSession(const ControlSession&) = delete;
    ControlSession& operator=(const ControlSession&) = delete;

    /// The conductor hands the session its control publication once the
    /// asynchronous add completes (`ControlSession.java:859-888`).
    void set_publication(ResponsePublication* publication) noexcept
    {
        control_publication_ = publication;
    }

    [[nodiscard]] const ResponsePublication* publication() const noexcept
    {
        return control_publication_;
    }

    [[nodiscard]] int do_work();

    [[nodiscard]] bool is_done() const noexcept
    {
        return State::done == state_;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] std::string_view abort_reason() const noexcept
    {
        return abort_reason_;
    }

    /// SessionAbort: the proxy reports a publication that cannot take a message.
    void abort(std::string_view reason) override;

    // SessionProxy.
    [[nodiscard]] std::int64_t session_id() const noexcept override
    {
        return config_.control_session_id;
    }

    [[nodiscard]] std::int64_t correlation_id() const noexcept override
    {
        return correlation_id_;
    }

    bool challenge(std::span<const std::uint8_t> encoded_challenge) override;
    bool authenticate(std::span<const std::uint8_t> encoded_principal) override;
    void reject() override;

    /// `ControlSession.onChallengeResponse` (`ControlSession.java:308`).
    void on_challenge_response(std::int64_t correlation_id, std::span<const std::uint8_t> encoded_credentials);

    /// `ControlSession.onKeepAlive` (`ControlSession.java:317`). Every request
    /// handler calls `attempt_to_activate()` first; this is the one the client
    /// sends unprompted.
    void on_keep_alive(std::int64_t correlation_id);

    /// The gate every request handler shares: AUTHENTICATED (and no version
    /// error) becomes ACTIVE.
    void attempt_to_activate() noexcept;

    // Responses (`ControlSession.java:683-731`). A send that cannot happen now
    // is queued and replayed on a later pass (`铁律 1`).
    void send_ok_response(std::int64_t correlation_id, std::int64_t relevant_id);
    void send_error_response(std::int64_t correlation_id, std::string_view message);
    void send_error_response(std::int64_t correlation_id, std::int64_t relevant_id, std::string_view message);

    /// `ControlSession.sendSignal` (`ControlSession.java:779-800`): the same
    /// "send now or queue it" discipline as a response.
    void send_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t subscription_id,
                     std::int64_t position, archive_codecs::RecordingSignal::Value signal);

    /// `ControlSession.sendDescriptor` (`ControlSession.java:747-761`): listing
    /// sessions retry on the next pass when this returns false.
    [[nodiscard]] bool send_descriptor(std::int64_t correlation_id, std::span<const std::uint8_t> descriptor);
    [[nodiscard]] bool send_subscription_descriptor(std::int64_t correlation_id, std::int64_t subscription_id,
                                                    std::int32_t stream_id, std::string_view channel);
    void send_recording_unknown(std::int64_t correlation_id, std::int64_t recording_id);
    void send_subscription_unknown(std::int64_t correlation_id);

    [[nodiscard]] bool has_active_listing() const noexcept
    {
        return active_listing_;
    }

    void set_active_listing(bool value) noexcept
    {
        active_listing_ = value;
    }

    [[nodiscard]] std::size_t pending_responses() const noexcept
    {
        return sync_response_queue_.size();
    }

    [[nodiscard]] std::span<const std::uint8_t> encoded_principal() const noexcept
    {
        return encoded_principal_;
    }

    /// `ControlSession.state` (`ControlSession.java:1078`).
    void set_state(State state, std::string_view reason);

private:
    struct PendingResponse
    {
        enum class Kind
        {
            response,
            signal
        };

        Kind kind{Kind::response};
        archive_codecs::ControlResponseCode::Value code{archive_codecs::ControlResponseCode::OK};
        archive_codecs::RecordingSignal::Value signal{archive_codecs::RecordingSignal::START};
        std::int64_t correlation_id{0};
        std::int64_t relevant_id{0};
        std::int64_t recording_id{0};
        std::int64_t subscription_id{0};
        std::int64_t position{0};
        axc::core::FixedString<256> message{};
    };

    [[nodiscard]] int init(std::int64_t now_ms);
    [[nodiscard]] int wait_for_connection();
    [[nodiscard]] int send_connect_response(std::int64_t now_ms);
    [[nodiscard]] int wait_for_challenge_response(std::int64_t now_ms);
    [[nodiscard]] int wait_for_request(std::int64_t now_ms);
    [[nodiscard]] int perform_liveness_check(std::int64_t now_ms);
    [[nodiscard]] int send_responses(std::int64_t now_ms);
    [[nodiscard]] int send_reject(std::int64_t now_ms);

    [[nodiscard]] bool has_no_activity(std::int64_t now_ms) const noexcept;
    void update_activity_deadline(std::int64_t now_ms) noexcept;
    void queue_response(std::int64_t correlation_id, std::int64_t relevant_id,
                        archive_codecs::ControlResponseCode::Value code, std::string_view message);
    void send_response(std::int64_t correlation_id, std::int64_t relevant_id,
                       archive_codecs::ControlResponseCode::Value code, std::string_view message);

    Config config_;
    ControlResponseProxy& proxy_;
    Authenticator& authenticator_;
    const EpochClockSource& clock_;

    ResponsePublication* control_publication_{nullptr};
    State state_{State::init};
    std::int64_t correlation_id_{0};
    bool active_listing_{false};
    std::int64_t resend_deadline_ms_{0};
    std::int64_t activity_deadline_ms_{0};
    std::int64_t session_liveness_check_deadline_ms_{0};
    std::string abort_reason_;
    std::span<const std::uint8_t> encoded_principal_{};
    std::deque<PendingResponse> sync_response_queue_;
};

}  // namespace deepmsg::archive::server
