#include "archive/server/control_session.h"

#include <algorithm>
#include <cstring>

namespace deepmsg::archive::server
{
namespace
{

/// `ArchiveException.GENERIC` / `AUTHENTICATION_REJECTED` (`ArchiveException.java:29`, `:79`).
constexpr std::int64_t ERROR_GENERIC = 0;
constexpr std::int64_t ERROR_AUTHENTICATION_REJECTED = 10;

[[nodiscard]] std::string_view state_name(ControlSession::State state) noexcept
{
    switch (state)
    {
        case ControlSession::State::init:
            return "INIT";
        case ControlSession::State::connecting:
            return "CONNECTING";
        case ControlSession::State::connected:
            return "CONNECTED";
        case ControlSession::State::challenged:
            return "CHALLENGED";
        case ControlSession::State::authenticated:
            return "AUTHENTICATED";
        case ControlSession::State::active:
            return "ACTIVE";
        case ControlSession::State::rejected:
            return "REJECTED";
        case ControlSession::State::done:
            return "DONE";
    }
    return "UNKNOWN";
}

}  // namespace

ControlSession::ControlSession(const Config& config, ControlResponseProxy& proxy, Authenticator& authenticator,
                               const EpochClockSource& clock)
    : config_(config),
      proxy_(proxy),
      authenticator_(authenticator),
      clock_(clock),
      correlation_id_(config.correlation_id),
      resend_deadline_ms_(0),
      activity_deadline_ms_(clock.time_ms() + config.connect_timeout_ms),
      session_liveness_check_deadline_ms_(clock.time_ms() + config.session_liveness_check_interval_ms)
{
}

void ControlSession::abort(std::string_view reason)
{
    // `ControlSession.abort` (`ControlSession.java:146-157`).
    if (State::done == state_)
    {
        return;
    }
    abort_reason_ = reason;
    set_state(State::done, reason);
}

void ControlSession::set_state(State state, std::string_view reason)
{
    // `ControlSession.state` (`ControlSession.java:1078`); the tracing event
    // (`ArchiveTracing.traceControlSessionStateChange`) lands with A2b's
    // event codes.
    (void)reason;
    state_ = state;
}

int ControlSession::do_work()
{
    // `ControlSession.doWork` (`ControlSession.java:212-271`).
    int work_count = 0;
    const std::int64_t now_ms = clock_.time_ms();

    if (has_no_activity(now_ms))
    {
        abort(State::active == state_
                  ? "failed to send response for more than connectTimeoutMs=" +
                        std::to_string(config_.connect_timeout_ms)
                  : "failed to establish initial connection: state=" + std::string{state_name(state_)});
        work_count++;
    }

    switch (state_)
    {
        case State::init:
            work_count += init(now_ms);
            break;

        case State::connecting:
            work_count += wait_for_connection();
            break;

        case State::connected:
            work_count += send_connect_response(now_ms);
            break;

        case State::challenged:
            work_count += wait_for_challenge_response(now_ms);
            break;

        case State::authenticated:
            work_count += wait_for_request(now_ms);
            break;

        case State::active:
            work_count += perform_liveness_check(now_ms);
            work_count += send_responses(now_ms);
            break;

        case State::rejected:
            work_count += send_reject(now_ms);
            break;

        case State::done:
            break;
    }

    return work_count;
}

int ControlSession::init(std::int64_t now_ms)
{
    // `ControlSession.init` (`ControlSession.java:855-911`): the asynchronous
    // publication/counter add is the conductor's; the session moves on once it
    // has been handed the publication.
    if (nullptr == control_publication_)
    {
        return 0;
    }

    activity_deadline_ms_ = now_ms + config_.connect_timeout_ms;
    set_state(State::connecting, "connecting");
    return 1;
}

int ControlSession::wait_for_connection()
{
    // `ControlSession.waitForConnection` (`ControlSession.java:913-924`).
    if (control_publication_->is_connected())
    {
        set_state(State::connected, "connected");
        return 1;
    }
    return 0;
}

int ControlSession::send_connect_response(std::int64_t now_ms)
{
    // `ControlSession.sendConnectResponse` (`ControlSession.java:926-952`).
    if (now_ms <= resend_deadline_ms_)
    {
        return 0;
    }

    resend_deadline_ms_ = now_ms + RESEND_INTERVAL_MS;
    if (!config_.invalid_version_message.empty())
    {
        // The reference ignores the result here too: a response that cannot be
        // sent now is retried after `RESEND_INTERVAL_MS`.
        const bool sent = proxy_.send_response(config_.control_session_id, correlation_id_, config_.control_session_id,
                                               archive_codecs::ControlResponseCode::ERROR,
                                               config_.invalid_version_message, *control_publication_, *this);
        if (!sent)
        {
            update_activity_deadline(now_ms);
        }
    }
    else
    {
        authenticator_.on_connected_session(*this, now_ms);
    }

    return 1;
}

int ControlSession::wait_for_challenge_response(std::int64_t now_ms)
{
    // `ControlSession.waitForChallengeResponse` (`ControlSession.java:954-958`).
    authenticator_.on_challenged_session(*this, now_ms);
    return 1;
}

int ControlSession::wait_for_request(std::int64_t now_ms)
{
    // `ControlSession.waitForRequest` (`ControlSession.java:960-981`): the
    // connect OK is re-sent until it gets through, then the session waits for a
    // request that activates it.
    if (now_ms <= resend_deadline_ms_)
    {
        return 0;
    }

    resend_deadline_ms_ = now_ms + RESEND_INTERVAL_MS;
    if (proxy_.send_response(config_.control_session_id, correlation_id_, config_.control_session_id,
                             archive_codecs::ControlResponseCode::OK, {}, *control_publication_, *this))
    {
        activity_deadline_ms_ = NULL_VALUE;
        return 1;
    }

    return 0;
}

int ControlSession::perform_liveness_check(std::int64_t now_ms)
{
    // `ControlSession.performLivenessCheck` (`ControlSession.java:983-999`).
    if (session_liveness_check_deadline_ms_ - now_ms >= 0)
    {
        return 0;
    }

    session_liveness_check_deadline_ms_ = now_ms + config_.session_liveness_check_interval_ms;
    if (!proxy_.send_ping(config_.control_session_id, *control_publication_))
    {
        update_activity_deadline(now_ms);
    }
    else
    {
        activity_deadline_ms_ = NULL_VALUE;
    }
    return 1;
}

int ControlSession::send_responses(std::int64_t now_ms)
{
    // `ControlSession.sendResponses` (`ControlSession.java:1001-1039`).
    if (!control_publication_->is_connected())
    {
        abort(RESPONSE_NOT_CONNECTED_MSG);
        return 1;
    }

    int work_count = 0;
    if (!sync_response_queue_.empty())
    {
        PendingResponse& response = sync_response_queue_.front();
        const bool sent =
            PendingResponse::Kind::signal == response.kind
                ? proxy_.send_signal(config_.control_session_id, response.correlation_id, response.recording_id,
                                     response.subscription_id, response.position, response.signal,
                                     *control_publication_)
                : proxy_.send_response(config_.control_session_id, response.correlation_id, response.relevant_id,
                                       response.code, response.message.view(), *control_publication_, *this);
        if (sent)
        {
            sync_response_queue_.pop_front();
            activity_deadline_ms_ = NULL_VALUE;
            work_count++;
        }
        else
        {
            update_activity_deadline(now_ms);
        }
    }

    return work_count;
}

int ControlSession::send_reject(std::int64_t now_ms)
{
    // `ControlSession.sendReject` (`ControlSession.java:1041-1055`).
    if (now_ms <= resend_deadline_ms_)
    {
        return 0;
    }

    resend_deadline_ms_ = now_ms + RESEND_INTERVAL_MS;
    // Ignored like the reference: `sendReject` retries after the resend interval.
    const bool sent = proxy_.send_response(config_.control_session_id, correlation_id_, ERROR_AUTHENTICATION_REJECTED,
                                           archive_codecs::ControlResponseCode::ERROR, SESSION_REJECTED_MSG,
                                           *control_publication_, *this);
    if (!sent)
    {
        update_activity_deadline(now_ms);
    }
    return 1;
}

void ControlSession::on_challenge_response(std::int64_t correlation_id,
                                           std::span<const std::uint8_t> encoded_credentials)
{
    // `ControlSession.onChallengeResponse` (`ControlSession.java:308-315`).
    if (State::challenged == state_)
    {
        correlation_id_ = correlation_id;
        authenticator_.on_challenge_response(config_.control_session_id, encoded_credentials, clock_.time_ms());
    }
}

void ControlSession::on_keep_alive(std::int64_t correlation_id)
{
    // `ControlSession.onKeepAlive` (`ControlSession.java:317-320`).
    (void)correlation_id;
    attempt_to_activate();
}

void ControlSession::attempt_to_activate() noexcept
{
    // `ControlSession.attemptToActivate` (`ControlSession.java:1070-1076`).
    if (State::authenticated == state_ && config_.invalid_version_message.empty())
    {
        set_state(State::active, "active");
    }
}

void ControlSession::send_ok_response(std::int64_t correlation_id, std::int64_t relevant_id)
{
    send_response(correlation_id, relevant_id, archive_codecs::ControlResponseCode::OK, {});
}

void ControlSession::send_error_response(std::int64_t correlation_id, std::string_view message)
{
    send_response(correlation_id, ERROR_GENERIC, archive_codecs::ControlResponseCode::ERROR, message);
}

void ControlSession::send_error_response(std::int64_t correlation_id, std::int64_t relevant_id,
                                         std::string_view message)
{
    send_response(correlation_id, relevant_id, archive_codecs::ControlResponseCode::ERROR, message);
}

void ControlSession::send_response(std::int64_t correlation_id, std::int64_t relevant_id,
                                   archive_codecs::ControlResponseCode::Value code, std::string_view message)
{
    // `ControlSession.sendResponse` (`ControlSession.java:713-731`).
    if (!sync_response_queue_.empty() || !proxy_.send_response(config_.control_session_id, correlation_id, relevant_id,
                                                               code, message, *control_publication_, *this))
    {
        update_activity_deadline(clock_.time_ms());
        queue_response(correlation_id, relevant_id, code, message);
    }
    else
    {
        activity_deadline_ms_ = NULL_VALUE;
    }
}

void ControlSession::send_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t subscription_id,
                                 std::int64_t position, archive_codecs::RecordingSignal::Value signal)
{
    // `ControlSession.sendSignal` (`ControlSession.java:779-800`).
    if (!sync_response_queue_.empty() || !proxy_.send_signal(config_.control_session_id, correlation_id, recording_id,
                                                             subscription_id, position, signal, *control_publication_))
    {
        update_activity_deadline(clock_.time_ms());
        PendingResponse pending;
        pending.kind = PendingResponse::Kind::signal;
        pending.correlation_id = correlation_id;
        pending.recording_id = recording_id;
        pending.subscription_id = subscription_id;
        pending.position = position;
        pending.signal = signal;
        sync_response_queue_.push_back(pending);
    }
    else
    {
        activity_deadline_ms_ = NULL_VALUE;
    }
}

void ControlSession::queue_response(std::int64_t correlation_id, std::int64_t relevant_id,
                                    archive_codecs::ControlResponseCode::Value code, std::string_view message)
{
    // `ControlSession.queueResponse` (`ControlSession.java:843-853`).
    PendingResponse pending;
    pending.kind = PendingResponse::Kind::response;
    pending.correlation_id = correlation_id;
    pending.relevant_id = relevant_id;
    pending.code = code;
    pending.message.assign(message);
    sync_response_queue_.push_back(pending);
}

bool ControlSession::challenge(std::span<const std::uint8_t> encoded_challenge)
{
    // `ControlSessionProxy.challenge` (`ControlSessionProxy.java:48-65`): the
    // challenge goes out first, and only a sent challenge moves the state.
    if (proxy_.send_challenge(config_.control_session_id, correlation_id_, encoded_challenge, *control_publication_,
                              *this))
    {
        set_state(State::challenged, "challenged");
        return true;
    }
    return false;
}

bool ControlSession::authenticate(std::span<const std::uint8_t> encoded_principal)
{
    // `ControlSessionProxy.authenticate` (`ControlSessionProxy.java:70-90`): the
    // OK response is sent first; failure leaves the session unauthenticated so
    // the authenticator retries on the next pass.
    if (proxy_.send_response(config_.control_session_id, correlation_id_, config_.control_session_id,
                             archive_codecs::ControlResponseCode::OK, {}, *control_publication_, *this))
    {
        encoded_principal_ = encoded_principal;
        activity_deadline_ms_ = NULL_VALUE;
        set_state(State::authenticated, "authenticated");
        return true;
    }
    return false;
}

void ControlSession::reject()
{
    // `ControlSession.reject` (`ControlSession.java:829-832`).
    set_state(State::rejected, SESSION_REJECTED_MSG);
}

bool ControlSession::has_no_activity(std::int64_t now_ms) const noexcept
{
    // `ControlSession.hasNoActivity` (`ControlSession.java:1057-1060`).
    return NULL_VALUE != activity_deadline_ms_ && now_ms > activity_deadline_ms_;
}

void ControlSession::update_activity_deadline(std::int64_t now_ms) noexcept
{
    // `ControlSession.updateActivityDeadline` (`ControlSession.java:1062-1068`).
    if (NULL_VALUE == activity_deadline_ms_)
    {
        activity_deadline_ms_ = now_ms + config_.connect_timeout_ms;
    }
}

}  // namespace deepmsg::archive::server
