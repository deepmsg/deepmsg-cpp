// Criterion for card A2a's control session state machine.
//
// The load-bearing case is `aRequestThatPassesTheGateActivatesItsSession`: in the
// reference `attemptToActivate()` is the first statement of 29 of the 30 request
// handlers (`ControlSession.java:324-676`), and a session that stays
// AUTHENTICATED re-sends its connect OK forever, which the client then reads as a
// response to its own request
// (`memory/deepmsg-archive-session-activation.md`).

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "archive/server/authenticator.h"
#include "archive/server/control_response_proxy.h"
#include "archive/server/control_session.h"
#include "archive/server/epoch_clock_source.h"

namespace
{

using deepmsg::archive::server::CachedEpochClock;
using deepmsg::archive::server::ControlResponseProxy;
using deepmsg::archive::server::ControlSession;
using deepmsg::archive::server::DefaultAuthenticator;
using deepmsg::archive::server::ResponsePublication;
using deepmsg::archive::server::SampleAuthenticator;
using deepmsg::archive::server::SessionAbort;
using deepmsg::archive::server::archive_codecs::ControlResponse;
using deepmsg::archive::server::archive_codecs::ControlResponseCode;

[[nodiscard]] std::span<const std::uint8_t> bytes_of(const std::string& text)
{
    return {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()};
}

class FakePublication final : public ResponsePublication
{
public:
    [[nodiscard]] OfferResult offer(std::span<const std::uint8_t> buffer) override
    {
        if (!connected)
        {
            return ControlResponseProxy::NOT_CONNECTED;
        }
        if (offer_failures > 0)
        {
            offer_failures--;
            return ControlResponseProxy::BACK_PRESSURED;
        }
        sent.emplace_back(buffer.begin(), buffer.end());
        return 1;
    }

    [[nodiscard]] OfferResult try_claim(std::int32_t length, Claim& claim) override
    {
        if (!connected)
        {
            return ControlResponseProxy::NOT_CONNECTED;
        }
        claim_storage.assign(static_cast<std::size_t>(length) + 64, 0);
        claim.buffer = claim_storage;
        claims++;
        return 1;
    }

    [[nodiscard]] bool is_connected() const override
    {
        return connected;
    }

    [[nodiscard]] std::int32_t max_payload_length() const override
    {
        return 1408;
    }

    bool connected{true};
    int offer_failures{0};
    int claims{0};
    std::vector<std::vector<std::uint8_t>> sent;
    std::vector<std::uint8_t> claim_storage;
};

/// Decodes the fields of a sent `ControlResponse` (the test's own reader).
struct DecodedResponse
{
    std::int64_t control_session_id{0};
    std::int64_t correlation_id{0};
    std::int64_t relevant_id{0};
    ControlResponseCode::Value code{ControlResponseCode::OK};
    std::string error_message;
};

[[nodiscard]] DecodedResponse decode(const std::vector<std::uint8_t>& message)
{
    ControlResponse decoder;
    decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(message.data())),
                          8,  // message header
                          ControlResponse::SBE_BLOCK_LENGTH, ControlResponse::SBE_SCHEMA_VERSION, message.size());

    DecodedResponse decoded;
    decoded.control_session_id = decoder.controlSessionId();
    decoded.correlation_id = decoder.correlationId();
    decoded.relevant_id = decoder.relevantId();
    decoded.code = decoder.code();
    decoded.error_message = decoder.getErrorMessageAsString();
    return decoded;
}

class ControlSessionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        clock_.update(10);
        session_ = std::make_unique<ControlSession>(config_, proxy_, default_authenticator_, clock_);
        session_->set_publication(&publication_);
    }

    void TearDown() override
    {
        session_.reset();
    }

    /// Drives the session to AUTHENTICATED, the state every request starts from.
    void drive_to_authenticated()
    {
        ASSERT_EQ(1, session_->do_work());
        ASSERT_EQ(ControlSession::State::connecting, session_->state()) << "INIT -> CONNECTING";
        ASSERT_EQ(1, session_->do_work());
        ASSERT_EQ(ControlSession::State::connected, session_->state()) << "CONNECTING -> CONNECTED";
        ASSERT_EQ(1, session_->do_work());
        ASSERT_EQ(ControlSession::State::authenticated, session_->state()) << "CONNECTED -> AUTHENTICATED";
    }

    ControlSession::Config config_{};
    CachedEpochClock clock_;
    FakePublication publication_;
    ControlResponseProxy proxy_;
    DefaultAuthenticator default_authenticator_;
    std::unique_ptr<ControlSession> session_;
};

TEST_F(ControlSessionTest, walksFromInitToActive)
{
    drive_to_authenticated();
    ASSERT_EQ(1u, publication_.sent.size());
    EXPECT_EQ(ControlResponseCode::OK, decode(publication_.sent[0]).code);

    // The connect OK is re-sent while the session waits for a request.
    clock_.update(10 + 2 * ControlSession::RESEND_INTERVAL_MS);
    EXPECT_GE(session_->do_work(), 0);
    EXPECT_EQ(2u, publication_.sent.size()) << "the connect response is re-sent until a request arrives";
    EXPECT_EQ(ControlSession::State::authenticated, session_->state());

    session_->on_keep_alive(3);
    EXPECT_EQ(ControlSession::State::active, session_->state());
}

TEST_F(ControlSessionTest, aRequestThatPassesTheGateActivatesItsSession)
{
    drive_to_authenticated();

    session_->on_keep_alive(42);

    EXPECT_EQ(ControlSession::State::active, session_->state());
}

TEST_F(ControlSessionTest, aSessionThatHasNotAuthenticatedIsNotActivated)
{
    ASSERT_EQ(1, session_->do_work());
    ASSERT_EQ(ControlSession::State::connecting, session_->state());

    session_->on_keep_alive(42);

    EXPECT_EQ(ControlSession::State::connecting, session_->state());
}

TEST_F(ControlSessionTest, theResponseIsQueuedWhenThePublicationIsBackPressuredAndReplayedLater)
{
    drive_to_authenticated();
    session_->on_keep_alive(1);
    ASSERT_EQ(ControlSession::State::active, session_->state());
    const std::size_t sent_before = publication_.sent.size();

    // Back pressure for a whole proxy call (SEND_ATTEMPTS tries).
    publication_.offer_failures = ControlResponseProxy::SEND_ATTEMPTS;
    session_->send_ok_response(7, 9);
    EXPECT_EQ(1u, session_->pending_responses());
    EXPECT_EQ(sent_before, publication_.sent.size());

    // The next pass replays it (`铁律 1`).
    ASSERT_EQ(1, session_->do_work());
    EXPECT_EQ(0u, session_->pending_responses());
    ASSERT_EQ(sent_before + 1, publication_.sent.size());

    const DecodedResponse response = decode(publication_.sent.back());
    EXPECT_EQ(7, response.correlation_id);
    EXPECT_EQ(9, response.relevant_id);
    EXPECT_EQ(ControlResponseCode::OK, response.code);

    // Nothing left to replay.
    EXPECT_EQ(0, session_->do_work());
}

TEST_F(ControlSessionTest, aLaterResponseWaitsBehindTheQueuedOne)
{
    drive_to_authenticated();
    session_->on_keep_alive(1);

    publication_.offer_failures = ControlResponseProxy::SEND_ATTEMPTS;
    session_->send_ok_response(11, 0);
    ASSERT_EQ(1u, session_->pending_responses());

    // Even though the publication is healthy again, the queued response goes
    // first, in order.
    session_->send_ok_response(12, 0);
    EXPECT_EQ(2u, session_->pending_responses());

    EXPECT_GE(session_->do_work(), 0);
    EXPECT_EQ(1u, session_->pending_responses());
    EXPECT_EQ(11, decode(publication_.sent.back()).correlation_id);

    EXPECT_GE(session_->do_work(), 0);
    EXPECT_EQ(0u, session_->pending_responses());
    EXPECT_EQ(12, decode(publication_.sent.back()).correlation_id);
}

TEST_F(ControlSessionTest, aVersionMismatchAnswersWithAnErrorAndNeverAuthenticates)
{
    config_.invalid_version_message = "incompatible archive version";
    session_ = std::make_unique<ControlSession>(config_, proxy_, default_authenticator_, clock_);
    session_->set_publication(&publication_);

    EXPECT_GE(session_->do_work(), 0);
    EXPECT_GE(session_->do_work(), 0);
    EXPECT_GE(session_->do_work(), 0);

    ASSERT_EQ(ControlSession::State::connected, session_->state());
    ASSERT_EQ(1u, publication_.sent.size());
    const DecodedResponse response = decode(publication_.sent[0]);
    EXPECT_EQ(ControlResponseCode::ERROR, response.code);
    EXPECT_EQ("incompatible archive version", response.error_message);

    // A request cannot activate it.
    session_->on_keep_alive(5);
    EXPECT_EQ(ControlSession::State::connected, session_->state());
}

TEST_F(ControlSessionTest, theAuthenticatorCanChallengeTheClient)
{
    SampleAuthenticator authenticator;
    session_ = std::make_unique<ControlSession>(config_, proxy_, authenticator, clock_);
    session_->set_publication(&publication_);

    authenticator.on_connect_request(session_->session_id(), bytes_of("admin:adminC"), 0);

    EXPECT_GE(session_->do_work(), 0);  // INIT -> CONNECTING
    EXPECT_GE(session_->do_work(), 0);  // -> CONNECTED
    EXPECT_GE(session_->do_work(), 0);  // sends the challenge
    ASSERT_EQ(ControlSession::State::challenged, session_->state());
    ASSERT_EQ(1u, publication_.sent.size());

    session_->on_challenge_response(9, bytes_of("admin:CSadmin"));
    EXPECT_GE(session_->do_work(), 0);  // CHALLENGED -> authenticator -> OK response
    EXPECT_EQ(ControlSession::State::authenticated, session_->state());
    EXPECT_EQ(ControlResponseCode::OK, decode(publication_.sent.back()).code);
}

TEST_F(ControlSessionTest, anUnknownCredentialIsRejected)
{
    SampleAuthenticator authenticator;
    session_ = std::make_unique<ControlSession>(config_, proxy_, authenticator, clock_);
    session_->set_publication(&publication_);

    authenticator.on_connect_request(session_->session_id(), bytes_of("bad"), 0);

    EXPECT_GE(session_->do_work(), 0);
    EXPECT_GE(session_->do_work(), 0);
    EXPECT_GE(session_->do_work(), 0);
    ASSERT_EQ(ControlSession::State::rejected, session_->state());

    // The reject is re-sent on the resend interval, with the reference's
    // relevant id (AUTHENTICATION_REJECTED = 10).
    clock_.update(clock_.time_ms() + 2 * ControlSession::RESEND_INTERVAL_MS);
    EXPECT_GE(session_->do_work(), 0);
    const DecodedResponse response = decode(publication_.sent.back());
    EXPECT_EQ(ControlResponseCode::ERROR, response.code);
    EXPECT_EQ(10, response.relevant_id);
    EXPECT_EQ("authentication rejected", response.error_message);
}

TEST_F(ControlSessionTest, aSessionThatCannotSendItsResponseAbortsAfterTheTimeout)
{
    drive_to_authenticated();
    session_->on_keep_alive(1);
    ASSERT_EQ(ControlSession::State::active, session_->state());

    // A response that cannot go out keeps an activity deadline alive
    // (`ControlSession.updateActivityDeadline`); past `connectTimeoutMs` the
    // session is aborted.
    publication_.offer_failures = ControlResponseProxy::SEND_ATTEMPTS;
    session_->send_ok_response(3, 0);
    ASSERT_EQ(1u, session_->pending_responses());

    clock_.update(clock_.time_ms() + config_.connect_timeout_ms + 1);
    EXPECT_GE(session_->do_work(), 0);

    EXPECT_TRUE(session_->is_done());
    EXPECT_EQ("failed to send response for more than connectTimeoutMs=3000", session_->abort_reason());
}

TEST_F(ControlSessionTest, anUnconnectedPublicationAbortsTheActiveSession)
{
    drive_to_authenticated();
    session_->on_keep_alive(1);

    publication_.connected = false;
    EXPECT_GE(session_->do_work(), 0);

    EXPECT_TRUE(session_->is_done());
    EXPECT_EQ(ControlSession::RESPONSE_NOT_CONNECTED_MSG, session_->abort_reason());
}

TEST_F(ControlSessionTest, livenessPingsKeepTheSessionAlive)
{
    drive_to_authenticated();
    session_->on_keep_alive(1);

    const int claims_before = publication_.claims;
    clock_.update(clock_.time_ms() + config_.session_liveness_check_interval_ms + 1);
    EXPECT_GE(session_->do_work(), 0);

    EXPECT_EQ(claims_before + 1, publication_.claims);
    EXPECT_FALSE(session_->is_done());
}

}  // namespace
