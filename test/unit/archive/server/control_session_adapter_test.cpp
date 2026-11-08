// Criterion for card A2a's request dispatch: real SBE frames go through
// `ControlSessionAdapter` and reach (or are turned away from) their session.
//
// Reference: `ControlSessionAdapter.java:126-1231`, the gate at `:1186-1230`.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "archive/server/authenticator.h"
#include "archive/server/control_response_proxy.h"
#include "archive/server/control_session.h"
#include "archive/server/control_session_adapter.h"
#include "archive/server/epoch_clock_source.h"
#include "io_aeron_archive_codecs/AuthConnectRequest.h"
#include "io_aeron_archive_codecs/BooleanType.h"
#include "io_aeron_archive_codecs/ChallengeResponse.h"
#include "io_aeron_archive_codecs/CloseSessionRequest.h"
#include "io_aeron_archive_codecs/KeepAliveRequest.h"
#include "io_aeron_archive_codecs/SourceLocation.h"
#include "io_aeron_archive_codecs/StartRecordingRequest2.h"

namespace
{

using deepmsg::archive::server::CachedEpochClock;
using deepmsg::archive::server::ConnectRequest;
using deepmsg::archive::server::ControlResponseProxy;
using deepmsg::archive::server::ControlSession;
using deepmsg::archive::server::ControlSessionAdapter;
using deepmsg::archive::server::ControlSessionHandler;
using deepmsg::archive::server::DefaultAuthenticator;
using deepmsg::archive::server::ResponsePublication;
using deepmsg::archive::server::StartRecordingRequest;
using deepmsg::archive::server::archive_codecs::AuthConnectRequest;
using deepmsg::archive::server::archive_codecs::BooleanType;
using deepmsg::archive::server::archive_codecs::ChallengeResponse;
using deepmsg::archive::server::archive_codecs::CloseSessionRequest;
using deepmsg::archive::server::archive_codecs::KeepAliveRequest;
using deepmsg::archive::server::archive_codecs::SourceLocation;
using deepmsg::archive::server::archive_codecs::StartRecordingRequest2;

class NullPublication final : public ResponsePublication
{
public:
    [[nodiscard]] OfferResult offer(std::span<const std::uint8_t>) override
    {
        return 1;
    }

    [[nodiscard]] OfferResult try_claim(std::int32_t length, Claim& claim) override
    {
        storage_.assign(static_cast<std::size_t>(length), 0);
        claim.buffer = storage_;
        return 1;
    }

    [[nodiscard]] bool is_connected() const override
    {
        return true;
    }

    [[nodiscard]] std::int32_t max_payload_length() const override
    {
        return 1408;
    }

private:
    std::vector<std::uint8_t> storage_;
};

class FakeHandler final : public ControlSessionHandler
{
public:
    ControlSession* on_connect_request(const ConnectRequest& request) override
    {
        connect_requests.push_back(request);

        ControlSession::Config config;
        config.control_session_id = 7;
        config.correlation_id = request.correlation_id;
        config.connect_timeout_ms = 3000;
        session = std::make_unique<ControlSession>(config, proxy, authenticator, clock);
        session->set_publication(&publication);
        return session.get();
    }

    void on_start_recording(const StartRecordingRequest& request) override
    {
        start_requests.push_back(request);
    }

    void log_warning(std::string_view message) override
    {
        warnings.emplace_back(message);
    }

    [[nodiscard]] std::int64_t archive_id() const override
    {
        return archive_id_value;
    }

    std::int64_t archive_id_value{-1};

    std::vector<ConnectRequest> connect_requests;
    std::vector<StartRecordingRequest> start_requests;
    std::vector<std::string> warnings;
    CachedEpochClock clock{10};
    ControlResponseProxy proxy;
    DefaultAuthenticator authenticator;
    NullPublication publication;
    std::unique_ptr<ControlSession> session;
};

/// Encodes an `AuthConnectRequest` exactly as the C client does.
[[nodiscard]] std::vector<std::uint8_t> encode_connect(std::int64_t correlation_id, const std::string& response_channel,
                                                       const std::string& credentials, const std::string& client_info)
{
    std::vector<std::uint8_t> buffer(1024, 0);
    AuthConnectRequest encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.correlationId(correlation_id)
        .responseStreamId(11)
        .version(ControlResponseProxy::PROTOCOL_SEMANTIC_VERSION)
        .putResponseChannel(response_channel)
        .putEncodedCredentials(credentials.data(), static_cast<std::uint32_t>(credentials.size()))
        .putClientInfo(client_info);

    buffer.resize(static_cast<std::size_t>(8 + encoder.encodedLength()));
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_keep_alive(std::int64_t control_session_id, std::int64_t correlation_id)
{
    std::vector<std::uint8_t> buffer(64, 0);
    KeepAliveRequest encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.controlSessionId(control_session_id).correlationId(correlation_id);
    buffer.resize(static_cast<std::size_t>(8 + encoder.encodedLength()));
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_start_recording2(std::int64_t control_session_id,
                                                                std::int64_t correlation_id, std::int32_t stream_id,
                                                                std::int32_t source_location, bool auto_stop,
                                                                const std::string& channel)
{
    std::vector<std::uint8_t> buffer(256, 0);
    StartRecordingRequest2 encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.controlSessionId(control_session_id)
        .correlationId(correlation_id)
        .streamId(stream_id)
        .sourceLocation(static_cast<SourceLocation::Value>(source_location))
        .autoStop(auto_stop ? BooleanType::TRUE : BooleanType::FALSE)
        .putChannel(channel.data(), static_cast<std::uint32_t>(channel.size()));
    buffer.resize(static_cast<std::size_t>(8 + encoder.encodedLength()));
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_close_session(std::int64_t control_session_id)
{
    std::vector<std::uint8_t> buffer(32, 0);
    CloseSessionRequest encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.controlSessionId(control_session_id);
    buffer.resize(static_cast<std::size_t>(8 + encoder.encodedLength()));
    return buffer;
}

class ControlSessionAdapterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        adapter_ = std::make_unique<ControlSessionAdapter>(handler_);
    }

    void TearDown() override
    {
        adapter_.reset();
    }

    FakeHandler handler_;
    std::unique_ptr<ControlSessionAdapter> adapter_;
    int image_{0};
    int other_image_{1};
};

TEST_F(ControlSessionAdapterTest, aConnectRequestCreatesAndRegistersASession)
{
    const std::vector<std::uint8_t> request = encode_connect(5, "aeron:ipc", "admin:admin", "client-info");

    EXPECT_EQ(1, adapter_->on_fragment(request.data(), static_cast<std::int32_t>(request.size()), &image_));

    ASSERT_EQ(1u, handler_.connect_requests.size());
    const ConnectRequest& decoded = handler_.connect_requests[0];
    EXPECT_EQ(5, decoded.correlation_id);
    EXPECT_EQ(11, decoded.response_stream_id);
    EXPECT_EQ(ControlResponseProxy::PROTOCOL_SEMANTIC_VERSION, decoded.protocol_version);
    EXPECT_EQ("aeron:ipc", decoded.response_channel);
    EXPECT_EQ("admin:admin", std::string(decoded.encoded_credentials.begin(), decoded.encoded_credentials.end()));
    EXPECT_EQ("client-info", decoded.client_info);
    EXPECT_EQ(1u, adapter_->session_count());
}

TEST_F(ControlSessionAdapterTest, aKeepAliveActivatesItsSession)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));
    ASSERT_TRUE(handler_.session);

    handler_.session->set_state(ControlSession::State::authenticated, "test");

    const std::vector<std::uint8_t> keep_alive = encode_keep_alive(7, 9);
    EXPECT_EQ(1, adapter_->on_fragment(keep_alive.data(), static_cast<std::int32_t>(keep_alive.size()), &image_));

    EXPECT_EQ(ControlSession::State::active, handler_.session->state());
}

TEST_F(ControlSessionAdapterTest, aRequestFromAnotherImageIsTurnedAway)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));
    ASSERT_TRUE(handler_.session);
    handler_.session->set_state(ControlSession::State::authenticated, "test");

    const std::vector<std::uint8_t> keep_alive = encode_keep_alive(7, 9);
    EXPECT_EQ(0, adapter_->on_fragment(keep_alive.data(), static_cast<std::int32_t>(keep_alive.size()), &other_image_));

    EXPECT_EQ(ControlSession::State::authenticated, handler_.session->state());
    ASSERT_FALSE(handler_.warnings.empty());
    EXPECT_EQ("unauthorised archive action=61 controlSessionId=7", handler_.warnings.back());
}

TEST_F(ControlSessionAdapterTest, aRequestForAnUnknownSessionIsTurnedAway)
{
    const std::vector<std::uint8_t> keep_alive = encode_keep_alive(99, 9);
    EXPECT_EQ(0, adapter_->on_fragment(keep_alive.data(), static_cast<std::int32_t>(keep_alive.size()), &image_));

    ASSERT_FALSE(handler_.warnings.empty());
    EXPECT_EQ("control request for unknown session: controlSessionId=99 templateId=61", handler_.warnings.back());
}

TEST_F(ControlSessionAdapterTest, aCloseSessionRequestAbortsTheSession)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));

    const std::vector<std::uint8_t> close = encode_close_session(7);
    EXPECT_EQ(1, adapter_->on_fragment(close.data(), static_cast<std::int32_t>(close.size()), &image_));

    EXPECT_TRUE(handler_.session->is_done());
    EXPECT_EQ(ControlSession::SESSION_CLOSED_MSG, handler_.session->abort_reason());
}

TEST_F(ControlSessionAdapterTest, aStartRecordingRequest2ReachesTheHandler)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));
    ASSERT_TRUE(handler_.session);
    handler_.session->set_state(ControlSession::State::authenticated, "test");

    const std::vector<std::uint8_t> start =
        encode_start_recording2(7, 42, 33, SourceLocation::LOCAL, true, "aeron:udp?endpoint=localhost:3333");
    EXPECT_EQ(1, adapter_->on_fragment(start.data(), static_cast<std::int32_t>(start.size()), &image_));

    ASSERT_EQ(1u, handler_.start_requests.size());
    const StartRecordingRequest& decoded = handler_.start_requests[0];
    EXPECT_EQ(42, decoded.correlation_id);
    EXPECT_EQ(33, decoded.stream_id);
    EXPECT_EQ(SourceLocation::LOCAL, decoded.source_location);
    EXPECT_TRUE(decoded.auto_stop);
    EXPECT_EQ("aeron:udp?endpoint=localhost:3333", decoded.channel);
    EXPECT_EQ(handler_.session.get(), decoded.session);
    EXPECT_EQ(ControlSession::State::active, handler_.session->state());
}

TEST_F(ControlSessionAdapterTest, anUnsupportedTemplateIsNamedAndDropped)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));

    // Re-use a valid frame but claim a template id the adapter does not serve
    // yet (`StopRecordingRequest` = 5).
    std::vector<std::uint8_t> frame = encode_keep_alive(7, 1);
    frame[2] = 5;  // message header template id (offset 2), little endian
    frame[3] = 0;

    EXPECT_EQ(0, adapter_->on_fragment(frame.data(), static_cast<std::int32_t>(frame.size()), &image_));
    ASSERT_FALSE(handler_.warnings.empty());
    EXPECT_EQ("unsupported control request templateId=5", handler_.warnings.back());
}

TEST_F(ControlSessionAdapterTest, aShortOrMalformedFrameIsRejectedBeforeDecode)
{
    const std::vector<std::uint8_t> short_frame(4, 0);
    EXPECT_EQ(0, adapter_->on_fragment(short_frame.data(), static_cast<std::int32_t>(short_frame.size()), &image_));

    std::vector<std::uint8_t> frame = encode_connect(5, "aeron:ipc", "", "");
    // A var-data length that runs past the end of the fragment.
    frame[frame.size() - 1] = 0x7F;
    EXPECT_EQ(0, adapter_->on_fragment(frame.data(), static_cast<std::int32_t>(frame.size()), &image_));

    EXPECT_FALSE(handler_.warnings.empty());
}

TEST_F(ControlSessionAdapterTest, aChallengeResponseReachesItsChallengedSession)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));
    ASSERT_TRUE(handler_.session);
    handler_.session->set_state(ControlSession::State::challenged, "test");

    std::vector<std::uint8_t> buffer(128, 0);
    deepmsg::archive::server::archive_codecs::ChallengeResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.controlSessionId(7).correlationId(21);
    encoder.putEncodedCredentials("admin:CSadmin", 13);
    buffer.resize(static_cast<std::size_t>(8 + encoder.encodedLength()));

    EXPECT_EQ(1, adapter_->on_fragment(buffer.data(), static_cast<std::int32_t>(buffer.size()), &image_));
    EXPECT_EQ(21, handler_.session->correlation_id());
}

TEST_F(ControlSessionAdapterTest, anUnavailableImageAbortsItsSessions)
{
    const std::vector<std::uint8_t> connect = encode_connect(5, "aeron:ipc", "", "");
    ASSERT_EQ(1, adapter_->on_fragment(connect.data(), static_cast<std::int32_t>(connect.size()), &image_));

    adapter_->abort_sessions_for_image(&image_, "control image unavailable");

    EXPECT_TRUE(handler_.session->is_done());
    EXPECT_EQ("control image unavailable", handler_.session->abort_reason());
}

}  // namespace
