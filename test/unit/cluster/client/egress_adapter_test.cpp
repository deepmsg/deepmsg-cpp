// C3c / XC-1: the ten `EgressAdapterTest` cases, ported by name
// (`aeron-cluster/src/test/java/io/aeron/cluster/client/EgressAdapterTest.java`).
//
// The reference mocks the `Subscription` and calls `onFragment(buffer, offset,
// length, header)` directly. There is no subscription here: `dispatch()` is the
// same method the C assembler calls, and the four `Header` variants the
// reference exercises (offset 0/4/8/18/24) become spans starting at that
// offset. The one structural difference is that the reference *throws*
// `ClusterException` for a foreign schema with no extension; our `dispatch`
// stores the error and `take_error()` hands it back (the same error `poll()`
// would return).

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/client/cluster_client.h"
#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/AdminResponse.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using deepmsg::cluster::client::ControlledEgressAdapter;
using deepmsg::cluster::client::ControlledEgressListener;
using deepmsg::cluster::client::ControlledEgressListenerExtension;
using deepmsg::cluster::client::EgressAdapter;
using deepmsg::cluster::client::EgressListener;
using deepmsg::cluster::client::EgressListenerExtension;

struct RecordingEgressListener final : EgressListener
{
    struct Message
    {
        std::int64_t session_id{};
        std::int64_t timestamp{};
        std::vector<std::uint8_t> payload{};
    };

    struct SessionEvent
    {
        std::int64_t correlation_id{};
        std::int64_t session_id{};
        std::int64_t leadership_term_id{};
        std::int32_t leader_member_id{};
        codecs::EventCode::Value code{};
        std::string detail{};
    };

    struct NewLeader
    {
        std::int64_t session_id{};
        std::int64_t leadership_term_id{};
        std::int32_t leader_member_id{};
        std::string ingress_endpoints{};
    };

    struct AdminResponse
    {
        std::int64_t session_id{};
        std::int64_t correlation_id{};
        codecs::AdminRequestType::Value request_type{};
        codecs::AdminResponseCode::Value response_code{};
        std::string message{};
        std::vector<std::uint8_t> payload{};
    };

    std::vector<Message> messages{};
    std::vector<SessionEvent> session_events{};
    std::vector<NewLeader> new_leaders{};
    std::vector<AdminResponse> admin_responses{};

    void on_message(std::int64_t cluster_session_id, std::int64_t timestamp,
                    std::span<const std::uint8_t> payload) override
    {
        messages.push_back(Message{cluster_session_id, timestamp, {payload.begin(), payload.end()}});
    }

    void on_session_event(std::int64_t correlation_id, std::int64_t cluster_session_id, std::int64_t leadership_term_id,
                          std::int32_t leader_member_id, codecs::EventCode::Value code,
                          std::string_view detail) override
    {
        session_events.push_back(SessionEvent{correlation_id, cluster_session_id, leadership_term_id, leader_member_id,
                                              code, std::string{detail}});
    }

    void on_new_leader(std::int64_t cluster_session_id, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                       std::string_view ingress_endpoints) override
    {
        new_leaders.push_back(
            NewLeader{cluster_session_id, leadership_term_id, leader_member_id, std::string{ingress_endpoints}});
    }

    void on_admin_response(std::int64_t cluster_session_id, std::int64_t correlation_id,
                           codecs::AdminRequestType::Value request_type, codecs::AdminResponseCode::Value response_code,
                           std::string_view message, std::span<const std::uint8_t> payload) override
    {
        admin_responses.push_back(AdminResponse{cluster_session_id,
                                                correlation_id,
                                                request_type,
                                                response_code,
                                                std::string{message},
                                                {payload.begin(), payload.end()}});
    }
};

struct RecordingEgressListenerExtension final : EgressListenerExtension
{
    struct Call
    {
        std::int32_t acting_block_length{};
        std::int32_t template_id{};
        std::int32_t schema_id{};
        std::int32_t acting_version{};
        std::vector<std::uint8_t> body{};
    };

    std::vector<Call> calls{};

    void on_extension_message(std::int32_t acting_block_length, std::int32_t template_id, std::int32_t schema_id,
                              std::int32_t acting_version, std::span<const std::uint8_t> buffer) override
    {
        calls.push_back(
            Call{acting_block_length, template_id, schema_id, acting_version, {buffer.begin(), buffer.end()}});
    }
};

using Buffer = std::vector<std::uint8_t>;

[[nodiscard]] std::span<const std::uint8_t> at(const Buffer& buffer, std::size_t offset, std::size_t length)
{
    return std::span<const std::uint8_t>(buffer.data() + offset, length);
}

// The reference's `new UnsafeBuffer(new byte[512])`: every byte the encoders do
// not write stays zero, which is what makes schemaId 0 in the second case.
[[nodiscard]] Buffer zeroed(std::size_t length = 512)
{
    return Buffer(length, 0);
}

TEST(EgressAdapterTest, onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId)
{
    constexpr std::int32_t schema_id = 17;
    constexpr std::int32_t template_id = 19;

    Buffer buffer = zeroed();
    codecs::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(buffer.data()), 0, 0, buffer.size());
    header.schemaId(static_cast<std::uint16_t>(schema_id)).templateId(static_cast<std::uint16_t>(template_id));

    RecordingEgressListener listener;
    RecordingEgressListenerExtension extension;
    EgressAdapter adapter(listener, &extension, 0, 3);

    adapter.dispatch(at(buffer, 0, codecs::MessageHeader::encodedLength() * 2));

    ASSERT_EQ(1U, extension.calls.size());
    const RecordingEgressListenerExtension::Call& call = extension.calls[0];
    EXPECT_EQ(0, call.acting_block_length);
    EXPECT_EQ(template_id, call.template_id);
    EXPECT_EQ(schema_id, call.schema_id);
    EXPECT_EQ(0, call.acting_version);
    // The body is what follows the 8-byte header (`EgressAdapter.java:109-111`).
    EXPECT_EQ(codecs::MessageHeader::encodedLength(), call.body.size());
    EXPECT_TRUE(listener.messages.empty());
}

TEST(EgressAdapterTest, defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId)
{
    Buffer buffer = zeroed();
    RecordingEgressListener listener;
    EgressAdapter adapter(listener, 42, 5);

    adapter.dispatch(at(buffer, 0, 64));

    // The reference: "expected schemaId=111, actual=0"
    // (`EgressAdapter.java:114-117`).
    const std::optional<axc::core::Error> error = adapter.take_error();
    ASSERT_TRUE(error.has_value());
    EXPECT_EQ("expected schemaId=111, actual=0", std::string{error->message.view()});
    EXPECT_TRUE(listener.messages.empty());
}

TEST(EgressAdapterTest, onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches)
{
    constexpr std::size_t offset = 4;
    constexpr std::int64_t session_id = 2973438724L;
    constexpr std::int64_t timestamp = -46328746238764832L;

    Buffer buffer = zeroed();
    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(session_id).timestamp(timestamp);

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, session_id, 3);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    ASSERT_EQ(1U, listener.messages.size());
    EXPECT_EQ(session_id, listener.messages[0].session_id);
    EXPECT_EQ(timestamp, listener.messages[0].timestamp);
    // `length - SESSION_HEADER_LENGTH` is zero here: a bare session header.
    EXPECT_TRUE(listener.messages[0].payload.empty());
    EXPECT_FALSE(adapter.take_error().has_value());
}

TEST(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage)
{
    constexpr std::size_t offset = 18;
    constexpr std::int64_t session_id = 21;
    constexpr std::int64_t timestamp = 1000;

    Buffer buffer = zeroed();
    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(session_id).timestamp(timestamp);

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, -19, 3);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    EXPECT_TRUE(listener.messages.empty());
}

TEST(EgressAdapterTest, onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches)
{
    constexpr std::size_t offset = 8;
    constexpr std::int64_t cluster_session_id = 42;
    constexpr std::int64_t correlation_id = 777;
    constexpr std::int64_t leadership_term_id = 6;
    constexpr std::int32_t leader_member_id = 3;
    constexpr codecs::EventCode::Value event_code = codecs::EventCode::REDIRECT;
    constexpr std::int32_t version = 18;
    constexpr std::string_view event_detail = "Event details";

    Buffer buffer = zeroed();
    codecs::SessionEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(correlation_id)
        .leadershipTermId(leadership_term_id)
        .leaderMemberId(leader_member_id)
        .code(event_code)
        .version(version);
    encoder.putDetail(static_cast<std::string_view>(event_detail));

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, cluster_session_id, 10);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    ASSERT_EQ(1U, listener.session_events.size());
    const RecordingEgressListener::SessionEvent& event = listener.session_events[0];
    EXPECT_EQ(correlation_id, event.correlation_id);
    EXPECT_EQ(cluster_session_id, event.session_id);
    EXPECT_EQ(leadership_term_id, event.leadership_term_id);
    EXPECT_EQ(leader_member_id, event.leader_member_id);
    EXPECT_EQ(event_code, event.code);
    EXPECT_EQ(event_detail, event.detail);
}

TEST(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionEvent)
{
    constexpr std::size_t offset = 8;
    constexpr std::int64_t cluster_session_id = 42;

    Buffer buffer = zeroed();
    codecs::SessionEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(777)
        .leadershipTermId(6)
        .leaderMemberId(3)
        .code(codecs::EventCode::REDIRECT)
        .version(18);
    encoder.putDetail(std::string_view{"Event details"});

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, cluster_session_id + 1, 3);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    EXPECT_TRUE(listener.session_events.empty());
}

TEST(EgressAdapterTest, onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches)
{
    constexpr std::size_t offset = 0;
    constexpr std::int64_t cluster_session_id = 0;
    constexpr std::int64_t leadership_term_id = 6;
    constexpr std::int32_t leader_member_id = 9999;
    constexpr std::string_view ingress_endpoints = "ingress endpoints ...";

    Buffer buffer = zeroed();
    codecs::NewLeaderEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.leadershipTermId(leadership_term_id).clusterSessionId(cluster_session_id).leaderMemberId(leader_member_id);
    encoder.putIngressEndpoints(static_cast<std::string_view>(ingress_endpoints));

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, cluster_session_id, 10);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    ASSERT_EQ(1U, listener.new_leaders.size());
    EXPECT_EQ(cluster_session_id, listener.new_leaders[0].session_id);
    EXPECT_EQ(leadership_term_id, listener.new_leaders[0].leadership_term_id);
    EXPECT_EQ(leader_member_id, listener.new_leaders[0].leader_member_id);
    EXPECT_EQ(ingress_endpoints, listener.new_leaders[0].ingress_endpoints);
}

TEST(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnNewLeader)
{
    constexpr std::size_t offset = 0;
    constexpr std::int64_t cluster_session_id = -100;

    Buffer buffer = zeroed();
    codecs::NewLeaderEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.leadershipTermId(6).clusterSessionId(cluster_session_id).leaderMemberId(9999);
    encoder.putIngressEndpoints(std::string_view{"ingress endpoints ..."});

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, 0, 10);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    EXPECT_TRUE(listener.new_leaders.empty());
}

TEST(EgressAdapterTest, onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches)
{
    constexpr std::size_t offset = 24;
    constexpr std::int64_t cluster_session_id = 18;
    constexpr std::int64_t correlation_id = 3274239749237498239L;
    constexpr codecs::AdminRequestType::Value type = codecs::AdminRequestType::SNAPSHOT;
    constexpr codecs::AdminResponseCode::Value response_code = codecs::AdminResponseCode::UNAUTHORISED_ACCESS;
    constexpr std::string_view message = "Unauthorised access detected!";
    const std::vector<std::uint8_t> payload = {0x1, 0x2, 0x3};

    Buffer buffer = zeroed();
    codecs::AdminResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(correlation_id)
        .requestType(type)
        .responseCode(response_code);
    encoder.putMessage(static_cast<std::string_view>(message));
    encoder.putPayload(reinterpret_cast<const char*>(payload.data()), static_cast<std::uint32_t>(payload.size()));

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, cluster_session_id, 10);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    ASSERT_EQ(1U, listener.admin_responses.size());
    const RecordingEgressListener::AdminResponse& response = listener.admin_responses[0];
    EXPECT_EQ(cluster_session_id, response.session_id);
    EXPECT_EQ(correlation_id, response.correlation_id);
    EXPECT_EQ(type, response.request_type);
    EXPECT_EQ(response_code, response.response_code);
    EXPECT_EQ(message, response.message);
    EXPECT_EQ(payload, response.payload);
}

TEST(EgressAdapterTest, onFragmentIsANoOpIfSessionIdDoesNotMatchOnAdminResponse)
{
    constexpr std::size_t offset = 24;
    constexpr std::int64_t cluster_session_id = 18;
    const std::vector<std::uint8_t> payload = {0x1, 0x2, 0x3};

    Buffer buffer = zeroed();
    codecs::AdminResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), offset, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(3274239749237498239L)
        .requestType(codecs::AdminRequestType::SNAPSHOT)
        .responseCode(codecs::AdminResponseCode::OK);
    encoder.putMessage(std::string_view{"Unauthorised access detected!"});
    encoder.putPayload(reinterpret_cast<const char*>(payload.data()), static_cast<std::uint32_t>(payload.size()));

    RecordingEgressListener listener;
    EgressAdapter adapter(listener, -cluster_session_id, 10);

    adapter.dispatch(at(buffer, offset, codecs::MessageHeader::encodedLength() + encoder.encodedLength()));

    EXPECT_TRUE(listener.admin_responses.empty());
}

}  // namespace
