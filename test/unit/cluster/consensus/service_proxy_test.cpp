// C4c: `ServiceProxy` encodes the five control messages the reference sends to
// a service (`ServiceProxy.java`), with the reference's uneven failure
// semantics. The reference's `ServiceProxyTest` cases are ported by name for
// the two membership responses; the rest pin the startup path.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/service_proxy.h"
#include "io_aeron_cluster_codecs/ClusterMembersExtendedResponse.h"
#include "io_aeron_cluster_codecs/ClusterMembersResponse.h"
#include "io_aeron_cluster_codecs/JoinLog.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/RequestServiceAck.h"
#include "io_aeron_cluster_codecs/ServiceTerminationPosition.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::Error;
using axc::core::ErrorHandler;
using deepmsg::cluster::consensus::ClusterRole;
using deepmsg::cluster::consensus::PublicationSend;
using deepmsg::cluster::consensus::ServiceProxy;
using deepmsg::cluster::consensus::ServiceSend;

class FakeServiceSend final : public ServiceSend
{
public:
    std::vector<std::int64_t> results{1};
    std::size_t calls{0};
    std::vector<std::uint8_t> last{};
    bool committed{false};
    bool closed{false};
    std::int64_t offer_result{1};
    std::int32_t term_length{65536};

    [[nodiscard]] std::int64_t try_claim(std::size_t length, std::span<std::uint8_t>& window) override
    {
        const std::int64_t result = results[std::min(calls, results.size() - 1)];
        ++calls;
        if (result > 0)
        {
            last.assign(length, 0);
            window = last;
        }
        return result;
    }

    void commit() noexcept override
    {
        committed = true;
    }

    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message) override
    {
        last.assign(message.begin(), message.end());
        return offer_result;
    }

    [[nodiscard]] bool is_closed() const noexcept override
    {
        return closed;
    }

    [[nodiscard]] std::int32_t term_buffer_length() const noexcept override
    {
        return term_length;
    }
};

class RecordingErrorHandler final : public ErrorHandler
{
public:
    std::vector<std::string> errors;

    void on_error(const Error& error) noexcept override
    {
        errors.emplace_back(error.message.view());
    }
};

template <class Decoder>
[[nodiscard]] Decoder decode_body(const std::vector<std::uint8_t>& buffer)
{
    codecs::MessageHeader header;
    header.wrap(const_cast<char*>(reinterpret_cast<const char*>(buffer.data())), 0,
                codecs::MessageHeader::sbeSchemaVersion(), buffer.size());
    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(buffer.data())),
                          codecs::MessageHeader::encodedLength(), header.blockLength(), header.version(),
                          buffer.size());
    return decoder;
}

[[nodiscard]] std::int32_t template_id(const std::vector<std::uint8_t>& buffer)
{
    codecs::MessageHeader header;
    header.wrap(const_cast<char*>(reinterpret_cast<const char*>(buffer.data())), 0,
                codecs::MessageHeader::sbeSchemaVersion(), buffer.size());
    return header.templateId();
}

TEST(ServiceProxyTest, joinLogEncodesTheReferenceFields)
{
    FakeServiceSend send;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    auto joined = proxy.join_log(1024, 4096, 2, 7, 100, true, ClusterRole::leader, "aeron:ipc?term-length=64m", false);
    ASSERT_TRUE(joined) << joined.error().message.view();
    EXPECT_TRUE(send.committed);
    EXPECT_EQ(codecs::JoinLog::SBE_TEMPLATE_ID, template_id(send.last));

    auto decoder = decode_body<codecs::JoinLog>(send.last);
    EXPECT_EQ(1024, decoder.logPosition());
    EXPECT_EQ(4096, decoder.maxLogPosition());
    EXPECT_EQ(2, decoder.memberId());
    EXPECT_EQ(7, decoder.logSessionId());
    EXPECT_EQ(100, decoder.logStreamId());
    EXPECT_EQ(codecs::BooleanType::TRUE, decoder.isStartup());
    EXPECT_EQ(2, decoder.role());
    EXPECT_EQ(codecs::BooleanType::FALSE, decoder.isStandby());
    EXPECT_EQ("aeron:ipc?term-length=64m", decoder.getLogChannelAsStringView());
}

TEST(ServiceProxyTest, joinLogRetriesBackPressure)
{
    FakeServiceSend send;
    send.results = {-2, -2, 1};
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    auto joined = proxy.join_log(0, 1, 0, 1, 100, true, ClusterRole::leader, "aeron:ipc", false);
    EXPECT_TRUE(joined);
    EXPECT_EQ(3u, send.calls);
}

TEST(ServiceProxyTest, joinLogFailsWhenThePublicationIsNotConnected)
{
    FakeServiceSend send;
    send.results = {-1};
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    auto joined = proxy.join_log(0, 1, 0, 1, 100, true, ClusterRole::leader, "aeron:ipc", false);
    ASSERT_FALSE(joined);
    EXPECT_EQ("publication is not connected", joined.error().message.view());
}

TEST(ServiceProxyTest, joinLogFailsAfterFiveBackPressures)
{
    FakeServiceSend send;
    send.results = {-2};
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    auto joined = proxy.join_log(0, 1, 0, 1, 100, true, ClusterRole::leader, "aeron:ipc", false);
    ASSERT_FALSE(joined);
    EXPECT_NE(std::string::npos, joined.error().message.view().find("failed to send join log request"));
}

TEST(ServiceProxyTest, requestServiceAckCarriesThePosition)
{
    FakeServiceSend send;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    auto requested = proxy.request_service_ack(55);
    ASSERT_TRUE(requested);
    EXPECT_EQ(codecs::RequestServiceAck::SBE_TEMPLATE_ID, template_id(send.last));
    EXPECT_EQ(55, decode_body<codecs::RequestServiceAck>(send.last).logPosition());
}

TEST(ServiceProxyTest, terminationPositionIsIgnoredOnAClosedPublication)
{
    FakeServiceSend send;
    send.closed = true;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    proxy.termination_position(9, true);
    EXPECT_EQ(0u, send.calls);
    EXPECT_TRUE(errors.errors.empty());
}

TEST(ServiceProxyTest, terminationPositionEncodesTheAckFlag)
{
    FakeServiceSend send;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    proxy.termination_position(9, true);
    EXPECT_EQ(codecs::ServiceTerminationPosition::SBE_TEMPLATE_ID, template_id(send.last));
    auto decoder = decode_body<codecs::ServiceTerminationPosition>(send.last);
    EXPECT_EQ(9, decoder.logPosition());
    EXPECT_EQ(codecs::BooleanType::TRUE, decoder.ackRequired());
}

TEST(ServiceProxyTest, shouldSendClusterMemberResponse)
{
    FakeServiceSend send;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    proxy.cluster_members_response(17, 0, "0,localhost:10000|1,localhost:10001");
    ASSERT_FALSE(send.last.empty());
    EXPECT_EQ(codecs::ClusterMembersResponse::SBE_TEMPLATE_ID, template_id(send.last));

    auto decoder = decode_body<codecs::ClusterMembersResponse>(send.last);
    EXPECT_EQ(17, decoder.correlationId());
    EXPECT_EQ(0, decoder.leaderMemberId());
    EXPECT_EQ("0,localhost:10000|1,localhost:10001", decoder.getActiveMembersAsStringView());
    EXPECT_EQ("", decoder.getPassiveFollowersAsStringView());
    EXPECT_TRUE(errors.errors.empty());
}

TEST(ServiceProxyTest, shouldLogWarningIfClusterMemberResponseFails)
{
    FakeServiceSend send;
    send.results = {-2};
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    proxy.cluster_members_response(17, 0, "0,localhost:10000");
    ASSERT_EQ(1u, errors.errors.size());
    EXPECT_NE(std::string::npos, errors.errors[0].find("failed to send cluster members response"));
}

TEST(ServiceProxyTest, shouldSendClusterMemberExtendedResponse)
{
    FakeServiceSend send;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    deepmsg::cluster::common::ClusterMember member;
    member.id = 0;
    member.leadership_term_id = 3;
    member.log_position = 1024;
    member.time_of_last_append_position_ns = 99;
    member.ingress_endpoint = "localhost:10000";
    member.consensus_endpoint = "localhost:10001";
    member.log_endpoint = "localhost:10002";
    member.catchup_endpoint = "localhost:10003";
    member.archive_endpoint = "localhost:10004";

    proxy.cluster_members_extended_response(21, 1234, 0, 0,
                                            std::span<const deepmsg::cluster::common::ClusterMember>(&member, 1));
    EXPECT_EQ(codecs::ClusterMembersExtendedResponse::SBE_TEMPLATE_ID, template_id(send.last));

    auto decoder = decode_body<codecs::ClusterMembersExtendedResponse>(send.last);
    EXPECT_EQ(21, decoder.correlationId());
    EXPECT_EQ(1234, decoder.currentTimeNs());
    EXPECT_EQ(0, decoder.leaderMemberId());
    EXPECT_EQ(0, decoder.memberId());
    auto& group = decoder.activeMembers();
    EXPECT_EQ(1u, group.count());
    ASSERT_TRUE(group.hasNext());
    auto& entry = group.next();
    EXPECT_EQ(3, entry.leadershipTermId());
    EXPECT_EQ(1024, entry.logPosition());
    EXPECT_EQ(99, entry.timeOfLastAppendNs());
    // SBE's var accessors advance: read the endpoints in schema order.
    EXPECT_EQ("localhost:10000", entry.getIngressEndpointAsStringView());
    EXPECT_EQ("localhost:10001", entry.getConsensusEndpointAsStringView());
    EXPECT_EQ("localhost:10002", entry.getLogEndpointAsStringView());
    EXPECT_EQ("localhost:10003", entry.getCatchupEndpointAsStringView());
    EXPECT_EQ("localhost:10004", entry.getArchiveEndpointAsStringView());
    EXPECT_EQ(0u, decoder.passiveMembers().count());
    EXPECT_TRUE(errors.errors.empty());
}

TEST(ServiceProxyTest, shouldLogWarningIfClusterMemberExtendedResponseFails)
{
    FakeServiceSend send;
    send.offer_result = -2;
    RecordingErrorHandler errors;
    ServiceProxy proxy(send, errors);

    deepmsg::cluster::common::ClusterMember member;
    proxy.cluster_members_extended_response(21, 1234, 0, 0,
                                            std::span<const deepmsg::cluster::common::ClusterMember>(&member, 1));
    ASSERT_EQ(1u, errors.errors.size());
    EXPECT_NE(std::string::npos, errors.errors[0].find("failed to send cluster members extended response"));
}

}  // namespace
