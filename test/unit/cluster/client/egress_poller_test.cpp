// C3a: `EgressPoller` (`EgressPoller.java:31-333`) without a driver. Each test
// hands the poller one encoded cluster message, the same bytes the C fragment
// assembler would deliver.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "cluster/client/egress_poller.h"
#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using deepmsg::cluster::client::EgressPoller;

// Never dereferenced: the poller only stores the image the header carried.
aeron_image_t* const TEST_IMAGE = reinterpret_cast<aeron_image_t*>(static_cast<std::uintptr_t>(0x5));
aeron_image_t* const OTHER_IMAGE = reinterpret_cast<aeron_image_t*>(static_cast<std::uintptr_t>(0x6));

[[nodiscard]] std::vector<std::uint8_t> encode_session_event(std::int64_t cluster_session_id,
                                                             std::int64_t correlation_id,
                                                             std::int64_t leadership_term_id,
                                                             std::int32_t leader_member_id,
                                                             codecs::EventCode::Value code, std::int32_t version,
                                                             std::int64_t heartbeat_ns, std::string_view detail)
{
    const std::size_t length =
        deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::SessionEvent::computeLength(detail.size());
    std::vector<std::uint8_t> buffer(length);

    codecs::SessionEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.clusterSessionId(cluster_session_id)
        .correlationId(correlation_id)
        .leadershipTermId(leadership_term_id)
        .leaderMemberId(leader_member_id)
        .code(code)
        .version(version)
        .leaderHeartbeatTimeoutNs(heartbeat_ns);
    encoder.putDetail(detail);
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_session_message_header(std::int64_t leadership_term_id,
                                                                      std::int64_t cluster_session_id)
{
    const std::size_t length =
        deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::SessionMessageHeader::sbeBlockLength();
    std::vector<std::uint8_t> buffer(length);

    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.leadershipTermId(leadership_term_id).clusterSessionId(cluster_session_id).timestamp(42);
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_challenge(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                                         std::string_view encoded_challenge)
{
    const std::size_t length =
        deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::Challenge::computeLength(encoded_challenge.size());
    std::vector<std::uint8_t> buffer(length);

    codecs::Challenge encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.correlationId(correlation_id).clusterSessionId(cluster_session_id);
    encoder.putEncodedChallenge(encoded_challenge);
    return buffer;
}

[[nodiscard]] std::vector<std::uint8_t> encode_new_leader_event(std::int64_t cluster_session_id,
                                                                std::int64_t leadership_term_id,
                                                                std::int32_t leader_member_id,
                                                                std::string_view ingress_endpoints)
{
    const std::size_t length = deepmsg::cluster::common::MESSAGE_HEADER_LENGTH +
                               codecs::NewLeaderEvent::computeLength(ingress_endpoints.size());
    std::vector<std::uint8_t> buffer(length);

    codecs::NewLeaderEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.clusterSessionId(cluster_session_id).leadershipTermId(leadership_term_id).leaderMemberId(leader_member_id);
    encoder.putIngressEndpoints(ingress_endpoints);
    return buffer;
}

/// A message with an unknown schema id (`EgressPoller.java:246-250`).
[[nodiscard]] std::vector<std::uint8_t> encode_foreign_schema()
{
    const std::size_t length = deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + 4;
    std::vector<std::uint8_t> buffer(length);

    codecs::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(buffer.data()), 0, 0, buffer.size());
    header.blockLength(4).templateId(35).schemaId(101).version(0);
    return buffer;
}

TEST(EgressPollerTest, aSessionEventFillsTheSlotAndBreaks)
{
    EgressPoller poller(10);
    const auto message = encode_session_event(7, 11, 3, 0, codecs::EventCode::OK, 768, 10'000'000'000, "");

    EXPECT_EQ(AERON_ACTION_BREAK, poller.on_message(message, TEST_IMAGE));
    EXPECT_TRUE(poller.is_poll_complete());
    EXPECT_FALSE(poller.is_challenged());
    EXPECT_EQ(7, poller.cluster_session_id());
    EXPECT_EQ(11, poller.correlation_id());
    EXPECT_EQ(3, poller.leadership_term_id());
    EXPECT_EQ(0, poller.leader_member_id());
    EXPECT_EQ(codecs::EventCode::OK, poller.event_code());
    EXPECT_EQ(768, poller.version());
    EXPECT_EQ(10'000'000'000, poller.leader_heartbeat_timeout_ns());
    EXPECT_EQ(TEST_IMAGE, poller.egress_image());
    EXPECT_EQ(codecs::SessionEvent::sbeTemplateId(), poller.template_id());
}

TEST(EgressPollerTest, theHeartbeatNullValueBecomesMinusOne)
{
    EgressPoller poller(10);
    const auto message = encode_session_event(7, 11, 3, 0, codecs::EventCode::OK, 768,
                                              codecs::SessionEvent::leaderHeartbeatTimeoutNsNullValue(), "");

    ASSERT_EQ(AERON_ACTION_BREAK, poller.on_message(message, TEST_IMAGE));
    EXPECT_EQ(-1, poller.leader_heartbeat_timeout_ns());
}

TEST(EgressPollerTest, aSessionMessageHeaderFillsTheSlotAndBreaks)
{
    EgressPoller poller(10);
    const auto message = encode_session_message_header(5, 9);

    EXPECT_EQ(AERON_ACTION_BREAK, poller.on_message(message, OTHER_IMAGE));
    EXPECT_TRUE(poller.is_poll_complete());
    EXPECT_EQ(5, poller.leadership_term_id());
    EXPECT_EQ(9, poller.cluster_session_id());
    // Template 1 does not touch the egress image (`EP:255-265`).
    EXPECT_EQ(nullptr, poller.egress_image());
}

TEST(EgressPollerTest, aChallengeCarriesItsEncodedBytes)
{
    EgressPoller poller(10);
    const auto message = encode_challenge(12, 8, "the-challenge");

    EXPECT_EQ(AERON_ACTION_BREAK, poller.on_message(message, TEST_IMAGE));
    EXPECT_TRUE(poller.is_poll_complete());
    EXPECT_TRUE(poller.is_challenged());
    EXPECT_EQ(12, poller.correlation_id());
    EXPECT_EQ(8, poller.cluster_session_id());
    const auto challenge = poller.encoded_challenge();
    EXPECT_EQ(std::string{"the-challenge"}, std::string(challenge.begin(), challenge.end()));
}

TEST(EgressPollerTest, aNewLeaderEventCarriesTheIngressEndpoints)
{
    EgressPoller poller(10);
    const auto message = encode_new_leader_event(9, 4, 1, "0=localhost:20111,1=localhost:20112");

    EXPECT_EQ(AERON_ACTION_BREAK, poller.on_message(message, OTHER_IMAGE));
    EXPECT_TRUE(poller.is_poll_complete());
    EXPECT_EQ(9, poller.cluster_session_id());
    EXPECT_EQ(4, poller.leadership_term_id());
    EXPECT_EQ(1, poller.leader_member_id());
    EXPECT_EQ(std::string{"0=localhost:20111,1=localhost:20112"}, poller.detail());
    EXPECT_EQ(OTHER_IMAGE, poller.egress_image());
    EXPECT_FALSE(poller.is_challenged());
}

// The `ingressEndpoints` var-data field carries its length; it is not
// NUL-terminated on the wire. Bytes that follow the message must not leak
// into the detail (`EP:296`).
TEST(EgressPollerTest, theNewLeaderDetailStopsAtTheEncodedFieldLength)
{
    EgressPoller poller(10);
    auto message = encode_new_leader_event(9, 4, 1, "0=localhost:20111");
    const std::size_t encoded_length = message.size();
    // A terminating NUL, so that a decoder that treats the field as a C
    // string fails deterministically instead of reading out of bounds.
    const char tail[] = "EXTRA";
    message.resize(encoded_length + sizeof(tail));
    std::memcpy(message.data() + encoded_length, tail, sizeof(tail));

    EXPECT_EQ(AERON_ACTION_BREAK, poller.on_message(message, OTHER_IMAGE));
    EXPECT_EQ(std::string{"0=localhost:20111"}, poller.detail());
}

TEST(EgressPollerTest, anUnknownSchemaIsSteppedOver)
{
    EgressPoller poller(10);
    const auto message = encode_foreign_schema();

    EXPECT_EQ(AERON_ACTION_CONTINUE, poller.on_message(message, TEST_IMAGE));
    EXPECT_FALSE(poller.is_poll_complete());
    EXPECT_EQ(-1, poller.correlation_id());
}

TEST(EgressPollerTest, aFullSlotAborts)
{
    EgressPoller poller(10);
    const auto message = encode_session_event(7, 11, 3, 0, codecs::EventCode::OK, 768, 0, "");

    ASSERT_EQ(AERON_ACTION_BREAK, poller.on_message(message, TEST_IMAGE));
    EXPECT_EQ(AERON_ACTION_ABORT, poller.on_message(message, TEST_IMAGE));
}

}  // namespace
