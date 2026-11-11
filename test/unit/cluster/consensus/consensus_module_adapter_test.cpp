// C4c: `ConsensusModuleAdapter` decodes the six messages a service sends on the
// control channel's consensus-module stream (`ConsensusModuleAdapter.java`),
// and the `ServiceAck` case stops the poll (`BREAK`).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/consensus_module_adapter.h"
#include "io_aeron_cluster_codecs/CancelTimer.h"
#include "io_aeron_cluster_codecs/CloseSession.h"
#include "io_aeron_cluster_codecs/ClusterMembersQuery.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/ScheduleTimer.h"
#include "io_aeron_cluster_codecs/ServiceAck.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::Error;
using deepmsg::cluster::consensus::ConsensusModuleAdapter;
using deepmsg::cluster::consensus::ServiceControlHandler;

class RecordingHandler final : public ServiceControlHandler
{
public:
    struct Call
    {
        std::string name;
        std::int64_t first{0};
        std::int64_t second{0};
        std::int64_t third{0};
        std::int64_t fourth{0};
        std::int32_t fifth{0};
        bool flag{false};
        std::vector<std::uint8_t> message;
    };

    std::vector<Call> calls;
    std::optional<Error> fail{};

    [[nodiscard]] std::expected<void, Error> on_service_message(std::int64_t cluster_session_id,
                                                                std::span<const std::uint8_t> message) override
    {
        if (fail)
        {
            return std::unexpected(*fail);
        }
        Call call;
        call.name = "service_message";
        call.first = cluster_session_id;
        call.message.assign(message.begin(), message.end());
        calls.push_back(std::move(call));
        return {};
    }

    [[nodiscard]] std::expected<void, Error> on_service_close_session(std::int64_t cluster_session_id) override
    {
        calls.push_back(Call{"close_session", cluster_session_id});
        return {};
    }

    void on_schedule_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        calls.push_back(Call{"schedule_timer", correlation_id, deadline});
    }

    void on_cancel_timer(std::int64_t correlation_id) override
    {
        calls.push_back(Call{"cancel_timer", correlation_id});
    }

    [[nodiscard]] std::expected<void, Error> on_service_ack(std::int64_t log_position, std::int64_t timestamp,
                                                            std::int64_t ack_id, std::int64_t relevant_id,
                                                            std::int32_t service_id) override
    {
        Call call;
        call.name = "service_ack";
        call.first = log_position;
        call.second = timestamp;
        call.third = ack_id;
        call.fourth = relevant_id;
        call.fifth = service_id;
        calls.push_back(std::move(call));
        return {};
    }

    [[nodiscard]] std::expected<void, Error> on_cluster_members_query(std::int64_t correlation_id,
                                                                      bool is_extended_request) override
    {
        Call call;
        call.name = "cluster_members_query";
        call.first = correlation_id;
        call.flag = is_extended_request;
        calls.push_back(std::move(call));
        return {};
    }
};

template <class Encoder, class Fill>
[[nodiscard]] std::vector<std::uint8_t> encode(std::size_t length, Fill fill)
{
    std::vector<std::uint8_t> buffer(length, 0);
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    fill(encoder);
    return buffer;
}

TEST(ConsensusModuleAdapterTest, aServiceMessageIsHandedOverWhole)
{
    RecordingHandler handler;
    ConsensusModuleAdapter adapter(handler);

    const auto message = encode<codecs::SessionMessageHeader>(
        codecs::MessageHeader::encodedLength() + codecs::SessionMessageHeader::sbeBlockLength() + 4,
        [](codecs::SessionMessageHeader& encoder)
        {
            encoder.leadershipTermId(3).clusterSessionId(9).timestamp(77);
            std::memcpy(encoder.buffer() + encoder.offset() + codecs::SessionMessageHeader::sbeBlockLength(), "abcd",
                        4);
        });

    auto action = adapter.dispatch_message(message);
    ASSERT_TRUE(action) << action.error().message.view();
    EXPECT_EQ(AERON_ACTION_CONTINUE, *action);
    ASSERT_EQ(1u, handler.calls.size());
    EXPECT_EQ("service_message", handler.calls[0].name);
    EXPECT_EQ(9, handler.calls[0].first);
    EXPECT_EQ(message.size(), handler.calls[0].message.size());
}

TEST(ConsensusModuleAdapterTest, theSixTemplatesReachTheirHandlers)
{
    RecordingHandler handler;
    ConsensusModuleAdapter adapter(handler);

    const auto close_message =
        encode<codecs::CloseSession>(codecs::MessageHeader::encodedLength() + codecs::CloseSession::sbeBlockLength(),
                                     [](codecs::CloseSession& encoder) { encoder.clusterSessionId(12); });
    EXPECT_EQ(AERON_ACTION_CONTINUE, *adapter.dispatch_message(close_message));

    const auto schedule =
        encode<codecs::ScheduleTimer>(codecs::MessageHeader::encodedLength() + codecs::ScheduleTimer::sbeBlockLength(),
                                      [](codecs::ScheduleTimer& encoder) { encoder.correlationId(5).deadline(1234); });
    EXPECT_EQ(AERON_ACTION_CONTINUE, *adapter.dispatch_message(schedule));

    const auto cancel =
        encode<codecs::CancelTimer>(codecs::MessageHeader::encodedLength() + codecs::CancelTimer::sbeBlockLength(),
                                    [](codecs::CancelTimer& encoder) { encoder.correlationId(6); });
    EXPECT_EQ(AERON_ACTION_CONTINUE, *adapter.dispatch_message(cancel));

    const auto ack = encode<codecs::ServiceAck>(
        codecs::MessageHeader::encodedLength() + codecs::ServiceAck::sbeBlockLength(), [](codecs::ServiceAck& encoder)
        { encoder.logPosition(1024).timestamp(7).ackId(0).relevantId(3).serviceId(0); });
    EXPECT_EQ(AERON_ACTION_BREAK, *adapter.dispatch_message(ack));

    const auto query = encode<codecs::ClusterMembersQuery>(
        codecs::MessageHeader::encodedLength() + codecs::ClusterMembersQuery::sbeBlockLength(),
        [](codecs::ClusterMembersQuery& encoder) { encoder.correlationId(44).extended(codecs::BooleanType::TRUE); });
    EXPECT_EQ(AERON_ACTION_CONTINUE, *adapter.dispatch_message(query));

    ASSERT_EQ(5u, handler.calls.size());
    EXPECT_EQ("close_session", handler.calls[0].name);
    EXPECT_EQ(12, handler.calls[0].first);
    EXPECT_EQ("schedule_timer", handler.calls[1].name);
    EXPECT_EQ(5, handler.calls[1].first);
    EXPECT_EQ(1234, handler.calls[1].second);
    EXPECT_EQ("cancel_timer", handler.calls[2].name);
    EXPECT_EQ(6, handler.calls[2].first);
    EXPECT_EQ("service_ack", handler.calls[3].name);
    EXPECT_EQ(1024, handler.calls[3].first);
    EXPECT_EQ(7, handler.calls[3].second);
    EXPECT_EQ(0, handler.calls[3].third);
    EXPECT_EQ(3, handler.calls[3].fourth);
    EXPECT_EQ(0, handler.calls[3].fifth);
    EXPECT_EQ("cluster_members_query", handler.calls[4].name);
    EXPECT_EQ(44, handler.calls[4].first);
    EXPECT_TRUE(handler.calls[4].flag);
}

TEST(ConsensusModuleAdapterTest, aForeignSchemaIsAnError)
{
    RecordingHandler handler;
    ConsensusModuleAdapter adapter(handler);

    std::vector<std::uint8_t> message(codecs::MessageHeader::encodedLength() + 4, 0);
    codecs::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(message.data()), 0, codecs::MessageHeader::sbeSchemaVersion(), message.size());
    header.schemaId(999).templateId(1).blockLength(4).version(0);

    auto action = adapter.dispatch_message(message);
    ASSERT_FALSE(action);
    EXPECT_NE(std::string::npos, action.error().message.view().find("expected schemaId=111"));
}

TEST(ConsensusModuleAdapterTest, anUnknownTemplateIsContinued)
{
    RecordingHandler handler;
    ConsensusModuleAdapter adapter(handler);

    std::vector<std::uint8_t> message(codecs::MessageHeader::encodedLength() + 4, 0);
    codecs::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(message.data()), 0, codecs::MessageHeader::sbeSchemaVersion(), message.size());
    header.schemaId(111).templateId(999).blockLength(4).version(0);

    auto action = adapter.dispatch_message(message);
    ASSERT_TRUE(action);
    EXPECT_EQ(AERON_ACTION_CONTINUE, *action);
    EXPECT_TRUE(handler.calls.empty());
}

TEST(ConsensusModuleAdapterTest, aHandlerErrorAbortsThePoll)
{
    RecordingHandler handler;
    handler.fail = axc::core::make_error(axc::core::ErrorCode::no_capacity, "buffer full");
    ConsensusModuleAdapter adapter(handler);

    const auto message = encode<codecs::SessionMessageHeader>(
        codecs::MessageHeader::encodedLength() + codecs::SessionMessageHeader::sbeBlockLength(),
        [](codecs::SessionMessageHeader& encoder) { encoder.leadershipTermId(3).clusterSessionId(9).timestamp(77); });

    auto action = adapter.dispatch_message(message);
    ASSERT_FALSE(action);
    EXPECT_EQ("buffer full", action.error().message.view());
}

}  // namespace
