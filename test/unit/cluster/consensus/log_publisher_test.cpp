// C4b: `LogPublisher` writes the reference's log records -- the two-part
// session message (32-byte header + payload) and the SessionOpen/SessionClose
// events -- and never copies the ingress payload through an intermediate
// buffer.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "cluster/consensus/cluster_session.h"
#include "cluster/consensus/log_publisher.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/SessionCloseEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SessionOpenEvent.h"
#include "io_aeron_cluster_codecs/TimerEvent.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using axc::core::ExPubPtr;
using axc::core::SubPtr;
using deepmsg::cluster::consensus::ClusterSession;
using deepmsg::cluster::consensus::LogPublisher;
using deepmsg::cluster::consensus::SESSION_HEADER_LENGTH;

constexpr std::int32_t STREAM_ID = 9001;
constexpr std::int64_t LEADERSHIP_TERM_ID = 3;
constexpr std::int64_t CLUSTER_SESSION_ID = 21;
constexpr std::int64_t TIMESTAMP = 42;

class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c4b-log-XXXXXX";
        if (nullptr == ::mkdtemp(pattern))
        {
            return;
        }
        dir_ = pattern;
        if (0 != aeron_driver_context_init(&context_))
        {
            return;
        }
        (void)aeron_driver_context_set_dir(context_, dir_.c_str());
        aeron_driver_context_set_dir_delete_on_start(context_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(context_, true);
        if (0 != aeron_driver_init(&driver_, context_))
        {
            return;
        }
        started_ = 0 == aeron_driver_start(driver_, false);
    }

    ~EmbeddedDriver()
    {
        if (nullptr != driver_)
        {
            aeron_driver_close(driver_);
        }
        if (nullptr != context_)
        {
            aeron_driver_context_close(context_);
        }
        if (!dir_.empty())
        {
            ::rmdir(dir_.c_str());
        }
    }

    [[nodiscard]] bool ok() const noexcept
    {
        return started_;
    }
    [[nodiscard]] const std::string& dir() const noexcept
    {
        return dir_;
    }

private:
    std::string dir_;
    aeron_driver_context_t* context_{nullptr};
    aeron_driver_t* driver_{nullptr};
    bool started_{false};
};

std::optional<DeepmsgClient> connect_with_retry(const std::string& dir)
{
    for (int attempt = 0; attempt < 200; ++attempt)
    {
        auto connected = DeepmsgClient::connect({dir, true});
        if (connected.has_value())
        {
            return std::optional<DeepmsgClient>{std::move(*connected)};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return std::nullopt;
}

template <class H>
std::optional<H> await_handle(DeepmsgClient& client, axc::core::AsyncAdd<H>& add)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline)
    {
        (void)client.do_work();
        auto polled = add.poll();
        if (polled && polled->has_value())
        {
            return std::optional<H>{std::move(**polled)};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return std::nullopt;
}

struct Collector
{
    std::vector<std::vector<std::uint8_t>> messages{};
};

void on_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t* /*header*/)
{
    static_cast<Collector*>(clientd)->messages.emplace_back(buffer, buffer + length);
}

class LogPublisherTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_TRUE(driver_.ok());
        client_ = connect_with_retry(driver_.dir());
        ASSERT_TRUE(client_.has_value());

        auto add_publication = client_->add_exclusive_publication("aeron:ipc", STREAM_ID);
        auto publication = await_handle(*client_, add_publication);
        ASSERT_TRUE(publication.has_value());
        publisher_.publication(std::move(*publication));

        auto add_subscription = client_->add_subscription("aeron:ipc", STREAM_ID);
        auto subscription = await_handle(*client_, add_subscription);
        ASSERT_TRUE(subscription.has_value());
        subscription_ = std::move(*subscription);
    }

    void TearDown() override
    {
        subscription_.reset();
        publisher_.disconnect();
        client_.reset();
    }

    [[nodiscard]] std::vector<std::uint8_t> next_message()
    {
        for (int i = 0; i < 500; ++i)
        {
            Collector collector{};
            (void)client_->do_work();
            (void)aeron_subscription_poll(subscription_.get(), on_fragment, &collector, 10);
            if (!collector.messages.empty())
            {
                return std::move(collector.messages.front());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return {};
    }

    EmbeddedDriver driver_{};
    std::optional<DeepmsgClient> client_{};
    LogPublisher publisher_{"aeron:ipc"};
    SubPtr subscription_{};
};

TEST_F(LogPublisherTest, aSessionMessageIsTheThirtyTwoByteHeaderThenThePayload)
{
    const std::uint8_t payload[] = {1, 2, 3, 4, 5, 6, 7};
    ASSERT_GT(publisher_.append_message(LEADERSHIP_TERM_ID, CLUSTER_SESSION_ID, TIMESTAMP, payload, sizeof(payload)),
              0);
    EXPECT_EQ(0U, publisher_.payload_bytes_copied());

    const std::vector<std::uint8_t> message = next_message();
    ASSERT_FALSE(message.empty());
    ASSERT_EQ(SESSION_HEADER_LENGTH + sizeof(payload), message.size());

    auto header = deepmsg::cluster::common::message_header(message);
    ASSERT_TRUE(header);
    EXPECT_EQ(codecs::SessionMessageHeader::SBE_TEMPLATE_ID, header->templateId());
    auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionMessageHeader>(message, *header);
    EXPECT_EQ(LEADERSHIP_TERM_ID, decoder.leadershipTermId());
    EXPECT_EQ(CLUSTER_SESSION_ID, decoder.clusterSessionId());
    EXPECT_EQ(TIMESTAMP, decoder.timestamp());
    EXPECT_EQ(0, std::memcmp(message.data() + SESSION_HEADER_LENGTH, payload, sizeof(payload)));
}

TEST_F(LogPublisherTest, aSessionOpenEventCarriesTheSessionFields)
{
    ClusterSession session(0, CLUSTER_SESSION_ID, 8001, "aeron:ipc?alias=resp", "sourceIdentity=ipc sessionId=1");
    session.last_activity_ns(0, 77);
    session.authenticate({});

    ASSERT_GT(publisher_.append_session_open(session, LEADERSHIP_TERM_ID, TIMESTAMP), 0);

    const std::vector<std::uint8_t> message = next_message();
    ASSERT_FALSE(message.empty());
    auto header = deepmsg::cluster::common::message_header(message);
    ASSERT_TRUE(header);
    EXPECT_EQ(codecs::SessionOpenEvent::SBE_TEMPLATE_ID, header->templateId());
    auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionOpenEvent>(message, *header);
    EXPECT_EQ(LEADERSHIP_TERM_ID, decoder.leadershipTermId());
    EXPECT_EQ(CLUSTER_SESSION_ID, decoder.clusterSessionId());
    EXPECT_EQ(77, decoder.correlationId());
    EXPECT_EQ(TIMESTAMP, decoder.timestamp());
    EXPECT_EQ(8001, decoder.responseStreamId());
    const std::uint32_t response_channel_length = decoder.responseChannelLength();
    const char* response_channel_ptr = decoder.responseChannel();
    const std::string response_channel(response_channel_ptr, response_channel_length);
    EXPECT_EQ(std::string{"aeron:ipc?alias=resp"}, response_channel);
}

TEST_F(LogPublisherTest, aSessionCloseEventCarriesTheReason)
{
    ClusterSession session(0, CLUSTER_SESSION_ID, 8001, "aeron:ipc", "ipc");
    session.closing(codecs::CloseReason::CLIENT_ACTION);

    ASSERT_TRUE(publisher_.append_session_close(session, LEADERSHIP_TERM_ID, TIMESTAMP));

    const std::vector<std::uint8_t> message = next_message();
    ASSERT_FALSE(message.empty());
    auto header = deepmsg::cluster::common::message_header(message);
    ASSERT_TRUE(header);
    EXPECT_EQ(codecs::SessionCloseEvent::SBE_TEMPLATE_ID, header->templateId());
    auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionCloseEvent>(message, *header);
    EXPECT_EQ(LEADERSHIP_TERM_ID, decoder.leadershipTermId());
    EXPECT_EQ(CLUSTER_SESSION_ID, decoder.clusterSessionId());
    EXPECT_EQ(codecs::CloseReason::CLIENT_ACTION, decoder.closeReason());
}

TEST_F(LogPublisherTest, anExpiredTimerIsATimerEventRecord)
{
    ASSERT_GT(publisher_.append_timer(4242, LEADERSHIP_TERM_ID, TIMESTAMP), 0);

    const std::vector<std::uint8_t> message = next_message();
    ASSERT_FALSE(message.empty());
    auto header = deepmsg::cluster::common::message_header(message);
    ASSERT_TRUE(header);
    EXPECT_EQ(codecs::TimerEvent::SBE_TEMPLATE_ID, header->templateId());
    auto decoder = deepmsg::cluster::common::wrap_body<codecs::TimerEvent>(message, *header);
    EXPECT_EQ(LEADERSHIP_TERM_ID, decoder.leadershipTermId());
    EXPECT_EQ(4242, decoder.correlationId());
    EXPECT_EQ(TIMESTAMP, decoder.timestamp());
}

}  // namespace
