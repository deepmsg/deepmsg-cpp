// C4b: `IngressAdapter` decodes the six session templates and hands them to
// the handler; a foreign schema goes to the extension path. The fragments are
// published through a real publication on an embedded driver and read through
// the adapter's controlled fragment assembler.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "cluster/consensus/ingress_adapter.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/AdminRequest.h"
#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/SessionCloseRequest.h"
#include "io_aeron_cluster_codecs/SessionConnectRequest.h"
#include "io_aeron_cluster_codecs/SessionKeepAlive.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using axc::core::PubPtr;
using axc::core::SubPtr;
using deepmsg::cluster::consensus::IngressAdapter;
using deepmsg::cluster::consensus::IngressHandler;
using deepmsg::cluster::consensus::IngressImageInfo;

constexpr std::int32_t STREAM_ID = 9101;
constexpr std::int64_t LEADERSHIP_TERM_ID = 5;
constexpr std::int64_t CLUSTER_SESSION_ID = 31;

class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c4b-ingress-XXXXXX";
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

struct RecordingHandler final : IngressHandler
{
    bool on_ingress_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                            const std::uint8_t* payload, std::size_t payload_length) override
    {
        message_term = leadership_term_id;
        message_session = cluster_session_id;
        message.assign(payload, payload + payload_length);
        ++messages;
        return continue_polling;
    }

    void on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id, std::int32_t version,
                            std::string_view response_channel, std::span<const std::uint8_t> encoded_credentials,
                            std::string_view client_info, const IngressImageInfo& image_info) override
    {
        connect_correlation = correlation_id;
        connect_stream = response_stream_id;
        connect_version = version;
        connect_channel = std::string{response_channel};
        connect_credentials.assign(encoded_credentials.begin(), encoded_credentials.end());
        connect_client_info = std::string{client_info};
        connect_image_session = image_info.session_id;
        ++connects;
    }

    void on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id) override
    {
        close_term = leadership_term_id;
        close_session = cluster_session_id;
        ++closes;
    }

    void on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                               std::int64_t /*image_correlation_id*/) override
    {
        keep_alive_term = leadership_term_id;
        keep_alive_session = cluster_session_id;
        ++keep_alives;
    }

    void on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                               std::span<const std::uint8_t> /*encoded_credentials*/) override
    {
        challenge_correlation = correlation_id;
        challenge_session = cluster_session_id;
        ++challenges;
    }

    void on_admin_request(std::int64_t leadership_term_id, std::int64_t cluster_session_id, std::int64_t correlation_id,
                          std::int32_t request_type, std::span<const std::uint8_t> payload) override
    {
        admin_term = leadership_term_id;
        admin_session = cluster_session_id;
        admin_correlation = correlation_id;
        admin_request_type = request_type;
        admin_payload.assign(payload.begin(), payload.end());
        ++admins;
    }

    void on_extension_message(std::int32_t schema_id, std::int32_t template_id,
                              std::span<const std::uint8_t> /*message*/) override
    {
        extension_schema = schema_id;
        extension_template = template_id;
        ++extensions;
    }

    bool continue_polling{true};
    int messages{0};
    int connects{0};
    int closes{0};
    int keep_alives{0};
    int challenges{0};
    int admins{0};
    int extensions{0};
    std::int64_t message_term{0};
    std::int64_t message_session{0};
    std::vector<std::uint8_t> message{};
    std::int64_t connect_correlation{0};
    std::int32_t connect_stream{0};
    std::int32_t connect_version{0};
    std::string connect_channel{};
    std::vector<std::uint8_t> connect_credentials{};
    std::string connect_client_info{};
    std::int32_t connect_image_session{0};
    std::int64_t close_term{0};
    std::int64_t close_session{0};
    std::int64_t keep_alive_term{0};
    std::int64_t keep_alive_session{0};
    std::int64_t challenge_correlation{0};
    std::int64_t challenge_session{0};
    std::int64_t admin_term{0};
    std::int64_t admin_session{0};
    std::int64_t admin_correlation{0};
    std::int32_t admin_request_type{0};
    std::vector<std::uint8_t> admin_payload{};
    std::int32_t extension_schema{0};
    std::int32_t extension_template{0};
};

class IngressAdapterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_TRUE(driver_.ok());
        client_ = connect_with_retry(driver_.dir());
        ASSERT_TRUE(client_.has_value());

        auto add_publication = client_->add_publication("aeron:ipc", STREAM_ID);
        auto publication = await_handle(*client_, add_publication);
        ASSERT_TRUE(publication.has_value());
        publication_ = std::move(*publication);

        auto add_subscription = client_->add_subscription("aeron:ipc", STREAM_ID);
        auto subscription = await_handle(*client_, add_subscription);
        ASSERT_TRUE(subscription.has_value());

        adapter_ = std::make_unique<IngressAdapter>(handler_, 50);
        adapter_->connect(std::move(*subscription), {});
    }

    void TearDown() override
    {
        adapter_.reset();
        publication_.reset();
        client_.reset();
    }

    void publish(const std::vector<std::uint8_t>& message)
    {
        for (int i = 0; i < 500; ++i)
        {
            if (aeron_publication_offer(publication_.get(), message.data(), message.size(), nullptr, nullptr) > 0)
            {
                return;
            }
            (void)client_->do_work();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void pump(std::size_t expected)
    {
        for (int i = 0; i < 500; ++i)
        {
            (void)client_->do_work();
            auto polled = adapter_->poll();
            ASSERT_TRUE(polled) << polled.error().message.view();
            if (expected <= static_cast<std::size_t>(handler_.messages + handler_.connects + handler_.closes +
                                                     handler_.keep_alives + handler_.challenges + handler_.admins +
                                                     handler_.extensions))
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        FAIL() << "handler did not receive the message";
    }

    EmbeddedDriver driver_{};
    std::optional<DeepmsgClient> client_{};
    PubPtr publication_{};
    RecordingHandler handler_{};
    std::unique_ptr<IngressAdapter> adapter_{};
};

TEST_F(IngressAdapterTest, aSessionConnectRequestIsDecoded)
{
    const std::size_t length = codecs::MessageHeader::encodedLength() +
                               codecs::SessionConnectRequest::computeLength(std::string_view{"aeron:ipc"}.size(), 3, 5);
    std::vector<std::uint8_t> message(length);

    codecs::SessionConnectRequest encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
    encoder.correlationId(11).responseStreamId(8001).version(768);
    encoder.putResponseChannel(std::string_view{"aeron:ipc"});
    encoder.putEncodedCredentials(std::string_view{"abc"});
    encoder.putClientInfo(std::string_view{"probe"});

    publish(message);
    pump(1);

    EXPECT_EQ(1, handler_.connects);
    EXPECT_EQ(11, handler_.connect_correlation);
    EXPECT_EQ(8001, handler_.connect_stream);
    EXPECT_EQ(768, handler_.connect_version);
    EXPECT_EQ(std::string{"aeron:ipc"}, handler_.connect_channel);
    EXPECT_EQ(std::vector<std::uint8_t>({'a', 'b', 'c'}), handler_.connect_credentials);
    EXPECT_EQ(std::string{"probe"}, handler_.connect_client_info);
    aeron_publication_constants_t constants{};
    ASSERT_EQ(0, aeron_publication_constants(publication_.get(), &constants));
    EXPECT_EQ(constants.session_id, handler_.connect_image_session);
}

TEST_F(IngressAdapterTest, aSessionMessageIsDecodedWithItsPayload)
{
    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::SessionMessageHeader::sbeBlockLength() + 3;
    std::vector<std::uint8_t> message(length);

    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
    encoder.leadershipTermId(LEADERSHIP_TERM_ID).clusterSessionId(CLUSTER_SESSION_ID).timestamp(99);
    message[length - 3] = 7;
    message[length - 2] = 8;
    message[length - 1] = 9;

    publish(message);
    pump(1);

    EXPECT_EQ(1, handler_.messages);
    EXPECT_EQ(LEADERSHIP_TERM_ID, handler_.message_term);
    EXPECT_EQ(CLUSTER_SESSION_ID, handler_.message_session);
    EXPECT_EQ(std::vector<std::uint8_t>({7, 8, 9}), handler_.message);
}

TEST_F(IngressAdapterTest, aCloseRequestAndAKeepAliveAreDecoded)
{
    {
        const std::size_t length =
            codecs::MessageHeader::encodedLength() + codecs::SessionCloseRequest::sbeBlockLength();
        std::vector<std::uint8_t> message(length);
        codecs::SessionCloseRequest encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
        encoder.leadershipTermId(LEADERSHIP_TERM_ID).clusterSessionId(CLUSTER_SESSION_ID);
        publish(message);
    }
    pump(1);
    EXPECT_EQ(1, handler_.closes);
    EXPECT_EQ(LEADERSHIP_TERM_ID, handler_.close_term);
    EXPECT_EQ(CLUSTER_SESSION_ID, handler_.close_session);

    {
        const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::SessionKeepAlive::sbeBlockLength();
        std::vector<std::uint8_t> message(length);
        codecs::SessionKeepAlive encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
        encoder.leadershipTermId(LEADERSHIP_TERM_ID).clusterSessionId(CLUSTER_SESSION_ID);
        publish(message);
    }
    pump(2);
    EXPECT_EQ(1, handler_.keep_alives);
    EXPECT_EQ(CLUSTER_SESSION_ID, handler_.keep_alive_session);
}

TEST_F(IngressAdapterTest, aChallengeResponseAndAnAdminRequestAreDecoded)
{
    {
        const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::ChallengeResponse::computeLength(2);
        std::vector<std::uint8_t> message(length);
        codecs::ChallengeResponse encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
        encoder.correlationId(3).clusterSessionId(CLUSTER_SESSION_ID);
        encoder.putEncodedCredentials(std::string_view{"xy"});
        publish(message);
    }
    pump(1);
    EXPECT_EQ(1, handler_.challenges);
    EXPECT_EQ(3, handler_.challenge_correlation);
    EXPECT_EQ(CLUSTER_SESSION_ID, handler_.challenge_session);

    {
        const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::AdminRequest::computeLength(2);
        std::vector<std::uint8_t> message(length);
        codecs::AdminRequest encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
        encoder.leadershipTermId(LEADERSHIP_TERM_ID)
            .clusterSessionId(CLUSTER_SESSION_ID)
            .correlationId(4)
            .requestType(codecs::AdminRequestType::SNAPSHOT);
        encoder.putPayload(std::string_view{"ab"});
        publish(message);
    }
    pump(2);
    EXPECT_EQ(1, handler_.admins);
    EXPECT_EQ(4, handler_.admin_correlation);
    EXPECT_EQ(static_cast<std::int32_t>(codecs::AdminRequestType::SNAPSHOT), handler_.admin_request_type);
    EXPECT_EQ(std::vector<std::uint8_t>({'a', 'b'}), handler_.admin_payload);
}

TEST_F(IngressAdapterTest, aForeignSchemaGoesToTheExtensionPath)
{
    std::vector<std::uint8_t> message(codecs::MessageHeader::encodedLength());
    codecs::MessageHeader header;
    header.wrap(reinterpret_cast<char*>(message.data()), 0, codecs::MessageHeader::sbeSchemaVersion(), message.size());
    header.blockLength(0).templateId(9).schemaId(999).version(0);

    publish(message);
    pump(1);

    EXPECT_EQ(1, handler_.extensions);
    EXPECT_EQ(999, handler_.extension_schema);
    EXPECT_EQ(9, handler_.extension_template);
}

TEST_F(IngressAdapterTest, backPressureAbortsThePoll)
{
    handler_.continue_polling = false;

    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::SessionMessageHeader::sbeBlockLength() + 1;
    std::vector<std::uint8_t> message(length);
    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message.data()), 0, message.size());
    encoder.leadershipTermId(LEADERSHIP_TERM_ID).clusterSessionId(CLUSTER_SESSION_ID).timestamp(1);

    publish(message);
    pump(1);

    EXPECT_EQ(1, handler_.messages);
}

}  // namespace
