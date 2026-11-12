// C3a: the seven-state `AsyncConnect` against an in-process, scripted cluster on
// an embedded media driver. The script plays the ConsensusModule side: it
// subscribes to the ingress stream, reads the connect requests and answers on
// the egress stream, exactly as `AeronClusterAsyncConnectTest`'s mocked
// `Aeron` does one level down.

#include <gtest/gtest.h>

#include <aeronmd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "cluster/client/async_connect.h"
#include "cluster/client/client_config.h"
#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"
#include "io_aeron_cluster_codecs/SessionConnectRequest.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using axc::core::PubPtr;
using axc::core::SubPtr;
using deepmsg::cluster::client::AsyncConnect;
using deepmsg::cluster::client::ClientConfig;
using deepmsg::cluster::client::ClusterClient;

constexpr std::int32_t SESSION_ID = 21;
constexpr std::int64_t LEADERSHIP_TERM_ID = 3;

/// Embedded C media driver on a private temp directory (the same helper as
/// `test/unit/core/deepmsg_client_test.cpp`).
class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c3a-XXXXXX";
        if (nullptr == ::mkdtemp(pattern))
        {
            return;
        }
        dir_ = pattern;

        if (0 != aeron_driver_context_init(&context_))
        {
            return;
        }
        if (0 != aeron_driver_context_set_dir(context_, dir_.c_str()))
        {
            return;
        }
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

    EmbeddedDriver(const EmbeddedDriver&) = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

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

template <class Predicate>
bool pump_until(DeepmsgClient& client, Predicate predicate,
                std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        if (0 > client.do_work())
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

/// The scripted cluster: ingress subscription on 101, egress publication on 102.
class FakeCluster
{
public:
    enum class Reply
    {
        ok,
        error,
        auth_rejected,
        challenge_then_ok,
        stale_then_ok,
        silent
    };

    FakeCluster(const std::string& dir, Reply reply) : reply_(reply)
    {
        auto connected = connect_with_retry(dir);
        if (!connected.has_value())
        {
            return;
        }
        client_ = std::move(*connected);

        auto sub_add = client_.add_subscription("aeron:ipc", 101);
        auto pub_add = client_.add_publication("aeron:ipc", 102);

        std::optional<SubPtr> subscription;
        std::optional<PubPtr> publication;
        const bool ready = pump_until(client_,
                                      [&]
                                      {
                                          if (!subscription.has_value())
                                          {
                                              auto polled = sub_add.poll();
                                              if (!polled || polled->has_value())
                                              {
                                                  if (polled && polled->has_value())
                                                  {
                                                      subscription = std::move(**polled);
                                                  }
                                                  else
                                                  {
                                                      return true;
                                                  }
                                              }
                                          }
                                          if (!publication.has_value())
                                          {
                                              auto polled = pub_add.poll();
                                              if (!polled || polled->has_value())
                                              {
                                                  if (polled && polled->has_value())
                                                  {
                                                      publication = std::move(**polled);
                                                  }
                                                  else
                                                  {
                                                      return true;
                                                  }
                                              }
                                          }
                                          return subscription.has_value() && publication.has_value();
                                      });
        if (!ready)
        {
            return;
        }

        subscription_ = std::move(*subscription);
        publication_ = std::move(*publication);
        ready_ = true;
    }

    [[nodiscard]] bool ready() const noexcept
    {
        return ready_;
    }

    [[nodiscard]] const std::vector<std::vector<std::uint8_t>>& requests() const noexcept
    {
        return requests_;
    }

    /// Drive the fake's client, read the requests and answer them.
    void pump()
    {
        if (!ready_)
        {
            return;
        }

        (void)client_.do_work();
        (void)aeron_subscription_poll(subscription_.get(), &FakeCluster::on_request, &requests_, 10);

        for (; handled_ < requests_.size(); ++handled_)
        {
            handle(requests_[handled_]);
        }

        if (!pending_.empty() && offer(pending_) > 0)
        {
            pending_.clear();
            if (!follow_up_.empty())
            {
                pending_ = std::move(follow_up_);
                follow_up_.clear();
            }
        }
    }

private:
    static void on_request(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t*)
    {
        auto* requests = static_cast<std::vector<std::vector<std::uint8_t>>*>(clientd);
        requests->emplace_back(buffer, buffer + length);
    }

    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message)
    {
        return aeron_publication_offer(publication_.get(), message.data(), message.size(), nullptr, nullptr);
    }

    void handle(std::span<const std::uint8_t> request)
    {
        auto header = deepmsg::cluster::common::message_header(request);
        if (!header)
        {
            return;
        }

        if (header->templateId() == codecs::SessionConnectRequest::sbeTemplateId())
        {
            auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionConnectRequest>(request, *header);
            const std::int64_t correlation_id = decoder.correlationId();

            switch (reply_)
            {
                case Reply::ok:
                    pending_ = session_event(correlation_id, codecs::EventCode::OK, "");
                    break;
                case Reply::error:
                    pending_ = session_event(correlation_id, codecs::EventCode::ERROR, "synthetic error");
                    break;
                case Reply::auth_rejected:
                    pending_ = session_event(correlation_id, codecs::EventCode::AUTHENTICATION_REJECTED,
                                             "the credentials were rejected");
                    break;
                case Reply::challenge_then_ok:
                    if (!challenged_)
                    {
                        challenged_ = true;
                        pending_ = challenge(correlation_id);
                    }
                    else
                    {
                        pending_ = session_event(correlation_id, codecs::EventCode::OK, "");
                    }
                    break;
                case Reply::stale_then_ok:
                    // An event for another correlation first
                    // (`AeronCluster.java:2488-2490` compares it).
                    pending_ = session_event(correlation_id + 1000, codecs::EventCode::OK, "", SESSION_ID + 1);
                    follow_up_ = session_event(correlation_id, codecs::EventCode::OK, "");
                    break;
                case Reply::silent:
                    break;
            }
            return;
        }

        if (header->templateId() == codecs::ChallengeResponse::sbeTemplateId())
        {
            auto decoder = deepmsg::cluster::common::wrap_body<codecs::ChallengeResponse>(request, *header);
            pending_ = session_event(decoder.correlationId(), codecs::EventCode::OK, "");
        }
    }

    [[nodiscard]] static std::vector<std::uint8_t> session_event(std::int64_t correlation_id,
                                                                 codecs::EventCode::Value code, std::string_view detail,
                                                                 std::int32_t session_id = SESSION_ID)
    {
        const std::size_t length =
            deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::SessionEvent::computeLength(detail.size());
        std::vector<std::uint8_t> buffer(length);

        codecs::SessionEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.clusterSessionId(session_id)
            .correlationId(correlation_id)
            .leadershipTermId(LEADERSHIP_TERM_ID)
            .leaderMemberId(0)
            .code(code)
            .version(deepmsg::cluster::client::PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(10'000'000'000);
        encoder.putDetail(detail);
        return buffer;
    }

    [[nodiscard]] static std::vector<std::uint8_t> challenge(std::int64_t correlation_id)
    {
        const std::size_t length =
            deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::Challenge::computeLength(CHALLENGE_BYTES.size());
        std::vector<std::uint8_t> buffer(length);

        codecs::Challenge encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.correlationId(correlation_id).clusterSessionId(SESSION_ID);
        encoder.putEncodedChallenge(CHALLENGE_BYTES);
        return buffer;
    }

public:
    static constexpr std::string_view CHALLENGE_BYTES = "server-challenge";

private:
    Reply reply_;
    DeepmsgClient client_{};
    SubPtr subscription_{};
    PubPtr publication_{};
    std::vector<std::vector<std::uint8_t>> requests_{};
    std::vector<std::uint8_t> pending_{};
    std::vector<std::uint8_t> follow_up_{};
    std::size_t handled_{0};
    bool challenged_{false};
    bool ready_{false};
};

struct ConnectOutcome
{
    std::optional<ClusterClient> cluster{};
    std::optional<axc::core::Error> error{};
};

/// Poll the state machine and drive both clients until it concludes or fails.
ConnectOutcome drive(AsyncConnect& connect, DeepmsgClient& client, FakeCluster* fake,
                     std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    ConnectOutcome outcome;
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline)
    {
        auto polled = connect.poll(client);
        if (!polled)
        {
            outcome.error = polled.error();
            return outcome;
        }
        if (polled->has_value())
        {
            outcome.cluster = std::move(**polled);
            return outcome;
        }

        (void)client.do_work();
        if (nullptr != fake)
        {
            fake->pump();
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    return outcome;
}

[[nodiscard]] ClientConfig ipc_config()
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:ipc";
    return config;
}

TEST(AsyncConnectTest, anAsyncConnectStartsAtCreateEgressSubscription)
{
    ClientConfig config = ipc_config();
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());
    EXPECT_EQ(AsyncConnect::State::create_egress_subscription, connect->state());
    EXPECT_EQ(-1, connect->step());
    EXPECT_EQ("CREATE_EGRESS_SUBSCRIPTION", AsyncConnect::step_name(connect->step()));
    EXPECT_EQ("DONE", AsyncConnect::step_name(5));
    EXPECT_EQ("<unknown>", AsyncConnect::step_name(6));
    EXPECT_EQ("<unknown>", AsyncConnect::step_name(-2));
}

TEST(AsyncConnectTest, anUnconcludedContextIsRefused)
{
    ClientConfig config = ipc_config();
    const auto connect = AsyncConnect::create(config);
    ASSERT_FALSE(connect.has_value());
    EXPECT_EQ(axc::core::ErrorCode::config_error, connect.error().code);
}

TEST(AsyncConnectTest, aMalformedIngressEndpointIsRefused)
{
    // Endpoints are only legal with a non-IPC ingress channel
    // (`AeronCluster.java:1483-1490`).
    ClientConfig config;
    config.ingress_channel = "aeron:udp?endpoint=localhost:0";
    config.egress_channel = "aeron:ipc";
    config.ingress_endpoints = "0=localhost:20111,broken";
    ASSERT_TRUE(config.conclude().has_value());

    const auto connect = AsyncConnect::create(config);
    ASSERT_FALSE(connect.has_value());
    EXPECT_EQ(axc::core::ErrorCode::config_error, connect.error().code);
    EXPECT_NE(std::string::npos, std::string{connect.error().message.view()}.find("endpoint missing '=' separator"));
}

TEST(AsyncConnectTest, theSevenStepsReachALiveSessionAndTheRequestCarriesTheReferencesFields)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    FakeCluster fake(driver.dir(), FakeCluster::Reply::ok);
    ASSERT_TRUE(fake.ready());

    ClientConfig config = ipc_config();
    config.client_name = "probe";
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, &fake);
    ASSERT_FALSE(outcome.error.has_value())
        << (outcome.error.has_value() ? std::string{outcome.error->message.view()} : "");
    ASSERT_TRUE(outcome.cluster.has_value());

    ClusterClient& cluster = *outcome.cluster;
    EXPECT_EQ(SESSION_ID, cluster.cluster_session_id());
    EXPECT_EQ(LEADERSHIP_TERM_ID, cluster.leadership_term_id());
    EXPECT_EQ(0, cluster.leader_member_id());
    // `concludeConnect` (`AeronCluster.java:2572-2578`): 2 x heartbeat.
    EXPECT_EQ(20'000'000'000, cluster.new_leader_timeout_ns());
    EXPECT_NE(nullptr, cluster.egress_image());
    EXPECT_TRUE(cluster.ingress_publication().is_connected());
    EXPECT_FALSE(cluster.is_closed());

    ASSERT_FALSE(fake.requests().empty());
    const auto& request = fake.requests().front();
    auto header = deepmsg::cluster::common::message_header(request);
    ASSERT_TRUE(header.has_value());
    ASSERT_EQ(codecs::SessionConnectRequest::sbeTemplateId(), header->templateId());

    auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionConnectRequest>(request, *header);
    EXPECT_GT(decoder.correlationId(), 0);
    EXPECT_EQ(102, decoder.responseStreamId());
    EXPECT_EQ(deepmsg::cluster::client::PROTOCOL_SEMANTIC_VERSION, decoder.version());
    EXPECT_EQ(std::string{"aeron:ipc"}, decoder.getResponseChannelAsString());
    // SBE's generated decoders advance their position on the getters that
    // return data (`encodedCredentials`), not on `...Length()`.
    const std::uint32_t credentials_length = decoder.encodedCredentialsLength();
    (void)decoder.encodedCredentials();
    EXPECT_EQ(0U, credentials_length);
    EXPECT_EQ(std::string{"name=probe version=1.54.0 commit=454da1e4e6"}, decoder.getClientInfoAsString());

    cluster.close();
}

TEST(AsyncConnectTest, aSessionEventForAnotherCorrelationIsIgnored)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    FakeCluster fake(driver.dir(), FakeCluster::Reply::stale_then_ok);
    ASSERT_TRUE(fake.ready());

    ClientConfig config = ipc_config();
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, &fake);
    ASSERT_FALSE(outcome.error.has_value());
    ASSERT_TRUE(outcome.cluster.has_value());
    // The stale event carried session SESSION_ID + 1; taking it would be
    // observable here (`AeronCluster.java:2488-2490`).
    EXPECT_EQ(SESSION_ID, outcome.cluster->cluster_session_id());

    outcome.cluster->close();
}

TEST(AsyncConnectTest, anErrorEventFailsTheConnectWithTheClustersDetail)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    FakeCluster fake(driver.dir(), FakeCluster::Reply::error);
    ASSERT_TRUE(fake.ready());

    ClientConfig config = ipc_config();
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, &fake);
    ASSERT_FALSE(outcome.cluster.has_value());
    ASSERT_TRUE(outcome.error.has_value());
    EXPECT_EQ(axc::core::ErrorCode::protocol_error, outcome.error->code);
    EXPECT_EQ(std::string{"synthetic error"}, outcome.error->message.view());
}

TEST(AsyncConnectTest, anAuthenticationRejectedEventFailsTheConnect)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    FakeCluster fake(driver.dir(), FakeCluster::Reply::auth_rejected);
    ASSERT_TRUE(fake.ready());

    ClientConfig config = ipc_config();
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, &fake);
    ASSERT_TRUE(outcome.error.has_value());
    EXPECT_EQ(axc::core::ErrorCode::protocol_error, outcome.error->code);
    EXPECT_EQ(std::string{"the credentials were rejected"}, outcome.error->message.view());
}

TEST(AsyncConnectTest, aChallengeIsAnsweredWithTheConfiguredCredentials)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    FakeCluster fake(driver.dir(), FakeCluster::Reply::challenge_then_ok);
    ASSERT_TRUE(fake.ready());

    std::vector<std::uint8_t> challenge_seen;
    ClientConfig config = ipc_config();
    config.credentials.encoded = [] { return std::vector<std::uint8_t>{1, 2, 3}; };
    config.credentials.on_challenge = [&challenge_seen](std::span<const std::uint8_t> challenge)
    {
        challenge_seen.assign(challenge.begin(), challenge.end());
        return std::vector<std::uint8_t>{9, 9};
    };
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, &fake);
    ASSERT_FALSE(outcome.error.has_value());
    ASSERT_TRUE(outcome.cluster.has_value());
    EXPECT_EQ(std::string{FakeCluster::CHALLENGE_BYTES}, std::string(challenge_seen.begin(), challenge_seen.end()));

    // The first request carried the configured credentials; the second is the
    // challenge response (`AeronCluster.java:2528-2541`).
    ASSERT_GE(fake.requests().size(), 2U);
    const auto& response = fake.requests()[1];
    auto header = deepmsg::cluster::common::message_header(response);
    ASSERT_TRUE(header.has_value());
    ASSERT_EQ(codecs::ChallengeResponse::sbeTemplateId(), header->templateId());

    auto decoder = deepmsg::cluster::common::wrap_body<codecs::ChallengeResponse>(response, *header);
    EXPECT_EQ(SESSION_ID, decoder.clusterSessionId());
    ASSERT_EQ(2U, decoder.encodedCredentialsLength());
    const char* credentials = decoder.encodedCredentials();
    EXPECT_EQ(9, static_cast<std::uint8_t>(credentials[0]));
    EXPECT_EQ(9, static_cast<std::uint8_t>(credentials[1]));

    outcome.cluster->close();
}

TEST(AsyncConnectTest, aConnectWithNobodyAnsweringTimesOutWithTheReferencesDetail)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    DeepmsgClient client = std::move(*connected);

    ClientConfig config = ipc_config();
    config.message_timeout_ns = 200'000'000;
    ASSERT_TRUE(config.conclude().has_value());

    auto connect = AsyncConnect::create(config);
    ASSERT_TRUE(connect.has_value());

    ConnectOutcome outcome = drive(*connect, client, nullptr, std::chrono::seconds(5));
    ASSERT_FALSE(outcome.cluster.has_value());
    ASSERT_TRUE(outcome.error.has_value());
    EXPECT_EQ(axc::core::ErrorCode::timed_out, outcome.error->code);

    const std::string message{outcome.error->message.view()};
    EXPECT_NE(std::string::npos, message.find("cluster connect timeout:"));
    EXPECT_NE(std::string::npos, message.find("state="));
    EXPECT_NE(std::string::npos, message.find("messageTimeout=200000000ns"));
    EXPECT_NE(std::string::npos, message.find("ingressChannel=aeron:ipc"));
    EXPECT_NE(std::string::npos, message.find("egress.isConnected="));
}

}  // namespace
