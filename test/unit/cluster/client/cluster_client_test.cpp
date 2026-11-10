// C3b: the connected session (`AeronCluster.java:95-1200`) against a scripted
// cluster on an embedded media driver. The script echoes session messages,
// records keep-alives and close requests, and can emit SessionEvent and
// NewLeaderEvent frames.

#include <gtest/gtest.h>

#include <aeronmd.h>

#include <algorithm>
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
#include "cluster/client/cluster_client.h"
#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionCloseRequest.h"
#include "io_aeron_cluster_codecs/SessionConnectRequest.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs/SessionKeepAlive.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using axc::core::PubPtr;
using axc::core::SubPtr;
using deepmsg::cluster::client::ClientConfig;
using deepmsg::cluster::client::ClusterClient;
using deepmsg::cluster::client::EgressListener;

constexpr std::int32_t SESSION_ID = 21;
constexpr std::int64_t TERM_ID = 3;

class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c3b-XXXXXX";
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
    FakeCluster(const std::string& dir)
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
                                              if (polled && polled->has_value())
                                              {
                                                  subscription = std::move(**polled);
                                              }
                                          }
                                          if (!publication.has_value())
                                          {
                                              auto polled = pub_add.poll();
                                              if (polled && polled->has_value())
                                              {
                                                  publication = std::move(**polled);
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

    [[nodiscard]] int keep_alives() const noexcept
    {
        return keep_alives_;
    }

    [[nodiscard]] int close_requests() const noexcept
    {
        return close_requests_;
    }

    [[nodiscard]] const std::vector<std::vector<std::uint8_t>>& payloads() const noexcept
    {
        return payloads_;
    }

    [[nodiscard]] std::int64_t last_session_id() const noexcept
    {
        return last_session_id_;
    }

    [[nodiscard]] std::int64_t last_term_id() const noexcept
    {
        return last_term_id_;
    }

    /// Drops the ingress subscription: the client's ingress publication turns
    /// NOT_CONNECTED, which is the reference's
    /// `makeIngressPublicationReturn(Publication.NOT_CONNECTED)`.
    void close_ingress_subscription()
    {
        subscription_.reset();
        (void)client_.do_work();
    }

    [[nodiscard]] bool reopen_ingress_subscription()
    {
        auto add = client_.add_subscription("aeron:ipc", 101);
        std::optional<SubPtr> subscription;
        const bool ready = pump_until(client_,
                                      [&]
                                      {
                                          if (!subscription.has_value())
                                          {
                                              auto polled = add.poll();
                                              if (polled && polled->has_value())
                                              {
                                                  subscription = std::move(**polled);
                                              }
                                          }
                                          return subscription.has_value();
                                      });
        if (ready)
        {
            subscription_ = std::move(*subscription);
        }
        return ready;
    }

    /// Drops the egress publication: the client's egress image is closed,
    /// which is the reference's `when(egressImage.isClosed()).thenReturn(true)`.
    void close_egress_publication()
    {
        publication_.reset();
        (void)client_.do_work();
    }

    [[nodiscard]] bool reopen_egress_publication()
    {
        auto add = client_.add_publication("aeron:ipc", 102);
        std::optional<PubPtr> publication;
        const bool ready = pump_until(client_,
                                      [&]
                                      {
                                          if (!publication.has_value())
                                          {
                                              auto polled = add.poll();
                                              if (polled && polled->has_value())
                                              {
                                                  publication = std::move(**polled);
                                              }
                                          }
                                          return publication.has_value();
                                      });
        if (ready)
        {
            publication_ = std::move(*publication);
        }
        return ready;
    }

    void pump()
    {
        if (!ready_)
        {
            return;
        }

        (void)client_.do_work();
        // Either half can be dropped by a failover test; the other still has
        // to work (an egress frame must reach the client with ingress gone).
        if (nullptr != subscription_)
        {
            (void)aeron_subscription_poll(subscription_.get(), &FakeCluster::on_request, this, 10);
        }
        if (nullptr == publication_)
        {
            return;
        }

        while (!pending_.empty())
        {
            if (offer(pending_.front()) <= 0)
            {
                break;
            }
            pending_.erase(pending_.begin());
        }
    }

    /// A `SessionEvent` for the connected session.
    void send_session_event(codecs::EventCode::Value code, std::string_view detail)
    {
        pending_.push_back(session_event(code, detail));
    }

    /// A `NewLeaderEvent` for the connected session.
    void send_new_leader_event(std::int64_t session_id, std::int64_t term_id, std::int32_t member_id,
                               std::string_view endpoints)
    {
        const std::size_t length =
            deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::NewLeaderEvent::computeLength(endpoints.size());
        std::vector<std::uint8_t> buffer(length);

        codecs::NewLeaderEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.clusterSessionId(session_id).leadershipTermId(term_id).leaderMemberId(member_id);
        encoder.putIngressEndpoints(endpoints);
        pending_.push_back(std::move(buffer));
    }

private:
    static void on_request(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t*)
    {
        auto* fake = static_cast<FakeCluster*>(clientd);
        const std::span<const std::uint8_t> message(buffer, length);

        auto header = deepmsg::cluster::common::message_header(message);
        if (!header)
        {
            return;
        }

        if (header->templateId() == codecs::SessionConnectRequest::sbeTemplateId())
        {
            auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionConnectRequest>(message, *header);
            fake->pending_.push_back(fake->session_ok(decoder.correlationId()));
            return;
        }

        if (header->templateId() == codecs::SessionKeepAlive::sbeTemplateId())
        {
            ++fake->keep_alives_;
            return;
        }

        if (header->templateId() == codecs::SessionCloseRequest::sbeTemplateId())
        {
            ++fake->close_requests_;
            return;
        }

        if (header->templateId() == codecs::SessionMessageHeader::sbeTemplateId())
        {
            auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionMessageHeader>(message, *header);
            fake->last_session_id_ = decoder.clusterSessionId();
            fake->last_term_id_ = decoder.leadershipTermId();
            const auto payload = message.subspan(deepmsg::cluster::client::SESSION_HEADER_LENGTH);
            fake->payloads_.emplace_back(payload.begin(), payload.end());

            // Echo: the same 32-byte header plus the payload, queued for the
            // next flush.
            fake->pending_.emplace_back(message.begin(), message.end());
        }
    }

    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message)
    {
        return aeron_publication_offer(publication_.get(), message.data(), message.size(), nullptr, nullptr);
    }

    [[nodiscard]] std::vector<std::uint8_t> session_ok(std::int64_t correlation_id) const
    {
        const std::size_t length =
            deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::SessionEvent::computeLength(0);
        std::vector<std::uint8_t> buffer(length);

        codecs::SessionEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.clusterSessionId(SESSION_ID)
            .correlationId(correlation_id)
            .leadershipTermId(TERM_ID)
            .leaderMemberId(0)
            .code(codecs::EventCode::OK)
            .version(deepmsg::cluster::client::PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(10'000'000'000);
        encoder.putDetail(std::string_view{});
        return buffer;
    }

    [[nodiscard]] static std::vector<std::uint8_t> session_event(codecs::EventCode::Value code, std::string_view detail)
    {
        const std::size_t length =
            deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + codecs::SessionEvent::computeLength(detail.size());
        std::vector<std::uint8_t> buffer(length);

        codecs::SessionEvent encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
        encoder.clusterSessionId(SESSION_ID)
            .correlationId(0)
            .leadershipTermId(TERM_ID)
            .leaderMemberId(0)
            .code(code)
            .version(deepmsg::cluster::client::PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(10'000'000'000);
        encoder.putDetail(detail);
        return buffer;
    }

    DeepmsgClient client_{};
    SubPtr subscription_{};
    PubPtr publication_{};
    std::vector<std::vector<std::uint8_t>> pending_{};
    std::vector<std::vector<std::uint8_t>> payloads_{};
    std::int64_t last_session_id_{-1};
    std::int64_t last_term_id_{-1};
    int keep_alives_{0};
    int close_requests_{0};
    bool ready_{false};
};

/// A settable clock for the AWAIT deadlines.
struct TestClock
{
    std::int64_t now_ns{1'000'000'000};
};

struct Session
{
    // The session holds a pointer to this client: it must be heap-allocated
    // (or otherwise stable) for the session's whole life.
    std::unique_ptr<DeepmsgClient> client;
    std::optional<ClusterClient> cluster;
    std::optional<axc::core::Error> last_error;
};

std::optional<Session> connect_session(const std::string& dir, ClientConfig config, FakeCluster& fake)
{
    auto connected = connect_with_retry(dir);
    if (!connected.has_value())
    {
        return std::nullopt;
    }

    if (!config.conclude().has_value())
    {
        return std::nullopt;
    }
    auto async = deepmsg::cluster::client::AsyncConnect::create(config);
    if (!async.has_value())
    {
        return std::nullopt;
    }

    Session session;
    session.client = std::make_unique<DeepmsgClient>(std::move(*connected));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline && !session.cluster.has_value())
    {
        auto polled = async->poll(*session.client);
        if (!polled || polled->has_value())
        {
            if (polled && polled->has_value())
            {
                session.cluster = std::move(**polled);
            }
            break;
        }
        (void)session.client->do_work();
        fake.pump();
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    if (!session.cluster.has_value())
    {
        return std::nullopt;
    }

    return session;
}

/// Draining both sides until `predicate`.
template <class Predicate>
bool pump_session(Session& session, FakeCluster& fake, Predicate predicate,
                  std::chrono::milliseconds timeout = std::chrono::seconds(5))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        (void)session.client->do_work();
        fake.pump();
        auto polled = session.cluster->poll_egress();
        if (!polled)
        {
            session.last_error = polled.error();
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

struct RecordingListener final : EgressListener
{
    struct Message
    {
        std::int64_t session_id;
        std::int64_t timestamp;
        std::string payload;
    };

    struct NewLeader
    {
        std::int64_t session_id;
        std::int64_t term_id;
        std::int32_t member_id;
        std::string endpoints;
    };

    std::vector<Message> messages{};
    std::vector<codecs::EventCode::Value> events{};
    std::vector<NewLeader> new_leaders{};

    void on_message(std::int64_t cluster_session_id, std::int64_t timestamp,
                    std::span<const std::uint8_t> payload) override
    {
        messages.push_back(Message{cluster_session_id, timestamp, std::string(payload.begin(), payload.end())});
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, codecs::EventCode::Value code,
                          std::string_view) override
    {
        events.push_back(code);
    }

    void on_new_leader(std::int64_t cluster_session_id, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                       std::string_view ingress_endpoints) override
    {
        new_leaders.push_back(
            NewLeader{cluster_session_id, leadership_term_id, leader_member_id, std::string{ingress_endpoints}});
    }

    void on_admin_response(std::int64_t, std::int64_t, codecs::AdminRequestType::Value,
                           codecs::AdminResponseCode::Value, std::string_view, std::span<const std::uint8_t>) override
    {
    }
};

[[nodiscard]] ClientConfig ipc_config()
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:ipc";
    return config;
}

TEST(ClusterClientTest, offerWritesTheSessionHeaderAndThePayload)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(SESSION_ID, session->cluster->cluster_session_id());

    const std::string payload{"hello-cluster"};
    const std::int64_t position = session->cluster->offer(
        std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size()));
    EXPECT_GT(position, 0);

    ASSERT_TRUE(pump_session(*session, fake, [&] { return !fake.payloads().empty(); }));
    EXPECT_EQ(SESSION_ID, fake.last_session_id());
    EXPECT_EQ(TERM_ID, fake.last_term_id());
    ASSERT_EQ(1U, fake.payloads().size());
    EXPECT_EQ(payload, std::string(fake.payloads()[0].begin(), fake.payloads()[0].end()));
}

TEST(ClusterClientTest, tryClaimFillsThePayloadAfterTheHeader)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    auto claim = session->cluster->try_claim(4);
    ASSERT_TRUE(claim.has_value());
    claim->payload()[0] = 'a';
    claim->payload()[1] = 'b';
    claim->payload()[2] = 'c';
    claim->payload()[3] = 'd';
    EXPECT_GT(claim->commit(), 0);

    ASSERT_TRUE(pump_session(*session, fake, [&] { return !fake.payloads().empty(); }));
    EXPECT_EQ(std::string{"abcd"}, std::string(fake.payloads()[0].begin(), fake.payloads()[0].end()));
}

TEST(ClusterClientTest, keepAliveCarriesTheTermAndSession)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    auto sent = session->cluster->send_keep_alive();
    ASSERT_TRUE(sent.has_value());
    EXPECT_TRUE(*sent);

    ASSERT_TRUE(pump_session(*session, fake, [&] { return fake.keep_alives() == 1; }));
}

TEST(ClusterClientTest, aKeepAliveBetweenTwoOffersLeavesBothOnTheWire)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    const std::string first{"first"};
    const std::string second{"second"};
    EXPECT_GT(session->cluster->offer(
                  std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(first.data()), first.size())),
              0);
    auto sent = session->cluster->send_keep_alive();
    ASSERT_TRUE(sent.has_value());
    EXPECT_TRUE(*sent);
    EXPECT_GT(session->cluster->offer(
                  std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(second.data()), second.size())),
              0);

    ASSERT_TRUE(pump_session(*session, fake, [&] { return fake.payloads().size() == 2 && fake.keep_alives() == 1; }));
    EXPECT_EQ("first", std::string(fake.payloads()[0].begin(), fake.payloads()[0].end()));
    EXPECT_EQ("second", std::string(fake.payloads()[1].begin(), fake.payloads()[1].end()));
}

TEST(ClusterClientTest, closeSendsSessionCloseRequestWhenConnected)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    session->cluster->close();
    EXPECT_TRUE(session->cluster->is_closed());

    ASSERT_TRUE(pump_session(*session, fake, [&] { return fake.close_requests() == 1; }));
}

TEST(ClusterClientTest, pollEgressDispatchesSessionMessagesToTheListener)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    RecordingListener listener;
    session->cluster->set_egress_listener(&listener);

    const std::string payload{"round-trip"};
    EXPECT_GT(session->cluster->offer(
                  std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size())),
              0);

    ASSERT_TRUE(pump_session(*session, fake, [&] { return !listener.messages.empty(); }));
    ASSERT_EQ(1U, listener.messages.size());
    EXPECT_EQ(SESSION_ID, listener.messages[0].session_id);
    EXPECT_EQ(payload, listener.messages[0].payload);
}

TEST(ClusterClientTest, aClosedSessionEventLeadsToPendingCloseThenClosed)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    RecordingListener listener;
    session->cluster->set_egress_listener(&listener);

    fake.send_session_event(codecs::EventCode::CLOSED, "cluster says bye");
    ASSERT_TRUE(pump_session(*session, fake, [&] { return !listener.events.empty(); }));
    EXPECT_EQ(codecs::EventCode::CLOSED, listener.events[0]);
    EXPECT_TRUE(session->cluster->is_closed());
}

TEST(ClusterClientTest, aNewLeaderEventMovesToAwaitConnectionAndAnOfferCompletesIt)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(TERM_ID, session->cluster->leadership_term_id());

    fake.send_new_leader_event(SESSION_ID, TERM_ID + 1, 0, "");
    ASSERT_TRUE(pump_session(*session, fake, [&] { return session->cluster->leadership_term_id() == TERM_ID + 1; }));
    EXPECT_EQ(ClusterClient::State::await_new_leader_connection, session->cluster->state());

    // The same channel is used again; the first successful offer promotes it
    // back to CONNECTED (`AeronCluster.java:727-733`).
    const std::uint8_t byte = 'x';
    bool connected = false;
    std::int64_t last_result = 0;
    for (int attempt = 0; attempt < 3000 && !connected; ++attempt)
    {
        last_result = session->cluster->offer(std::span<const std::uint8_t>(&byte, 1));
        if (last_result > 0)
        {
            connected = true;
        }
        (void)session->client->do_work();
        fake.pump();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(connected) << "state=" << static_cast<int>(session->cluster->state()) << " last_offer=" << last_result
                           << " error="
                           << (session->last_error.has_value()
                                   ? std::string{session->last_error->message.view()} +
                                         " errcode=" + std::to_string(session->last_error->errcode)
                                   : std::string{"none"});
    EXPECT_EQ(ClusterClient::State::connected, session->cluster->state());
}

TEST(ClusterClientTest, aDisconnectedSessionClosesAfterTheNewLeaderTimeout)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    ClientConfig config = ipc_config();
    TestClock clock;
    config.clock = [&clock] { return clock.now_ns; };
    auto session = connect_session(driver.dir(), config, fake);
    ASSERT_TRUE(session.has_value());

    // Destroying the fake's publication makes the ingress publication
    // NOT_CONNECTED on the next offer (`AeronCluster.java:717-720`).
    // There is no way to drop it through the API, so simulate the result path.
    session->cluster->track_ingress_publication_result(AERON_PUBLICATION_NOT_CONNECTED);
    EXPECT_EQ(ClusterClient::State::await_new_leader, session->cluster->state());

    clock.now_ns += config.new_leader_timeout_ns.value_or(20'000'000'000) - 1;
    EXPECT_EQ(0, session->cluster->poll_state_changes());
    clock.now_ns += 1;
    EXPECT_EQ(1, session->cluster->poll_state_changes());
    EXPECT_TRUE(session->cluster->is_closed());
}

// ---------------------------------------------------------------------------
// C3c / XC-1: the `AeronClusterTest` cases, ported by name
// (`aeron-cluster/src/test/java/io/aeron/cluster/client/AeronClusterTest.java`).
//
// The reference mocks `Aeron`, `ExclusivePublication` and `Image`; here the
// driver is real and the scripted cluster supplies the same three states:
// a dropped ingress subscription (publication NOT_CONNECTED), a dropped egress
// publication (image closed) and `NewLeaderEvent` frames. The clock is
// injected through `ClientConfig::clock`, the seam the reference uses too
// (`ctx.nanoClock()`).
// ---------------------------------------------------------------------------

/// Pump both sides until `poll_egress()` does work, and return that call's
/// count -- the reference's `pollEgress()` value.
[[nodiscard]] std::optional<int> poll_first_work(Session& session, FakeCluster& fake,
                                                 std::chrono::milliseconds timeout = std::chrono::seconds(5))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        (void)session.client->do_work();
        fake.pump();
        auto polled = session.cluster->poll_egress();
        if (!polled)
        {
            session.last_error = polled.error();
            return std::nullopt;
        }
        if (*polled > 0)
        {
            return *polled;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return std::nullopt;
}

[[nodiscard]] bool wait_for_ingress_connection(Session& session, bool connected,
                                               std::chrono::milliseconds timeout = std::chrono::seconds(5))
{
    return pump_until(
        *session.client, [&] { return session.cluster->ingress_publication().is_connected() == connected; }, timeout);
}

[[nodiscard]] ClientConfig failover_config(TestClock& clock)
{
    ClientConfig config = ipc_config();
    config.clock = [&clock] { return clock.now_ns; };
    // The reference's `newLeaderTimeoutNs(1s)`; the message timeout stays the
    // 5 s default (`AeronCluster.java:1240-1241`).
    config.new_leader_timeout_ns = 1'000'000'000;
    return config;
}

TEST(ClusterClientTest, shouldStayConnectedAfterSuccessfulFailover)
{
    for (const bool with_ingress_disconnect : {false, true})
    {
        for (const bool with_app_messages : {false, true})
        {
            SCOPED_TRACE(std::string{"withIngressDisconnect="} + (with_ingress_disconnect ? "true" : "false") +
                         " withAppMessages=" + (with_app_messages ? "true" : "false"));

            EmbeddedDriver driver;
            ASSERT_TRUE(driver.ok());
            FakeCluster fake(driver.dir());
            ASSERT_TRUE(fake.ready());

            TestClock clock;
            const ClientConfig config = failover_config(clock);
            auto session = connect_session(driver.dir(), config, fake);
            ASSERT_TRUE(session.has_value());
            RecordingListener listener;
            session->cluster->set_egress_listener(&listener);

            if (with_ingress_disconnect)
            {
                fake.close_ingress_subscription();
                ASSERT_TRUE(wait_for_ingress_connection(*session, false));
            }

            const std::uint8_t payload[] = {'a', 'p', 'p'};
            if (with_app_messages)
            {
                const std::int64_t result = session->cluster->offer(payload);
                if (with_ingress_disconnect)
                {
                    EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, result);
                }
                else
                {
                    EXPECT_GT(result, 0);
                    // A real cluster echoes the message; the reference's mock
                    // does not. Drain it so the next egress poll counts only
                    // the NewLeaderEvent.
                    ASSERT_TRUE(pump_session(*session, fake, [&] { return !listener.messages.empty(); }));
                    listener.messages.clear();
                }
            }
            else
            {
                auto sent = session->cluster->send_keep_alive();
                ASSERT_TRUE(sent.has_value());
                EXPECT_EQ(!with_ingress_disconnect, *sent);
            }

            clock.now_ns += config.new_leader_timeout_ns.value() - 1;

            fake.send_new_leader_event(SESSION_ID, TERM_ID + 1, 1, "0=localhost:20000");
            const auto polled = poll_first_work(*session, fake);
            ASSERT_TRUE(polled.has_value())
                << (session->last_error.has_value() ? std::string{session->last_error->message.view()}
                                                    : std::string{"no work polled"});
            EXPECT_EQ(1, *polled);
            ASSERT_EQ(1U, listener.new_leaders.size());
            EXPECT_EQ(SESSION_ID, listener.new_leaders[0].session_id);
            EXPECT_EQ(TERM_ID + 1, listener.new_leaders[0].term_id);
            EXPECT_EQ(1, listener.new_leaders[0].member_id);
            EXPECT_EQ("0=localhost:20000", listener.new_leaders[0].endpoints);
            EXPECT_EQ(ClusterClient::State::await_new_leader_connection, session->cluster->state());
            EXPECT_EQ(0, session->cluster->poll_egress().value());

            clock.now_ns += config.message_timeout_ns - 1;

            if (with_ingress_disconnect)
            {
                ASSERT_TRUE(fake.reopen_ingress_subscription());
            }
            ASSERT_TRUE(wait_for_ingress_connection(*session, true));

            if (with_app_messages)
            {
                EXPECT_GT(session->cluster->offer(payload), 0);
            }
            else
            {
                auto sent = session->cluster->send_keep_alive();
                ASSERT_TRUE(sent.has_value());
                EXPECT_TRUE(*sent);
            }
            EXPECT_EQ(ClusterClient::State::connected, session->cluster->state());

            clock.now_ns += 1;

            EXPECT_EQ(0, session->cluster->poll_egress().value());
            EXPECT_FALSE(session->cluster->is_closed());
        }
    }
}

TEST(ClusterClientTest, shouldCloseItselfWhenDisconnectedForLongerThanNewLeaderTimeout)
{
    for (const bool with_app_messages : {false, true})
    {
        SCOPED_TRACE(std::string{"withAppMessages="} + (with_app_messages ? "true" : "false"));

        EmbeddedDriver driver;
        ASSERT_TRUE(driver.ok());
        FakeCluster fake(driver.dir());
        ASSERT_TRUE(fake.ready());

        TestClock clock;
        const ClientConfig config = failover_config(clock);
        auto session = connect_session(driver.dir(), config, fake);
        ASSERT_TRUE(session.has_value());

        fake.close_ingress_subscription();
        ASSERT_TRUE(wait_for_ingress_connection(*session, false));

        const std::uint8_t payload[] = {'a', 'p', 'p'};
        if (with_app_messages)
        {
            EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, session->cluster->offer(payload));
        }
        else
        {
            auto sent = session->cluster->send_keep_alive();
            ASSERT_TRUE(sent.has_value());
            EXPECT_FALSE(*sent);
        }
        EXPECT_EQ(ClusterClient::State::await_new_leader, session->cluster->state());

        clock.now_ns += config.new_leader_timeout_ns.value() - 1;

        EXPECT_EQ(0, session->cluster->poll_egress().value());
        EXPECT_FALSE(session->cluster->is_closed());

        clock.now_ns += 1;

        EXPECT_EQ(1, session->cluster->poll_egress().value());
        EXPECT_TRUE(session->cluster->is_closed());
    }
}

TEST(ClusterClientTest, shouldCloseItselfWhenUnableToSendMessageForLongerThanNewLeaderConnectionTimeout)
{
    for (const bool with_app_messages : {false, true})
    {
        SCOPED_TRACE(std::string{"withAppMessages="} + (with_app_messages ? "true" : "false"));

        EmbeddedDriver driver;
        ASSERT_TRUE(driver.ok());
        FakeCluster fake(driver.dir());
        ASSERT_TRUE(fake.ready());

        TestClock clock;
        const ClientConfig config = failover_config(clock);
        auto session = connect_session(driver.dir(), config, fake);
        ASSERT_TRUE(session.has_value());

        fake.close_ingress_subscription();
        ASSERT_TRUE(wait_for_ingress_connection(*session, false));

        const std::uint8_t payload[] = {'a', 'p', 'p'};
        if (with_app_messages)
        {
            EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, session->cluster->offer(payload));
        }
        else
        {
            auto sent = session->cluster->send_keep_alive();
            ASSERT_TRUE(sent.has_value());
            EXPECT_FALSE(*sent);
        }

        clock.now_ns += config.new_leader_timeout_ns.value() / 2;

        fake.send_new_leader_event(SESSION_ID, TERM_ID + 1, 1, "0=localhost:20000");
        const auto polled = poll_first_work(*session, fake);
        ASSERT_TRUE(polled.has_value());
        EXPECT_EQ(1, *polled);
        EXPECT_FALSE(session->cluster->is_closed());
        EXPECT_EQ(ClusterClient::State::await_new_leader_connection, session->cluster->state());

        clock.now_ns += config.message_timeout_ns - 1;
        if (with_app_messages)
        {
            EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, session->cluster->offer(payload));
        }
        else
        {
            auto sent = session->cluster->send_keep_alive();
            ASSERT_TRUE(sent.has_value());
            EXPECT_FALSE(*sent);
        }

        clock.now_ns += 1;

        EXPECT_EQ(1, session->cluster->poll_egress().value());
        EXPECT_TRUE(session->cluster->is_closed());
    }
}

TEST(ClusterClientTest, shouldCloseIngressPublicationWhenEgressImageCloses)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    TestClock clock;
    const ClientConfig config = failover_config(clock);
    auto session = connect_session(driver.dir(), config, fake);
    ASSERT_TRUE(session.has_value());
    RecordingListener listener;
    session->cluster->set_egress_listener(&listener);

    // in CONNECTED state
    fake.close_egress_publication();
    ASSERT_TRUE(pump_until(*session->client,
                           [&]
                           {
                               aeron_image_t* image = session->cluster->egress_image();
                               return nullptr != image && aeron_image_is_closed(image);
                           }));
    EXPECT_EQ(1, session->cluster->poll_egress().value());
    EXPECT_FALSE(session->cluster->ingress_publication().valid());
    EXPECT_EQ(ClusterClient::State::await_new_leader, session->cluster->state());

    // and the new leader replaces the publication, so a second close is a
    // second `onDisconnected` (`AeronCluster.java:933` sets the new image).
    ASSERT_TRUE(fake.reopen_egress_publication());
    fake.send_new_leader_event(SESSION_ID, TERM_ID + 1, 1, "0=localhost:20000");
    const auto polled = poll_first_work(*session, fake);
    ASSERT_TRUE(polled.has_value());
    EXPECT_EQ(1, *polled);
    EXPECT_EQ(ClusterClient::State::await_new_leader_connection, session->cluster->state());
    ASSERT_EQ(1U, listener.new_leaders.size());

    // and in AWAIT_NEW_LEADER_CONNECTION state too
    fake.close_egress_publication();
    ASSERT_TRUE(pump_until(*session->client,
                           [&]
                           {
                               aeron_image_t* image = session->cluster->egress_image();
                               return nullptr != image && aeron_image_is_closed(image);
                           }));
    EXPECT_EQ(1, session->cluster->poll_egress().value());
    EXPECT_FALSE(session->cluster->ingress_publication().valid());
    EXPECT_EQ(ClusterClient::State::await_new_leader, session->cluster->state());
}

TEST(ClusterClientTest, shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());
    FakeCluster fake(driver.dir());
    ASSERT_TRUE(fake.ready());

    auto session = connect_session(driver.dir(), ipc_config(), fake);
    ASSERT_TRUE(session.has_value());

    // The reference makes `tryClaim`/`offer` return MAX_POSITION_EXCEEDED
    // (`AeronCluster.java:713-734`); the C publication only reaches it after
    // 2^31 terms, so the same result goes in through the public tracking seam
    // `trackIngressPublicationResult`.
    session->cluster->track_ingress_publication_result(AERON_PUBLICATION_MAX_POSITION_EXCEEDED);

    EXPECT_FALSE(session->cluster->ingress_publication().valid());
    EXPECT_EQ(1, session->cluster->poll_state_changes());
    EXPECT_TRUE(session->cluster->is_closed());
}

}  // namespace
