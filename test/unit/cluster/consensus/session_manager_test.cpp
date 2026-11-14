// C4b: `SessionManager` + `ClusterSession` on an in-process media driver.
// Covers the card's criteria that do not need the service side:
// connect/authenticate/open with the Open event on the wire, challenge,
// redirect, the two rejection paths, keep-alive, timeout, and ingress -> log
// (one append, ABORT on back-pressure).

#include <gtest/gtest.h>

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/consensus/session_manager.h"
#include "core/deepmsg/deepmsg_client.h"
#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/EventCode.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using axc::core::SubPtr;
using deepmsg::cluster::consensus::Authenticator;
using deepmsg::cluster::consensus::ClusterRole;
using deepmsg::cluster::consensus::ClusterSession;
using deepmsg::cluster::consensus::LogAppender;
using deepmsg::cluster::consensus::SessionManager;
using deepmsg::cluster::consensus::SessionManagerConfig;
using deepmsg::cluster::consensus::SessionProxy;
using deepmsg::cluster::consensus::SessionState;

constexpr std::int32_t CLUSTER_ID = 4;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t RESPONSE_STREAM_ID = 8001;
constexpr std::int64_t LEADERSHIP_TERM_ID = 7;
constexpr const char* RESPONSE_CHANNEL = "aeron:ipc";
constexpr const char* INGRESS_ENDPOINTS = "localhost:9000";

class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c4b-XXXXXX";
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

/// Records what `SessionManager` asked the log to append.
class FakeLogAppender final : public LogAppender
{
public:
    long append_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id, std::int64_t /*timestamp*/,
                        const std::uint8_t* payload, std::size_t payload_length) override
    {
        last_team = leadership_term_id;
        last_session = cluster_session_id;
        payloads.emplace_back(payload, payload + payload_length);
        position_ += static_cast<std::int64_t>(payload_length) + 32;
        return back_pressured ? AERON_PUBLICATION_BACK_PRESSURED : position_;
    }

    long append_session_open(const ClusterSession& session, std::int64_t /*leadership_term_id*/,
                             std::int64_t /*timestamp*/) override
    {
        opened_ids.push_back(session.id());
        position_ += 64;
        return position_;
    }

    bool append_session_close(ClusterSession& /*session*/, std::int64_t /*leadership_term_id*/,
                              std::int64_t /*timestamp*/) override
    {
        ++closed_count;
        position_ += 32;
        return true;
    }

    std::int64_t position() const override
    {
        return position_;
    }

    std::vector<std::vector<std::uint8_t>> payloads{};
    std::vector<std::int64_t> opened_ids{};
    std::int64_t last_team{0};
    std::int64_t last_session{0};
    std::int64_t position_{0};
    int closed_count{0};
    bool back_pressured{false};
};

class ChallengingAuthenticator final : public Authenticator
{
public:
    void on_connect_request(std::int64_t /*session_id*/, std::span<const std::uint8_t> /*credentials*/,
                            std::int64_t /*now_ms*/) override
    {
        ++connect_requests;
    }

    void on_challenge_response(std::int64_t /*session_id*/, std::span<const std::uint8_t> /*credentials*/,
                               std::int64_t /*now_ms*/) override
    {
        ++challenge_responses;
        challenged = false;
    }

    void on_connected_session(SessionProxy& session_proxy, std::int64_t /*now_ms*/) override
    {
        if (challenged)
        {
            return;
        }
        challenged = session_proxy.challenge(std::span<const std::uint8_t>{});
    }

    void on_challenged_session(SessionProxy& session_proxy, std::int64_t /*now_ms*/) override
    {
        if (!challenged)
        {
            (void)session_proxy.authenticate({});
        }
    }

    int connect_requests{0};
    int challenge_responses{0};
    bool challenged{false};
};

/// Collects every fragment a subscriber receives.
struct FragmentCollector
{
    std::vector<std::vector<std::uint8_t>> messages{};
};

void on_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t* /*header*/)
{
    static_cast<FragmentCollector*>(clientd)->messages.emplace_back(buffer, buffer + length);
}

class SessionManagerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_TRUE(driver_.ok());
        client_ = connect_with_retry(driver_.dir());
        ASSERT_TRUE(client_.has_value());

        auto subscription = await_subscription(*client_, RESPONSE_CHANNEL, RESPONSE_STREAM_ID);
        ASSERT_TRUE(subscription.has_value());
        response_subscription_ = std::move(*subscription);
    }

    void TearDown() override
    {
        manager_.reset();
        response_subscription_.reset();
        client_.reset();
    }

    static std::optional<DeepmsgClient> connect_with_retry(const std::string& dir)
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

    static std::optional<SubPtr> await_subscription(DeepmsgClient& client, std::string_view channel,
                                                    std::int32_t stream_id)
    {
        auto add = client.add_subscription(channel, stream_id);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline)
        {
            (void)client.do_work();
            auto polled = add.poll();
            if (polled && polled->has_value())
            {
                return std::optional<SubPtr>{std::move(**polled)};
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return std::nullopt;
    }

    void make_manager(ChallengingAuthenticator* authenticator = nullptr)
    {
        SessionManagerConfig config;
        config.member_id = MEMBER_ID;
        config.cluster_id = CLUSTER_ID;
        config.session_timeout_ns = 10'000'000'000;
        config.max_concurrent_sessions = 2;
        config.service_count = 1;

        if (nullptr != authenticator)
        {
            authenticator_override_ = authenticator;
        }

        manager_ = std::make_unique<SessionManager>(
            config, *client_, log_, egress_, authenticator_or_default(), authorisation_, [this]() { return now_ns_; },
            [this]() { return now_ns_ / 1'000'000; }, [this]() { ++timed_out_; }, [](const auto&) {});
    }

    [[nodiscard]] Authenticator& authenticator_or_default()
    {
        return nullptr != authenticator_override_ ? static_cast<Authenticator&>(*authenticator_override_)
                                                  : static_cast<Authenticator&>(default_authenticator_);
    }

    void connect_session(std::int64_t correlation_id, std::int32_t version = 768,
                         ClusterRole role = ClusterRole::leader, std::string client_info = "probe")
    {
        manager_->on_session_connect(correlation_id, RESPONSE_STREAM_ID, version, RESPONSE_CHANNEL, {}, client_info,
                                     1'000, "ipc", 1'000, role, INGRESS_ENDPOINTS);
    }

    bool pump_until_open(std::int64_t session_id)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline)
        {
            (void)client_->do_work();
            manager_->process_all_pending_sessions(now_ns_, MEMBER_ID, LEADERSHIP_TERM_ID);
            manager_->send_redirects(LEADERSHIP_TERM_ID, MEMBER_ID, now_ns_);
            manager_->send_rejections(LEADERSHIP_TERM_ID, MEMBER_ID, now_ns_);
            if (nullptr != manager_->find_by_session_id(session_id))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    }

    void pump_events()
    {
        FragmentCollector collector{};
        int quiet_iterations = 0;
        for (int i = 0; i < 500; ++i)
        {
            (void)client_->do_work();
            if (manager_)
            {
                manager_->process_all_pending_sessions(now_ns_, MEMBER_ID, LEADERSHIP_TERM_ID);
                manager_->send_redirects(LEADERSHIP_TERM_ID, MEMBER_ID, now_ns_);
                manager_->send_rejections(LEADERSHIP_TERM_ID, MEMBER_ID, now_ns_);
            }
            const std::size_t before = collector.messages.size();
            (void)aeron_subscription_poll(response_subscription_.get(), on_fragment, &collector, 10);
            quiet_iterations = collector.messages.size() == before ? quiet_iterations + 1 : 0;
            if (!collector.messages.empty() && quiet_iterations >= 50)
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        last_events_ = std::move(collector.messages);
    }

    [[nodiscard]] std::int32_t last_message_template() const
    {
        if (last_events_.empty())
        {
            return -1;
        }
        auto header = deepmsg::cluster::common::message_header(last_events_.back());
        return header ? header->templateId() : -1;
    }

    [[nodiscard]] std::string last_event_detail() const
    {
        if (last_events_.empty())
        {
            return {};
        }
        const auto& message = last_events_.back();
        auto header = deepmsg::cluster::common::message_header(message);
        if (!header || header->templateId() != codecs::SessionEvent::SBE_TEMPLATE_ID)
        {
            return {};
        }
        auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionEvent>(message, *header);
        return std::string{decoder.getDetailAsStringView()};
    }

    [[nodiscard]] std::optional<codecs::EventCode::Value> last_event_code() const
    {
        if (last_events_.empty())
        {
            return std::nullopt;
        }
        const auto& message = last_events_.back();
        auto header = deepmsg::cluster::common::message_header(message);
        if (!header || header->templateId() != codecs::SessionEvent::SBE_TEMPLATE_ID)
        {
            return std::nullopt;
        }
        auto decoder = deepmsg::cluster::common::wrap_body<codecs::SessionEvent>(message, *header);
        return decoder.code();
    }

    EmbeddedDriver driver_{};
    std::optional<DeepmsgClient> client_{};
    SubPtr response_subscription_{};
    FakeLogAppender log_{};
    deepmsg::cluster::consensus::EgressPublisher egress_{10'000'000'000};
    deepmsg::cluster::consensus::DefaultAuthenticator default_authenticator_{};
    deepmsg::cluster::consensus::AllowAllAuthorisationService authorisation_{};
    std::unique_ptr<SessionManager> manager_{};
    std::int64_t now_ns_{1'000'000'000};
    int timed_out_{0};
    ChallengingAuthenticator* authenticator_override_{nullptr};
    std::vector<std::vector<std::uint8_t>> last_events_{};
};

TEST_F(SessionManagerTest, aSessionConnectsAuthenticatesOpensAndSendsTheOpenEvent)
{
    make_manager();
    connect_session(42);

    ASSERT_TRUE(pump_until_open(1));
    ClusterSession* session = manager_->find_by_session_id(1);
    ASSERT_NE(nullptr, session);
    EXPECT_EQ(SessionState::open, session->state());
    EXPECT_EQ(1U, log_.opened_ids.size());
    EXPECT_EQ(1, log_.opened_ids[0]);
    EXPECT_EQ(1U, manager_->sessions().size());

    pump_events();
    ASSERT_FALSE(last_events_.empty());
    EXPECT_EQ(codecs::EventCode::OK, last_event_code());
}

TEST_F(SessionManagerTest, aChallengeIsSentAndTheResponseAuthenticatesTheSession)
{
    ChallengingAuthenticator authenticator;
    make_manager(&authenticator);
    connect_session(7);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline)
    {
        (void)client_->do_work();
        manager_->process_all_pending_sessions(now_ns_, MEMBER_ID, LEADERSHIP_TERM_ID);
        ClusterSession* pending = manager_->find_pending_session_by_id(1);
        if (nullptr != pending && SessionState::challenged == pending->state())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    pump_events();
    ASSERT_FALSE(last_events_.empty());
    EXPECT_EQ(codecs::Challenge::SBE_TEMPLATE_ID, last_message_template());

    manager_->on_challenge_response(7, 1, {});
    ASSERT_TRUE(pump_until_open(1));
    EXPECT_EQ(1, authenticator.challenge_responses);
    EXPECT_EQ(SessionState::open, manager_->find_by_session_id(1)->state());
}

TEST_F(SessionManagerTest, anInvalidProtocolVersionIsRejectedWithTheReferenceDetail)
{
    make_manager();
    connect_session(1, 1 << 16);

    pump_events();
    ASSERT_FALSE(last_events_.empty());
    EXPECT_EQ(codecs::EventCode::ERROR, last_event_code());
    EXPECT_NE(std::string::npos, last_event_detail().find("invalid client version 1.0.0, cluster is 0.3.0"));
    EXPECT_EQ(0U, manager_->sessions().size());
}

TEST_F(SessionManagerTest, theSessionLimitIsRejected)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));

    connect_session(2);
    ASSERT_TRUE(pump_until_open(2));

    connect_session(3);
    pump_events();
    ASSERT_FALSE(last_events_.empty());
    EXPECT_EQ(codecs::EventCode::ERROR, last_event_code());
    EXPECT_EQ(std::string{"concurrent session limit"}, last_event_detail());
    EXPECT_EQ(2U, manager_->sessions().size());
}

TEST_F(SessionManagerTest, aFollowerRedirectsTheSession)
{
    make_manager();
    connect_session(1, 768, ClusterRole::follower);

    pump_events();
    ASSERT_FALSE(last_events_.empty());
    EXPECT_EQ(codecs::EventCode::REDIRECT, last_event_code());
    EXPECT_EQ(std::string{INGRESS_ENDPOINTS}, last_event_detail());
}

TEST_F(SessionManagerTest, keepAliveRefreshesTheActivityAndTimeoutClosesTheSession)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));

    now_ns_ += 9'000'000'000;
    manager_->on_session_keep_alive(LEADERSHIP_TERM_ID, 1, 1'000);
    now_ns_ += 5'000'000'000;
    manager_->check_sessions(now_ns_, LEADERSHIP_TERM_ID, MEMBER_ID, INGRESS_ENDPOINTS);
    EXPECT_NE(nullptr, manager_->find_by_session_id(1));

    now_ns_ += 6'000'000'000;
    manager_->check_sessions(now_ns_, LEADERSHIP_TERM_ID, MEMBER_ID, INGRESS_ENDPOINTS);
    EXPECT_EQ(nullptr, manager_->find_by_session_id(1));
    EXPECT_EQ(1, timed_out_);
    EXPECT_EQ(1, log_.closed_count);
}

TEST_F(SessionManagerTest, anUnavailableIngressImageTimesOutOnlyItsSession)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));

    manager_->on_session_connect(43, RESPONSE_STREAM_ID, 768, RESPONSE_CHANNEL, {}, "probe-2", 2'000, "ipc", 2'000,
                                 ClusterRole::leader, INGRESS_ENDPOINTS);
    ASSERT_TRUE(pump_until_open(2));

    ClusterSession* first = manager_->find_by_session_id(1);
    ClusterSession* second = manager_->find_by_session_id(2);
    ASSERT_NE(nullptr, first);
    ASSERT_NE(nullptr, second);
    ASSERT_EQ(1'000, first->ingress_image_correlation_id());
    ASSERT_EQ(2'000, second->ingress_image_correlation_id());

    // `SessionManager.timeoutOnUnavailableImage` (`:1121-1132`).
    manager_->timeout_on_unavailable_image(1'000);
    EXPECT_EQ(SessionState::closing, first->state());
    EXPECT_EQ(SessionState::open, second->state());
    EXPECT_NE(nullptr, manager_->find_by_session_id(1));
    EXPECT_NE(nullptr, manager_->find_by_session_id(2));
}

// C4e (C4d gap): `clearSessionsAfter` (`SessionManager.java:1028-1047`): a
// session opened after the cut is closed, an earlier one stays.
TEST_F(SessionManagerTest, clearSessionsAfterClosesOnlyTheLaterSessions)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));
    manager_->on_session_connect(43, RESPONSE_STREAM_ID, 768, RESPONSE_CHANNEL, {}, "probe-2", 2'000, "ipc", 2'000,
                                 ClusterRole::leader, INGRESS_ENDPOINTS);
    ASSERT_TRUE(pump_until_open(2));

    manager_->clear_sessions_after(64, LEADERSHIP_TERM_ID);

    EXPECT_NE(nullptr, manager_->find_by_session_id(1));
    EXPECT_EQ(nullptr, manager_->find_by_session_id(2));
}

// C4e (C4d gap): `sweepUncommittedSessions` / `restoreUncommittedSessions`
// (`SessionManager.java:1057-1096`): the commit position is the gate.
TEST_F(SessionManagerTest, anUncommittedServiceCloseIsRestoredUntilItIsCommitted)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));
    manager_->on_service_close_session(1, true, LEADERSHIP_TERM_ID);
    ASSERT_EQ(nullptr, manager_->find_by_session_id(1)) << "the close is uncommitted";

    manager_->sweep_uncommitted_sessions(95);
    manager_->restore_uncommitted_sessions(64);

    ClusterSession* restored = manager_->find_by_session_id(1);
    ASSERT_NE(nullptr, restored);
    EXPECT_EQ(SessionState::closing, restored->state());
}

TEST_F(SessionManagerTest, aCommittedServiceCloseIsNotRestored)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));
    manager_->on_service_close_session(1, true, LEADERSHIP_TERM_ID);
    ASSERT_EQ(nullptr, manager_->find_by_session_id(1));

    manager_->sweep_uncommitted_sessions(96);
    manager_->restore_uncommitted_sessions(96);

    EXPECT_EQ(nullptr, manager_->find_by_session_id(1));
    EXPECT_EQ(1, log_.closed_count);
}

TEST_F(SessionManagerTest, anIngressMessageIsAppendedToTheLogOnceAndBackPressureAborts)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));

    const std::uint8_t payload[] = {1, 2, 3, 4, 5};
    EXPECT_TRUE(manager_->on_session_message(LEADERSHIP_TERM_ID, 1, payload, sizeof(payload)));
    ASSERT_EQ(1U, log_.payloads.size());
    EXPECT_EQ(std::vector<std::uint8_t>(std::begin(payload), std::end(payload)), log_.payloads[0]);
    EXPECT_EQ(1, log_.last_session);
    EXPECT_EQ(LEADERSHIP_TERM_ID, log_.last_team);

    log_.back_pressured = true;
    EXPECT_FALSE(manager_->on_session_message(LEADERSHIP_TERM_ID, 1, payload, sizeof(payload)));
}

TEST_F(SessionManagerTest, aSessionCounterUsesTheClusterAndSessionKey)
{
    make_manager();
    connect_session(1);
    ASSERT_TRUE(pump_until_open(1));

    for (int attempt = 0; attempt < 100; ++attempt)
    {
        (void)client_->do_work();
        if (AERON_NULL_COUNTER_ID !=
            deepmsg::cluster::common::find_cluster_counter(
                client_->counters_reader(), deepmsg::cluster::common::CLUSTER_SESSION_TYPE_ID, CLUSTER_ID))
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FAIL() << "no cluster-session counter (type 241) for clusterId=" << CLUSTER_ID;
}

}  // namespace
