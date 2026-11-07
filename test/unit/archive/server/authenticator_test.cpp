// Criterion for card A2a's authentication layer.
//
// `authenticator.h` transcribes `io.aeron.security.DefaultAuthenticatorSupplier`
// and `io.aeron.samples.archive.SampleAuthenticator` (Aeron 1.54.0).

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "archive/server/authenticator.h"

namespace
{

using deepmsg::archive::server::Authenticator;
using deepmsg::archive::server::DefaultAuthenticator;
using deepmsg::archive::server::SampleAuthenticator;
using deepmsg::archive::server::SessionProxy;

[[nodiscard]] std::span<const std::uint8_t> bytes_of(const std::string& text)
{
    return {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()};
}

class FakeSessionProxy final : public SessionProxy
{
public:
    [[nodiscard]] std::int64_t session_id() const noexcept override
    {
        return session_id_;
    }

    [[nodiscard]] std::int64_t correlation_id() const noexcept override
    {
        return 0;
    }

    bool challenge(std::span<const std::uint8_t> encoded_challenge) override
    {
        challenges.emplace_back(encoded_challenge.begin(), encoded_challenge.end());
        return challenge_succeeds;
    }

    bool authenticate(std::span<const std::uint8_t> encoded_principal) override
    {
        principals.emplace_back(encoded_principal.begin(), encoded_principal.end());
        return authenticate_succeeds;
    }

    void reject() override
    {
        rejects++;
    }

    std::int64_t session_id_{1};
    bool challenge_succeeds{true};
    bool authenticate_succeeds{true};
    std::vector<std::vector<std::uint8_t>> challenges;
    std::vector<std::vector<std::uint8_t>> principals;
    int rejects{0};
};

TEST(DefaultAuthenticatorTest, acceptsEverySessionWithAnEmptyPrincipal)
{
    DefaultAuthenticator authenticator;
    FakeSessionProxy proxy;

    authenticator.on_connect_request(1, bytes_of("whoever"), 0);
    authenticator.on_connected_session(proxy, 0);

    ASSERT_EQ(1u, proxy.principals.size());
    EXPECT_TRUE(proxy.principals[0].empty());
    EXPECT_EQ(0, proxy.rejects);
}

TEST(SampleAuthenticatorTest, connectsStraightThroughWithoutAChallenge)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:admin"), 0);
    authenticator.on_connected_session(proxy, 0);

    ASSERT_EQ(1u, proxy.principals.size());
    EXPECT_EQ("admin", std::string(proxy.principals[0].begin(), proxy.principals[0].end()));
    EXPECT_TRUE(proxy.challenges.empty());
    EXPECT_EQ(0u, authenticator.tracked_sessions());  // forgotten once authenticated
}

TEST(SampleAuthenticatorTest, challengesThenAcceptsTheRightResponse)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:adminC"), 0);
    authenticator.on_connected_session(proxy, 0);

    ASSERT_EQ(1u, proxy.challenges.size());
    EXPECT_EQ("challenge!", std::string(proxy.challenges[0].begin(), proxy.challenges[0].end()));
    EXPECT_TRUE(proxy.principals.empty());
    EXPECT_EQ(1u, authenticator.tracked_sessions());

    authenticator.on_challenge_response(proxy.session_id(), bytes_of("admin:CSadmin"), 0);
    authenticator.on_challenged_session(proxy, 0);

    ASSERT_EQ(1u, proxy.principals.size());
    // `SampleAuthenticator.onChallengedSession` authenticates with an empty
    // principal.
    EXPECT_TRUE(proxy.principals[0].empty());
    EXPECT_EQ(0u, authenticator.tracked_sessions());
}

TEST(SampleAuthenticatorTest, rejectsAnUnknownCredential)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:nope"), 0);
    authenticator.on_connected_session(proxy, 0);

    EXPECT_EQ(1, proxy.rejects);
    EXPECT_TRUE(proxy.principals.empty());
    EXPECT_EQ(0u, authenticator.tracked_sessions());
}

TEST(SampleAuthenticatorTest, rejectsAWrongChallengeResponse)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:adminC"), 0);
    authenticator.on_connected_session(proxy, 0);
    ASSERT_EQ(1u, proxy.challenges.size());

    authenticator.on_challenge_response(proxy.session_id(), bytes_of("wrong"), 0);
    authenticator.on_challenged_session(proxy, 0);

    EXPECT_EQ(1, proxy.rejects);
    EXPECT_TRUE(proxy.principals.empty());
}

TEST(SampleAuthenticatorTest, keepsTheSessionOpenWhenTheChallengeCannotBeSent)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;
    proxy.challenge_succeeds = false;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:adminC"), 0);
    authenticator.on_connected_session(proxy, 0);

    EXPECT_EQ(1u, proxy.challenges.size());
    EXPECT_EQ(1u, authenticator.tracked_sessions());  // retried on the next pass
}

TEST(SampleAuthenticatorTest, keepsTheSessionOpenWhenTheAcceptanceCannotBeSent)
{
    SampleAuthenticator authenticator;
    FakeSessionProxy proxy;
    proxy.authenticate_succeeds = false;

    authenticator.on_connect_request(proxy.session_id(), bytes_of("admin:admin"), 0);
    authenticator.on_connected_session(proxy, 0);

    EXPECT_EQ(1u, proxy.principals.size());
    EXPECT_EQ(1u, authenticator.tracked_sessions());
}

}  // namespace
