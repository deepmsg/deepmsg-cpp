#include "archive/server/authenticator.h"

#include <algorithm>
#include <cstring>
#include <string_view>

namespace deepmsg::archive::server
{
namespace
{

constexpr std::string_view CREDENTIALS_NO_CHALLENGE = "admin:admin";
constexpr std::string_view CREDENTIALS_REQUIRING_CHALLENGE = "admin:adminC";
constexpr std::string_view CHALLENGE_CREDENTIALS = "admin:CSadmin";

[[nodiscard]] std::string_view as_text(std::span<const std::uint8_t> bytes) noexcept
{
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

}  // namespace

void DefaultAuthenticator::on_connect_request(std::int64_t, std::span<const std::uint8_t>, std::int64_t)
{
    // `DefaultAuthenticatorSupplier.DefaultAuthenticator` does nothing here.
}

void DefaultAuthenticator::on_challenge_response(std::int64_t, std::span<const std::uint8_t>, std::int64_t) {}

void DefaultAuthenticator::on_connected_session(SessionProxy& session_proxy, std::int64_t)
{
    session_proxy.authenticate({});
}

void DefaultAuthenticator::on_challenged_session(SessionProxy& session_proxy, std::int64_t)
{
    session_proxy.authenticate({});
}

std::array<std::uint8_t, SampleAuthenticator::PRINCIPAL.size()> SampleAuthenticator::principal_bytes() noexcept
{
    std::array<std::uint8_t, PRINCIPAL.size()> bytes{};
    std::copy(PRINCIPAL.begin(), PRINCIPAL.end(), bytes.begin());
    return bytes;
}

void SampleAuthenticator::on_connect_request(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                                             std::int64_t)
{
    const std::string_view credentials = as_text(encoded_credentials);

    if (credentials == CREDENTIALS_NO_CHALLENGE)
    {
        session_states_[session_id] = SessionState::authenticated;
    }
    else if (credentials == CREDENTIALS_REQUIRING_CHALLENGE)
    {
        session_states_[session_id] = SessionState::challenge;
    }
    else
    {
        session_states_[session_id] = SessionState::reject;
    }
}

void SampleAuthenticator::on_challenge_response(std::int64_t session_id,
                                                std::span<const std::uint8_t> encoded_credentials, std::int64_t)
{
    const std::string_view credentials = as_text(encoded_credentials);
    const auto found = session_states_.find(session_id);

    if (found != session_states_.end() && SessionState::challenge == found->second &&
        credentials == CHALLENGE_CREDENTIALS)
    {
        found->second = SessionState::authenticated;
        accepted_challenge_responses_++;
    }
    else if (credentials != CHALLENGE_CREDENTIALS)
    {
        session_states_[session_id] = SessionState::reject;
    }
}

void SampleAuthenticator::on_connected_session(SessionProxy& session_proxy, std::int64_t)
{
    const auto found = session_states_.find(session_proxy.session_id());
    if (found == session_states_.end())
    {
        return;
    }

    static constexpr std::string_view challenge = CHALLENGE;
    switch (found->second)
    {
        case SessionState::challenge:
            session_proxy.challenge({reinterpret_cast<const std::uint8_t*>(challenge.data()), challenge.size()});
            break;

        case SessionState::authenticated:
            if (session_proxy.authenticate(encoded_principal()))
            {
                session_states_.erase(found);
            }
            break;

        case SessionState::reject:
            session_proxy.reject();
            session_states_.erase(found);
            break;
    }
}

void SampleAuthenticator::on_challenged_session(SessionProxy& session_proxy, std::int64_t)
{
    const auto found = session_states_.find(session_proxy.session_id());
    if (found == session_states_.end())
    {
        return;
    }

    switch (found->second)
    {
        case SessionState::challenge:
            break;

        case SessionState::authenticated:
            // The reference passes an empty principal here (`SampleAuthenticator.
            // onChallengedSession`).
            if (session_proxy.authenticate({}))
            {
                session_states_.erase(found);
            }
            break;

        case SessionState::reject:
            session_proxy.reject();
            session_states_.erase(found);
            break;
    }
}

}  // namespace deepmsg::archive::server
