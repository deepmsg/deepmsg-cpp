#pragma once

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace deepmsg::archive::server
{

/// `io.aeron.security.SessionProxy`: the authenticator's view of the session it
/// is deciding about.
class SessionProxy
{
public:
    SessionProxy() = default;
    virtual ~SessionProxy() = default;

    SessionProxy(const SessionProxy&) = delete;
    SessionProxy& operator=(const SessionProxy&) = delete;

    [[nodiscard]] virtual std::int64_t session_id() const noexcept = 0;
    [[nodiscard]] virtual std::int64_t correlation_id() const noexcept = 0;

    /// Ask the client for a challenge response. Returns false when the message
    /// could not be sent now and will be retried.
    virtual bool challenge(std::span<const std::uint8_t> encoded_challenge) = 0;

    /// Accept the session with this principal. Returns false when the message
    /// could not be sent now and will be retried.
    virtual bool authenticate(std::span<const std::uint8_t> encoded_principal) = 0;

    virtual void reject() = 0;
};

/// `io.aeron.security.Authenticator` (`Authenticator.java:24-60`).
class Authenticator
{
public:
    Authenticator() = default;
    virtual ~Authenticator() = default;

    Authenticator(const Authenticator&) = delete;
    Authenticator& operator=(const Authenticator&) = delete;

    virtual void on_connect_request(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                                    std::int64_t now_ms) = 0;
    virtual void on_challenge_response(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                                       std::int64_t now_ms) = 0;
    virtual void on_connected_session(SessionProxy& session_proxy, std::int64_t now_ms) = 0;
    virtual void on_challenged_session(SessionProxy& session_proxy, std::int64_t now_ms) = 0;
};

/// `DefaultAuthenticatorSupplier.DefaultAuthenticator`: every connection is
/// accepted with the empty principal, no challenge.
class DefaultAuthenticator final : public Authenticator
{
public:
    void on_connect_request(std::int64_t, std::span<const std::uint8_t>, std::int64_t) override;
    void on_challenge_response(std::int64_t, std::span<const std::uint8_t>, std::int64_t) override;
    void on_connected_session(SessionProxy& session_proxy, std::int64_t) override;
    void on_challenged_session(SessionProxy& session_proxy, std::int64_t) override;
};

/// `io.aeron.samples.archive.SampleAuthenticator`: the credentials the reference
/// tests use. `admin:admin` connects straight through, `admin:adminC` has to
/// answer the challenge `challenge!` with `admin:CSadmin`, anything else is
/// rejected.
class SampleAuthenticator final : public Authenticator
{
public:
    static constexpr std::string_view PRINCIPAL = "admin";
    /// `SampleAuthenticator.CHALLENGE_STRING`.
    static constexpr std::string_view CHALLENGE = "challenge!";

    void on_connect_request(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                            std::int64_t now_ms) override;
    void on_challenge_response(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                               std::int64_t now_ms) override;
    void on_connected_session(SessionProxy& session_proxy, std::int64_t now_ms) override;
    void on_challenged_session(SessionProxy& session_proxy, std::int64_t now_ms) override;

    [[nodiscard]] std::span<const std::uint8_t> encoded_principal() const noexcept
    {
        return principal_;
    }

    /// Sessions the authenticator is still tracking (test observable).
    [[nodiscard]] std::size_t tracked_sessions() const noexcept
    {
        return session_states_.size();
    }

private:
    enum class SessionState
    {
        challenge,
        authenticated,
        reject
    };

    static std::array<std::uint8_t, PRINCIPAL.size()> principal_bytes() noexcept;

    std::unordered_map<std::int64_t, SessionState> session_states_;
    std::array<std::uint8_t, PRINCIPAL.size()> principal_{principal_bytes()};
};

}  // namespace deepmsg::archive::server
