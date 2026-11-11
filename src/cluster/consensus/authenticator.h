#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace deepmsg::cluster::consensus
{

/// `io.aeron.security.SessionProxy`: what an `Authenticator` may do to the
/// session it is authenticating.
class SessionProxy
{
public:
    SessionProxy() = default;
    SessionProxy(const SessionProxy&) = delete;
    SessionProxy& operator=(const SessionProxy&) = delete;
    virtual ~SessionProxy() = default;

    [[nodiscard]] virtual std::int64_t session_id() = 0;
    [[nodiscard]] virtual bool challenge(std::span<const std::uint8_t> encoded_challenge) = 0;
    [[nodiscard]] virtual bool authenticate(std::span<const std::uint8_t> encoded_principal) = 0;
    virtual void reject() = 0;
};

/// `io.aeron.security.Authenticator`.
class Authenticator
{
public:
    Authenticator() = default;
    Authenticator(const Authenticator&) = delete;
    Authenticator& operator=(const Authenticator&) = delete;
    virtual ~Authenticator() = default;

    virtual void on_connect_request(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                                    std::int64_t now_ms) = 0;
    virtual void on_challenge_response(std::int64_t session_id, std::span<const std::uint8_t> encoded_credentials,
                                       std::int64_t now_ms) = 0;
    virtual void on_connected_session(SessionProxy& session_proxy, std::int64_t now_ms) = 0;
    virtual void on_challenged_session(SessionProxy& session_proxy, std::int64_t now_ms) = 0;
};

/// `DefaultAuthenticatorSupplier.DefaultAuthenticator`: every connection is
/// authenticated immediately with the null (empty) principal.
class DefaultAuthenticator final : public Authenticator
{
public:
    void on_connect_request(std::int64_t /*session_id*/, std::span<const std::uint8_t> /*credentials*/,
                            std::int64_t /*now_ms*/) override
    {
    }

    void on_challenge_response(std::int64_t /*session_id*/, std::span<const std::uint8_t> /*credentials*/,
                               std::int64_t /*now_ms*/) override
    {
    }

    void on_connected_session(SessionProxy& session_proxy, std::int64_t /*now_ms*/) override
    {
        (void)session_proxy.authenticate({});
    }

    void on_challenged_session(SessionProxy& session_proxy, std::int64_t /*now_ms*/) override
    {
        (void)session_proxy.authenticate({});
    }
};

/// `io.aeron.security.AuthorisationService`.
class AuthorisationService
{
public:
    AuthorisationService() = default;
    AuthorisationService(const AuthorisationService&) = delete;
    AuthorisationService& operator=(const AuthorisationService&) = delete;
    virtual ~AuthorisationService() = default;

    [[nodiscard]] virtual bool is_authorised(std::int32_t protocol_id, std::int32_t action_id, std::int32_t type,
                                             std::span<const std::uint8_t> encoded_principal) const = 0;
};

/// `AuthorisationService.ALLOW_ALL`.
class AllowAllAuthorisationService final : public AuthorisationService
{
public:
    [[nodiscard]] bool is_authorised(std::int32_t /*protocol_id*/, std::int32_t /*action_id*/, std::int32_t /*type*/,
                                     std::span<const std::uint8_t> /*encoded_principal*/) const override
    {
        return true;
    }
};

}  // namespace deepmsg::cluster::consensus
