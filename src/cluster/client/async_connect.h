#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/client/client_config.h"
#include "cluster/client/cluster_client.h"
#include "cluster/client/egress_poller.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::client
{

/// `AeronCluster.AsyncConnect` (`AeronCluster.java:2116-2599`): seven states
/// from the egress subscription to a connected `ClusterClient`.
///
/// `poll()` advances **one state per call**, exactly like the reference's
/// `switch`; the caller drives the client between calls. `poll()` does not
/// drive the client itself (a caller that forgets `do_work()` deadlocks, not
/// times out -- `close()` and the deadline are the only ways out).
class AsyncConnect
{
public:
    /// Step numbers are the reference's (`AeronCluster.java:2121-2168`).
    enum class State : std::int32_t
    {
        create_egress_subscription = -1,
        create_ingress_publications = 0,
        await_publication_connected = 1,
        send_message = 2,
        poll_response = 3,
        conclude_connect = 4,
        done = 5
    };

    /// `State.fromStep` (`AeronCluster.java:2161-2168`); nullopt outside the
    /// range.
    [[nodiscard]] static std::optional<State> state_from_step(std::int32_t step) noexcept;

    /// `State.name()`, or `"<unknown>"` (`AeronCluster.java:2272-2276`).
    [[nodiscard]] static std::string_view step_name(std::int32_t step) noexcept;

    /// Parse the ingress endpoints and take the deadline
    /// (`AeronCluster.java:2193-2205`). `config` must be concluded. The client
    /// is not touched until the first `poll()`.
    [[nodiscard]] static std::expected<AsyncConnect, Error> create(const ClientConfig& config);

    AsyncConnect(AsyncConnect&&) noexcept = default;
    AsyncConnect& operator=(AsyncConnect&&) noexcept = default;
    AsyncConnect(const AsyncConnect&) = delete;
    AsyncConnect& operator=(const AsyncConnect&) = delete;
    ~AsyncConnect() = default;

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    /// `AeronCluster.AsyncConnect.step()` (`AeronCluster.java:2243-2246`).
    [[nodiscard]] std::int32_t step() const noexcept
    {
        return static_cast<std::int32_t>(state_);
    }

    /// Advance one state. Returns the connected cluster on the `DONE`
    /// transition, `std::nullopt` while work remains, and an error on
    /// deadline, a cluster `ERROR`/`AUTHENTICATION_REJECTED`, or a driver
    /// failure.
    [[nodiscard]] std::expected<std::optional<ClusterClient>, Error> poll(axc::core::DeepmsgClient& client);

    /// `AsyncConnect.close()` (`AeronCluster.java:2210-2236`): release what
    /// landed and cancel what is still pending. A no-op after `DONE`.
    void close(axc::core::DeepmsgClient& client) noexcept;

private:
    struct MemberIngress
    {
        std::int32_t member_id{-1};
        std::string endpoint{};
        std::optional<axc::core::AsyncAdd<axc::core::PubPtr>> concurrent_add{};
        std::optional<axc::core::AsyncAdd<axc::core::ExPubPtr>> exclusive_add{};
        IngressPublication publication{};
        std::optional<Error> publication_exception{};
    };

    AsyncConnect(const ClientConfig& config, std::int64_t deadline_ns);

    /// `parseIngressEndpoints` (`AeronCluster.java:736-756`): `<memberId>=<endpoint>,…`.
    [[nodiscard]] static std::expected<std::vector<MemberIngress>, Error> parse_ingress_endpoints(
        std::string_view endpoints);

    [[nodiscard]] std::expected<void, Error> create_egress_subscription(axc::core::DeepmsgClient& client);
    [[nodiscard]] std::expected<void, Error> create_ingress_publications(axc::core::DeepmsgClient& client);
    [[nodiscard]] std::expected<void, Error> await_publication_connected(axc::core::DeepmsgClient& client);
    [[nodiscard]] std::expected<void, Error> send_message();
    [[nodiscard]] std::expected<void, Error> poll_response(axc::core::DeepmsgClient& client);
    [[nodiscard]] std::expected<std::optional<ClusterClient>, Error> conclude_connect();

    void prepare_connect_request(axc::core::DeepmsgClient& client, std::string_view response_channel);
    void prepare_challenge_response(axc::core::DeepmsgClient& client, std::span<const std::uint8_t> credentials);
    void update_members();

    void async_add_member_publication(axc::core::DeepmsgClient& client, MemberIngress& member);
    void async_get_member_publication(MemberIngress& member);
    void close_members() noexcept;

    [[nodiscard]] std::expected<std::optional<std::string>, Error> resolved_response_channel() const;
    [[nodiscard]] Error deadline_error(axc::core::DeepmsgClient& client) const;

    ClientConfig config_;
    // The concluded channels; `create` refuses a context without them, so the
    // state machine never re-checks an optional.
    std::string ingress_channel_{};
    std::string egress_channel_{};
    std::int64_t deadline_ns_{0};
    State state_{State::create_egress_subscription};

    std::optional<axc::core::AsyncAdd<axc::core::SubPtr>> egress_add_{};
    axc::core::SubPtr egress_subscription_{};
    std::unique_ptr<EgressPoller> egress_poller_{};

    /// The C API has two publication types; the config picks one
    /// (`is_ingress_exclusive`, default true, `AeronCluster.java:1436`).
    std::optional<axc::core::AsyncAdd<axc::core::PubPtr>> ingress_concurrent_add_{};
    std::optional<axc::core::AsyncAdd<axc::core::ExPubPtr>> ingress_exclusive_add_{};
    IngressPublication ingress_publication_{};

    std::vector<MemberIngress> members_{};
    std::vector<std::uint8_t> buffer_{};

    std::int64_t correlation_id_{NULL_VALUE};
    std::int64_t cluster_session_id_{0};
    std::int64_t leadership_term_id_{0};
    std::int64_t leader_heartbeat_timeout_ns_{NULL_VALUE};
    std::int32_t leader_member_id_{0};
    std::size_t message_length_{0};
};

}  // namespace deepmsg::cluster::client
