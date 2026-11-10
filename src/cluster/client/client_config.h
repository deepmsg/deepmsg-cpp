#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::cluster::client
{

using axc::core::Error;

/// `AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION` (0.3.0,
/// `AeronCluster.java:1209-1228`).
inline constexpr std::int32_t PROTOCOL_SEMANTIC_VERSION = (0 << 16) | (3 << 8) | 0;
inline constexpr std::int32_t PROTOCOL_MAJOR_VERSION = 0;
inline constexpr std::int32_t PROTOCOL_MINOR_VERSION = 3;
inline constexpr std::int32_t PROTOCOL_PATCH_VERSION = 0;

/// `MESSAGE_TIMEOUT_DEFAULT_NS` (`AeronCluster.java:1240-1241`).
inline constexpr std::int64_t MESSAGE_TIMEOUT_DEFAULT_NS = 5'000'000'000;
/// `INGRESS_STREAM_ID_DEFAULT` (`AeronCluster.java:1285`).
inline constexpr std::int32_t INGRESS_STREAM_ID_DEFAULT = 101;
/// `EGRESS_STREAM_ID_DEFAULT` (`AeronCluster.java:1320`).
inline constexpr std::int32_t EGRESS_STREAM_ID_DEFAULT = 102;
/// `ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS`
/// (`ConsensusModule.java:751`).
inline constexpr std::int64_t LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS = 10'000'000'000;
/// `Aeron.Configuration.MAX_CLIENT_NAME_LENGTH` (`Aeron.java:1098`).
inline constexpr std::size_t MAX_CLIENT_NAME_LENGTH = 100;
/// `Aeron.NULL_VALUE` (`Aeron.java:82`).
inline constexpr std::int64_t NULL_VALUE = -1;
/// `CommonContext.IPC_CHANNEL`.
inline constexpr std::string_view IPC_CHANNEL = "aeron:ipc";
/// `CommonContext.REJOIN_PARAM_NAME`.
inline constexpr std::string_view REJOIN_PARAM_NAME = "rejoin";

/// `AeronClusterVersion.VERSION` / `GIT_SHA` at the pinned submodule
/// (`AeronClusterVersion.java`, 1.54.0 = `454da1e4e6`). `clientInfo` is byte
/// for byte the reference's (`AeronCluster.java:2457-2458`).
inline constexpr std::string_view CLUSTER_VERSION = "1.54.0";
inline constexpr std::string_view CLUSTER_GIT_SHA = "454da1e4e6";

/// The credentials hooks (`CredentialsSupplier`). The default sends an empty
/// encoded credential and answers a challenge with an empty response, which is
/// what the reference's default authenticator accepts
/// (`AeronCluster.java:1527-1530`, `NullCredentialsSupplier`).
struct ClientCredentials
{
    std::function<std::vector<std::uint8_t>()> encoded = []() { return std::vector<std::uint8_t>{}; };
    std::function<std::vector<std::uint8_t>(std::span<const std::uint8_t>)> on_challenge =
        [](std::span<const std::uint8_t>) { return std::vector<std::uint8_t>{}; };
};

/// `AeronCluster.Context`, reduced to the connection half (C3a,
/// `AeronCluster.java:1407-1551`). The egress listeners, the idle strategy and
/// the owned `Aeron` client belong to C3b.
struct ClientConfig
{
    std::int64_t message_timeout_ns{MESSAGE_TIMEOUT_DEFAULT_NS};
    std::optional<std::string> ingress_endpoints{};
    std::optional<std::string> ingress_channel{};
    std::int32_t ingress_stream_id{INGRESS_STREAM_ID_DEFAULT};
    std::optional<std::string> egress_channel{};
    std::int32_t egress_stream_id{EGRESS_STREAM_ID_DEFAULT};
    std::string client_name{};
    bool is_ingress_exclusive{true};
    /// Empty means "derive from the leader heartbeat timeout"
    /// (`concludeConnect`, `AeronCluster.java:2572-2578`).
    std::optional<std::int64_t> new_leader_timeout_ns{};
    ClientCredentials credentials{};

    /// The `Context.conclude()` rules (`AeronCluster.java:1471-1508`), in the
    /// reference's order and with the reference's messages. Mutates
    /// `egress_channel` (a UDP egress gets `rejoin=false`) and may be called
    /// once -- the reference throws `ConcurrentConcludeException` on the
    /// second call (`AeronCluster.java:1473-1476`).
    [[nodiscard]] std::expected<void, Error> conclude();

    /// `isConcluded()` (`AeronCluster.java:1558`).
    [[nodiscard]] bool is_concluded() const noexcept
    {
        return concluded_;
    }

private:
    bool concluded_{false};
};

/// `AeronCounters.formatVersionInfo` (`AeronCounters.java:1633-1636`).
[[nodiscard]] std::string format_version_info();

/// `clientInfo = "name=" + clientName + " " + formatVersionInfo(...)`
/// (`AeronCluster.java:2457-2458`).
[[nodiscard]] std::string client_info(const ClientConfig& config);

/// The seven `aeron.cluster.*` client names (`AeronCluster.java:1237-1401`).
[[nodiscard]] std::expected<ClientConfig, Error> client_config_from_properties(
    const axc::core::PropertySource& properties);

}  // namespace deepmsg::cluster::client
