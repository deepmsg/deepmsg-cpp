#pragma once

// An in-process cluster client for the interop tests. It speaks the same
// seven-state `AsyncConnect` the C++ client ships, so F2/F1 scenarios that
// need "the client sends and receives" do not depend on the fork's Java
// probes. The egress listener records messages and new-leader events.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/client/async_connect.h"
#include "cluster/client/client_config.h"
#include "cluster/client/cluster_client.h"
#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/deepmsg_client.h"

namespace axc::interop
{

using axc::core::DeepmsgClient;
using deepmsg::cluster::client::AsyncConnect;
using deepmsg::cluster::client::ClientConfig;
using deepmsg::cluster::client::ClusterClient;
using deepmsg::cluster::client::EgressListener;

struct RecordingEgressListener final : EgressListener
{
    std::vector<std::string> messages{};
    std::int32_t new_leader_events{0};
    std::int64_t last_leadership_term_id{0};

    void on_message(std::int64_t, std::int64_t, std::span<const std::uint8_t> payload) override
    {
        messages.emplace_back(payload.begin(), payload.end());
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t,
                          deepmsg::cluster::common::codecs::EventCode::Value, std::string_view) override
    {
    }

    void on_new_leader(std::int64_t, std::int64_t leadership_term_id, std::int32_t, std::string_view) override
    {
        new_leader_events++;
        last_leadership_term_id = leadership_term_id;
    }

    void on_admin_response(std::int64_t, std::int64_t, deepmsg::cluster::common::codecs::AdminRequestType::Value,
                           deepmsg::cluster::common::codecs::AdminResponseCode::Value, std::string_view,
                           std::span<const std::uint8_t>) override
    {
    }
};

class ClusterClientSession
{
public:
    ClusterClientSession() = default;
    ClusterClientSession(const ClusterClientSession&) = delete;
    ClusterClientSession& operator=(const ClusterClientSession&) = delete;

    ~ClusterClientSession()
    {
        close();
    }

    /// UDP ingress with the member endpoint list and the rig's egress channel.
    [[nodiscard]] bool connect_udp(const std::string& aeron_dir, const std::string& ingress_endpoints,
                                   const std::string& egress_channel,
                                   std::chrono::milliseconds timeout = std::chrono::seconds(30))
    {
        ClientConfig config;
        config.ingress_channel = "aeron:udp?term-length=128k";
        config.ingress_endpoints = ingress_endpoints;
        config.egress_channel = egress_channel;
        config.client_name = "c6c-client";
        return connect(config, aeron_dir, timeout);
    }

    /// IPC ingress and egress through one node's media driver.
    [[nodiscard]] bool connect_ipc(const std::string& aeron_dir,
                                   std::chrono::milliseconds timeout = std::chrono::seconds(30))
    {
        ClientConfig config;
        config.ingress_channel = "aeron:ipc";
        config.egress_channel = "aeron:ipc";
        config.client_name = "c6c-ipc-client";
        return connect(config, aeron_dir, timeout);
    }

    [[nodiscard]] bool is_open() const noexcept
    {
        return cluster_.has_value() && !cluster_->is_closed();
    }

    [[nodiscard]] const std::vector<std::string>& messages() const noexcept
    {
        return listener_.messages;
    }

    [[nodiscard]] std::int32_t new_leader_events() const noexcept
    {
        return listener_.new_leader_events;
    }

    [[nodiscard]] bool send_keep_alive()
    {
        // `AsyncConnect` returns at DONE before the ingress publication's
        // connection is guaranteed; retry the claim while pumping.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (cluster_)
            {
                auto sent = cluster_->send_keep_alive();
                if (sent.has_value() && *sent)
                {
                    return true;
                }
            }
            if (!pump_once())
            {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    /// Offer `payload` once, then pump until `expected` egress messages exist.
    [[nodiscard]] bool offer_and_await(std::string_view payload, std::size_t expected,
                                       std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        if (!cluster_)
        {
            return false;
        }
        const auto bytes =
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        bool offered = false;
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (!offered)
            {
                const std::int64_t position = cluster_->offer(bytes);
                if (position >= 0)
                {
                    offered = true;
                }
            }
            if (!pump_once())
            {
                return false;
            }
            if (offered && listener_.messages.size() >= expected)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    /// Pump the client/egress until `predicate` holds.
    template <class Predicate>
    [[nodiscard]] bool pump_until(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (!pump_once())
            {
                return false;
            }
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    void close()
    {
        if (cluster_)
        {
            cluster_->close();
            cluster_.reset();
        }
        client_.reset();
    }

private:
    [[nodiscard]] bool connect(ClientConfig config, const std::string& aeron_dir, std::chrono::milliseconds timeout)
    {
        if (client_ || cluster_)
        {
            return false;
        }
        auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir});
        if (!client)
        {
            return false;
        }
        client_ = std::make_unique<DeepmsgClient>(std::move(*client));

        if (!config.conclude().has_value())
        {
            return false;
        }
        auto async = AsyncConnect::create(config);
        if (!async.has_value())
        {
            return false;
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto polled = async->poll(*client_);
            if (!polled.has_value())
            {
                return false;
            }
            if (polled->has_value())
            {
                cluster_.emplace(std::move(**polled));
                cluster_->set_egress_listener(&listener_);
                return true;
            }
            (void)client_->do_work();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    [[nodiscard]] bool pump_once()
    {
        if (!client_ || !cluster_)
        {
            return false;
        }
        (void)client_->do_work();
        auto polled = cluster_->poll_egress();
        return polled.has_value();
    }

    std::unique_ptr<DeepmsgClient> client_{};
    std::optional<ClusterClient> cluster_{};
    RecordingEgressListener listener_{};
};

}  // namespace axc::interop
