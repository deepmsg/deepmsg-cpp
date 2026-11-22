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

#include <io_aeron_cluster_codecs/AdminRequest.h>

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
    struct AdminResponse
    {
        std::int64_t session_id;
        std::int64_t correlation_id;
        deepmsg::cluster::common::codecs::AdminRequestType::Value request_type;
        deepmsg::cluster::common::codecs::AdminResponseCode::Value code;
        std::string message;
        std::size_t payload_length;
    };

    std::vector<std::string> messages{};
    std::vector<deepmsg::cluster::common::codecs::EventCode::Value> events{};
    std::vector<AdminResponse> admin_responses{};
    std::int32_t new_leader_events{0};
    std::int64_t last_leadership_term_id{0};

    void on_message(std::int64_t, std::int64_t, std::span<const std::uint8_t> payload) override
    {
        messages.emplace_back(payload.begin(), payload.end());
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t,
                          deepmsg::cluster::common::codecs::EventCode::Value code, std::string_view) override
    {
        events.push_back(code);
    }

    void on_new_leader(std::int64_t, std::int64_t leadership_term_id, std::int32_t, std::string_view) override
    {
        new_leader_events++;
        last_leadership_term_id = leadership_term_id;
    }

    void on_admin_response(std::int64_t session_id, std::int64_t correlation_id,
                           deepmsg::cluster::common::codecs::AdminRequestType::Value request_type,
                           deepmsg::cluster::common::codecs::AdminResponseCode::Value code, std::string_view message,
                           std::span<const std::uint8_t> payload) override
    {
        admin_responses.push_back(
            AdminResponse{session_id, correlation_id, request_type, code, std::string{message}, payload.size()});
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
                                   std::chrono::milliseconds timeout = std::chrono::seconds(30),
                                   std::chrono::milliseconds new_leader_timeout = std::chrono::milliseconds::zero(),
                                   bool ingress_exclusive = true)
    {
        ClientConfig config;
        config.ingress_channel = "aeron:udp?term-length=128k";
        config.ingress_endpoints = ingress_endpoints;
        config.egress_channel = egress_channel;
        config.client_name = "c6c-client";
        config.is_ingress_exclusive = ingress_exclusive;
        if (new_leader_timeout > std::chrono::milliseconds::zero())
        {
            config.new_leader_timeout_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(new_leader_timeout).count();
        }
        // The reference default is 5 s. A full parallel gate (three nodes,
        // three archives, a client driver) can miss that on a loaded runner and
        // report a connect timeout on a healthy cluster; the scenario retries
        // and this budget is per attempt.
        config.message_timeout_ns = 10'000'000'000;
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
        config.message_timeout_ns = 10'000'000'000;
        return connect(config, aeron_dir, timeout);
    }

    [[nodiscard]] bool is_open() const noexcept
    {
        return cluster_.has_value() && !cluster_->is_closed();
    }

    [[nodiscard]] std::int64_t cluster_session_id() const noexcept
    {
        return cluster_ ? cluster_->cluster_session_id() : -1;
    }

    [[nodiscard]] const std::vector<deepmsg::cluster::common::codecs::EventCode::Value>& events() const noexcept
    {
        return listener_.events;
    }

    /// `AeronCluster.ingressPublication().channel()`: after a redirect this
    /// names the member the session actually publishes to.
    [[nodiscard]] std::string ingress_channel() const
    {
        return cluster_ ? cluster_->ingress_publication().channel() : std::string{};
    }

    [[nodiscard]] const std::vector<std::string>& messages() const noexcept
    {
        return listener_.messages;
    }

    [[nodiscard]] std::int32_t new_leader_events() const noexcept
    {
        return listener_.new_leader_events;
    }

    [[nodiscard]] const std::string& last_error() const noexcept
    {
        return last_error_;
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

    /// `sendAdminRequestToTakeASnapshot` with the same connect-time retry:
    /// the ingress publication may not be connected at DONE.
    [[nodiscard]] bool send_admin_request_to_take_a_snapshot(
        std::int64_t correlation_id, std::chrono::milliseconds timeout = std::chrono::seconds(5))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (cluster_)
            {
                auto sent = cluster_->send_admin_request_to_take_a_snapshot(correlation_id);
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

    /// Write an `AdminRequest` straight onto the ingress publication, the way
    /// `ClusterTest.shouldRejectAnInvalidAdminRequest` writes NULL_VAL (the
    /// client API only sends SNAPSHOT).
    [[nodiscard]] bool offer_admin_request(deepmsg::cluster::common::codecs::AdminRequestType::Value type,
                                           std::int64_t correlation_id)
    {
        if (!cluster_)
        {
            return false;
        }
        namespace codecs = deepmsg::cluster::common::codecs;
        const std::size_t length = deepmsg::cluster::common::MESSAGE_HEADER_LENGTH +
                                   codecs::AdminRequest::sbeBlockLength() + codecs::AdminRequest::payloadHeaderLength();
        aeron_buffer_claim_t claim{};
        const std::int64_t position = cluster_->ingress_publication().try_claim(length, claim);
        if (position <= 0)
        {
            return false;
        }
        codecs::AdminRequest encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
        encoder.leadershipTermId(cluster_->leadership_term_id())
            .clusterSessionId(cluster_->cluster_session_id())
            .correlationId(correlation_id)
            .requestType(type)
            .putPayload("", 0);
        return 0 == aeron_buffer_claim_commit(&claim);
    }

    void set_controlled_listener(deepmsg::cluster::client::ControlledEgressListener* listener)
    {
        if (cluster_)
        {
            cluster_->set_controlled_egress_listener(listener);
        }
    }

    [[nodiscard]] bool controlled_pump_once()
    {
        if (!client_ || !cluster_)
        {
            return false;
        }
        (void)client_->do_work();
        return cluster_->controlled_poll_egress().has_value();
    }

    [[nodiscard]] const std::vector<RecordingEgressListener::AdminResponse>& admin_responses() const noexcept
    {
        return listener_.admin_responses;
    }

    /// Offer `payload` once, then pump until `expected` egress messages exist
    /// *and* at least one of them is new. The new-message half is what makes a
    /// count safe: a count that is already satisfied by an earlier batch (a
    /// caller that starts over at one after a reconnect, or a test that offers
    /// twice on one connection) must still wait for this offer. (2026-11-21/22:
    /// the C9 leader-change case counted echoes that belonged to the previous
    /// batch; the same shape was latent in the other call sites.)
    [[nodiscard]] bool offer_and_await(std::string_view payload, std::size_t expected,
                                       std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        if (!cluster_)
        {
            return false;
        }
        const std::size_t before = listener_.messages.size();
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
            if (offered && listener_.messages.size() >= expected && listener_.messages.size() > before)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    /// Offer `payload` and wait for *its* echo: the first message recorded after
    /// the offer must be the payload itself. The echo business returns the
    /// client's bytes unchanged, so a count that an older message satisfies and
    /// a duplicate or reordered response both fail here.
    [[nodiscard]] bool offer_and_await_echo(std::string_view payload,
                                            std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        const std::size_t before = listener_.messages.size();
        if (!offer_and_await(payload, before + 1, timeout))
        {
            return false;
        }
        return listener_.messages.size() > before && listener_.messages[before] == payload;
    }

    /// Offer `payload` and pump until the publication takes it. No echo is
    /// expected: the leadership has no quorum, so the message is appended but
    /// never commits (C7's crash injection).
    [[nodiscard]] bool offer_only(std::string_view payload,
                                  std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        if (!cluster_)
        {
            return false;
        }
        const auto bytes =
            std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (cluster_->offer(bytes) >= 0)
            {
                return true;
            }
            if (!pump_once())
            {
                return false;
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
            last_error_ = "DeepmsgClient::connect failed";
            return false;
        }
        client_ = std::make_unique<DeepmsgClient>(std::move(*client));

        auto concluded = config.conclude();
        if (!concluded.has_value())
        {
            last_error_ = "conclude: " + std::string{concluded.error().message.view()};
            return false;
        }
        auto async = AsyncConnect::create(config);
        if (!async.has_value())
        {
            last_error_ = "AsyncConnect::create: " + std::string{async.error().message.view()};
            return false;
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto polled = async->poll(*client_);
            if (!polled.has_value())
            {
                last_error_ = "AsyncConnect::poll: " + std::string{polled.error().message.view()};
                return false;
            }
            if (polled->has_value())
            {
                // A new client is a new egress stream: the listener outlives the
                // connection and `offer_and_await` counts from one, so an echo
                // counted here must belong to this connection. (2026-11-21: the
                // C9 leader-change case counted the previous batch's echoes and
                // never awaited the ones it had just sent.)
                listener_.messages.clear();
                cluster_.emplace(std::move(**polled));
                cluster_->set_egress_listener(&listener_);
                return true;
            }
            (void)client_->do_work();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        last_error_ = "AsyncConnect deadline";
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
    std::string last_error_{};
};

}  // namespace axc::interop
