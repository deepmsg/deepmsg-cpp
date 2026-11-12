// The C ABI behind `include/deepmsg_cluster_client.h` (C3b). The header is the
// contract; everything here is a thin translation into the C++ session.

#include <cstdio>
#include <cstring>
#include <ctime>
#include <memory>
#include <optional>
#include <string>

#include "cluster/client/async_connect.h"
#include "cluster/client/client_config.h"
#include "cluster/client/cluster_client.h"
#include "core/deepmsg/deepmsg_client.h"
#include "deepmsg_cluster_client.h"

namespace
{

thread_local std::string last_error{"no error"};

void set_error(const std::string& text)
{
    last_error = text;
}

/// Forwards session messages to the C handler; everything else is the
/// session's own business (failover, close).
class CBridge final : public deepmsg::cluster::client::EgressListener
{
public:
    void on_message(std::int64_t cluster_session_id, std::int64_t timestamp,
                    std::span<const std::uint8_t> payload) override
    {
        if (nullptr != handler)
        {
            handler(clientd, cluster_session_id, timestamp, payload.data(), payload.size());
        }
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t,
                          deepmsg::cluster::common::codecs::EventCode::Value, std::string_view) override
    {
    }

    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}

    void on_admin_response(std::int64_t, std::int64_t, deepmsg::cluster::common::codecs::AdminRequestType::Value,
                           deepmsg::cluster::common::codecs::AdminResponseCode::Value, std::string_view,
                           std::span<const std::uint8_t>) override
    {
    }

    deepmsg_cluster_message_handler_t handler{nullptr};
    void* clientd{nullptr};
};

}  // namespace

struct deepmsg_cluster_async_connect_stct
{
    std::string aeron_dir{};
    std::unique_ptr<axc::core::DeepmsgClient> client{};
    std::optional<deepmsg::cluster::client::AsyncConnect> connect{};
    deepmsg::cluster::client::ClientConfig config{};
};

struct deepmsg_cluster_client_stct
{
    std::unique_ptr<axc::core::DeepmsgClient> client{};
    std::optional<deepmsg::cluster::client::ClusterClient> cluster{};
    CBridge bridge{};
};

extern "C"
{
    int deepmsg_cluster_async_connect(deepmsg_cluster_async_connect_t** async, const char* aeron_dir,
                                      const char* ingress_channel, const char* egress_channel)
    {
        if (nullptr == async || nullptr == ingress_channel || nullptr == egress_channel)
        {
            set_error("async connect needs the handle and both channels");
            return -1;
        }

        auto handle = std::make_unique<deepmsg_cluster_async_connect_stct>();
        handle->aeron_dir = nullptr == aeron_dir ? "" : aeron_dir;
        handle->config.ingress_channel = ingress_channel;
        handle->config.egress_channel = egress_channel;
        if (auto concluded = handle->config.conclude(); !concluded)
        {
            set_error(std::string{concluded.error().message.view()});
            return -1;
        }

        auto connect = deepmsg::cluster::client::AsyncConnect::create(handle->config);
        if (!connect)
        {
            set_error(std::string{connect.error().message.view()});
            return -1;
        }
        handle->connect = std::move(*connect);
        *async = handle.release();
        return 0;
    }

    int deepmsg_cluster_async_connect_poll(deepmsg_cluster_client_t** client, deepmsg_cluster_async_connect_t* async)
    {
        if (nullptr == client || nullptr == async || !async->connect.has_value())
        {
            set_error("async connect poll on an empty handle");
            return -1;
        }

        if (!async->client)
        {
            auto connected = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{async->aeron_dir});
            if (!connected)
            {
                set_error(std::string{connected.error().message.view()});
                return -1;
            }
            async->client = std::make_unique<axc::core::DeepmsgClient>(std::move(*connected));
        }

        (void)async->client->do_work();

        auto polled = async->connect->poll(*async->client);
        if (!polled)
        {
            set_error(std::string{polled.error().message.view()});
            return -1;
        }
        if (!polled->has_value())
        {
            return 0;
        }

        auto handle = std::make_unique<deepmsg_cluster_client_stct>();
        handle->client = std::move(async->client);
        handle->cluster = std::move(**polled);
        *client = handle.release();
        return 1;
    }

    void deepmsg_cluster_async_connect_free(deepmsg_cluster_async_connect_t* async)
    {
        delete async;
    }

    int deepmsg_cluster_connect(deepmsg_cluster_client_t** client, const char* aeron_dir, const char* ingress_channel,
                                const char* egress_channel)
    {
        deepmsg_cluster_async_connect_t* async = nullptr;
        if (0 != deepmsg_cluster_async_connect(&async, aeron_dir, ingress_channel, egress_channel))
        {
            return -1;
        }

        while (true)
        {
            const int result = deepmsg_cluster_async_connect_poll(client, async);
            if (1 == result)
            {
                deepmsg_cluster_async_connect_free(async);
                return 0;
            }
            if (result < 0)
            {
                deepmsg_cluster_async_connect_free(async);
                return -1;
            }

            const struct timespec pause{0, 1'000'000};
            (void)nanosleep(&pause, nullptr);
        }
    }

    int64_t deepmsg_cluster_offer(deepmsg_cluster_client_t* client, const uint8_t* buffer, size_t length)
    {
        if (nullptr == client || !client->cluster.has_value())
        {
            set_error("offer on a closed client");
            return -1;
        }
        return client->cluster->offer(std::span<const std::uint8_t>(buffer, length));
    }

    int deepmsg_cluster_poll_egress(deepmsg_cluster_client_t* client, deepmsg_cluster_message_handler_t handler,
                                    void* clientd, size_t fragment_limit)
    {
        if (nullptr == client || !client->cluster.has_value())
        {
            set_error("poll on a closed client");
            return -1;
        }

        client->bridge.handler = handler;
        client->bridge.clientd = clientd;
        client->cluster->set_egress_listener(&client->bridge);

        auto polled = client->cluster->poll_egress(0 == fragment_limit ? deepmsg::cluster::client::FRAGMENT_LIMIT
                                                                       : fragment_limit);
        if (!polled)
        {
            set_error(std::string{polled.error().message.view()});
            return -1;
        }
        return *polled;
    }

    int64_t deepmsg_cluster_session_id(const deepmsg_cluster_client_t* client)
    {
        return nullptr == client || !client->cluster.has_value() ? -1 : client->cluster->cluster_session_id();
    }

    int deepmsg_cluster_is_closed(const deepmsg_cluster_client_t* client)
    {
        return nullptr == client || !client->cluster.has_value() || client->cluster->is_closed() ? 1 : 0;
    }

    int deepmsg_cluster_close(deepmsg_cluster_client_t* client)
    {
        if (nullptr == client)
        {
            return -1;
        }
        if (client->cluster.has_value())
        {
            client->cluster->close();
            client->cluster.reset();
        }
        delete client;
        return 0;
    }

    const char* deepmsg_cluster_error(void)
    {
        return last_error.c_str();
    }

}  // extern "C"
