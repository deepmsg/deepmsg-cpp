#include "core/deepmsg/deepmsg_client.h"

#include <cstring>

namespace axc::core
{
namespace
{

template <std::size_t N>
bool copy_cstr(std::string_view text, std::array<char, N>& buffer) noexcept
{
    if (text.size() >= N)
    {
        return false;
    }
    std::memcpy(buffer.data(), text.data(), text.size());
    buffer[text.size()] = '\0';
    return true;
}

}  // namespace

std::expected<DeepmsgClient, Error> DeepmsgClient::connect(const ClientConfig& config) noexcept
{
    aeron_context_t* context = nullptr;
    if (0 != aeron_context_init(&context))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "aeron_context_init failed"));
    }
    ContextPtr context_guard{context};

    if (!config.dir.empty())
    {
        std::array<char, MAX_DIR_LENGTH> dir{};
        if (!copy_cstr(config.dir, dir))
        {
            return std::unexpected(make_error(ErrorCode::config_error, "client dir is too long"));
        }
        if (0 != aeron_context_set_dir(context, dir.data()))
        {
            return std::unexpected(make_error(ErrorCode::config_error, "aeron_context_set_dir failed"));
        }
    }

    if (0 != aeron_context_set_use_conductor_agent_invoker(context, config.use_conductor_agent_invoker))
    {
        return std::unexpected(make_error(ErrorCode::config_error, "set use_conductor_agent_invoker failed"));
    }

    aeron_t* client = nullptr;
    if (0 != aeron_init(&client, context))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "aeron_init failed"));
    }
    ClientPtr client_guard{client};

    if (0 != aeron_start(client))
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "aeron_start failed"));
    }

    DeepmsgClient result;
    result.client_ = std::move(client_guard);
    result.context_ = std::move(context_guard);
    return result;
}

int DeepmsgClient::do_work() noexcept
{
    return nullptr == client_ ? -1 : aeron_main_do_work(client_.get());
}

aeron_counters_reader_t* DeepmsgClient::counters_reader() noexcept
{
    return nullptr == client_ ? nullptr : aeron_counters_reader(client_.get());
}

AsyncAdd<CounterPtr> DeepmsgClient::add_counter(std::int32_t type_id, std::span<const std::uint8_t> key,
                                                std::string_view label) noexcept
{
    if (nullptr == client_)
    {
        return AsyncAdd<CounterPtr>::failed(make_error(ErrorCode::invalid_argument, "client is not connected"));
    }

    static constexpr std::uint8_t EMPTY_KEY = 0;
    static constexpr char EMPTY_LABEL[] = "";

    const std::uint8_t* key_data = key.empty() ? &EMPTY_KEY : key.data();
    const char* label_data = label.empty() ? EMPTY_LABEL : label.data();

    aeron_async_add_counter_t* async = nullptr;
    if (0 != aeron_async_add_counter(&async, client_.get(), type_id, key_data, key.size(), label_data, label.size()))
    {
        return AsyncAdd<CounterPtr>::failed(make_error(ErrorCode::internal_error, "async add counter failed"));
    }

    return AsyncAdd<CounterPtr>(
        client_.get(), async, [](aeron_counter_t** handle, void* resource)
        { return aeron_async_add_counter_poll(handle, static_cast<aeron_async_add_counter_t*>(resource)); },
        [](aeron_t* client, void* resource)
        { return aeron_async_add_counter_cancel(client, static_cast<aeron_async_add_counter_t*>(resource)); });
}

AsyncAdd<ExPubPtr> DeepmsgClient::add_exclusive_publication(std::string_view channel, std::int32_t stream_id) noexcept
{
    if (nullptr == client_)
    {
        return AsyncAdd<ExPubPtr>::failed(make_error(ErrorCode::invalid_argument, "client is not connected"));
    }

    std::array<char, MAX_CHANNEL_LENGTH> channel_buffer{};
    if (!copy_cstr(channel, channel_buffer))
    {
        return AsyncAdd<ExPubPtr>::failed(make_error(ErrorCode::config_error, "channel is too long"));
    }

    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (0 != aeron_async_add_exclusive_publication(&async, client_.get(), channel_buffer.data(), stream_id))
    {
        return AsyncAdd<ExPubPtr>::failed(
            make_error(ErrorCode::internal_error, "async add exclusive publication failed"));
    }

    return AsyncAdd<ExPubPtr>(
        client_.get(), async,
        [](aeron_exclusive_publication_t** handle, void* resource)
        {
            return aeron_async_add_exclusive_publication_poll(
                handle, static_cast<aeron_async_add_exclusive_publication_t*>(resource));
        },
        [](aeron_t* client, void* resource)
        {
            return aeron_async_add_exclusive_publication_cancel(
                client, static_cast<aeron_async_add_exclusive_publication_t*>(resource));
        });
}

AsyncAdd<PubPtr> DeepmsgClient::add_publication(std::string_view channel, std::int32_t stream_id) noexcept
{
    if (nullptr == client_)
    {
        return AsyncAdd<PubPtr>::failed(make_error(ErrorCode::invalid_argument, "client is not connected"));
    }

    std::array<char, MAX_CHANNEL_LENGTH> channel_buffer{};
    if (!copy_cstr(channel, channel_buffer))
    {
        return AsyncAdd<PubPtr>::failed(make_error(ErrorCode::config_error, "channel is too long"));
    }

    aeron_async_add_publication_t* async = nullptr;
    if (0 != aeron_async_add_publication(&async, client_.get(), channel_buffer.data(), stream_id))
    {
        return AsyncAdd<PubPtr>::failed(make_error(ErrorCode::internal_error, "async add publication failed"));
    }

    return AsyncAdd<PubPtr>(
        client_.get(), async, [](aeron_publication_t** handle, void* resource)
        { return aeron_async_add_publication_poll(handle, static_cast<aeron_async_add_publication_t*>(resource)); },
        [](aeron_t* client, void* resource)
        { return aeron_async_add_publication_cancel(client, static_cast<aeron_async_add_publication_t*>(resource)); });
}

AsyncAdd<SubPtr> DeepmsgClient::add_subscription(std::string_view channel, std::int32_t stream_id,
                                                 ImageHandlers handlers) noexcept
{
    if (nullptr == client_)
    {
        return AsyncAdd<SubPtr>::failed(make_error(ErrorCode::invalid_argument, "client is not connected"));
    }

    std::array<char, MAX_CHANNEL_LENGTH> channel_buffer{};
    if (!copy_cstr(channel, channel_buffer))
    {
        return AsyncAdd<SubPtr>::failed(make_error(ErrorCode::config_error, "channel is too long"));
    }

    aeron_async_add_subscription_t* async = nullptr;
    if (0 != aeron_async_add_subscription(&async, client_.get(), channel_buffer.data(), stream_id,
                                          handlers.on_available, handlers.on_available_clientd, handlers.on_unavailable,
                                          handlers.on_unavailable_clientd))
    {
        return AsyncAdd<SubPtr>::failed(make_error(ErrorCode::internal_error, "async add subscription failed"));
    }

    return AsyncAdd<SubPtr>(
        client_.get(), async, [](aeron_subscription_t** handle, void* resource)
        { return aeron_async_add_subscription_poll(handle, static_cast<aeron_async_add_subscription_t*>(resource)); },
        [](aeron_t* client, void* resource)
        {
            return aeron_async_add_subscription_cancel(client, static_cast<aeron_async_add_subscription_t*>(resource));
        });
}

}  // namespace axc::core
