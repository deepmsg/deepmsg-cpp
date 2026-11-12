#include "cluster/service/session.h"

#include <utility>

#include "cluster/service/client_util.h"

namespace deepmsg::cluster::service
{
namespace
{

constexpr std::int64_t NULL_ID = -1;

}  // namespace

ContainerClientSession::ContainerClientSession(std::int64_t id, std::int32_t response_stream_id,
                                               std::string response_channel,
                                               std::vector<std::uint8_t> encoded_principal)
    : id_(id),
      response_stream_id_(response_stream_id),
      response_channel_(std::move(response_channel)),
      encoded_principal_(std::move(encoded_principal))
{
}

std::expected<void, Error> ContainerClientSession::connect(axc::core::DeepmsgClient& client)
{
    if (publication_.has_value())
    {
        return {};
    }

    auto add = client.add_publication(response_channel_, response_stream_id_);
    auto publication = await_async(client, add, EGRESS_COMMAND_TIMEOUT_NS);
    if (!publication)
    {
        return std::unexpected(publication.error());
    }

    publication_ = std::move(*publication);
    return {};
}

std::expected<void, Error> ContainerClientSession::disconnect() noexcept
{
    if (publication_.has_value())
    {
        publication_.reset();
    }
    return {};
}

SessionTable::SessionTable()
    : index_by_id_(axc::core::Long2LongHashMap::MIN_CAPACITY, axc::core::Long2LongHashMap::DEFAULT_LOAD_FACTOR, NULL_ID)
{
}

ContainerClientSession* SessionTable::get(std::int64_t id) noexcept
{
    const std::int64_t index = index_by_id_.get(id);
    if (index < 0 || static_cast<std::size_t>(index) >= sessions_.size())
    {
        return nullptr;
    }
    return &sessions_[static_cast<std::size_t>(index)];
}

const ContainerClientSession* SessionTable::get(std::int64_t id) const noexcept
{
    const std::int64_t index = index_by_id_.get(id);
    if (index < 0 || static_cast<std::size_t>(index) >= sessions_.size())
    {
        return nullptr;
    }
    return &sessions_[static_cast<std::size_t>(index)];
}

void SessionTable::add(ContainerClientSession session)
{
    const std::int64_t id = session.id();
    std::size_t position = 0;
    for (std::size_t i = sessions_.size(); i-- > 0;)
    {
        if (sessions_[i].id() < id)
        {
            position = i + 1;
            break;
        }
    }

    sessions_.insert(sessions_.begin() + static_cast<std::ptrdiff_t>(position), std::move(session));
    reindex_from(position);
}

std::optional<ContainerClientSession> SessionTable::remove(std::int64_t id)
{
    const std::int64_t index = index_by_id_.get(id);
    if (index < 0 || static_cast<std::size_t>(index) >= sessions_.size() ||
        sessions_[static_cast<std::size_t>(index)].id() != id)
    {
        return std::nullopt;
    }

    const auto position = static_cast<std::size_t>(index);
    ContainerClientSession session = std::move(sessions_[position]);
    sessions_.erase(sessions_.begin() + static_cast<std::ptrdiff_t>(position));
    reindex_from(position);
    return session;
}

void SessionTable::reindex_from(std::size_t position)
{
    for (std::size_t i = position; i < sessions_.size(); ++i)
    {
        (void)index_by_id_.put(sessions_[i].id(), static_cast<std::int64_t>(i));
    }
}

}  // namespace deepmsg::cluster::service
