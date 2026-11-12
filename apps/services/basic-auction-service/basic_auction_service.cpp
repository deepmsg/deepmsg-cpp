#include "apps/services/basic-auction-service/basic_auction_service.h"

#include <cstring>

namespace deepmsg::apps
{
namespace
{

class StateLoader final : public deepmsg::cluster::service::SnapshotFragmentHandler
{
public:
    explicit StateLoader(BasicAuctionService& service) : service_(service) {}

    void on_snapshot_fragment(const std::uint8_t* buffer, std::size_t length) override
    {
        if (length < BasicAuctionService::SNAPSHOT_MESSAGE_LENGTH)
        {
            return;
        }

        std::int64_t customer_id = -1;
        std::int64_t price = 0;
        std::memcpy(&customer_id, buffer + BasicAuctionService::SNAPSHOT_CUSTOMER_ID_OFFSET, sizeof(customer_id));
        std::memcpy(&price, buffer + BasicAuctionService::SNAPSHOT_PRICE_OFFSET, sizeof(price));
        service_.set_state(price, customer_id);
        loaded_ = true;
    }

    [[nodiscard]] bool loaded() const noexcept
    {
        return loaded_;
    }

private:
    BasicAuctionService& service_;
    bool loaded_{false};
};

}  // namespace

bool BasicAuctionService::attempt_bid(std::int64_t price, std::int64_t customer_id) noexcept
{
    if (price <= best_price_)
    {
        return false;
    }
    best_price_ = price;
    winning_customer_id_ = customer_id;
    return true;
}

std::array<std::uint8_t, BasicAuctionService::EGRESS_MESSAGE_LENGTH> BasicAuctionService::egress_message(
    std::int64_t correlation_id, bool bid_succeeded) const noexcept
{
    std::array<std::uint8_t, EGRESS_MESSAGE_LENGTH> buffer{};
    std::memcpy(buffer.data() + CORRELATION_ID_OFFSET, &correlation_id, sizeof(correlation_id));

    const std::int64_t customer = winning_customer_id_;
    const std::int64_t price = best_price_;
    std::memcpy(buffer.data() + CUSTOMER_ID_OFFSET, &customer, sizeof(customer));
    std::memcpy(buffer.data() + PRICE_OFFSET, &price, sizeof(price));
    buffer[BID_SUCCEEDED_OFFSET] = bid_succeeded ? 1 : 0;
    return buffer;
}

std::array<std::uint8_t, BasicAuctionService::SNAPSHOT_MESSAGE_LENGTH> BasicAuctionService::snapshot_message()
    const noexcept
{
    std::array<std::uint8_t, SNAPSHOT_MESSAGE_LENGTH> buffer{};
    std::memcpy(buffer.data() + SNAPSHOT_CUSTOMER_ID_OFFSET, &winning_customer_id_, sizeof(winning_customer_id_));
    std::memcpy(buffer.data() + SNAPSHOT_PRICE_OFFSET, &best_price_, sizeof(best_price_));
    return buffer;
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::load_snapshot(
    Cluster& cluster, deepmsg::cluster::service::SnapshotImage& image)
{
    StateLoader loader{*this};
    while (!image.is_end_of_stream() && !loader.loaded())
    {
        const int fragments = image.poll(loader, 1);
        if (fragments < 0)
        {
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::internal_error, "the snapshot image poll failed"));
        }
        if (loader.loaded())
        {
            break;
        }
        auto idled = cluster.idle(fragments);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
    }

    if (!loader.loaded())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                     "the basic auction's snapshot carried no state"));
    }
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_start(
    Cluster& cluster, deepmsg::cluster::service::SnapshotImage* snapshot)
{
    if (nullptr != snapshot)
    {
        return load_snapshot(cluster, *snapshot);
    }
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_session_open(
    deepmsg::cluster::service::ClientSession&, std::int64_t)
{
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_session_close(
    deepmsg::cluster::service::ClientSession&, std::int64_t, deepmsg::cluster::service::CloseReason)
{
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::offer_to_session(
    Cluster& cluster, deepmsg::cluster::service::SessionId session, std::span<const std::uint8_t> payload)
{
    for (int attempt = 0; attempt < 3; ++attempt)
    {
        auto view = cluster.session(session);
        if (!view.has_value())
        {
            return {};
        }
        auto offered = view->offer(payload);
        if (!offered)
        {
            return std::unexpected(offered.error());
        }
        if (*offered > 0)
        {
            return {};
        }
        auto idled = cluster.idle(1);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }
    }
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_session_message(
    Cluster& cluster, deepmsg::cluster::service::ClientSession* session, std::int64_t,
    std::span<const std::uint8_t> payload, const deepmsg::cluster::service::LogHeader&)
{
    if (payload.size() < BID_MESSAGE_LENGTH)
    {
        return {};
    }

    std::int64_t correlation_id = 0;
    std::int64_t customer_id = 0;
    std::int64_t price = 0;
    std::memcpy(&correlation_id, payload.data() + CORRELATION_ID_OFFSET, sizeof(correlation_id));
    std::memcpy(&customer_id, payload.data() + CUSTOMER_ID_OFFSET, sizeof(customer_id));
    std::memcpy(&price, payload.data() + PRICE_OFFSET, sizeof(price));

    const bool bid_succeeded = attempt_bid(price, customer_id);
    if (nullptr != session)
    {
        const auto egress = egress_message(correlation_id, bid_succeeded);
        return offer_to_session(cluster, session->id(), egress);
    }
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_timer_event(Cluster&, std::int64_t,
                                                                                    std::int64_t)
{
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_take_snapshot(
    deepmsg::cluster::service::SnapshotPublication& writer)
{
    const auto record = snapshot_message();
    while (true)
    {
        const std::int64_t position = writer.offer(record);
        if (position > 0)
        {
            return {};
        }
        if (deepmsg::cluster::service::OFFER_BACK_PRESSURED == position ||
            deepmsg::cluster::service::OFFER_ADMIN_ACTION == position)
        {
            writer.idle();
            continue;
        }
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                  "the snapshot publication refused the auction state: " + std::to_string(position)));
    }
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_role_change(deepmsg::cluster::service::Role)
{
    return {};
}

std::expected<void, BasicAuctionService::Error> BasicAuctionService::on_terminate(deepmsg::cluster::service::Cluster&)
{
    return {};
}

}  // namespace deepmsg::apps
