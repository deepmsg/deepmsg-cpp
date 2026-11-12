#pragma once

#include <array>
#include <cstdint>
#include <span>

#include "cluster/service/cluster.h"
#include "cluster/service/clustered_service.h"
#include "cluster/service/snapshot.h"

namespace deepmsg::apps
{

/// `BasicAuctionClusteredService` (`aeron-samples/.../cluster/tutorial/`), byte
/// for byte. It exists as the twin: the same log processed by this business and
/// by the Java one must produce the same snapshot bytes.
///
/// | record | layout |
/// |---|---|
/// | bid (in) | `correlationId i64 @0`, `customerId i64 @8`, `price i64 @16` — 24 B |
/// | egress (out) | `correlationId @0`, `winningCustomerId @8`, `bestPrice @16`, `bidSucceeded u8 @24` — 25 B |
/// | snapshot | `customerId @0`, `price @8` — 16 B |
class BasicAuctionService
{
public:
    static constexpr std::size_t CORRELATION_ID_OFFSET = 0;
    static constexpr std::size_t CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + 8;
    static constexpr std::size_t PRICE_OFFSET = CUSTOMER_ID_OFFSET + 8;
    static constexpr std::size_t BID_MESSAGE_LENGTH = PRICE_OFFSET + 8;
    static constexpr std::size_t BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
    static constexpr std::size_t EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + 1;
    static constexpr std::size_t SNAPSHOT_CUSTOMER_ID_OFFSET = 0;
    static constexpr std::size_t SNAPSHOT_PRICE_OFFSET = SNAPSHOT_CUSTOMER_ID_OFFSET + 8;
    static constexpr std::size_t SNAPSHOT_MESSAGE_LENGTH = SNAPSHOT_PRICE_OFFSET + 8;

    std::expected<void, deepmsg::cluster::service::Error> on_start(deepmsg::cluster::service::Cluster&,
                                                                   deepmsg::cluster::service::SnapshotImage*);
    std::expected<void, deepmsg::cluster::service::Error> on_session_open(deepmsg::cluster::service::ClientSession&,
                                                                          std::int64_t);
    std::expected<void, deepmsg::cluster::service::Error> on_session_close(deepmsg::cluster::service::ClientSession&,
                                                                           std::int64_t,
                                                                           deepmsg::cluster::service::CloseReason);
    std::expected<void, deepmsg::cluster::service::Error> on_session_message(
        deepmsg::cluster::service::Cluster&, deepmsg::cluster::service::ClientSession*, std::int64_t,
        std::span<const std::uint8_t>, const deepmsg::cluster::service::LogHeader&);
    std::expected<void, deepmsg::cluster::service::Error> on_timer_event(deepmsg::cluster::service::Cluster&,
                                                                         std::int64_t, std::int64_t);
    std::expected<void, deepmsg::cluster::service::Error> on_take_snapshot(
        deepmsg::cluster::service::SnapshotPublication&);
    std::expected<void, deepmsg::cluster::service::Error> on_role_change(deepmsg::cluster::service::Role);
    std::expected<void, deepmsg::cluster::service::Error> on_terminate(deepmsg::cluster::service::Cluster&);

    [[nodiscard]] std::int64_t best_price() const noexcept
    {
        return best_price_;
    }

    [[nodiscard]] std::int64_t winning_customer_id() const noexcept
    {
        return winning_customer_id_;
    }

    /// `auction.attemptBid`: strictly higher wins.
    [[nodiscard]] bool attempt_bid(std::int64_t price, std::int64_t customer_id) noexcept;

    /// `loadInitialState` (used by the snapshot loader and the unit tests).
    void set_state(std::int64_t price, std::int64_t customer_id) noexcept
    {
        best_price_ = price;
        winning_customer_id_ = customer_id;
    }

    [[nodiscard]] std::array<std::uint8_t, EGRESS_MESSAGE_LENGTH> egress_message(std::int64_t correlation_id,
                                                                                 bool bid_succeeded) const noexcept;

    [[nodiscard]] std::array<std::uint8_t, SNAPSHOT_MESSAGE_LENGTH> snapshot_message() const noexcept;

private:
    using Cluster = deepmsg::cluster::service::Cluster;
    using ClientSession = deepmsg::cluster::service::ClientSession;
    using Error = deepmsg::cluster::service::Error;

    [[nodiscard]] std::expected<void, Error> load_snapshot(Cluster& cluster,
                                                           deepmsg::cluster::service::SnapshotImage& image);
    [[nodiscard]] static std::expected<void, Error> offer_to_session(Cluster& cluster,
                                                                     deepmsg::cluster::service::SessionId session,
                                                                     std::span<const std::uint8_t> payload);

    std::int64_t best_price_{0};
    std::int64_t winning_customer_id_{-1};
};

}  // namespace deepmsg::apps
