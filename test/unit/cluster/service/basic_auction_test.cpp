// C2b's business twin: the reference's record layouts and bid rule, without a
// driver or a container.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

#include "apps/services/basic-auction-service/basic_auction_service.h"

namespace
{

using deepmsg::apps::BasicAuctionService;

TEST(BasicAuctionServiceTest, aBidMustBeStrictlyHigherToWin)
{
    BasicAuctionService auction;
    EXPECT_FALSE(auction.attempt_bid(0, 1));
    EXPECT_TRUE(auction.attempt_bid(1, 1));
    EXPECT_FALSE(auction.attempt_bid(1, 2));
    EXPECT_TRUE(auction.attempt_bid(2, 2));
    EXPECT_EQ(2, auction.best_price());
    EXPECT_EQ(2, auction.winning_customer_id());
}

TEST(BasicAuctionServiceTest, theRecordsAreTheReferenceLayout)
{
    EXPECT_EQ(24U, BasicAuctionService::BID_MESSAGE_LENGTH);
    EXPECT_EQ(25U, BasicAuctionService::EGRESS_MESSAGE_LENGTH);
    EXPECT_EQ(16U, BasicAuctionService::SNAPSHOT_MESSAGE_LENGTH);

    BasicAuctionService auction;
    ASSERT_TRUE(auction.attempt_bid(1'000, 3));
    const auto egress = auction.egress_message(7, true);
    std::int64_t correlation_id = 0;
    std::int64_t customer_id = 0;
    std::int64_t price = 0;
    std::memcpy(&correlation_id, egress.data(), sizeof(correlation_id));
    std::memcpy(&customer_id, egress.data() + 8, sizeof(customer_id));
    std::memcpy(&price, egress.data() + 16, sizeof(price));
    EXPECT_EQ(7, correlation_id);
    EXPECT_EQ(3, customer_id);
    EXPECT_EQ(1'000, price);
    EXPECT_EQ(1, egress[24]);

    auction.set_state(42, 9);
    const auto snapshot = auction.snapshot_message();
    std::int64_t snapshot_customer = 0;
    std::int64_t snapshot_price = 0;
    std::memcpy(&snapshot_customer, snapshot.data(), sizeof(snapshot_customer));
    std::memcpy(&snapshot_price, snapshot.data() + 8, sizeof(snapshot_price));
    EXPECT_EQ(9, snapshot_customer);
    EXPECT_EQ(42, snapshot_price);
}

}  // namespace
