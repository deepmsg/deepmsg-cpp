// Reference: `aeron-archive/src/test/java/io/aeron/archive/CatalogIndexTest.java`
// (Aeron 1.54.0, 454da1e4e6). Test names are the reference method names.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "archive/server/catalog_index.h"

namespace
{

using deepmsg::archive::server::CatalogIndex;

constexpr std::int64_t NULL_VALUE = CatalogIndex::NULL_VALUE;
constexpr int DEFAULT_INDEX_SIZE = CatalogIndex::DEFAULT_INDEX_SIZE;

TEST(CatalogIndexTest, defaultIndexCapacityIsTenEntries)
{
    const CatalogIndex catalog_index;
    ASSERT_EQ(static_cast<std::size_t>(DEFAULT_INDEX_SIZE << 1), catalog_index.index().size());
    for (const std::int64_t value : catalog_index.index())
    {
        EXPECT_EQ(0, value);
    }
}

TEST(CatalogIndexTest, sizeReturnsZeroForAnEmptyIndex)
{
    const CatalogIndex catalog_index;
    EXPECT_EQ(0, catalog_index.size());
}

TEST(CatalogIndexTest, addThrowsIllegalArgumentExceptionIfRecordingIdIsNegative)
{
    CatalogIndex catalog_index;
    EXPECT_THROW(catalog_index.add(-1, 0), std::invalid_argument);
}

TEST(CatalogIndexTest, addThrowsIllegalArgumentExceptionIfRecordingOffsetIsNegative)
{
    CatalogIndex catalog_index;
    EXPECT_THROW(catalog_index.add(1024, std::numeric_limits<std::int32_t>::min()), std::invalid_argument);
}

TEST(CatalogIndexTest, addOneRecording)
{
    CatalogIndex catalog_index;
    catalog_index.add(3, 100);

    EXPECT_EQ(3, catalog_index.index()[0]);
    EXPECT_EQ(100, catalog_index.index()[1]);
    EXPECT_EQ(1, catalog_index.size());
}

TEST(CatalogIndexTest, addAppendsToTheEndOfTheIndex)
{
    CatalogIndex catalog_index;
    catalog_index.add(6, 50);
    catalog_index.add(21, 64);

    EXPECT_EQ(6, catalog_index.index()[0]);
    EXPECT_EQ(50, catalog_index.index()[1]);
    EXPECT_EQ(21, catalog_index.index()[2]);
    EXPECT_EQ(64, catalog_index.index()[3]);
    EXPECT_EQ(2, catalog_index.size());
}

TEST(CatalogIndexTest,
     addAppendsThrowsIllegalArgumentExceptionIfNewRecordingIdIsLessThanOrEqualToTheExistingRecordingId)
{
    for (const std::int64_t recording_id : {5, 100})
    {
        CatalogIndex catalog_index;
        catalog_index.add(100, 0);

        try
        {
            catalog_index.add(recording_id, 200);
            FAIL() << "expected std::invalid_argument";
        }
        catch (const std::invalid_argument& exception)
        {
            EXPECT_STREQ(
                ("recordingId " + std::to_string(recording_id) + " is less than or equal to the last recordingId 100")
                    .c_str(),
                exception.what());
        }
    }
}

TEST(CatalogIndexTest, addExpandsIndexWhenFull)
{
    CatalogIndex catalog_index;
    const auto expected_length = static_cast<std::size_t>((DEFAULT_INDEX_SIZE + (DEFAULT_INDEX_SIZE >> 1)) << 1);
    std::vector<std::int64_t> expected(expected_length, 0);

    int position = 0;
    for (int index = 0; index < DEFAULT_INDEX_SIZE; index++, position += 2)
    {
        expected[static_cast<std::size_t>(position)] = index;
        expected[static_cast<std::size_t>(position) + 1] = index;
        catalog_index.add(index, index);
    }
    expected[static_cast<std::size_t>(position)] = std::numeric_limits<std::int64_t>::max();
    expected[static_cast<std::size_t>(position) + 1] = std::numeric_limits<std::int64_t>::max();
    catalog_index.add(std::numeric_limits<std::int64_t>::max(), std::numeric_limits<std::int64_t>::max());

    ASSERT_EQ(expected_length, catalog_index.index().size());
    for (std::size_t index = 0; index < expected_length; index++)
    {
        EXPECT_EQ(expected[index], catalog_index.index()[index]) << "index " << index;
    }
    EXPECT_EQ(DEFAULT_INDEX_SIZE + 1, catalog_index.size());
}

TEST(CatalogIndexTest, getThrowsIllegalArgumentExceptionIfRecordingIdIsNegative)
{
    const CatalogIndex catalog_index;
    EXPECT_THROW(static_cast<void>(catalog_index.recording_offset(-100)), std::invalid_argument);
}

TEST(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex)
{
    const CatalogIndex catalog_index;
    for (const std::int64_t recording_id :
         {std::int64_t{0}, std::int64_t{1}, std::int64_t{DEFAULT_INDEX_SIZE}, std::int64_t{DEFAULT_INDEX_SIZE << 1},
          std::int64_t{100}, std::numeric_limits<std::int64_t>::max()})
    {
        EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(recording_id));
    }
}

TEST(CatalogIndexTest, getReturnsNullValueIfRecordingIdIsNotFoundSingleEntry)
{
    CatalogIndex catalog_index;
    catalog_index.add(200, 0);

    for (const std::int64_t recording_id :
         {std::int64_t{0}, std::int64_t{199}, std::int64_t{201}, std::numeric_limits<std::int64_t>::max()})
    {
        EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(recording_id));
    }
}

TEST(CatalogIndexTest, getReturnsNullValueIfRecordingIdIsNotFoundMultipleEntries)
{
    CatalogIndex catalog_index;
    for (std::int64_t id = 10; id < 100; id++)
    {
        catalog_index.add(id, id);
    }

    for (const std::int64_t recording_id : {std::int64_t{9}, std::int64_t{101}, std::int64_t{1'000'000'000},
                                            std::int64_t{4'002'662'252}, std::numeric_limits<std::int64_t>::max()})
    {
        EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(recording_id));
    }
}

TEST(CatalogIndexTest, getReturnsOffsetAssociatedWithRecordingId)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 0);
    catalog_index.add(1, 1);
    catalog_index.add(2, 3);
    catalog_index.add(100, 777);
    catalog_index.add(1000, 2000);
    catalog_index.add(1001, 2002);

    EXPECT_EQ(0, catalog_index.recording_offset(0));
    EXPECT_EQ(1, catalog_index.recording_offset(1));
    EXPECT_EQ(3, catalog_index.recording_offset(2));
    EXPECT_EQ(777, catalog_index.recording_offset(100));
    EXPECT_EQ(2000, catalog_index.recording_offset(1000));
    EXPECT_EQ(2002, catalog_index.recording_offset(1001));
}

TEST(CatalogIndexTest, getSingleElement)
{
    for (const std::int64_t recording_id :
         {std::int64_t{0}, std::int64_t{100}, std::numeric_limits<std::int64_t>::max()})
    {
        CatalogIndex catalog_index;
        catalog_index.add(recording_id, recording_id * 3);
        EXPECT_EQ(recording_id * 3, catalog_index.recording_offset(recording_id));
    }
}

TEST(CatalogIndexTest, getFirstElement)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 10);
    catalog_index.add(1, 20);
    catalog_index.add(2, 30);

    EXPECT_EQ(10, catalog_index.recording_offset(0));
}

TEST(CatalogIndexTest, getLastElement)
{
    CatalogIndex catalog_index;
    catalog_index.add(1, 10);
    catalog_index.add(5, 20);
    catalog_index.add(12, 30);

    EXPECT_EQ(30, catalog_index.recording_offset(12));
}

TEST(CatalogIndexTest, getLastElementWhenIndexIsFull)
{
    // The reference draws ten distinct sorted ids from `new Random(8327434)`;
    // the shape (ten large distinct sorted ids, lookup of the last) is what the
    // test pins, so fixed ids replace the RNG.
    constexpr std::int64_t values[DEFAULT_INDEX_SIZE] = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89};

    CatalogIndex catalog_index;
    for (int index = 0; index < DEFAULT_INDEX_SIZE; index++)
    {
        catalog_index.add(values[index], index + 1);
    }

    EXPECT_EQ(DEFAULT_INDEX_SIZE, catalog_index.recording_offset(values[DEFAULT_INDEX_SIZE - 1]));
}

TEST(CatalogIndexTest, removeThrowsIllegalArgumentExceptionIfNegativeRecordingIdIsProvided)
{
    CatalogIndex catalog_index;
    EXPECT_THROW(catalog_index.remove(-1), std::invalid_argument);
}

TEST(CatalogIndexTest, removeReturnsNullValueWhenIndexIsEmpty)
{
    CatalogIndex catalog_index;
    EXPECT_EQ(NULL_VALUE, catalog_index.remove(1));
}

TEST(CatalogIndexTest, removeReturnsNullValueWhenNonExistingRecordingIdIsProvided)
{
    CatalogIndex catalog_index;
    catalog_index.add(1, 0);
    catalog_index.add(20, 10);

    EXPECT_EQ(NULL_VALUE, catalog_index.remove(7));
    EXPECT_EQ(2, catalog_index.size());
}

TEST(CatalogIndexTest, removeTheOnlyEntryFromTheIndex)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 1024);

    EXPECT_EQ(1024, catalog_index.remove(0));
    EXPECT_EQ(0, catalog_index.size());
    EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(0));
}

TEST(CatalogIndexTest, removeFirstElement)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 500);
    catalog_index.add(1, 1000);
    catalog_index.add(2, 1500);

    EXPECT_EQ(500, catalog_index.remove(0));
    EXPECT_EQ(2, catalog_index.size());
    EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(0));
    EXPECT_EQ(1000, catalog_index.recording_offset(1));
    EXPECT_EQ(1500, catalog_index.recording_offset(2));
}

TEST(CatalogIndexTest, removeLastElement)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 500);
    catalog_index.add(1, 1000);
    catalog_index.add(2, 1500);

    EXPECT_EQ(1500, catalog_index.remove(2));
    EXPECT_EQ(2, catalog_index.size());
    EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(2));
    EXPECT_EQ(500, catalog_index.recording_offset(0));
    EXPECT_EQ(1000, catalog_index.recording_offset(1));
}

TEST(CatalogIndexTest, removeMiddleElement)
{
    CatalogIndex catalog_index;
    catalog_index.add(0, 500);
    catalog_index.add(10, 1000);
    catalog_index.add(20, 1500);
    catalog_index.add(30, 7777);

    EXPECT_EQ(1000, catalog_index.remove(10));
    EXPECT_EQ(3, catalog_index.size());
    EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(10));
    EXPECT_EQ(500, catalog_index.recording_offset(0));
    EXPECT_EQ(1500, catalog_index.recording_offset(20));
    EXPECT_EQ(7777, catalog_index.recording_offset(30));
}

TEST(CatalogIndexTest, removeLastElementFromTheFullIndex)
{
    CatalogIndex catalog_index;
    for (std::int64_t index = 1; index <= DEFAULT_INDEX_SIZE; index++)
    {
        catalog_index.add(index, index);
    }

    EXPECT_EQ(DEFAULT_INDEX_SIZE, catalog_index.remove(DEFAULT_INDEX_SIZE));
    EXPECT_EQ(DEFAULT_INDEX_SIZE - 1, catalog_index.size());
    EXPECT_EQ(NULL_VALUE, catalog_index.recording_offset(DEFAULT_INDEX_SIZE));
}

}  // namespace
