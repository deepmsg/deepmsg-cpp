#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

#include "core/collections/long2long_hash_map.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must be defined by the build"
#endif

namespace
{

using axc::core::Long2LongHashMap;

std::string join_keys(const Long2LongHashMap& map)
{
    std::string result;
    map.for_each(
        [&result](std::int64_t key, std::int64_t)
        {
            if (!result.empty())
            {
                result.push_back(',');
            }
            result.append(std::to_string(key));
        });
    return result;
}

}  // namespace

TEST(Long2LongHashMap, AgronaIterationGolden)
{
    Long2LongHashMap map(8);

    std::ifstream ops(std::string(AXC_GOLDEN_DIR) + "/core/long2long-iteration/ops.tsv");
    std::ifstream expected(std::string(AXC_GOLDEN_DIR) + "/core/long2long-iteration/expected.txt");
    ASSERT_TRUE(ops.is_open());
    ASSERT_TRUE(expected.is_open());

    std::string line;
    std::string expected_line;
    int checkpoints = 0;

    while (std::getline(ops, line))
    {
        if (line.empty() || '#' == line[0])
        {
            continue;
        }

        std::istringstream fields(line);
        std::string op;
        fields >> op;

        if ("put" == op)
        {
            long long key = 0;
            long long value = 0;
            fields >> key >> value;
            map.put(key, value);
        }
        else if ("remove" == op)
        {
            long long key = 0;
            fields >> key;
            map.remove(key);
        }
        else if ("clear" == op)
        {
            map.clear();
        }
        else if ("hash" == op)
        {
            long long key = 0;
            fields >> key;
            ASSERT_TRUE(std::getline(expected, expected_line));
            EXPECT_EQ(expected_line, std::to_string(Long2LongHashMap::hash(key))) << "hash of " << key;
            ++checkpoints;
        }
        else if ("check" == op)
        {
            ASSERT_TRUE(std::getline(expected, expected_line));
            EXPECT_EQ(expected_line, join_keys(map)) << "after: " << line;
            ++checkpoints;
        }
        else
        {
            FAIL() << "unknown op: " << line;
        }
    }

    EXPECT_GT(checkpoints, 0);
    EXPECT_FALSE(std::getline(expected, expected_line)) << "extra expected line: " << expected_line;
}

TEST(Long2LongHashMap, PutGetRemove)
{
    Long2LongHashMap map;

    EXPECT_EQ(map.missing_value(), map.get(42));
    EXPECT_EQ(map.missing_value(), map.put(42, 100));
    EXPECT_EQ(100, map.get(42));
    EXPECT_EQ(100, map.put(42, 101));
    EXPECT_EQ(101, map.get(42));
    EXPECT_EQ(101, map.get(42, -7));
    EXPECT_EQ(-7, map.get(43, -7));
    EXPECT_TRUE(map.contains_key(42));

    EXPECT_TRUE(map.remove(42));
    EXPECT_FALSE(map.remove(42));
    EXPECT_EQ(map.missing_value(), map.get(42));
    EXPECT_TRUE(map.empty());
}

TEST(Long2LongHashMap, RemoveMatchingValue)
{
    Long2LongHashMap map;
    map.put(1, 10);

    EXPECT_FALSE(map.remove(1, 11));
    EXPECT_TRUE(map.contains_key(1));
    EXPECT_TRUE(map.remove(1, 10));
    EXPECT_FALSE(map.contains_key(1));
}

TEST(Long2LongHashMap, GrowsAndKeepsEveryKey)
{
    Long2LongHashMap map(8);
    for (int i = 0; i < 1000; ++i)
    {
        map.put(i, static_cast<std::int64_t>(i) * 3);
    }

    EXPECT_EQ(1000U, map.size());
    for (int i = 0; i < 1000; ++i)
    {
        EXPECT_EQ(static_cast<std::int64_t>(i) * 3, map.get(i));
    }
    EXPECT_GE(map.capacity(), 1024);
    EXPECT_EQ(static_cast<int>(static_cast<float>(map.capacity()) * Long2LongHashMap::DEFAULT_LOAD_FACTOR),
              map.resize_threshold());
}

TEST(Long2LongHashMap, ClearRemovesEverything)
{
    Long2LongHashMap map;
    map.put(1, 2);
    map.put(3, 4);
    map.clear();

    EXPECT_TRUE(map.empty());
    EXPECT_FALSE(map.contains_key(1));
    EXPECT_EQ("", join_keys(map));
}

TEST(Long2LongHashMap, RejectsMissingValue)
{
    Long2LongHashMap map(8, Long2LongHashMap::DEFAULT_LOAD_FACTOR, -1);

    EXPECT_EQ(map.missing_value(), map.put(1, map.missing_value()));
    EXPECT_TRUE(map.empty());
}
