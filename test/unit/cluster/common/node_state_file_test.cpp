// Port of `NodeStateFileTest` (Aeron 1.54.0, `454da1e4e6`) plus byte-layout
// criteria for the node-state record framing.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "cluster/common/cluster_versions.h"
#include "cluster/common/node_state_file.h"
#include "core/io/mapped_file.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::CLUSTER_MARK_MAJOR_VERSION;
using deepmsg::cluster::common::CLUSTER_MARK_SEMANTIC_VERSION;
using deepmsg::cluster::common::compose_semantic_version;
using deepmsg::cluster::common::NodeStateFile;

constexpr std::int64_t V_1_42_X_CANDIDATE_TERM_ID = 982374;
constexpr std::int64_t V_1_42_X_LOG_POSITION = 89723648762342LL;
constexpr std::int64_t V_1_42_X_TIMESTAMP_MS = 9878967687234LL;

class NodeStateFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-node-state-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::string path() const
    {
        return directory_ + "/node-state.dat";
    }

    const std::int32_t sync_level_{1};
    std::string directory_{};
};

TEST_F(NodeStateFileTest, shouldFailIfCreateNewFalseAndFileDoesNotExist)
{
    auto file = NodeStateFile::open(directory_, false, sync_level_);
    EXPECT_FALSE(file);
}

TEST_F(NodeStateFileTest, shouldCreateIfCreateNewTrueAndFileDoesNotExist)
{
    EXPECT_TRUE(std::filesystem::is_empty(directory_));

    {
        auto file = NodeStateFile::open(directory_, true, sync_level_);
        ASSERT_TRUE(file) << file.error().message.view();
        EXPECT_TRUE(std::filesystem::exists(path()));
        EXPECT_EQ(NodeStateFile::MINIMUM_FILE_LENGTH, std::filesystem::file_size(path()));
    }

    EXPECT_TRUE(std::filesystem::exists(path()));
}

TEST_F(NodeStateFileTest, shouldHaveNullCandidateTermIdOnInitialCreation)
{
    auto file = NodeStateFile::open(directory_, true, sync_level_);
    ASSERT_TRUE(file) << file.error().message.view();
    EXPECT_EQ(-1, file->candidate_term().candidate_term_id);
}

TEST_F(NodeStateFileTest, shouldPersistCandidateTermId)
{
    constexpr std::int64_t candidate_term_id = 832234;
    constexpr std::int64_t timestamp_ms = 324234;
    constexpr std::int64_t log_position = 8923423;

    {
        auto file = NodeStateFile::open(directory_, true, sync_level_);
        ASSERT_TRUE(file) << file.error().message.view();
        auto updated = file->update_candidate_term_id(candidate_term_id, log_position, timestamp_ms);
        ASSERT_TRUE(updated) << updated.error().message.view();
    }

    auto file = NodeStateFile::open(directory_, false, sync_level_);
    ASSERT_TRUE(file) << file.error().message.view();
    const auto candidate_term = file->candidate_term();
    EXPECT_EQ(candidate_term_id, candidate_term.candidate_term_id);
    EXPECT_EQ(timestamp_ms, candidate_term.timestamp);
    EXPECT_EQ(log_position, candidate_term.log_position);
}

TEST_F(NodeStateFileTest, shouldThrowIfVersionMismatch)
{
    {
        auto file = NodeStateFile::open(directory_, true, sync_level_);
        ASSERT_TRUE(file) << file.error().message.view();
    }

    const std::int32_t invalid_version = compose_semantic_version(CLUSTER_MARK_MAJOR_VERSION + 1, 3, 0);
    {
        auto mapped = axc::core::MappedFile::open(path(), true);
        ASSERT_TRUE(mapped) << mapped.error().message.view();
        auto mapping = mapped->mapping();
        std::memcpy(mapping.data(), &invalid_version, sizeof(invalid_version));
        auto flushed = mapped->flush();
        ASSERT_TRUE(flushed) << flushed.error().message.view();
    }

    auto file = NodeStateFile::open(directory_, false, sync_level_);
    EXPECT_FALSE(file);
}

TEST_F(NodeStateFileTest, shouldProposeNewMaxTermId)
{
    std::int64_t next_candidate_term_id = 0;
    {
        auto file = NodeStateFile::open(directory_, true, sync_level_);
        ASSERT_TRUE(file) << file.error().message.view();

        auto updated = file->update_candidate_term_id(5, 10, 10);
        ASSERT_TRUE(updated) << updated.error().message.view();

        next_candidate_term_id = file->candidate_term().candidate_term_id + 1;
        auto proposed = file->propose_max_candidate_term_id(next_candidate_term_id, 20, 20);
        ASSERT_TRUE(proposed) << proposed.error().message.view();
        EXPECT_EQ(next_candidate_term_id, *proposed);

        const std::int64_t too_low_candidate_term_id = file->candidate_term().candidate_term_id - 1;
        auto rejected = file->propose_max_candidate_term_id(too_low_candidate_term_id, 30, 30);
        ASSERT_TRUE(rejected) << rejected.error().message.view();
        EXPECT_EQ(next_candidate_term_id, *rejected);
    }

    auto file = NodeStateFile::open(directory_, false, sync_level_);
    ASSERT_TRUE(file) << file.error().message.view();
    EXPECT_EQ(next_candidate_term_id, file->candidate_term().candidate_term_id);
}

TEST_F(NodeStateFileTest, shouldHandleNodeStateFileCreatedWithEarlierVersion)
{
    const std::string golden = std::string(AXC_GOLDEN_DIR) + "/cluster/node-state/v1_42_x/node-state.dat";
    std::error_code error;
    std::filesystem::copy_file(golden, path(), std::filesystem::copy_options::overwrite_existing, error);
    ASSERT_FALSE(error) << error.message();

    constexpr std::int64_t candidate_term_id = 10;
    constexpr std::int64_t log_position = 20;
    constexpr std::int64_t timestamp_ms = 30;
    constexpr std::int64_t new_candidate_term_id = candidate_term_id + 100;

    auto file = NodeStateFile::open(directory_, false, sync_level_);
    ASSERT_TRUE(file) << file.error().message.view();

    auto candidate_term = file->candidate_term();
    EXPECT_EQ(V_1_42_X_CANDIDATE_TERM_ID, candidate_term.candidate_term_id);
    EXPECT_EQ(V_1_42_X_LOG_POSITION, candidate_term.log_position);
    EXPECT_EQ(V_1_42_X_TIMESTAMP_MS, candidate_term.timestamp);

    auto updated = file->update_candidate_term_id(candidate_term_id, log_position, timestamp_ms);
    ASSERT_TRUE(updated) << updated.error().message.view();
    candidate_term = file->candidate_term();
    EXPECT_EQ(candidate_term_id, candidate_term.candidate_term_id);
    EXPECT_EQ(log_position, candidate_term.log_position);
    EXPECT_EQ(timestamp_ms, candidate_term.timestamp);

    auto proposed = file->propose_max_candidate_term_id(new_candidate_term_id, log_position, timestamp_ms);
    ASSERT_TRUE(proposed) << proposed.error().message.view();
    EXPECT_EQ(new_candidate_term_id, *proposed);
    EXPECT_EQ(new_candidate_term_id, file->candidate_term().candidate_term_id);
}

TEST_F(NodeStateFileTest, shouldWriteTheReferenceRecordLayout)
{
    {
        auto file = NodeStateFile::open(directory_, true, sync_level_);
        ASSERT_TRUE(file) << file.error().message.view();
        EXPECT_EQ(CLUSTER_MARK_SEMANTIC_VERSION, file->semantic_version());
    }

    std::ifstream input{path(), std::ios::binary};
    ASSERT_TRUE(input.is_open());
    std::vector<std::uint8_t> bytes(64);
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    ASSERT_EQ(static_cast<std::streamsize>(bytes.size()), input.gcount());

    const auto i32 = [&bytes](std::size_t offset)
    {
        std::int32_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };
    const auto u16 = [&bytes](std::size_t offset)
    {
        std::uint16_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };
    const auto i64 = [&bytes](std::size_t offset)
    {
        std::int64_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };

    // NodeStateHeader body (`aeron-cluster-node-state-codecs.xml:13-20`).
    EXPECT_EQ(CLUSTER_MARK_SEMANTIC_VERSION, i32(0));
    EXPECT_EQ(0, i32(4));

    // CandidateTerm frame at 8: 16-byte header + 24-byte body.
    EXPECT_EQ(24, u16(8));
    EXPECT_EQ(301, u16(10));
    EXPECT_EQ(112, u16(12));
    EXPECT_EQ(10, u16(14));
    EXPECT_EQ(40, i32(16));
    EXPECT_EQ(0, i32(20));
    EXPECT_EQ(-1, i64(24));
    EXPECT_EQ(-1, i64(32));
    EXPECT_EQ(-1, i64(40));

    // Footer frame at 48: 16-byte header, no body.
    EXPECT_EQ(0, u16(48));
    EXPECT_EQ(304, u16(50));
    EXPECT_EQ(112, u16(52));
    EXPECT_EQ(10, u16(54));
    EXPECT_EQ(16, i32(56));
    EXPECT_EQ(0, i32(60));
}

}  // namespace
