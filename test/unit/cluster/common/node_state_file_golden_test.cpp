// Golden criterion for C1a: `node-state.dat` written by the reference Java
// implementation is both read back field by field and reproduced byte for byte
// by our writer.
//
// `test/golden/cluster/node-state/<case>/node-state.dat` was produced by
// `NodeStateGoldenGen` (reference Aeron 1.54.0); `expected.txt` is its dump.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <vector>

#include "cluster/common/node_state_file.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::NodeStateFile;

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/cluster/node-state";
}

[[nodiscard]] std::map<std::string, std::string> read_expected(const std::string& directory)
{
    std::map<std::string, std::string> values;
    std::ifstream input{directory + "/expected.txt"};
    std::string line;
    while (std::getline(input, line))
    {
        const std::size_t equals = line.find('=');
        if (std::string::npos == equals)
        {
            continue;
        }
        values[line.substr(0, equals)] = line.substr(equals + 1);
    }
    return values;
}

[[nodiscard]] std::vector<std::uint8_t> read_bytes(const std::string& path)
{
    std::ifstream input{path, std::ios::binary};
    return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

class TempDir
{
public:
    TempDir()
    {
        std::string pattern = "/tmp/axc-node-state-golden-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        EXPECT_NE(nullptr, created);
        directory_ = nullptr == created ? std::string{} : std::string{created};
    }

    ~TempDir()
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::string& path() const
    {
        return directory_;
    }

private:
    std::string directory_{};
};

void check_fields(const std::map<std::string, std::string>& expected, const NodeStateFile& file)
{
    const auto candidate_term = file.candidate_term();
    EXPECT_EQ(std::stoi(expected.at("semanticVersion")), file.semantic_version());
    EXPECT_EQ(std::stoll(expected.at("candidateTermId")), candidate_term.candidate_term_id);
    EXPECT_EQ(std::stoll(expected.at("timestamp")), candidate_term.timestamp);
    EXPECT_EQ(std::stoll(expected.at("logPosition")), candidate_term.log_position);
}

TEST(NodeStateFileGoldenTest, readsEveryFieldOfTheReferenceFreshFile)
{
    const std::string directory = golden_dir() + "/fresh";
    const auto expected = read_expected(directory);

    auto file = NodeStateFile::open(directory, false, 1);
    ASSERT_TRUE(file) << file.error().message.view();
    check_fields(expected, *file);
}

TEST(NodeStateFileGoldenTest, writesTheReferenceFreshFileByteForByte)
{
    const std::string directory = golden_dir() + "/fresh";
    const auto expected = read_expected(directory);
    TempDir temp;

    {
        auto file = NodeStateFile::open(temp.path(), true, 1);
        ASSERT_TRUE(file) << file.error().message.view();
        check_fields(expected, *file);
    }

    const auto ours = read_bytes(temp.path() + "/node-state.dat");
    const auto reference = read_bytes(directory + "/node-state.dat");
    ASSERT_EQ(reference.size(), ours.size());
    EXPECT_TRUE(reference == ours);
}

TEST(NodeStateFileGoldenTest, writesTheReferenceUpdatedFileByteForByte)
{
    const std::string directory = golden_dir() + "/updated";
    const auto expected = read_expected(directory);
    TempDir temp;

    {
        auto file = NodeStateFile::open(temp.path(), true, 1);
        ASSERT_TRUE(file) << file.error().message.view();

        auto updated = file->update_candidate_term_id(std::stoll(expected.at("candidateTermId")),
                                                      std::stoll(expected.at("logPosition")),
                                                      std::stoll(expected.at("timestamp")));
        ASSERT_TRUE(updated) << updated.error().message.view();
    }

    const auto ours = read_bytes(temp.path() + "/node-state.dat");
    const auto reference = read_bytes(directory + "/node-state.dat");
    ASSERT_EQ(reference.size(), ours.size());
    EXPECT_TRUE(reference == ours);
}

}  // namespace
