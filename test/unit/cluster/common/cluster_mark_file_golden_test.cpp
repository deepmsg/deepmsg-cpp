// Golden criterion for C1b: `cluster-mark.dat` written by the reference Java
// implementation is read back field by field.

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#include "cluster/common/cluster_mark_file.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::ClusterComponentType;
using deepmsg::cluster::common::ClusterMarkFile;

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/cluster/mark-file";
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

[[nodiscard]] ClusterComponentType component_type_from_name(const std::string& name)
{
    if ("CONSENSUS_MODULE" == name)
    {
        return ClusterComponentType::consensus_module;
    }
    if ("CONTAINER" == name)
    {
        return ClusterComponentType::container;
    }
    if ("BACKUP" == name)
    {
        return ClusterComponentType::backup;
    }
    if ("STANDBY" == name)
    {
        return ClusterComponentType::standby;
    }
    return ClusterComponentType::unknown;
}

void check_case(const std::string& name)
{
    const std::string directory = golden_dir() + "/" + name;
    const auto expected = read_expected(directory);
    ASSERT_FALSE(expected.empty()) << "missing " << directory << "/expected.txt";

    auto mark_file = ClusterMarkFile::open_existing(directory + "/cluster-mark.dat", 0);
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    EXPECT_EQ(std::stoi(expected.at("semanticVersion")), mark_file->semantic_version());
    EXPECT_EQ(component_type_from_name(expected.at("componentType")), mark_file->component_type());
    EXPECT_EQ(std::stoll(expected.at("activityTimestamp")), mark_file->activity_timestamp_volatile());
    EXPECT_EQ(std::stoll(expected.at("startTimestamp")), mark_file->start_timestamp());
    EXPECT_EQ(std::stoll(expected.at("pid")), mark_file->pid());
    EXPECT_EQ(std::stoll(expected.at("candidateTermId")), mark_file->candidate_term_id());
    EXPECT_EQ(std::stoi(expected.at("archiveStreamId")), mark_file->archive_stream_id());
    EXPECT_EQ(std::stoi(expected.at("serviceStreamId")), mark_file->service_stream_id());
    EXPECT_EQ(std::stoi(expected.at("consensusModuleStreamId")), mark_file->consensus_module_stream_id());
    EXPECT_EQ(std::stoi(expected.at("ingressStreamId")), mark_file->ingress_stream_id());
    EXPECT_EQ(std::stoi(expected.at("memberId")), mark_file->member_id());
    EXPECT_EQ(std::stoi(expected.at("serviceId")), mark_file->service_id());
    EXPECT_EQ(std::stoi(expected.at("headerLength")), mark_file->header_length());
    EXPECT_EQ(std::stoi(expected.at("errorBufferLength")), mark_file->error_buffer_length());
    EXPECT_EQ(std::stoi(expected.at("clusterId")), mark_file->cluster_id());
    EXPECT_EQ(expected.at("aeronDirectory"), mark_file->aeron_directory());
    EXPECT_EQ(expected.at("controlChannel"), mark_file->control_channel());
    EXPECT_EQ(expected.at("ingressChannel"), mark_file->ingress_channel());
    EXPECT_EQ(expected.at("serviceName"), mark_file->service_name());
    EXPECT_EQ(expected.at("authenticator"), mark_file->authenticator());
    EXPECT_EQ(expected.at("servicesClusterDir"), mark_file->services_cluster_dir());
    EXPECT_EQ(std::stoull(expected.at("fileLength")), std::filesystem::file_size(directory + "/cluster-mark.dat"));
}

TEST(ClusterMarkFileGoldenTest, readsEveryFieldOfTheReferenceConsensusModuleMarkFile)
{
    check_case("consensus-module");
}

TEST(ClusterMarkFileGoldenTest, readsEveryFieldOfTheReferenceContainerMarkFile)
{
    check_case("container");
}

TEST(ClusterMarkFileGoldenTest, readsEveryFieldOfTheReferenceBackupMarkFile)
{
    check_case("backup");
}

}  // namespace
