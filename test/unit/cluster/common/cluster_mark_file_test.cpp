// Port of the offline `ClusterMarkFileTest` behaviours (Aeron 1.54.0) plus the
// layout, takeover and error-buffer criteria for C1b.

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/cluster_versions.h"

namespace
{

using deepmsg::cluster::common::ClusterComponentType;
using deepmsg::cluster::common::ClusterMarkFile;

constexpr std::int32_t PAGE_MIN_SIZE = 4096;
constexpr std::int32_t NULL_VALUE = -1;

class ClusterMarkFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-cluster-mark-XXXXXX";
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
        return directory_ + "/cluster-mark.dat";
    }

    [[nodiscard]] ClusterMarkFile::Options options(
        ClusterComponentType type = ClusterComponentType::consensus_module, std::int64_t now_ms = 123456,
        std::int32_t error_buffer_length = ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH) const
    {
        ClusterMarkFile::Options options;
        options.path = path();
        options.component_type = type;
        options.error_buffer_length = error_buffer_length;
        options.timeout_ms = 1000;
        options.file_page_size = PAGE_MIN_SIZE;
        options.now_ms = now_ms;
        return options;
    }

    std::string directory_{};
};

TEST_F(ClusterMarkFileTest, throwsExceptionIfErrorBufferLengthIsInvalid)
{
    for (const std::int32_t error_buffer_length :
         {INT32_MIN, -100, ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH - 1, ClusterMarkFile::ERROR_BUFFER_MAX_LENGTH + 1})
    {
        auto mark_file = ClusterMarkFile::create_or_open(
            options(ClusterComponentType::consensus_module, 123456, error_buffer_length));
        ASSERT_FALSE(mark_file);
        EXPECT_EQ("Invalid errorBufferLength: " + std::to_string(error_buffer_length),
                  mark_file.error().message.view());
    }
}

TEST_F(ClusterMarkFileTest, shouldCreateNewMarkFile)
{
    for (const ClusterComponentType component_type :
         {ClusterComponentType::unknown, ClusterComponentType::consensus_module, ClusterComponentType::container,
          ClusterComponentType::backup, ClusterComponentType::standby})
    {
        std::error_code error;
        std::filesystem::remove(path(), error);

        auto mark_file = ClusterMarkFile::create_or_open(options(component_type));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();

        EXPECT_TRUE(std::filesystem::exists(path()));
        EXPECT_EQ(
            static_cast<std::uintmax_t>(ClusterMarkFile::HEADER_LENGTH + ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH),
            std::filesystem::file_size(path()));

        auto ready = mark_file->signal_ready(123456);
        ASSERT_TRUE(ready) << ready.error().message.view();

        EXPECT_EQ(deepmsg::cluster::common::CLUSTER_MARK_SEMANTIC_VERSION, mark_file->semantic_version());
        EXPECT_EQ(component_type, mark_file->component_type());
        EXPECT_EQ(123456, mark_file->activity_timestamp_volatile());
        EXPECT_EQ(123456, mark_file->start_timestamp());
        EXPECT_EQ(static_cast<std::int64_t>(::getpid()), mark_file->pid());
        EXPECT_EQ(NULL_VALUE, mark_file->candidate_term_id());
        EXPECT_EQ(0, mark_file->archive_stream_id());
        EXPECT_EQ(0, mark_file->service_stream_id());
        EXPECT_EQ(0, mark_file->consensus_module_stream_id());
        EXPECT_EQ(0, mark_file->ingress_stream_id());
        EXPECT_EQ(0, mark_file->member_id());
        EXPECT_EQ(0, mark_file->service_id());
        EXPECT_EQ(ClusterMarkFile::HEADER_LENGTH, mark_file->header_length());
        EXPECT_EQ(ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH, mark_file->error_buffer_length());
        EXPECT_EQ(0, mark_file->cluster_id());
        EXPECT_EQ("", mark_file->aeron_directory());
        EXPECT_EQ("", mark_file->control_channel());
        EXPECT_EQ("", mark_file->ingress_channel());
        EXPECT_EQ("", mark_file->service_name());
        EXPECT_EQ("", mark_file->authenticator());
        EXPECT_EQ("", mark_file->services_cluster_dir());
    }
}

TEST_F(ClusterMarkFileTest, shouldUpdateExistingMarkFile)
{
    constexpr std::int64_t candidate_term_id = 753475487;
    constexpr std::int64_t activity_timestamp = 112211443311;

    {
        auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::backup, 123456));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        auto ready = mark_file->signal_ready(10);
        ASSERT_TRUE(ready) << ready.error().message.view();

        mark_file->set_candidate_term_id(candidate_term_id);
        mark_file->update_activity_timestamp(activity_timestamp);
        mark_file->set_archive_stream_id(4);
        mark_file->set_service_stream_id(5);
        mark_file->set_consensus_module_stream_id(108);
        mark_file->set_ingress_stream_id(101);
        mark_file->set_member_id(8);
        mark_file->set_service_id(1);
        mark_file->set_cluster_id(-9);
        mark_file->set_channels(directory_ + "/aeron", "aeron:ipc", "aeron:udp?alias=ingress",
                                "io.aeron.cluster.TestService", "authenticator", "cluster dir");
    }

    auto mark_file = ClusterMarkFile::create_or_open(
        options(ClusterComponentType::consensus_module, 753498573948593, 2 * ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH));
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    EXPECT_EQ(ClusterMarkFile::ACTIVATION_IN_PROGRESS_TIMESTAMP, mark_file->activity_timestamp_volatile());
    EXPECT_EQ(753498573948593, mark_file->start_timestamp());
    EXPECT_EQ(static_cast<std::int64_t>(::getpid()), mark_file->pid());
    EXPECT_EQ(candidate_term_id, mark_file->candidate_term_id());
    EXPECT_EQ(4, mark_file->archive_stream_id());
    EXPECT_EQ(5, mark_file->service_stream_id());
    EXPECT_EQ(108, mark_file->consensus_module_stream_id());
    EXPECT_EQ(101, mark_file->ingress_stream_id());
    EXPECT_EQ(8, mark_file->member_id());
    EXPECT_EQ(1, mark_file->service_id());
    EXPECT_EQ(ClusterMarkFile::HEADER_LENGTH, mark_file->header_length());
    EXPECT_EQ(2 * ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH, mark_file->error_buffer_length());
    EXPECT_EQ(-9, mark_file->cluster_id());
    EXPECT_EQ(directory_ + "/aeron", mark_file->aeron_directory());
    EXPECT_EQ("aeron:ipc", mark_file->control_channel());
    EXPECT_EQ("aeron:udp?alias=ingress", mark_file->ingress_channel());
    EXPECT_EQ("io.aeron.cluster.TestService", mark_file->service_name());
    EXPECT_EQ("authenticator", mark_file->authenticator());
    EXPECT_EQ("cluster dir", mark_file->services_cluster_dir());
    EXPECT_EQ(ClusterComponentType::consensus_module, mark_file->component_type());
}

TEST_F(ClusterMarkFileTest, shouldRejectAnActiveMarkFile)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::container, 1000));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        auto ready = mark_file->signal_ready(1000);
        ASSERT_TRUE(ready) << ready.error().message.view();
    }

    auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::container, 1001));
    ASSERT_FALSE(mark_file);
    EXPECT_EQ(0U, std::string{mark_file.error().message.view()}.find("active mark file"));
}

TEST_F(ClusterMarkFileTest, shouldRejectAConflictingComponentTypeAndResetTheActivityTimestamp)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::container, 444444449888888));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        auto ready = mark_file->signal_ready(444444449888888);
        ASSERT_TRUE(ready) << ready.error().message.view();
    }

    auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::standby, 444444449893888));
    ASSERT_FALSE(mark_file);
    EXPECT_EQ("existing Mark file type CONTAINER not same as required type STANDBY", mark_file.error().message.view());

    auto existing = ClusterMarkFile::open_existing(path(), 444444449893888);
    ASSERT_TRUE(existing) << existing.error().message.view();
    EXPECT_EQ(NULL_VALUE, existing->activity_timestamp_volatile());
}

TEST_F(ClusterMarkFileTest, shouldAllowBackupToConsensusModuleTakeover)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::backup, 1000));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    }

    auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::consensus_module, 100000));
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    EXPECT_EQ(ClusterComponentType::consensus_module, mark_file->component_type());
}

TEST_F(ClusterMarkFileTest, shouldRejectFilePageSizeIfNotPowerOf2)
{
    auto options_value = options();
    options_value.file_page_size = 4199;
    auto mark_file = ClusterMarkFile::create_or_open(options_value);
    ASSERT_FALSE(mark_file);
    EXPECT_EQ("Page size not a power of 2: page size=4199", mark_file.error().message.view());
}

TEST_F(ClusterMarkFileTest, shouldRejectFilePageSizeIfTooSmall)
{
    for (const std::int32_t page_size : {0, -5, 4095})
    {
        auto options_value = options();
        options_value.file_page_size = page_size;
        auto mark_file = ClusterMarkFile::create_or_open(options_value);
        ASSERT_FALSE(mark_file);
        EXPECT_EQ("Page size less than min size of 4096: page size=" + std::to_string(page_size),
                  mark_file.error().message.view());
    }
}

TEST_F(ClusterMarkFileTest, shouldRejectFilePageSizeIfTooBig)
{
    auto options_value = options();
    options_value.file_page_size = INT32_MAX;
    auto mark_file = ClusterMarkFile::create_or_open(options_value);
    ASSERT_FALSE(mark_file);
    EXPECT_EQ("Page size more than max size of 1073741824: page size=2147483647", mark_file.error().message.view());
}

TEST_F(ClusterMarkFileTest, shouldAlignMarkFileLengthBasedOnTheFilePageSize)
{
    for (const std::int32_t page_size : {4096, 8192, 16384})
    {
        std::error_code error;
        std::filesystem::remove(path(), error);

        auto options_value = options(ClusterComponentType::standby, 123456, 1345679);
        options_value.file_page_size = page_size;
        auto mark_file = ClusterMarkFile::create_or_open(options_value);
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();

        const std::uintmax_t expected =
            (static_cast<std::size_t>(ClusterMarkFile::HEADER_LENGTH) + 1345679U + page_size - 1) & ~(page_size - 1);
        EXPECT_EQ(expected, std::filesystem::file_size(path()));
    }
}

TEST_F(ClusterMarkFileTest, shouldSignalFailedStartAndTermination)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options());
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();

        auto failed = mark_file->signal_failed_start();
        ASSERT_TRUE(failed) << failed.error().message.view();
        EXPECT_EQ(ClusterMarkFile::VERSION_FAILED, mark_file->semantic_version());
    }

    auto existing = ClusterMarkFile::open_existing(path(), 200000);
    ASSERT_TRUE(existing) << existing.error().message.view();
    EXPECT_TRUE(existing->previous_startup_failed());
    EXPECT_EQ(NULL_VALUE, existing->activity_timestamp_volatile());
}

TEST_F(ClusterMarkFileTest, shouldWriteTheReferenceRecordLayout)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options());
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        auto ready = mark_file->signal_ready(123456);
        ASSERT_TRUE(ready) << ready.error().message.view();
    }

    std::ifstream input{path(), std::ios::binary};
    ASSERT_TRUE(input.is_open());
    std::vector<std::uint8_t> bytes(136);
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    ASSERT_EQ(static_cast<std::streamsize>(bytes.size()), input.gcount());

    const auto u16 = [&bytes](std::size_t offset)
    {
        std::uint16_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };
    const auto i32 = [&bytes](std::size_t offset)
    {
        std::int32_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };
    const auto i64 = [&bytes](std::size_t offset)
    {
        std::int64_t value = 0;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        return value;
    };

    // 8-byte message header at 0.
    EXPECT_EQ(128, u16(0));
    EXPECT_EQ(200, u16(2));
    EXPECT_EQ(110, u16(4));
    EXPECT_EQ(2, u16(6));

    // MarkFileHeader body at 8.
    EXPECT_EQ(768, i32(8));      // version
    EXPECT_EQ(1, i32(12));       // CONSENSUS_MODULE
    EXPECT_EQ(123456, i64(16));  // activity timestamp
    EXPECT_EQ(123456, i64(24));  // start timestamp
    EXPECT_EQ(static_cast<std::int64_t>(::getpid()), i64(32));
    EXPECT_EQ(NULL_VALUE, i64(40));   // candidateTermId
    EXPECT_EQ(8192, i32(8 + 64));     // headerLength
    EXPECT_EQ(1048576, i32(8 + 68));  // errorBufferLength
    EXPECT_EQ(0, i32(8 + 72));        // clusterId
}

TEST_F(ClusterMarkFileTest, shouldRenderAndSaveExistingErrorsOnTakeover)
{
    {
        auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::consensus_module, 1000));
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();

        auto errors = mark_file->error_buffer();
        const std::string description = "boom";
        const std::size_t length = 24 + description.size();
        std::int32_t length_i32 = static_cast<std::int32_t>(length);
        std::int32_t count_i32 = 2;
        std::int64_t last_ts = 1000;
        std::int64_t first_ts = 900;
        std::memcpy(errors.data(), &length_i32, sizeof(length_i32));
        std::memcpy(errors.data() + 4, &count_i32, sizeof(count_i32));
        std::memcpy(errors.data() + 8, &last_ts, sizeof(last_ts));
        std::memcpy(errors.data() + 16, &first_ts, sizeof(first_ts));
        std::memcpy(errors.data() + 24, description.data(), description.size());

        auto ready = mark_file->signal_ready(1000);
        ASSERT_TRUE(ready) << ready.error().message.view();
    }

    auto mark_file = ClusterMarkFile::create_or_open(options(ClusterComponentType::consensus_module, 100000));
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    EXPECT_NE(std::string::npos, mark_file->existing_errors().find("2 observations from"));
    EXPECT_NE(std::string::npos, mark_file->existing_errors().find("boom"));
    EXPECT_NE(std::string::npos, mark_file->existing_errors().find("1 distinct errors observed."));

    bool all_zero = true;
    for (const std::uint8_t value : mark_file->error_buffer())
    {
        all_zero = all_zero && 0 == value;
    }
    EXPECT_TRUE(all_zero);

    bool report_found = false;
    for (const auto& entry : std::filesystem::directory_iterator(directory_))
    {
        const std::string name = entry.path().filename().string();
        report_found = report_found || (std::string::npos != name.find("CONSENSUS_MODULE-") &&
                                        std::string::npos != name.find("-error.log"));
    }
    EXPECT_TRUE(report_found);
}

TEST_F(ClusterMarkFileTest, shouldRenderTheReferenceErrorLogShape)
{
    std::vector<std::uint8_t> buffer(128, 0);
    const std::string description = "first error";
    const std::size_t length = 24 + description.size();
    std::int32_t length_i32 = static_cast<std::int32_t>(length);
    std::int32_t count_i32 = 3;
    std::int64_t last_ts = 2000;
    std::int64_t first_ts = 1000;
    std::memcpy(buffer.data(), &length_i32, sizeof(length_i32));
    std::memcpy(buffer.data() + 4, &count_i32, sizeof(count_i32));
    std::memcpy(buffer.data() + 8, &last_ts, sizeof(last_ts));
    std::memcpy(buffer.data() + 16, &first_ts, sizeof(first_ts));
    std::memcpy(buffer.data() + 24, description.data(), description.size());

    const std::string rendered = ClusterMarkFile::render_error_log(buffer);
    EXPECT_NE(std::string::npos, rendered.find("3 observations from"));
    EXPECT_NE(std::string::npos, rendered.find("for:\nfirst error\n"));
    EXPECT_NE(std::string::npos, rendered.find("1 distinct errors observed.\n"));

    const std::string empty = ClusterMarkFile::render_error_log(std::span<const std::uint8_t>{});
    EXPECT_EQ("\n0 distinct errors observed\n", empty);
}

TEST_F(ClusterMarkFileTest, shouldNameAndResolveServiceMarkFiles)
{
    EXPECT_EQ("cluster-mark-service-7.dat", ClusterMarkFile::mark_filename_for_service(7));
    EXPECT_EQ("cluster-mark-service-7.lnk", ClusterMarkFile::link_filename_for_service(7));

    const std::string real_dir = directory_ + "/real";
    std::filesystem::create_directories(real_dir);
    {
        std::ofstream link{directory_ + "/cluster-mark.lnk"};
        link << real_dir << "\n";
    }

    EXPECT_EQ(real_dir, ClusterMarkFile::resolve_mark_file_dir(directory_));
}

}  // namespace
