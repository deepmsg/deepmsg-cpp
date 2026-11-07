#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>

#include "core/io/fs.h"
#include "core/io/mapped_file.h"
#include "core/io/mark_file.h"

namespace
{

class TempDir
{
public:
    TempDir()
    {
        char pattern[] = "/tmp/axc-io-XXXXXX";
        char* made = ::mkdtemp(pattern);
        if (nullptr != made)
        {
            path_ = made;
        }
    }

    ~TempDir()
    {
        std::error_code error;
        if (!path_.empty())
        {
            std::filesystem::remove_all(path_, error);
        }
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::string& path() const noexcept
    {
        return path_;
    }

    [[nodiscard]] std::string file(std::string_view name) const
    {
        return path_ + "/" + std::string{name};
    }

private:
    std::string path_;
};

}  // namespace

TEST(Fs, CreatesDirectoriesAndReportsFiles)
{
    TempDir temp;
    ASSERT_FALSE(temp.path().empty());

    const std::string nested = temp.file("a/b/c");
    ASSERT_TRUE(axc::core::create_directories(nested).has_value());
    EXPECT_TRUE(axc::core::directory_exists(nested));
    EXPECT_FALSE(axc::core::file_exists(nested));

    const std::string file = nested + "/data.bin";
    {
        auto mapped = axc::core::MappedFile::create(file, 128);
        ASSERT_TRUE(mapped.has_value());
    }

    EXPECT_TRUE(axc::core::file_exists(file));
    const auto size = axc::core::file_size(file);
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(128U, *size);

    ASSERT_TRUE(axc::core::remove_file(file).has_value());
    EXPECT_FALSE(axc::core::file_exists(file));
    EXPECT_TRUE(axc::core::remove_file(file).has_value());
}

TEST(MappedFile, CreateWriteFlushReopenRead)
{
    TempDir temp;
    const std::string path = temp.file("map.bin");

    auto file = axc::core::MappedFile::create(path, 4096);
    ASSERT_TRUE(file.has_value()) << file.error().message.c_str();
    EXPECT_EQ(4096U, file->length());

    auto bytes = file->mapping();
    bytes[0] = 42;
    bytes[4095] = 7;
    ASSERT_TRUE(file->flush().has_value());

    auto reopened = axc::core::MappedFile::open(path);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().message.c_str();
    EXPECT_EQ(4096U, reopened->length());
    EXPECT_EQ(42, reopened->mapping()[0]);
    EXPECT_EQ(7, reopened->mapping()[4095]);
}

TEST(MappedFile, GrowPreservesExistingData)
{
    TempDir temp;
    const std::string path = temp.file("grow.bin");

    auto file = axc::core::MappedFile::create(path, 1024);
    ASSERT_TRUE(file.has_value());
    file->mapping()[100] = 9;

    ASSERT_TRUE(file->grow(8192).has_value());
    EXPECT_EQ(8192U, file->length());
    auto bytes = file->mapping();
    EXPECT_EQ(9, bytes[100]);
    bytes[8000] = 5;
    ASSERT_TRUE(file->flush().has_value());

    auto reopened = axc::core::MappedFile::open(path);
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(8192U, reopened->length());
    EXPECT_EQ(5, reopened->mapping()[8000]);
}

TEST(MappedFile, RejectsCreateWithZeroLength)
{
    TempDir temp;
    EXPECT_FALSE(axc::core::MappedFile::create(temp.file("zero.bin"), 0).has_value());
}

TEST(MarkFile, CreateSetReopen)
{
    TempDir temp;
    const std::string path = temp.file("mark.dat");

    auto mark = axc::core::MarkFile::create(path, 1024, 8, 16);
    ASSERT_TRUE(mark.has_value()) << mark.error().message.c_str();
    EXPECT_EQ(0, mark->version());
    EXPECT_GT(mark->activity_timestamp(), 0);

    mark->set_version(5);
    mark->update_activity(1234);
    ASSERT_TRUE(mark->flush().has_value());

    auto reopened = axc::core::MarkFile::open_existing(path, 8, 16);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().message.c_str();
    EXPECT_EQ(5, reopened->version());
    EXPECT_EQ(1234, reopened->activity_timestamp());
}

TEST(MarkFile, RejectsFieldsOutsideMapping)
{
    TempDir temp;
    const std::string path = temp.file("small-mark.dat");

    const auto mark = axc::core::MarkFile::create(path, 16, 8, 16);
    EXPECT_FALSE(mark.has_value());
}
