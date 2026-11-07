#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "core/config/name_table.h"
#include "core/config/property_source.h"

namespace
{

class TempDir
{
public:
    TempDir()
    {
        char pattern[] = "/tmp/axc-config-XXXXXX";
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

private:
    std::string path_;
};

}  // namespace

TEST(PropertySource, ParsesDashDArguments)
{
    const char* argv[] = {"prog", "-Dfoo.bar=1", "-Dbaz", "positional", "-Dqux="};
    const auto source = axc::core::PropertySource::from_args(5, argv);
    ASSERT_TRUE(source.has_value());

    EXPECT_EQ("1", source->get_or("foo.bar", ""));
    EXPECT_TRUE(source->contains("baz"));
    EXPECT_EQ("", source->get_or("baz", "fallback"));
    EXPECT_TRUE(source->contains("qux"));
    EXPECT_FALSE(source->contains("positional"));
}

TEST(PropertySource, FileValuesDoNotOverrideArguments)
{
    TempDir temp;
    ASSERT_FALSE(temp.path().empty());

    const std::string file = temp.path() + "/config.properties";
    {
        std::ofstream out{file};
        out << "# comment\n"
            << "a=file\n"
            << " b = 2 \n"
            << "! ignored\n";
    }

    const char* argv[] = {"prog", "-Da=arg"};
    auto source = axc::core::PropertySource::from_args(2, argv);
    ASSERT_TRUE(source.has_value());
    ASSERT_TRUE(source->load_file(file).has_value());

    EXPECT_EQ("arg", source->get_or("a", ""));
    EXPECT_EQ("2", source->get_or("b", ""));
}

TEST(PropertySource, EnvironmentMapsUnderscoresToDots)
{
    ::setenv("AERON_TEST_SAMPLE", "42", 1);
    ::setenv("AERON_TEST_SAMPLE_KEPT", "1", 1);

    const char* argv[] = {"prog"};
    auto source = axc::core::PropertySource::from_args(1, argv);
    ASSERT_TRUE(source.has_value());
    ASSERT_TRUE(source->load_environment().has_value());

    EXPECT_EQ("42", source->get_or("aeron.test.sample", ""));

    ::unsetenv("AERON_TEST_SAMPLE");
    ::unsetenv("AERON_TEST_SAMPLE_KEPT");
}

TEST(PropertySource, EnvironmentDoesNotOverrideExplicitValues)
{
    ::setenv("AERON_TEST_EXPLICIT", "env", 1);

    const char* argv[] = {"prog", "-Daeron.test.explicit=arg"};
    auto source = axc::core::PropertySource::from_args(2, argv);
    ASSERT_TRUE(source.has_value());
    ASSERT_TRUE(source->load_environment().has_value());
    EXPECT_EQ("arg", source->get_or("aeron.test.explicit", ""));

    ::unsetenv("AERON_TEST_EXPLICIT");
}

TEST(NameTable, DispositionsAndUnknownNames)
{
    axc::core::NameTable table;
    table.add_honoured("aeron.archive.dir");
    table.add_accepted_ignored("aeron.archive.foo", "accepted for compatibility");
    table.add_refused("aeron.archive.bar", "not supported");

    EXPECT_EQ(axc::core::DispositionKind::honoured, table.lookup("aeron.archive.dir").kind);
    EXPECT_EQ(axc::core::DispositionKind::accepted_ignored, table.lookup("aeron.archive.foo").kind);
    EXPECT_EQ(axc::core::DispositionKind::refused, table.lookup("aeron.archive.bar").kind);
    EXPECT_FALSE(table.lookup("aeron.archive.unknown").is_known());
    EXPECT_EQ(3U, table.size());

    const char* argv[] = {"prog"};
    auto source = axc::core::PropertySource::from_args(1, argv);
    ASSERT_TRUE(source.has_value());
    source->set("aeron.archive.dir", "x");
    source->set("aeron.archive.bar", "y");
    source->set("aeron.archive.unknown", "z");

    const auto unknown = table.unknown_names(*source);
    ASSERT_EQ(1U, unknown.size());
    EXPECT_EQ("aeron.archive.unknown", unknown.front());

    const auto refused = table.refused_names(*source);
    ASSERT_EQ(1U, refused.size());
    EXPECT_EQ("aeron.archive.bar", refused.front());
}
