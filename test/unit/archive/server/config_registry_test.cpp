// A5a-2 / XA-4 criteria: every `aeron.archive.*` name of Aeron 1.54.0 has a
// disposition and a reason, and none is `Pending`
// (`analysis/cpp/records/x0-6/config-names.tsv`, 48 names).

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "archive/server/config_registry.h"

namespace
{

using deepmsg::archive::server::archive_config_names;
using deepmsg::archive::server::ConfigDisposition;
using deepmsg::archive::server::disposition_name;
using deepmsg::archive::server::find_archive_config;

TEST(ConfigRegistryTest, everyReferenceNameHasADispositionAndAReason)
{
    const auto entries = archive_config_names();

    ASSERT_EQ(48u, entries.size());

    std::set<std::string> names;
    for (const auto& entry : entries)
    {
        EXPECT_EQ(0u, entry.name.substr(0, 14).compare("aeron.archive.")) << entry.name;
        EXPECT_FALSE(entry.reason.empty()) << entry.name;
        EXPECT_TRUE(disposition_name(entry.disposition) == "honoured" ||
                    disposition_name(entry.disposition) == "accepted_ignored")
            << entry.name;
        names.insert(std::string{entry.name});
    }
    EXPECT_EQ(entries.size(), names.size()) << "duplicate name";
}

TEST(ConfigRegistryTest, theThreadingNamesAreHonoured)
{
    for (const char* name :
         {"aeron.archive.threading.mode", "aeron.archive.idle.strategy", "aeron.archive.recorder.idle.strategy",
          "aeron.archive.replayer.idle.strategy", "aeron.archive.conductor.cpu.affinity",
          "aeron.archive.recorder.cpu.affinity", "aeron.archive.replayer.cpu.affinity",
          "aeron.archive.conductor.cycle.threshold", "aeron.archive.recorder.cycle.threshold",
          "aeron.archive.replayer.cycle.threshold", "aeron.archive.control.channel.enabled",
          "aeron.archive.control.response.channel", "aeron.archive.control.response.stream.id",
          "aeron.archive.message.timeout", "aeron.archive.message.retry.attempts"})
    {
        const auto* entry = find_archive_config(name);
        ASSERT_NE(nullptr, entry) << name;
        EXPECT_EQ(ConfigDisposition::honoured, entry->disposition) << name;
    }
}

TEST(ConfigRegistryTest, theDeliberateGapsAreNamed)
{
    for (const char* name : {"aeron.archive.authorisation.service.supplier", "aeron.archive.client.name",
                             "aeron.archive.max.catalog.entries", "aeron.archive.record.checksum"})
    {
        const auto* entry = find_archive_config(name);
        ASSERT_NE(nullptr, entry) << name;
        EXPECT_EQ(ConfigDisposition::accepted_ignored, entry->disposition) << name;
    }

    EXPECT_EQ(nullptr, find_archive_config("aeron.archive.not.a.reference.name"));
}

}  // namespace
