// C1b configuration table: every `aeron.cluster.*` name from the 1.54.0
// extraction has a disposition (XC-6 baseline).

#include <gtest/gtest.h>

#include <string>

#include "cluster/common/cluster_config.h"
#include "core/config/property_source.h"

namespace
{

using axc::core::DispositionKind;
using deepmsg::cluster::common::cluster_name_table;

TEST(ClusterConfigTest, everyReferenceNameHasADisposition)
{
    const auto table = cluster_name_table();
    EXPECT_EQ(82U, table.size());
    EXPECT_EQ(DispositionKind::honoured, table.lookup("aeron.cluster.members").kind);
    EXPECT_EQ(DispositionKind::honoured, table.lookup("aeron.cluster.ingress.channel").kind);
    EXPECT_EQ(DispositionKind::honoured, table.lookup("aeron.cluster.cpu.affinity").kind);
    EXPECT_EQ(DispositionKind::honoured, table.lookup("aeron.cluster.service.cpu.affinity").kind);
    EXPECT_EQ(DispositionKind::accepted_ignored, table.lookup("aeron.cluster.accept.standby.snapshots").kind);
    EXPECT_EQ(DispositionKind::unknown, table.lookup("aeron.cluster.not.a.real.name").kind);
}

TEST(ClusterConfigTest, unknownAndRefusedNamesAreReportedNotIgnored)
{
    axc::core::PropertySource source;
    source.set("aeron.cluster.members", "0,a,b,c,d,e");
    source.set("aeron.cluster.not.a.real.name", "1");
    source.set("aeron.archive.dir", "/tmp");

    const auto table = cluster_name_table();
    const auto unknown = table.unknown_names(source);
    ASSERT_EQ(2U, unknown.size());
    EXPECT_EQ("aeron.archive.dir", unknown[0]);
    EXPECT_EQ("aeron.cluster.not.a.real.name", unknown[1]);
    EXPECT_TRUE(table.refused_names(source).empty());
}

}  // namespace
