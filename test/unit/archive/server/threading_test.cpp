// A5a-2 criteria: the threading configuration names
// (`Archive.java:404-449`, `:571-601`, `:793-795`, `:1489-1507`;
// `CommonContext.java:538-591`).

#include <gtest/gtest.h>

#include <string>

#include "archive/server/threading.h"
#include "core/config/property_source.h"

namespace
{

using deepmsg::archive::server::AgentRole;
using deepmsg::archive::server::ArchiveThreadingMode;
using deepmsg::archive::server::parse_thread_naming;
using deepmsg::archive::server::parse_threading_mode;
using deepmsg::archive::server::parse_threading_settings;
using deepmsg::archive::server::thread_name;
using deepmsg::archive::server::ThreadNaming;

TEST(ThreadingTest, modesFollowTheReferenceNames)
{
    ASSERT_TRUE(parse_threading_mode("INVOKER").has_value());
    EXPECT_EQ(ArchiveThreadingMode::invoker, *parse_threading_mode("INVOKER"));
    EXPECT_EQ(ArchiveThreadingMode::shared, *parse_threading_mode("SHARED"));
    EXPECT_EQ(ArchiveThreadingMode::dedicated, *parse_threading_mode("DEDICATED"));

    EXPECT_FALSE(parse_threading_mode("dedicated").has_value());
    EXPECT_FALSE(parse_threading_mode("").has_value());
}

TEST(ThreadingTest, theDefaultModeIsDedicated)
{
    axc::core::PropertySource source;
    auto settings = parse_threading_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();
    EXPECT_EQ(ArchiveThreadingMode::dedicated, settings->mode);
    EXPECT_EQ(ThreadNaming::classic, settings->naming);
    // `Archive.java:449`: the archive's default idle strategy is backoff.
    EXPECT_EQ(axc::core::IdleKind::backoff, settings->idle.kind);
    EXPECT_EQ(settings->idle.kind, settings->recorder_idle.kind);
    EXPECT_EQ(settings->idle.kind, settings->replayer_idle.kind);
}

TEST(ThreadingTest, threadNamesFollowTheNamingMode)
{
    EXPECT_EQ("archive-conductor", thread_name(ThreadNaming::classic, AgentRole::conductor));
    EXPECT_EQ("archive-conductor", thread_name(ThreadNaming::classic, AgentRole::shared));
    EXPECT_EQ("archive-recorder", thread_name(ThreadNaming::classic, AgentRole::recorder));
    EXPECT_EQ("archive-replayer", thread_name(ThreadNaming::classic, AgentRole::replayer));

    EXPECT_EQ("aeron-ar-cnd", thread_name(ThreadNaming::new_names, AgentRole::conductor));
    EXPECT_EQ("aeron-ar-shd", thread_name(ThreadNaming::new_names, AgentRole::shared));
    EXPECT_EQ("aeron-ar-rec", thread_name(ThreadNaming::new_names, AgentRole::recorder));
    EXPECT_EQ("aeron-ar-rep", thread_name(ThreadNaming::new_names, AgentRole::replayer));
}

TEST(ThreadingTest, unknownThreadNamingIsRefused)
{
    EXPECT_EQ(ThreadNaming::classic, *parse_thread_naming("classic"));
    EXPECT_EQ(ThreadNaming::new_names, *parse_thread_naming("new"));
    EXPECT_FALSE(parse_thread_naming("modern").has_value());
}

TEST(ThreadingTest, recorderAndReplayerIdleFallBackToTheArchivesOwn)
{
    axc::core::PropertySource source;
    source.set("aeron.archive.idle.strategy", "yield");

    auto settings = parse_threading_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();
    EXPECT_EQ(axc::core::IdleKind::yielding, settings->idle.kind);
    EXPECT_EQ(axc::core::IdleKind::yielding, settings->recorder_idle.kind);
    EXPECT_EQ(axc::core::IdleKind::yielding, settings->replayer_idle.kind);

    source.set("aeron.archive.recorder.idle.strategy", "spin");
    settings = parse_threading_settings(source);
    ASSERT_TRUE(settings.has_value());
    EXPECT_EQ(axc::core::IdleKind::yielding, settings->idle.kind);
    EXPECT_EQ(axc::core::IdleKind::busy_spin, settings->recorder_idle.kind);
    EXPECT_EQ(axc::core::IdleKind::yielding, settings->replayer_idle.kind);
}

TEST(ThreadingTest, thresholdsAndAffinitiesAreParsed)
{
    axc::core::PropertySource source;
    source.set("aeron.archive.conductor.cycle.threshold", "200ms");
    source.set("aeron.archive.recorder.cycle.threshold", "50ms");
    source.set("aeron.archive.replayer.cycle.threshold", "250000000");
    source.set("aeron.archive.conductor.cpu.affinity", "0");
    source.set("aeron.archive.recorder.cpu.affinity", "1,2");

    auto settings = parse_threading_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();
    EXPECT_EQ(200'000'000, settings->conductor_cycle_threshold_ns);
    EXPECT_EQ(50'000'000, settings->recorder_cycle_threshold_ns);
    EXPECT_EQ(250'000'000, settings->replayer_cycle_threshold_ns);
    EXPECT_TRUE(settings->conductor_affinity.any());
    EXPECT_TRUE(settings->conductor_affinity.is_index());
    EXPECT_FALSE(settings->replayer_affinity.any());
}

TEST(ThreadingTest, badValuesCarryThePropertyName)
{
    axc::core::PropertySource source;
    source.set("aeron.archive.idle.strategy", "no-such-strategy");

    const auto settings = parse_threading_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_NE(std::string::npos, std::string{settings.error().message.view()}.find("aeron.archive.idle.strategy"));

    axc::core::PropertySource bad_mode;
    bad_mode.set("aeron.archive.threading.mode", "THREADS");
    EXPECT_FALSE(parse_threading_settings(bad_mode).has_value());
}

}  // namespace
