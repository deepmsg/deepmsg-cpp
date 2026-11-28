// BUG-1 (2026-11-28): a recorder or replayer whose queues were never set must
// close without touching a null inbox.
//
// The conductor wires the queues only once both control subscriptions are up
// (`ArchiveConductor::init`). An init that fails earlier still destroys the
// conductor, and `ArchiveConductor::close` then reaches these workers in its
// DEDICATED branch. Before this guard, `close_threaded` dereferenced
// `inbox_ == nullptr`; the resulting core dump hid the error that had failed
// the init in the first place (observed on the 2026-11-28 DPDK bring-up).

#include <gtest/gtest.h>

#include "archive/server/recorder.h"
#include "archive/server/replayer.h"

namespace
{

using deepmsg::archive::server::Recorder;
using deepmsg::archive::server::Replayer;

TEST(WorkerCloseTest, RecorderClosesThreadedBeforeItsQueuesAreSet)
{
    Recorder recorder;
    ASSERT_FALSE(recorder.threaded());
    recorder.close_threaded();
    EXPECT_EQ(0U, recorder.size());
}

TEST(WorkerCloseTest, ReplayerClosesThreadedBeforeItsQueuesAreSet)
{
    Replayer replayer;
    ASSERT_FALSE(replayer.threaded());
    replayer.close_threaded();
    EXPECT_EQ(0U, replayer.size());
}

// The guard has to stay idempotent: the conductor's close can run from an
// explicit close and again from its destructor.
TEST(WorkerCloseTest, CloseThreadedIsIdempotentBeforeTheQueues)
{
    Recorder recorder;
    recorder.close_threaded();
    recorder.close_threaded();
    EXPECT_EQ(0U, recorder.size());

    Replayer replayer;
    replayer.close_threaded();
    replayer.close_threaded();
    EXPECT_EQ(0U, replayer.size());
}

}  // namespace
