// A5a-1 criteria: `SessionWorker` (`SessionWorker.java:25-160`), including the
// DEDICATED-mode variant that hands a finished session's ownership out
// (`DedicatedModeArchiveConductor.java:220-250`).

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "archive/server/session_worker.h"

namespace
{

class FakeSession
{
public:
    explicit FakeSession(int passes_to_done) : remaining_(passes_to_done) {}

    int do_work()
    {
        ++work_calls;
        if (remaining_ > 0)
        {
            --remaining_;
        }
        return 1;
    }

    [[nodiscard]] bool is_done() const noexcept
    {
        return 0 == remaining_;
    }

    int work_calls{0};

private:
    int remaining_;
};

TEST(SessionWorkerTest, aDoneSessionIsClosedAndDropped)
{
    deepmsg::archive::server::SessionWorker<FakeSession> worker;
    worker.add(std::make_unique<FakeSession>(0));
    worker.add(std::make_unique<FakeSession>(2));

    std::vector<FakeSession*> closed;
    worker.do_work([&closed](FakeSession& session) { closed.push_back(&session); });

    EXPECT_EQ(1u, closed.size());
    EXPECT_EQ(1u, worker.size());
}

TEST(SessionWorkerTest, releasingHandsTheOwnerShipOutInsteadOfDestroying)
{
    deepmsg::archive::server::SessionWorker<FakeSession> worker;
    worker.add(std::make_unique<FakeSession>(0));

    std::vector<std::unique_ptr<FakeSession>> released;
    const int work = worker.do_work_releasing([&released](std::unique_ptr<FakeSession> session)
                                              { released.push_back(std::move(session)); });

    EXPECT_EQ(2, work);
    ASSERT_EQ(1u, released.size());
    EXPECT_NE(nullptr, released[0]);
    EXPECT_EQ(1, released[0]->work_calls);
    EXPECT_EQ(0u, worker.size());
}

TEST(SessionWorkerTest, releasingKeepsARunningSession)
{
    deepmsg::archive::server::SessionWorker<FakeSession> worker;
    worker.add(std::make_unique<FakeSession>(3));

    int released = 0;
    for (int pass = 0; pass < 2; ++pass)
    {
        worker.do_work_releasing([&released](std::unique_ptr<FakeSession>) { ++released; });
    }

    EXPECT_EQ(0, released);
    EXPECT_EQ(1u, worker.size());

    worker.do_work_releasing([&released](std::unique_ptr<FakeSession>) { ++released; });
    EXPECT_EQ(1, released);
    EXPECT_EQ(0u, worker.size());
}

}  // namespace
