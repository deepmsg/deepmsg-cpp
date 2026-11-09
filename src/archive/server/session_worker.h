#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace deepmsg::archive::server
{

/// `SessionWorker` (`SessionWorker.java:25-160`): the conductor's array of live
/// sessions. `do_work` walks them, and a session that reports `is_done()` is
/// closed and dropped from the array (the reference swaps it with the last).
///
/// No virtual calls: `Session` is a concrete type per conductor.
template <class Session>
class SessionWorker
{
public:
    using SessionPtr = std::unique_ptr<Session>;

    /// `SessionWorker.add` (`SessionWorker.java:64`).
    void add(SessionPtr session)
    {
        sessions_.push_back(std::move(session));
    }

    /// `SessionWorker.doWork` (`SessionWorker.java:80-130`).
    template <class OnClose>
    int do_work(const OnClose& on_close)
    {
        int work_count = 0;

        for (std::size_t index = 0; index < sessions_.size();)
        {
            Session& session = *sessions_[index];
            work_count += session.do_work();

            if (session.is_done())
            {
                on_close(session);
                sessions_[index] = std::move(sessions_.back());
                sessions_.pop_back();
                work_count++;
            }
            else
            {
                index++;
            }
        }

        return work_count;
    }

    /// DEDICATED mode: a finished session's ownership is handed to `on_close`
    /// (it goes back to the conductor through the close queue) instead of being
    /// destroyed here (`DedicatedModeArchiveConductor.java:220-250`).
    template <class OnClose>
    int do_work_releasing(const OnClose& on_close)
    {
        int work_count = 0;

        for (std::size_t index = 0; index < sessions_.size();)
        {
            Session& session = *sessions_[index];
            work_count += session.do_work();

            if (session.is_done())
            {
                SessionPtr done = std::move(sessions_[index]);
                sessions_[index] = std::move(sessions_.back());
                sessions_.pop_back();
                on_close(std::move(done));
                work_count++;
            }
            else
            {
                index++;
            }
        }

        return work_count;
    }

    /// Shutdown of a worker that does not close its sessions itself: hands every
    /// remaining session's ownership to `on_session` and empties the worker.
    template <class OnSession>
    void drain(const OnSession& on_session)
    {
        for (SessionPtr& session : sessions_)
        {
            on_session(std::move(session));
        }
        sessions_.clear();
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return sessions_.size();
    }

    template <class F>
    void for_each(F&& function)
    {
        for (auto& session : sessions_)
        {
            function(*session);
        }
    }

private:
    std::vector<SessionPtr> sessions_;
};

}  // namespace deepmsg::archive::server
