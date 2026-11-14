// An explicit spin lock for the stack's cold paths.
//
// The standard mutex type is not used in this repository. The receive path takes no lock
// at all; bind/release and the neighbour table's writers do, and they are cold.

#pragma once

#include <sched.h>
#include <atomic>

namespace axc::core::media::dpdk::stack
{

class SpinLock
{
public:
    void lock() noexcept
    {
        while (flag_.test_and_set(std::memory_order_acquire))
        {
            ::sched_yield();
        }
    }

    void unlock() noexcept
    {
        flag_.clear(std::memory_order_release);
    }

private:
    std::atomic_flag flag_{};
};

class SpinGuard
{
public:
    explicit SpinGuard(SpinLock& lock) noexcept : lock_{lock}
    {
        lock_.lock();
    }
    ~SpinGuard()
    {
        lock_.unlock();
    }

    SpinGuard(const SpinGuard&) = delete;
    SpinGuard& operator=(const SpinGuard&) = delete;

private:
    SpinLock& lock_;
};

}  // namespace axc::core::media::dpdk::stack
