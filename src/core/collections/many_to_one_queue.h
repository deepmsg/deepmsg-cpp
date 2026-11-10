#pragma once

#include <atomic>
#include <cstddef>
#include <new>

namespace axc::core
{

/// `org.agrona.concurrent.ManyToOneConcurrentLinkedQueue`: an unbounded
/// intrusive linked queue with any number of producers and **one** consumer.
///
/// The archive's DEDICATED mode is the only user: the conductor hands sessions
/// to the recorder and the replayer, and both workers hand finished sessions
/// back on one shared close queue (`DedicatedModeArchiveConductor.java:143-434`).
///
/// Reclamation: a producer allocates and links only its own node and never
/// touches a node again; the single consumer is the only one that frees. A
/// producer that exchanged the stub out of `tail_` must link it before any
/// consumer can observe a non-null `next` after it, so the old stub the
/// consumer frees is never referenced by a live producer (argument in
/// `analysis/cpp/plan-a5a-runtime.md` §3.2).
template <class T>
class ManyToOneQueue
{
public:
    ManyToOneQueue()
    {
        auto* stub = new (std::nothrow) Node();
        head_.store(stub, std::memory_order_relaxed);
        tail_.store(stub, std::memory_order_relaxed);
    }

    ~ManyToOneQueue()
    {
        while (nullptr != pop())
        {
        }
        delete head_.load(std::memory_order_relaxed);
    }

    ManyToOneQueue(const ManyToOneQueue&) = delete;
    ManyToOneQueue& operator=(const ManyToOneQueue&) = delete;

    /// Never blocks. False only when the node allocation fails (the stub case
    /// leaves the queue unusable, so everything after it fails too).
    [[nodiscard]] bool push(T* value) noexcept
    {
        Node* previous = tail_.load(std::memory_order_relaxed);
        if (nullptr == previous)
        {
            return false;
        }

        auto* node = new (std::nothrow) Node();
        if (nullptr == node)
        {
            return false;
        }
        node->value = value;

        // Each producer takes exclusive ownership of the previous tail, so the
        // link store cannot race another producer.
        previous = tail_.exchange(node, std::memory_order_acq_rel);
        previous->next.store(node, std::memory_order_release);
        return true;
    }

    /// Consumer side only. Null when the queue is empty for this pass (a
    /// producer between its exchange and its link shows up on a later pass).
    [[nodiscard]] T* pop() noexcept
    {
        Node* head = head_.load(std::memory_order_acquire);
        if (nullptr == head)
        {
            return nullptr;
        }

        Node* next = head->next.load(std::memory_order_acquire);
        if (nullptr == next)
        {
            return nullptr;
        }

        head_.store(next, std::memory_order_release);
        T* value = next->value;
        delete head;
        return value;
    }

    /// Consumer side only.
    [[nodiscard]] bool empty() const noexcept
    {
        const Node* head = head_.load(std::memory_order_acquire);
        return nullptr == head || nullptr == head->next.load(std::memory_order_acquire);
    }

private:
    struct Node
    {
        std::atomic<Node*> next{nullptr};
        T* value{nullptr};
    };

    alignas(64) std::atomic<Node*> head_{nullptr};
    alignas(64) std::atomic<Node*> tail_{nullptr};
};

}  // namespace axc::core
