// Counting allocator for the zero-allocation test. Only this binary replaces
// the global allocator; `__libc_*` bypasses the interposed `malloc` so the
// counter itself never recurses.
//
// The replacement is glibc-specific (`__libc_malloc` and friends); the alloc
// target is not built in sanitizer presets (CMake guards it).

#include "alloc_probe.h"

#include <atomic>
#include <cstdlib>
#include <new>

extern "C"
{
    void* __libc_malloc(std::size_t size);
    void* __libc_calloc(std::size_t count, std::size_t size);
    void* __libc_realloc(void* pointer, std::size_t size);
    void __libc_free(void* pointer);
    void* __libc_memalign(std::size_t alignment, std::size_t size);
}

namespace axc::alloc_test
{
namespace
{

constexpr int LABEL_COUNT = 4;

thread_local Label t_label{Label::other};
std::atomic<std::int64_t> g_counts[LABEL_COUNT];

[[nodiscard]] Label label_for(std::string_view name) noexcept
{
    if ("cm" == name)
    {
        return Label::cm;
    }
    if ("container" == name)
    {
        return Label::container;
    }
    if ("client" == name)
    {
        return Label::client;
    }
    return Label::other;
}

}  // namespace

void record() noexcept
{
    g_counts[static_cast<int>(t_label)].fetch_add(1, std::memory_order_relaxed);
}

void label_thread(std::string_view name) noexcept
{
    t_label = label_for(name);
}

void reset() noexcept
{
    for (auto& counter : g_counts)
    {
        counter.store(0, std::memory_order_relaxed);
    }
}

std::int64_t count(Label label) noexcept
{
    return g_counts[static_cast<int>(label)].load(std::memory_order_relaxed);
}

std::int64_t total() noexcept
{
    std::int64_t sum = 0;
    for (const auto& counter : g_counts)
    {
        sum += counter.load(std::memory_order_relaxed);
    }
    return sum;
}

}  // namespace axc::alloc_test

// ---- the global replacement ------------------------------------------------

void* operator new(std::size_t size)
{
    axc::alloc_test::record();
    if (void* pointer = ::__libc_malloc(size))
    {
        return pointer;
    }
    throw std::bad_alloc();
}

void* operator new[](std::size_t size)
{
    return ::operator new(size);
}

void operator delete(void* pointer) noexcept
{
    ::__libc_free(pointer);
}

void operator delete[](void* pointer) noexcept
{
    ::__libc_free(pointer);
}

void operator delete(void* pointer, std::size_t /*size*/) noexcept
{
    ::__libc_free(pointer);
}

void operator delete[](void* pointer, std::size_t /*size*/) noexcept
{
    ::__libc_free(pointer);
}

void* operator new(std::size_t size, std::align_val_t alignment)
{
    axc::alloc_test::record();
    if (void* pointer = ::__libc_memalign(static_cast<std::size_t>(alignment), size))
    {
        return pointer;
    }
    throw std::bad_alloc();
}

void* operator new[](std::size_t size, std::align_val_t alignment)
{
    return ::operator new(size, alignment);
}

void operator delete(void* pointer, std::align_val_t /*alignment*/) noexcept
{
    ::__libc_free(pointer);
}

void operator delete[](void* pointer, std::align_val_t /*alignment*/) noexcept
{
    ::__libc_free(pointer);
}

void operator delete(void* pointer, std::size_t /*size*/, std::align_val_t /*alignment*/) noexcept
{
    ::__libc_free(pointer);
}

void operator delete[](void* pointer, std::size_t /*size*/, std::align_val_t /*alignment*/) noexcept
{
    ::__libc_free(pointer);
}

extern "C" void* malloc(std::size_t size)
{
    axc::alloc_test::record();
    return ::__libc_malloc(size);
}

extern "C" void* calloc(std::size_t count, std::size_t size)
{
    axc::alloc_test::record();
    return ::__libc_calloc(count, size);
}

extern "C" void* realloc(void* pointer, std::size_t size)
{
    axc::alloc_test::record();
    return ::__libc_realloc(pointer, size);
}

extern "C" void free(void* pointer)
{
    ::__libc_free(pointer);
}
