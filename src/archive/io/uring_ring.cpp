#include "archive/io/uring_ring.h"

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#if defined(__has_include)
#if __has_include(<linux/io_uring.h>)
#include <linux/io_uring.h>
#define AXC_HAVE_IO_URING_UAPI 1
#endif
#endif

#if !defined(AXC_HAVE_IO_URING_UAPI) || !defined(SYS_io_uring_setup) || !defined(SYS_io_uring_enter)
#define AXC_HAVE_IO_URING_UAPI 0
#endif

namespace deepmsg::archive::server::io
{
namespace
{

#if !AXC_HAVE_IO_URING_UAPI
[[nodiscard]] axc::core::Error unavailable() noexcept
{
    return axc::core::make_error(axc::core::ErrorCode::not_found,
                                 "the io_uring UAPI is not available on this platform");
}
#else
[[nodiscard]] axc::core::Error syscall_error(const char* message) noexcept
{
    return axc::core::make_error(axc::core::ErrorCode::io_error, message, errno);
}

[[nodiscard]] unsigned load(const unsigned* value) noexcept
{
    return __atomic_load_n(value, __ATOMIC_RELAXED);
}

[[nodiscard]] unsigned load_acquire(const unsigned* value) noexcept
{
    return __atomic_load_n(value, __ATOMIC_ACQUIRE);
}

void store_release(unsigned* value, unsigned next) noexcept
{
    __atomic_store_n(value, next, __ATOMIC_RELEASE);
}

template <typename T>
[[nodiscard]] T* at(void* base, std::uint32_t offset) noexcept
{
    return reinterpret_cast<T*>(static_cast<char*>(base) + offset);
}
#endif

}  // namespace

UringRing::~UringRing() noexcept
{
    release();
}

UringRing::UringRing(UringRing&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      sq_entries_(std::exchange(other.sq_entries_, 0)),
      cq_entries_(std::exchange(other.cq_entries_, 0)),
      pending_(std::exchange(other.pending_, 0)),
      sq_ring_(std::exchange(other.sq_ring_, nullptr)),
      sq_ring_size_(std::exchange(other.sq_ring_size_, 0)),
      cq_ring_(std::exchange(other.cq_ring_, nullptr)),
      cq_ring_size_(std::exchange(other.cq_ring_size_, 0)),
      sqes_(std::exchange(other.sqes_, nullptr)),
      sqes_size_(std::exchange(other.sqes_size_, 0)),
      sq_head_(std::exchange(other.sq_head_, nullptr)),
      sq_tail_(std::exchange(other.sq_tail_, nullptr)),
      sq_mask_(std::exchange(other.sq_mask_, nullptr)),
      sq_array_(std::exchange(other.sq_array_, nullptr)),
      cq_head_(std::exchange(other.cq_head_, nullptr)),
      cq_tail_(std::exchange(other.cq_tail_, nullptr)),
      cq_mask_(std::exchange(other.cq_mask_, nullptr)),
      cqes_(std::exchange(other.cqes_, nullptr))
{
}

UringRing& UringRing::operator=(UringRing&& other) noexcept
{
    if (this != &other)
    {
        release();
        fd_ = std::exchange(other.fd_, -1);
        sq_entries_ = std::exchange(other.sq_entries_, 0);
        cq_entries_ = std::exchange(other.cq_entries_, 0);
        pending_ = std::exchange(other.pending_, 0);
        sq_ring_ = std::exchange(other.sq_ring_, nullptr);
        sq_ring_size_ = std::exchange(other.sq_ring_size_, 0);
        cq_ring_ = std::exchange(other.cq_ring_, nullptr);
        cq_ring_size_ = std::exchange(other.cq_ring_size_, 0);
        sqes_ = std::exchange(other.sqes_, nullptr);
        sqes_size_ = std::exchange(other.sqes_size_, 0);
        sq_head_ = std::exchange(other.sq_head_, nullptr);
        sq_tail_ = std::exchange(other.sq_tail_, nullptr);
        sq_mask_ = std::exchange(other.sq_mask_, nullptr);
        sq_array_ = std::exchange(other.sq_array_, nullptr);
        cq_head_ = std::exchange(other.cq_head_, nullptr);
        cq_tail_ = std::exchange(other.cq_tail_, nullptr);
        cq_mask_ = std::exchange(other.cq_mask_, nullptr);
        cqes_ = std::exchange(other.cqes_, nullptr);
    }
    return *this;
}

bool UringRing::is_available() noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    struct io_uring_params params{};
    const long fd = ::syscall(SYS_io_uring_setup, 1u, &params);
    if (fd < 0)
    {
        return false;
    }
    ::close(static_cast<int>(fd));
    return true;
#else
    return false;
#endif
}

std::expected<UringRing, axc::core::Error> UringRing::create(unsigned entries) noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    if (0 == entries)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::invalid_argument, "an io_uring ring needs at least one entry"));
    }

    struct io_uring_params params{};
    const long setup_fd = ::syscall(SYS_io_uring_setup, entries, &params);
    if (setup_fd < 0)
    {
        return std::unexpected(syscall_error("io_uring_setup failed"));
    }

    UringRing ring;
    ring.fd_ = static_cast<int>(setup_fd);
    ring.sq_entries_ = params.sq_entries;
    ring.cq_entries_ = params.cq_entries;

    const std::size_t sq_ring_size =
        static_cast<std::size_t>(params.sq_off.array) + static_cast<std::size_t>(params.sq_entries) * sizeof(unsigned);
    const std::size_t cq_ring_size = static_cast<std::size_t>(params.cq_off.cqes) +
                                     static_cast<std::size_t>(params.cq_entries) * sizeof(struct io_uring_cqe);

    // `IORING_FEAT_SINGLE_MMAP`: the SQ and CQ rings share one mapping, whose
    // size is the larger of the two.
    const bool single_mmap = 0 != (params.features & IORING_FEAT_SINGLE_MMAP);
    const std::size_t ring_size =
        single_mmap ? (sq_ring_size > cq_ring_size ? sq_ring_size : cq_ring_size) : sq_ring_size;

    void* sq_ring =
        ::mmap(nullptr, ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring.fd_, IORING_OFF_SQ_RING);
    if (MAP_FAILED == sq_ring)
    {
        return std::unexpected(syscall_error("mmap of the io_uring submission ring failed"));
    }
    ring.sq_ring_ = sq_ring;
    ring.sq_ring_size_ = ring_size;
    ring.sq_head_ = at<unsigned>(sq_ring, params.sq_off.head);
    ring.sq_tail_ = at<unsigned>(sq_ring, params.sq_off.tail);
    ring.sq_mask_ = at<unsigned>(sq_ring, params.sq_off.ring_mask);
    ring.sq_array_ = at<unsigned>(sq_ring, params.sq_off.array);

    if (single_mmap)
    {
        ring.cq_ring_ = sq_ring;
        ring.cq_ring_size_ = 0;
    }
    else
    {
        void* cq_ring = ::mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring.fd_,
                               IORING_OFF_CQ_RING);
        if (MAP_FAILED == cq_ring)
        {
            return std::unexpected(syscall_error("mmap of the io_uring completion ring failed"));
        }
        ring.cq_ring_ = cq_ring;
        ring.cq_ring_size_ = cq_ring_size;
    }
    ring.cq_head_ = at<unsigned>(ring.cq_ring_, params.cq_off.head);
    ring.cq_tail_ = at<unsigned>(ring.cq_ring_, params.cq_off.tail);
    ring.cq_mask_ = at<unsigned>(ring.cq_ring_, params.cq_off.ring_mask);
    ring.cqes_ = at<struct io_uring_cqe>(ring.cq_ring_, params.cq_off.cqes);

    const std::size_t sqes_size = static_cast<std::size_t>(params.sq_entries) * sizeof(struct io_uring_sqe);
    void* sqes =
        ::mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring.fd_, IORING_OFF_SQES);
    if (MAP_FAILED == sqes)
    {
        return std::unexpected(syscall_error("mmap of the io_uring submission entries failed"));
    }
    ring.sqes_ = static_cast<struct io_uring_sqe*>(sqes);
    ring.sqes_size_ = sqes_size;

    return std::expected<UringRing, axc::core::Error>{std::move(ring)};
#else
    (void)entries;
    return std::unexpected(unavailable());
#endif
}

std::expected<void, axc::core::Error> UringRing::queue_write(int fd, std::uint64_t offset, const std::uint8_t* data,
                                                             std::uint32_t length, std::uint64_t user_data,
                                                             bool link) noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    auto sqe = next_sqe();
    if (!sqe)
    {
        return std::unexpected(sqe.error());
    }
    (*sqe)->opcode = IORING_OP_WRITE;
    (*sqe)->flags = link ? IOSQE_IO_LINK : 0;
    (*sqe)->fd = fd;
    (*sqe)->off = offset;
    (*sqe)->addr = reinterpret_cast<std::uint64_t>(data);
    (*sqe)->len = length;
    (*sqe)->user_data = user_data;
    return {};
#else
    (void)fd;
    (void)offset;
    (void)data;
    (void)length;
    (void)user_data;
    (void)link;
    return std::unexpected(unavailable());
#endif
}

std::expected<void, axc::core::Error> UringRing::queue_fsync(int fd, bool datasync, std::uint64_t user_data) noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    auto sqe = next_sqe();
    if (!sqe)
    {
        return std::unexpected(sqe.error());
    }
    (*sqe)->opcode = IORING_OP_FSYNC;
    (*sqe)->fd = fd;
    (*sqe)->fsync_flags = datasync ? IORING_FSYNC_DATASYNC : 0;
    (*sqe)->user_data = user_data;
    return {};
#else
    (void)fd;
    (void)datasync;
    (void)user_data;
    return std::unexpected(unavailable());
#endif
}

std::expected<unsigned, axc::core::Error> UringRing::submit() noexcept
{
    if (0 == pending_)
    {
        return 0u;
    }

    auto entered = enter(pending_, 0, 0);
    if (!entered)
    {
        return std::unexpected(entered.error());
    }

    const unsigned submitted = static_cast<unsigned>(*entered);
    pending_ = pending_ > submitted ? pending_ - submitted : 0;
    return submitted;
}

std::expected<void, axc::core::Error> UringRing::wait(unsigned min_complete) noexcept
{
    if (0 == min_complete || available() >= min_complete)
    {
        return {};
    }

#if AXC_HAVE_IO_URING_UAPI
    auto entered = enter(0, min_complete, IORING_ENTER_GETEVENTS);
    if (!entered)
    {
        return std::unexpected(entered.error());
    }
    return {};
#else
    return std::unexpected(unavailable());
#endif
}

bool UringRing::peek(UringCompletion& completion) noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    if (nullptr == cq_head_)
    {
        return false;
    }

    const unsigned head = load(cq_head_);
    if (head == load_acquire(cq_tail_))
    {
        return false;
    }

    const struct io_uring_cqe* cqe = &cqes_[head & *cq_mask_];
    completion.user_data = cqe->user_data;
    completion.result = cqe->res;
    completion.flags = cqe->flags;
    store_release(cq_head_, head + 1);
    return true;
#else
    (void)completion;
    return false;
#endif
}

unsigned UringRing::sq_space() const noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    if (nullptr == sq_head_)
    {
        return 0;
    }
    const unsigned tail = load(sq_tail_);
    const unsigned head = load_acquire(sq_head_);
    return sq_entries_ - (tail - head);
#else
    return 0;
#endif
}

std::expected<struct io_uring_sqe*, axc::core::Error> UringRing::next_sqe() noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    const unsigned tail = load(sq_tail_);
    const unsigned head = load_acquire(sq_head_);
    if (tail - head >= sq_entries_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::no_capacity, "io_uring submission queue is full"));
    }

    const unsigned index = tail & *sq_mask_;
    struct io_uring_sqe* sqe = &sqes_[index];
    std::memset(sqe, 0, sizeof(*sqe));
    sq_array_[index] = index;
    store_release(sq_tail_, tail + 1);
    ++pending_;
    return sqe;
#else
    return std::unexpected(unavailable());
#endif
}

std::expected<int, axc::core::Error> UringRing::enter(unsigned to_submit, unsigned min_complete,
                                                      unsigned flags) noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    for (;;)
    {
        const long result = ::syscall(SYS_io_uring_enter, fd_, to_submit, min_complete, flags, nullptr, 0);
        if (result >= 0)
        {
            return static_cast<int>(result);
        }
        if (EINTR != errno)
        {
            return std::unexpected(syscall_error("io_uring_enter failed"));
        }
    }
#else
    (void)to_submit;
    (void)min_complete;
    (void)flags;
    return std::unexpected(unavailable());
#endif
}

unsigned UringRing::available() const noexcept
{
#if AXC_HAVE_IO_URING_UAPI
    if (nullptr == cq_head_)
    {
        return 0;
    }
    const unsigned head = load(cq_head_);
    return load_acquire(cq_tail_) - head;
#else
    return 0;
#endif
}

void UringRing::release() noexcept
{
    if (nullptr != sqes_)
    {
        ::munmap(sqes_, sqes_size_);
        sqes_ = nullptr;
    }
    if (nullptr != cq_ring_ && cq_ring_ != sq_ring_)
    {
        ::munmap(cq_ring_, cq_ring_size_);
    }
    cq_ring_ = nullptr;
    if (nullptr != sq_ring_)
    {
        ::munmap(sq_ring_, sq_ring_size_);
        sq_ring_ = nullptr;
    }
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace deepmsg::archive::server::io
