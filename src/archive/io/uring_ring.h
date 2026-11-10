#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>

#include "core/error/error.h"

struct io_uring_sqe;
struct io_uring_cqe;

namespace deepmsg::archive::server::io
{

/// One completion as the kernel published it (`struct io_uring_cqe`).
struct UringCompletion
{
    std::uint64_t user_data{0};
    /// Bytes written for a `WRITE`, zero for a successful `FSYNC`, negative
    /// errno for a failed operation.
    std::int32_t result{0};
    std::uint32_t flags{0};
};

/// The slice of the io_uring UAPI the archive writer needs: one submission
/// ring and one completion ring, `WRITE` and `FSYNC` SQEs, and polling.
///
/// It is built on the raw UAPI (`io_uring_setup`/`io_uring_enter` plus the
/// SQ/CQ `mmap` offsets in `linux/io_uring.h`) rather than liburing, so the
/// backend has no external dependency and the failure mode is a probe that
/// says no. There is no `SQPOLL`/`IOPOLL` (both need privileges and the
/// recorder is already a busy-spinning thread).
///
/// Like the recorder that owns it the ring is single-threaded: no locking and
/// no allocation after `create`.
class UringRing
{
public:
    UringRing() noexcept = default;
    ~UringRing() noexcept;

    UringRing(const UringRing&) = delete;
    UringRing& operator=(const UringRing&) = delete;
    UringRing(UringRing&& other) noexcept;
    UringRing& operator=(UringRing&& other) noexcept;

    /// Whether this kernel (and sandbox) accepts `io_uring_setup`. Creates and
    /// immediately closes a one-entry ring; never throws.
    [[nodiscard]] static bool is_available() noexcept;

    /// Creates a ring with `entries` submission slots (the kernel may round
    /// up; `cq_entries` is at least twice `sq_entries` by default).
    [[nodiscard]] static std::expected<UringRing, axc::core::Error> create(unsigned entries) noexcept;

    /// Queue `IORING_OP_WRITE`. With `link` set the next SQE is chained behind
    /// it (`IOSQE_IO_LINK`), so a failed write cancels its force.
    [[nodiscard]] std::expected<void, axc::core::Error> queue_write(int fd, std::uint64_t offset,
                                                                    const std::uint8_t* data, std::uint32_t length,
                                                                    std::uint64_t user_data, bool link) noexcept;

    /// Queue `IORING_OP_FSYNC`; `datasync` asks for `IORING_FSYNC_DATASYNC`.
    [[nodiscard]] std::expected<void, axc::core::Error> queue_fsync(int fd, bool datasync,
                                                                    std::uint64_t user_data) noexcept;

    /// Submit everything queued since the last call. Returns how many SQEs the
    /// kernel consumed (normally all of `pending()`).
    [[nodiscard]] std::expected<unsigned, axc::core::Error> submit() noexcept;

    /// Wait until at least `min_complete` completions are available. The caller
    /// must have that many requests in flight, otherwise this can block
    /// forever. Returns immediately when they already are.
    [[nodiscard]] std::expected<void, axc::core::Error> wait(unsigned min_complete) noexcept;

    /// Pop one published completion. `false` when the CQ ring is empty.
    [[nodiscard]] bool peek(UringCompletion& completion) noexcept;

    /// SQEs queued but not submitted yet.
    [[nodiscard]] unsigned pending() const noexcept
    {
        return pending_;
    }

    /// Free submission slots.
    [[nodiscard]] unsigned sq_space() const noexcept;

private:
    [[nodiscard]] std::expected<struct io_uring_sqe*, axc::core::Error> next_sqe() noexcept;
    [[nodiscard]] std::expected<int, axc::core::Error> enter(unsigned to_submit, unsigned min_complete,
                                                             unsigned flags) noexcept;
    [[nodiscard]] unsigned available() const noexcept;
    void release() noexcept;

    int fd_{-1};
    unsigned sq_entries_{0};
    unsigned cq_entries_{0};
    unsigned pending_{0};

    void* sq_ring_{nullptr};
    std::size_t sq_ring_size_{0};
    void* cq_ring_{nullptr};
    std::size_t cq_ring_size_{0};
    struct io_uring_sqe* sqes_{nullptr};
    std::size_t sqes_size_{0};

    unsigned* sq_head_{nullptr};
    unsigned* sq_tail_{nullptr};
    unsigned* sq_mask_{nullptr};
    unsigned* sq_array_{nullptr};
    unsigned* cq_head_{nullptr};
    unsigned* cq_tail_{nullptr};
    unsigned* cq_mask_{nullptr};
    struct io_uring_cqe* cqes_{nullptr};
};

}  // namespace deepmsg::archive::server::io
