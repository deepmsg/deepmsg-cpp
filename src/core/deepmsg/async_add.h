#pragma once

#include <expected>
#include <optional>
#include <utility>

#include <aeronc.h>
#include <cerrno>

#include "core/error/error.h"

namespace axc::core
{

/// Two-phase async add: never block an agent waiting for a resource, poll it on
/// a later pass (the iron rule from the Rust track).
///
/// `poll()` returns nullopt while pending, the handle once completed, or an
/// error. The async object is owned by this wrapper until completion; if it is
/// destroyed first, the pending add is cancelled.
template <class HandlePtr>
class AsyncAdd
{
public:
    using Raw = typename HandlePtr::pointer;  // e.g. aeron_counter_t*
    using PollFn = int (*)(Raw* handle, void* async);
    using CancelFn = int (*)(aeron_t* client, void* async);

    AsyncAdd() noexcept = default;

    AsyncAdd(aeron_t* client, void* async, PollFn poll_fn, CancelFn cancel_fn) noexcept
        : client_(client), async_(async), poll_fn_(poll_fn), cancel_fn_(cancel_fn)
    {
    }

    AsyncAdd(const AsyncAdd&) = delete;
    AsyncAdd& operator=(const AsyncAdd&) = delete;

    AsyncAdd(AsyncAdd&& other) noexcept
        : client_(other.client_),
          async_(other.async_),
          poll_fn_(other.poll_fn_),
          cancel_fn_(other.cancel_fn_),
          error_(other.error_),
          failed_(other.failed_)
    {
        other.async_ = nullptr;
    }

    AsyncAdd& operator=(AsyncAdd&& other) noexcept
    {
        if (this != &other)
        {
            cancel();
            client_ = other.client_;
            async_ = other.async_;
            poll_fn_ = other.poll_fn_;
            cancel_fn_ = other.cancel_fn_;
            error_ = other.error_;
            failed_ = other.failed_;
            other.async_ = nullptr;
        }
        return *this;
    }

    ~AsyncAdd()
    {
        cancel();
    }

    static AsyncAdd failed(Error error) noexcept
    {
        AsyncAdd add;
        add.error_ = error;
        add.failed_ = true;
        return add;
    }

    [[nodiscard]] std::expected<std::optional<HandlePtr>, Error> poll() noexcept
    {
        if (failed_)
        {
            return std::unexpected(error_);
        }

        if (nullptr == async_ || nullptr == poll_fn_)
        {
            return std::unexpected(make_error(ErrorCode::invalid_argument, "async add is not pending"));
        }

        Raw raw = nullptr;
        const int result = poll_fn_(&raw, async_);

        if (result < 0)
        {
            failed_ = true;
            async_ = nullptr;
            error_ = make_error(ErrorCode::internal_error, std::string{"async add failed: "} + aeron_errmsg(),
                                aeron_errcode());
            return std::unexpected(error_);
        }

        if (0 == result)
        {
            return std::optional<HandlePtr>{};
        }

        async_ = nullptr;
        return std::optional<HandlePtr>{HandlePtr{raw}};
    }

    [[nodiscard]] bool pending() const noexcept
    {
        return nullptr != async_ && !failed_;
    }

private:
    void cancel() noexcept
    {
        if (nullptr != async_ && nullptr != cancel_fn_ && nullptr != client_)
        {
            cancel_fn_(client_, async_);
        }
        async_ = nullptr;
    }

    aeron_t* client_{nullptr};
    void* async_{nullptr};
    PollFn poll_fn_{nullptr};
    CancelFn cancel_fn_{nullptr};
    Error error_{};
    bool failed_{false};
};

}  // namespace axc::core
