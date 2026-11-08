#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string_view>

#include <aeronc.h>

#include "archive/server/control_response_proxy.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `ResponsePublication` over `aeron_exclusive_publication_t`
/// (`aeron_exclusive_publication_offer` / `_try_claim`).
///
/// The publication is the session's control publication: the reference adds it
/// asynchronously and the session waits for `is_connected()` in CONNECTING
/// (`ControlSession.java:913`).
class DeepmsgExclusivePublication final : public ResponsePublication
{
public:
    DeepmsgExclusivePublication() = default;
    ~DeepmsgExclusivePublication() override;

    DeepmsgExclusivePublication(const DeepmsgExclusivePublication&) = delete;
    DeepmsgExclusivePublication& operator=(const DeepmsgExclusivePublication&) = delete;

    /// Wraps a publication the caller obtained from
    /// `aeron_async_add_exclusive_publication` + `..._poll`.
    explicit DeepmsgExclusivePublication(aeron_exclusive_publication_t* publication) noexcept
        : publication_(publication)
    {
    }

    [[nodiscard]] OfferResult offer(std::span<const std::uint8_t> buffer) override;
    [[nodiscard]] OfferResult try_claim(std::int32_t length, Claim& claim) override;
    [[nodiscard]] bool is_connected() const override;
    [[nodiscard]] std::int32_t max_payload_length() const override;

    void close();

    [[nodiscard]] aeron_exclusive_publication_t* handle() const noexcept
    {
        return publication_;
    }

private:
    static void commit_claim(void* context) noexcept;

    aeron_exclusive_publication_t* publication_{nullptr};
    aeron_buffer_claim_t claim_{};
};

}  // namespace deepmsg::archive::server
