#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include <aeronc.h>

namespace deepmsg::archive::server
{

/// `ExclusivePublication` as `ReplaySession` uses it
/// (`ReplaySession.java:101-110`, `:385-455`).
///
/// The seam exists so the replay criteria can run without a media driver, the
/// same way `RecordingImage` (A2b) does. The production implementation is
/// `ExclusiveReplayPublication` over `aeron_exclusive_publication_t`.
class ReplayPublication
{
public:
    ReplayPublication() = default;
    virtual ~ReplayPublication() = default;

    ReplayPublication(const ReplayPublication&) = delete;
    ReplayPublication& operator=(const ReplayPublication&) = delete;

    [[nodiscard]] virtual bool is_connected() const noexcept = 0;
    /// `ExclusivePublication.availableWindow()` = position limit − position.
    [[nodiscard]] virtual std::int64_t available_window() const noexcept = 0;
    [[nodiscard]] virtual std::int32_t session_id() const noexcept = 0;
    [[nodiscard]] virtual std::int32_t stream_id() const noexcept = 0;
    [[nodiscard]] virtual std::int32_t initial_term_id() const noexcept = 0;
    [[nodiscard]] virtual std::int32_t position_bits_to_shift() const noexcept = 0;
    [[nodiscard]] virtual std::string_view channel() const noexcept = 0;

    /// `ExclusivePublication.offerBlock` / `appendPadding`.
    [[nodiscard]] virtual std::int64_t offer_block(std::span<const std::uint8_t> buffer) noexcept = 0;
    [[nodiscard]] virtual std::int64_t append_padding(std::int32_t length) noexcept = 0;

    /// `Publication.close()` (linger) and `Publication.revoke()`.
    virtual void close() noexcept = 0;
    virtual void revoke() noexcept = 0;
};

/// `aeron_exclusive_publication_t*` behind the seam.
class ExclusiveReplayPublication final : public ReplayPublication
{
public:
    /// Takes ownership of `publication` (may be null for an already-failed add).
    /// `channel` is carried alongside because the C API exposes the channel only
    /// on `aeron_publication_t`, not on the exclusive handle.
    ExclusiveReplayPublication(aeron_exclusive_publication_t* publication, std::string_view channel) noexcept;
    ~ExclusiveReplayPublication() override;

    [[nodiscard]] bool is_connected() const noexcept override;
    [[nodiscard]] std::int64_t available_window() const noexcept override;
    [[nodiscard]] std::int32_t session_id() const noexcept override;
    [[nodiscard]] std::int32_t stream_id() const noexcept override;
    [[nodiscard]] std::int32_t initial_term_id() const noexcept override;
    [[nodiscard]] std::int32_t position_bits_to_shift() const noexcept override;
    [[nodiscard]] std::string_view channel() const noexcept override;

    [[nodiscard]] std::int64_t offer_block(std::span<const std::uint8_t> buffer) noexcept override;
    [[nodiscard]] std::int64_t append_padding(std::int32_t length) noexcept override;

    void close() noexcept override;
    void revoke() noexcept override;

    [[nodiscard]] aeron_exclusive_publication_t* handle() const noexcept
    {
        return publication_;
    }

private:
    aeron_exclusive_publication_t* publication_{nullptr};
    aeron_publication_constants_t constants_{};
    std::string channel_{};
    bool released_{false};
};

}  // namespace deepmsg::archive::server
