#include "archive/server/replay_publication.h"

namespace deepmsg::archive::server
{

ExclusiveReplayPublication::ExclusiveReplayPublication(aeron_exclusive_publication_t* publication,
                                                       std::string_view channel) noexcept
    : publication_(publication), channel_(channel)
{
    if (nullptr != publication_)
    {
        (void)aeron_exclusive_publication_constants(publication_, &constants_);
    }
}

ExclusiveReplayPublication::~ExclusiveReplayPublication()
{
    if (!released_)
    {
        close();
    }
}

bool ExclusiveReplayPublication::is_connected() const noexcept
{
    return nullptr != publication_ && aeron_exclusive_publication_is_connected(publication_);
}

std::int64_t ExclusiveReplayPublication::available_window() const noexcept
{
    // `ExclusivePublication.availableWindow` (`ExclusivePublication.java:195-204`).
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_CLOSED;
    }

    const std::int64_t limit = aeron_exclusive_publication_position_limit(publication_);
    if (limit < 0)
    {
        return limit;
    }

    const std::int64_t position = aeron_exclusive_publication_position(publication_);
    if (position < 0)
    {
        return position;
    }

    return limit - position;
}

std::int32_t ExclusiveReplayPublication::session_id() const noexcept
{
    return constants_.session_id;
}

std::int32_t ExclusiveReplayPublication::stream_id() const noexcept
{
    return constants_.stream_id;
}

std::int32_t ExclusiveReplayPublication::initial_term_id() const noexcept
{
    return constants_.initial_term_id;
}

std::int32_t ExclusiveReplayPublication::position_bits_to_shift() const noexcept
{
    return static_cast<std::int32_t>(constants_.position_bits_to_shift);
}

std::string_view ExclusiveReplayPublication::channel() const noexcept
{
    return channel_;
}

std::int64_t ExclusiveReplayPublication::offer_block(std::span<const std::uint8_t> buffer) noexcept
{
    return nullptr == publication_
               ? AERON_PUBLICATION_CLOSED
               : aeron_exclusive_publication_offer_block(publication_, buffer.data(), buffer.size());
}

std::int64_t ExclusiveReplayPublication::append_padding(std::int32_t length) noexcept
{
    return nullptr == publication_
               ? AERON_PUBLICATION_CLOSED
               : aeron_exclusive_publication_append_padding(publication_, static_cast<std::size_t>(length));
}

void ExclusiveReplayPublication::close() noexcept
{
    if (nullptr != publication_ && !released_)
    {
        released_ = true;
        (void)aeron_exclusive_publication_close(publication_, nullptr, nullptr);
    }
}

void ExclusiveReplayPublication::revoke() noexcept
{
    // Asynchronous revoke-and-close; never wait for the driver (Rust S4: a
    // blocking revoke from the conductor's own thread stalls every publication).
    if (nullptr != publication_ && !released_)
    {
        released_ = true;
        (void)aeron_exclusive_publication_revoke(publication_, nullptr, nullptr);
    }
}

}  // namespace deepmsg::archive::server
