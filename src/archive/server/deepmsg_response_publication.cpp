#include "archive/server/deepmsg_response_publication.h"

// The payload length lives on the internal struct; the public API has no
// accessor for exclusive publications in 1.54.
#include "aeron_exclusive_publication.h"

namespace deepmsg::archive::server
{

DeepmsgExclusivePublication::~DeepmsgExclusivePublication()
{
    close();
}

DeepmsgExclusivePublication::OfferResult DeepmsgExclusivePublication::offer(std::span<const std::uint8_t> buffer)
{
    return aeron_exclusive_publication_offer(publication_, buffer.data(), buffer.size(), nullptr, nullptr);
}

DeepmsgExclusivePublication::OfferResult DeepmsgExclusivePublication::try_claim(std::int32_t length, Claim& claim)
{
    const std::int64_t result =
        aeron_exclusive_publication_try_claim(publication_, static_cast<std::size_t>(length), &claim_);
    if (result > 0)
    {
        claim.buffer = {claim_.data, claim_.length};
        claim.commit_fn = &DeepmsgExclusivePublication::commit_claim;
        claim.context = &claim_;
    }
    return result;
}

bool DeepmsgExclusivePublication::is_connected() const
{
    return 0 != aeron_exclusive_publication_is_connected(publication_);
}

std::int32_t DeepmsgExclusivePublication::max_payload_length() const
{
    return static_cast<std::int32_t>(publication_->max_payload_length);
}

void DeepmsgExclusivePublication::close()
{
    if (nullptr != publication_)
    {
        aeron_exclusive_publication_close(publication_, nullptr, nullptr);
        publication_ = nullptr;
    }
}

void DeepmsgExclusivePublication::commit_claim(void* context) noexcept
{
    auto* claim = static_cast<aeron_buffer_claim_t*>(context);
    aeron_buffer_claim_commit(claim);
}

}  // namespace deepmsg::archive::server
