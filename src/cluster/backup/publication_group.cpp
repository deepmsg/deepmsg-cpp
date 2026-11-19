#include "cluster/backup/publication_group.h"

#include <algorithm>
#include <random>
#include <utility>

#include "cluster/backup/channel_uri.h"

namespace deepmsg::cluster::backup
{

PublicationGroup::PublicationGroup(std::vector<std::string> endpoints, std::string channel_template,
                                   std::int32_t stream_id)
    : endpoints_(std::move(endpoints)), channel_template_(std::move(channel_template)), stream_id_(stream_id)
{
}

PublicationGroup::~PublicationGroup()
{
    close();
}

void PublicationGroup::shuffle()
{
    close();
    has_excluded_ = false;
    std::random_device device;
    std::mt19937 random{device()};
    std::shuffle(endpoints_.begin(), endpoints_.end(), random);
}

std::size_t PublicationGroup::next_cursor() noexcept
{
    do
    {
        ++cursor_;
        if (endpoints_.size() <= cursor_)
        {
            cursor_ = 0;
        }
    } while (has_excluded_ && cursor_ == excluded_cursor_ && endpoints_.size() > 1);
    return cursor_;
}

void PublicationGroup::next(axc::core::DeepmsgClient& client)
{
    // The reference's add is synchronous, so its caller can ask for `next()`
    // every pass. The C add is asynchronous: re-issuing it while the first one
    // is in flight would queue one command per pass and fill the driver's
    // to-client ring. `poll()` drives the pending add instead.
    if (endpoints_.empty() || adding_)
    {
        return;
    }

    const std::size_t cursor = next_cursor();
    const std::string channel = channel_with_endpoint(channel_template_, endpoints_[cursor]);

    current_.reset();
    add_ = client.add_exclusive_publication(channel, stream_id_);
    adding_ = true;
    (void)client;
}

int PublicationGroup::poll(axc::core::DeepmsgClient& client)
{
    if (!adding_)
    {
        return 0;
    }

    (void)client;
    auto polled = add_.poll();
    if (!polled)
    {
        adding_ = false;
        failed_ = true;
        return -1;
    }
    if (!polled->has_value())
    {
        return 0;
    }
    current_ = std::move(**polled);
    adding_ = false;
    failed_ = false;
    return 1;
}

bool PublicationGroup::is_connected() const noexcept
{
    return nullptr != current_ && aeron_exclusive_publication_is_connected(current_.get());
}

void PublicationGroup::close_and_exclude_current()
{
    has_excluded_ = true;
    excluded_cursor_ = cursor_;
    current_.reset();
    adding_ = false;
}

void PublicationGroup::close() noexcept
{
    current_.reset();
    adding_ = false;
}

}  // namespace deepmsg::cluster::backup
