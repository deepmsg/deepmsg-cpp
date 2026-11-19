#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <aeronc.h>

#include "core/deepmsg/deepmsg_client.h"

namespace deepmsg::cluster::backup
{

/// `PublicationGroup<Publication>` (`PublicationGroup.java:28-139`): a group of
/// exclusive publications to the cluster members' consensus endpoints. The
/// group walks the endpoint list, skips the endpoint that answered with a
/// REDIRECT, and can be reshuffled.
///
/// The reference creates the publication synchronously; the C client's add is
/// asynchronous, so `next()` starts the add and `poll()` drives it.
class PublicationGroup
{
public:
    PublicationGroup(std::vector<std::string> endpoints, std::string channel_template, std::int32_t stream_id);

    PublicationGroup(const PublicationGroup&) = delete;
    PublicationGroup& operator=(const PublicationGroup&) = delete;
    PublicationGroup(PublicationGroup&&) = delete;
    PublicationGroup& operator=(PublicationGroup&&) = delete;
    ~PublicationGroup();

    /// `shuffle()` (`:97-109`): close the current publication and shuffle the
    /// endpoint list.
    void shuffle();

    /// `next(aeron)` (`:62-74`): advance the cursor and start a publication to
    /// the next endpoint.
    void next(axc::core::DeepmsgClient& client);

    /// Drives an in-flight add. Returns 1 when a publication became available,
    /// 0 while pending, -1 when the add failed (the group is left able to try
    /// the next endpoint).
    [[nodiscard]] int poll(axc::core::DeepmsgClient& client);

    [[nodiscard]] aeron_exclusive_publication_t* current() const noexcept
    {
        return current_.get();
    }

    /// `isConnected()` (`:130-133`).
    [[nodiscard]] bool is_connected() const noexcept;

    /// `closeAndExcludeCurrent()` (`:88-92`).
    void close_and_exclude_current();

    void close() noexcept;

private:
    [[nodiscard]] std::size_t next_cursor() noexcept;

    std::vector<std::string> endpoints_{};
    std::string channel_template_{};
    std::int32_t stream_id_{0};
    std::size_t cursor_{0};
    bool has_excluded_{false};
    std::size_t excluded_cursor_{0};
    axc::core::ExPubPtr current_{};
    axc::core::AsyncAdd<axc::core::ExPubPtr> add_{};
    bool adding_{false};
    bool failed_{false};
};

}  // namespace deepmsg::cluster::backup
