#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/collections/long2long_hash_map.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// How long a publication add/remove may take (`Segment`'s pattern, used by the
/// reference's synchronous `addPublication`).
inline constexpr std::int64_t EGRESS_COMMAND_TIMEOUT_NS = 1'000'000'000;

/// `ContainerClientSession` (`ContainerClientSession.java`), as the container
/// holds it. The egress publication is per session; on a follower it is absent.
class ContainerClientSession
{
public:
    ContainerClientSession() = default;
    ContainerClientSession(std::int64_t id, std::int32_t response_stream_id, std::string response_channel,
                           std::vector<std::uint8_t> encoded_principal);

    ContainerClientSession(const ContainerClientSession&) = delete;
    ContainerClientSession& operator=(const ContainerClientSession&) = delete;
    ContainerClientSession(ContainerClientSession&&) noexcept = default;
    ContainerClientSession& operator=(ContainerClientSession&&) noexcept = default;
    ~ContainerClientSession() = default;

    [[nodiscard]] std::int64_t id() const noexcept
    {
        return id_;
    }

    [[nodiscard]] std::int32_t response_stream_id() const noexcept
    {
        return response_stream_id_;
    }

    [[nodiscard]] const std::string& response_channel() const noexcept
    {
        return response_channel_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& encoded_principal() const noexcept
    {
        return encoded_principal_;
    }

    [[nodiscard]] bool is_closing() const noexcept
    {
        return is_closing_;
    }

    void mark_closing() noexcept
    {
        is_closing_ = true;
    }

    void reset_closing() noexcept
    {
        is_closing_ = false;
    }

    [[nodiscard]] aeron_publication_t* publication() noexcept
    {
        return publication_.has_value() ? publication_->get() : nullptr;
    }

    [[nodiscard]] bool has_publication() const noexcept
    {
        return publication_.has_value();
    }

    /// `ContainerClientSession.connect(Aeron)`.
    [[nodiscard]] std::expected<void, Error> connect(axc::core::DeepmsgClient& client);

    /// `disconnect(ErrorHandler)`: the publication is closed locally and the
    /// driver is told; failure is reported by the caller.
    [[nodiscard]] std::expected<void, Error> disconnect() noexcept;

private:
    std::int64_t id_{0};
    std::int32_t response_stream_id_{0};
    std::string response_channel_{};
    std::vector<std::uint8_t> encoded_principal_{};
    std::optional<axc::core::PubPtr> publication_{};
    bool is_closing_{false};
};

/// The session table: insertion order by id (the snapshot order), lookup by id.
class SessionTable
{
public:
    SessionTable();

    SessionTable(const SessionTable&) = delete;
    SessionTable& operator=(const SessionTable&) = delete;
    SessionTable(SessionTable&&) noexcept = default;
    SessionTable& operator=(SessionTable&&) noexcept = default;
    ~SessionTable() = default;

    [[nodiscard]] std::size_t size() const noexcept
    {
        return sessions_.size();
    }

    [[nodiscard]] bool empty() const noexcept
    {
        return sessions_.empty();
    }

    [[nodiscard]] ContainerClientSession* get(std::int64_t id) noexcept;
    [[nodiscard]] const ContainerClientSession* get(std::int64_t id) const noexcept;

    [[nodiscard]] std::vector<ContainerClientSession>& sessions() noexcept
    {
        return sessions_;
    }

    [[nodiscard]] const std::vector<ContainerClientSession>& sessions() const noexcept
    {
        return sessions_;
    }

    /// `addSession` (`CSA:628-652`): insert after the last session with a
    /// smaller id.
    void add(ContainerClientSession session);

    /// Remove by id; `std::nullopt` when it is not in the table.
    [[nodiscard]] std::optional<ContainerClientSession> remove(std::int64_t id);

private:
    void reindex_from(std::size_t position);

    std::vector<ContainerClientSession> sessions_{};
    axc::core::Long2LongHashMap index_by_id_;
};

}  // namespace deepmsg::cluster::service
