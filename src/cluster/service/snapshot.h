#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <aeronc.h>

#include <client/aeron_archive.h>

#include "cluster/service/container_config.h"
#include "cluster/service/container_context.h"
#include "cluster/service/role.h"
#include "cluster/service/session.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

namespace detail
{

/// `ChannelUri.addSessionId`: replace an existing `session-id` parameter, or
/// append one (the replay/recording channel the reference builds).
[[nodiscard]] inline std::string with_session_id(std::string_view channel, std::int32_t session_id)
{
    const std::string suffix = "session-id=" + std::to_string(session_id);
    const std::size_t pipe = channel.find('|');
    const std::string base{channel.substr(0, pipe)};
    std::string result = base;
    bool replaced = false;

    if (std::string::npos != pipe)
    {
        std::size_t start = pipe + 1;
        while (start <= channel.size())
        {
            const std::size_t end = channel.find('|', start);
            const std::string_view parameter =
                channel.substr(start, std::string::npos == end ? std::string::npos : end - start);
            result.push_back('|');
            if (0 == parameter.rfind("session-id=", 0))
            {
                result.append(suffix);
                replaced = true;
            }
            else
            {
                result.append(parameter);
            }
            if (std::string::npos == end)
            {
                break;
            }
            start = end + 1;
        }
    }

    if (!replaced)
    {
        result.push_back('|');
        result.append(suffix);
    }
    return result;
}

}  // namespace detail

/// `ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID`.
inline constexpr std::int64_t SNAPSHOT_TYPE_ID = 2;
/// `ServiceSnapshotLoader.FRAGMENT_LIMIT`.
inline constexpr std::size_t SNAPSHOT_FRAGMENT_LIMIT = 10;

/// What the business reads its own records out of (`on_start`'s image).
class SnapshotFragmentHandler
{
public:
    virtual ~SnapshotFragmentHandler() = default;
    virtual void on_snapshot_fragment(const std::uint8_t* buffer, std::size_t length) = 0;
};

/// The replay image handed to `on_start`, positioned just after the container's
/// `SnapshotMarker(END)`.
class SnapshotImage
{
public:
    explicit SnapshotImage(aeron_image_t* image) noexcept : image_(image) {}

    [[nodiscard]] int poll(SnapshotFragmentHandler& handler, std::size_t fragment_limit);
    [[nodiscard]] bool is_end_of_stream() const noexcept;
    [[nodiscard]] bool is_closed() const noexcept;
    [[nodiscard]] std::int64_t position() const noexcept;

private:
    aeron_image_t* image_;
};

/// The snapshot recording's exclusive publication, as the business sees it.
class SnapshotPublication
{
public:
    SnapshotPublication(axc::core::DeepmsgClient& client, ContainerContext& context,
                        aeron_exclusive_publication_t* publication) noexcept
        : client_(&client), context_(&context), publication_(publication)
    {
    }

    /// An exclusive-publication claim; the write goes through `payload()` and
    /// `commit()`.
    class Claim
    {
    public:
        Claim(aeron_buffer_claim_t claim, std::int64_t position, std::span<std::uint8_t> payload) noexcept
            : claim_(claim), position_(position), payload_(payload)
        {
        }

        [[nodiscard]] std::span<std::uint8_t> payload() const noexcept
        {
            return payload_;
        }

        [[nodiscard]] std::int64_t position() const noexcept
        {
            return position_;
        }

        [[nodiscard]] std::expected<void, Error> commit() noexcept;

    private:
        aeron_buffer_claim_t claim_{};
        std::int64_t position_{0};
        std::span<std::uint8_t> payload_{};
    };

    /// The reference's `Publication.offer` codes (negative on refusal).
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> payload) noexcept;
    [[nodiscard]] std::expected<Claim, std::int64_t> try_claim(std::size_t length) noexcept;

    /// `IdleStrategy.idle` + the agent invoker's poll (`SnapshotTaker`).
    void idle() noexcept;

    [[nodiscard]] std::int64_t position() const noexcept;
    [[nodiscard]] std::size_t max_payload_length() const noexcept;
    [[nodiscard]] std::int32_t session_id() const noexcept;

private:
    axc::core::DeepmsgClient* client_;
    ContainerContext* context_;
    aeron_exclusive_publication_t* publication_;
};

/// `ServiceSnapshotTaker` (`ServiceSnapshotTaker.java:28-90`) with the
/// `SnapshotTaker` markers: BEGIN, one `ClientSession` per session in table
/// order, END, then the business's own records.
class ServiceSnapshotTaker
{
public:
    ServiceSnapshotTaker(SnapshotPublication& publication, TimeUnit time_unit, std::int32_t app_version) noexcept
        : publication_(&publication), time_unit_(time_unit), app_version_(app_version)
    {
    }

    [[nodiscard]] std::expected<void, Error> mark_begin(std::int64_t log_position, std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> mark_end(std::int64_t log_position, std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> snapshot_session(const ContainerClientSession& session);

private:
    [[nodiscard]] std::expected<void, Error> mark(std::int32_t mark, std::int64_t log_position,
                                                  std::int64_t leadership_term_id);
    [[nodiscard]] std::expected<void, Error> claim(std::span<const std::uint8_t> record);
    [[nodiscard]] std::expected<void, Error> offer(std::span<const std::uint8_t> record);

    SnapshotPublication* publication_;
    TimeUnit time_unit_;
    std::int32_t app_version_;
};

/// What the loader does with each record (`ClusteredServiceAgent.onLoad*`).
class SnapshotLoadHandler
{
public:
    virtual ~SnapshotLoadHandler() = default;
    [[nodiscard]] virtual std::expected<void, Error> on_load_begin_snapshot(std::int32_t app_version,
                                                                            TimeUnit time_unit) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_cluster_session(
        std::int64_t cluster_session_id, std::int32_t response_stream_id, std::string_view response_channel,
        std::span<const std::uint8_t> encoded_principal) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_load_end_snapshot() = 0;
};

/// `ServiceSnapshotLoader` (`ServiceSnapshotLoader.java:29-140`): walks the
/// container's records and stops at END, leaving the image where the business's
/// own records begin.
class ServiceSnapshotLoader
{
public:
    ServiceSnapshotLoader();
    ~ServiceSnapshotLoader();

    ServiceSnapshotLoader(const ServiceSnapshotLoader&) = delete;
    ServiceSnapshotLoader& operator=(const ServiceSnapshotLoader&) = delete;

    [[nodiscard]] std::expected<int, Error> poll(aeron_image_t* image, SnapshotLoadHandler& handler);

    [[nodiscard]] bool is_done() const noexcept
    {
        return done_;
    }

    [[nodiscard]] std::int32_t app_version() const noexcept
    {
        return app_version_;
    }

    [[nodiscard]] TimeUnit time_unit() const noexcept
    {
        return time_unit_;
    }

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);
    [[nodiscard]] aeron_controlled_fragment_handler_action_t dispatch(std::span<const std::uint8_t> message);

    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    SnapshotLoadHandler* handler_{nullptr};
    std::optional<Error> error_{};
    bool in_snapshot_{false};
    bool done_{false};
    std::int32_t app_version_{0};
    TimeUnit time_unit_{TimeUnit::milliseconds};
};

/// A local archive client for the container's snapshot recording/replay
/// (`AeronArchive.connect` in the reference, per operation).
class ArchiveClient
{
public:
    static std::expected<ArchiveClient, Error> connect(axc::core::DeepmsgClient& client,
                                                       const ServiceContainerConfig& config);
    ArchiveClient() = default;
    ArchiveClient(const ArchiveClient&) = delete;
    ArchiveClient& operator=(const ArchiveClient&) = delete;
    ArchiveClient(ArchiveClient&& other) noexcept;
    ArchiveClient& operator=(ArchiveClient&& other) noexcept;
    ~ArchiveClient();

    [[nodiscard]] aeron_archive_t* handle() noexcept
    {
        return archive_;
    }

    [[nodiscard]] std::int64_t archive_id() const noexcept
    {
        return archive_id_;
    }

    /// Reads one error response if there is one; any error returned by the
    /// archive is raised here, as the reference's `checkForErrorResponse`.
    [[nodiscard]] std::expected<void, Error> check_error();

    [[nodiscard]] std::expected<std::int64_t, Error> start_recording(std::string_view channel, std::int32_t stream_id);
    [[nodiscard]] std::expected<std::int64_t, Error> start_replay(std::int64_t recording_id, std::string_view channel,
                                                                  std::int32_t stream_id,
                                                                  std::int64_t subscription_registration_id);

private:
    aeron_archive_context_t* context_{nullptr};
    aeron_archive_t* archive_{nullptr};
    std::int64_t archive_id_{0};
};

}  // namespace deepmsg::cluster::service
