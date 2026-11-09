#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>

#include "archive/server/catalog.h"

namespace deepmsg::archive::server
{

/// What a paging listing sends (`ControlSession.sendDescriptor` /
/// `sendRecordingUnknown` / `isDone` / `activeListing`). An interface so the
/// cursor rules can be tested without a conductor, exactly like the delete
/// session.
class ListRecordingsResponses
{
public:
    ListRecordingsResponses() = default;
    ListRecordingsResponses(const ListRecordingsResponses&) = delete;
    ListRecordingsResponses& operator=(const ListRecordingsResponses&) = delete;
    virtual ~ListRecordingsResponses() = default;

    [[nodiscard]] virtual bool send_descriptor(std::int64_t control_session_id, std::int64_t correlation_id,
                                               std::span<const std::uint8_t> frame) = 0;
    virtual void send_recording_unknown(std::int64_t control_session_id, std::int64_t correlation_id,
                                        std::int64_t recording_id) = 0;
    [[nodiscard]] virtual bool control_session_is_done(std::int64_t control_session_id) const = 0;
    virtual void clear_active_listing(std::int64_t control_session_id) = 0;
};

/// `ArchiveConductor.newListRecordingsSession` (`ArchiveConductor.java:640-645`)
/// and `newListRecordingsForUriSession` (`:666-671`): the one gate both paging
/// listings share. Returns the error message to send with `ACTIVE_LISTING`.
[[nodiscard]] std::optional<std::string> listing_start_error(bool has_active_listing);

/// `ListRecordingsSession` (8) and `ListRecordingsForUriSession` (9) differ only
/// in `accept_descriptor`; `filter_by_uri` selects that branch.
struct ListRecordingsConfig
{
    std::int64_t control_session_id{0};
    std::int64_t correlation_id{0};
    std::int64_t from_recording_id{0};
    std::int32_t count{0};
    /// 9 only, ignored when `filter_by_uri` is false.
    std::int32_t stream_id{0};
    std::string channel_fragment;
    bool filter_by_uri{false};
};

/// `AbstractListRecordingsSession` (`AbstractListRecordingsSession.java:22-142`):
/// the paging cursor, the 64-candidate scan bound and the terminal
/// `RECORDING_UNKNOWN` response.
class ListRecordingsSession
{
public:
    /// `AbstractListRecordingsSession.MAX_SCANS_PER_WORK_CYCLE` (`:22`).
    static constexpr std::int32_t MAX_SCANS_PER_WORK_CYCLE = 64;

    ListRecordingsSession(const Catalog& catalog, ListRecordingsResponses& responses,
                          const ListRecordingsConfig& config);

    ListRecordingsSession(const ListRecordingsSession&) = delete;
    ListRecordingsSession& operator=(const ListRecordingsSession&) = delete;

    /// `AbstractListRecordingsSession.doWork` (`:79-142`).
    int do_work();

    [[nodiscard]] bool is_done() const noexcept
    {
        return is_done_;
    }

    /// `:141-145`: release the control session's active listing slot.
    void close();

    /// The cursor, i.e. the id the next pass starts from (test observable).
    [[nodiscard]] std::int64_t cursor() const noexcept
    {
        return recording_id_;
    }

    [[nodiscard]] std::int32_t sent() const noexcept
    {
        return sent_;
    }

private:
    /// `ListRecordingsSession.acceptDescriptor` (`:39-42`) /
    /// `ListRecordingsForUriSession.acceptDescriptor` (`:52-61`).
    [[nodiscard]] bool accept_descriptor(std::span<const std::uint8_t> frame) const;

    const Catalog& catalog_;
    ListRecordingsResponses& responses_;
    ListRecordingsConfig config_;
    std::int64_t recording_id_{0};
    std::int32_t sent_{0};
    bool is_done_{false};
};

}  // namespace deepmsg::archive::server
