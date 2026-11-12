#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/consensus/cluster_session.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"

namespace deepmsg::cluster::consensus
{

/// `AeronCluster.SESSION_HEADER_LENGTH`: the 32-byte session-message header
/// (`SessionMessageHeader` = 8-byte SBE header + 24-byte block).
inline constexpr std::size_t SESSION_HEADER_LENGTH = 32;

/// `LogPublisher` (`LogPublisher.java`): the leader's exclusive publication on
/// the log channel. `appendMessage` is a two-vector `offerv` -- the ingress
/// fragment is never copied into an intermediate buffer -- and the control
/// records are `try_claim` encoded in place.
///
/// The publication is created by the agent (`aeron_exclusive_publication_t`)
/// and handed over once the async add completes.
///
/// `LogAppender` is the seam `SessionManager` sees; the real log and a test
/// fake are interchangeable.
class LogAppender
{
public:
    LogAppender() = default;
    LogAppender(const LogAppender&) = delete;
    LogAppender& operator=(const LogAppender&) = delete;
    virtual ~LogAppender() = default;

    [[nodiscard]] virtual long append_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                              std::int64_t timestamp, const std::uint8_t* payload,
                                              std::size_t payload_length) = 0;
    [[nodiscard]] virtual long append_session_open(const ClusterSession& session, std::int64_t leadership_term_id,
                                                   std::int64_t timestamp) = 0;
    [[nodiscard]] virtual bool append_session_close(ClusterSession& session, std::int64_t leadership_term_id,
                                                    std::int64_t timestamp) = 0;
    [[nodiscard]] virtual std::int64_t position() const = 0;
};

class LogPublisher final : public LogAppender
{
public:
    explicit LogPublisher(std::string destination_channel);

    LogPublisher(const LogPublisher&) = delete;
    LogPublisher& operator=(const LogPublisher&) = delete;
    ~LogPublisher() = default;

    void publication(axc::core::ExPubPtr publication) noexcept
    {
        publication_ = std::move(publication);
    }

    [[nodiscard]] bool has_publication() const noexcept
    {
        return nullptr != publication_;
    }

    void disconnect() noexcept
    {
        publication_.reset();
    }

    [[nodiscard]] std::int64_t position() const noexcept override;
    [[nodiscard]] std::int32_t session_id() const noexcept;

    /// `LogPublisher.appendMessage` (`:115-132`): a two-part offer, the session
    /// header then the caller's payload bytes. Returns the log position, or the
    /// negative publication result (the caller aborts ingress on it).
    [[nodiscard]] long append_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                      std::int64_t timestamp, const std::uint8_t* payload,
                                      std::size_t payload_length) override;

    /// `LogPublisher.appendSessionOpen` (`:134-168`).
    [[nodiscard]] long append_session_open(const ClusterSession& session, std::int64_t leadership_term_id,
                                           std::int64_t timestamp) override;

    /// `LogPublisher.appendSessionClose` (`:170-218`).
    [[nodiscard]] bool append_session_close(ClusterSession& session, std::int64_t leadership_term_id,
                                            std::int64_t timestamp) override;

    /// `LogPublisher.appendTimer` (`:242-267`): the `TimerEvent` record an
    /// expired timer appends. Returns the log position, or the negative
    /// publication result.
    [[nodiscard]] long append_timer(std::int64_t correlation_id, std::int64_t leadership_term_id,
                                    std::int64_t timestamp);

    /// The C4b criterion: the ingress payload's bytes that went through an
    /// intermediate copy. `append_message` is `offerv`, so this stays 0.
    [[nodiscard]] std::size_t payload_bytes_copied() const noexcept
    {
        return payload_bytes_copied_;
    }

    [[nodiscard]] const std::string& destination_channel() const noexcept
    {
        return destination_channel_;
    }

private:
    [[nodiscard]] static bool is_fatal(long position) noexcept;

    std::string destination_channel_{};
    axc::core::ExPubPtr publication_{};
    std::array<std::uint8_t, SESSION_HEADER_LENGTH> session_header_{};
    std::vector<std::uint8_t> encode_buffer_{};
    std::size_t payload_bytes_copied_{0};
};

}  // namespace deepmsg::cluster::consensus
