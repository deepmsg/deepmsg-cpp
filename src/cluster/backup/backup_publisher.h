#pragma once

#include <cstdint>
#include <span>
#include <string_view>

#include <aeronc.h>

namespace deepmsg::cluster::backup
{

/// The two consensus messages the backup sends, encoded exactly as
/// `ConsensusPublisher.backupQuery` / `.challengeResponse`
/// (`ConsensusPublisher.java:452-481`, `:558-580`).
class BackupPublisher
{
public:
    [[nodiscard]] static bool backup_query(aeron_exclusive_publication_t* publication, std::int64_t correlation_id,
                                           std::int32_t response_stream_id, std::int32_t version,
                                           std::int64_t log_position, std::string_view response_channel,
                                           std::span<const std::uint8_t> encoded_credentials);

    [[nodiscard]] static bool challenge_response(aeron_exclusive_publication_t* publication,
                                                 std::int64_t correlation_id, std::int64_t cluster_session_id,
                                                 std::span<const std::uint8_t> encoded_credentials);
};

}  // namespace deepmsg::cluster::backup
