#include "cluster/backup/backup_publisher.h"

#include <cstddef>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/BackupQuery.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"

namespace deepmsg::cluster::backup
{
namespace
{

namespace codecs = ::io::aeron::cluster::codecs;

constexpr std::size_t EXTRA_LENGTH = 256;

template <class Encoder, class Fill>
std::vector<std::uint8_t> encode(Fill&& fill)
{
    std::vector<std::uint8_t> buffer(common::MESSAGE_HEADER_LENGTH + Encoder::SBE_BLOCK_LENGTH + EXTRA_LENGTH, 0);
    Encoder encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    fill(encoder);
    buffer.resize(common::MESSAGE_HEADER_LENGTH + encoder.encodedLength());
    return buffer;
}

[[nodiscard]] bool offer(aeron_exclusive_publication_t* publication, std::span<const std::uint8_t> message)
{
    if (nullptr == publication)
    {
        return false;
    }
    const std::int64_t result =
        aeron_exclusive_publication_offer(publication, message.data(), message.size(), nullptr, nullptr);
    return result > 0;
}

}  // namespace

bool BackupPublisher::backup_query(aeron_exclusive_publication_t* publication, std::int64_t correlation_id,
                                   std::int32_t response_stream_id, std::int32_t version, std::int64_t log_position,
                                   std::string_view response_channel, std::span<const std::uint8_t> encoded_credentials)
{
    const std::int64_t log_position_value =
        -1 == log_position ? codecs::BackupQuery::logPositionNullValue() : log_position;
    const auto buffer = encode<codecs::BackupQuery>(
        [&](codecs::BackupQuery& encoder)
        {
            encoder.correlationId(correlation_id)
                .responseStreamId(response_stream_id)
                .version(version)
                .logPosition(log_position_value)
                .putResponseChannel(reinterpret_cast<const char*>(response_channel.data()),
                                    static_cast<std::uint32_t>(response_channel.size()))
                .putEncodedCredentials(reinterpret_cast<const char*>(encoded_credentials.data()),
                                       static_cast<std::uint32_t>(encoded_credentials.size()));
        });
    return offer(publication, buffer);
}

bool BackupPublisher::challenge_response(aeron_exclusive_publication_t* publication, std::int64_t correlation_id,
                                         std::int64_t cluster_session_id,
                                         std::span<const std::uint8_t> encoded_credentials)
{
    const auto buffer = encode<codecs::ChallengeResponse>(
        [&](codecs::ChallengeResponse& encoder)
        {
            encoder.correlationId(correlation_id)
                .clusterSessionId(cluster_session_id)
                .putEncodedCredentials(reinterpret_cast<const char*>(encoded_credentials.data()),
                                       static_cast<std::uint32_t>(encoded_credentials.size()));
        });
    return offer(publication, buffer);
}

}  // namespace deepmsg::cluster::backup
