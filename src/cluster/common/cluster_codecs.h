#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>

#include "core/error/error.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"

namespace deepmsg::cluster::common
{

namespace codecs = ::io::aeron::cluster::codecs;

/// The cluster schema (`aeron-cluster-codecs.xml`).
inline constexpr std::int32_t SBE_SCHEMA_ID = 111;
inline constexpr std::size_t MESSAGE_HEADER_LENGTH = codecs::MessageHeader::encodedLength();

/// The 8-byte SBE header of one message, guarded for length but not for
/// schema: the extension listener (`EgressAdapter.java:99-113`) needs the
/// foreign header's four fields before deciding who reads the body.
///
/// The reference reads the header without a guard (`EgressPoller.java:244-252`);
/// a short buffer is a malformed message here and is answered, never a crash.
[[nodiscard]] inline std::expected<codecs::MessageHeader, axc::core::Error> raw_message_header(
    std::span<const std::uint8_t> message) noexcept
{
    if (message.size() < MESSAGE_HEADER_LENGTH)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                     "a cluster message is too short for its own header"));
    }

    codecs::MessageHeader header;
    header.wrap(const_cast<char*>(reinterpret_cast<const char*>(message.data())), 0,
                codecs::MessageHeader::sbeSchemaVersion(), message.size());
    return header;
}

/// The 8-byte SBE header of one cluster message, guarded for length and schema.
[[nodiscard]] inline std::expected<codecs::MessageHeader, axc::core::Error> message_header(
    std::span<const std::uint8_t> message) noexcept
{
    auto header = raw_message_header(message);
    if (!header)
    {
        return header;
    }
    if (header->schemaId() != SBE_SCHEMA_ID)
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::protocol_error,
            "expected schemaId=" + std::to_string(SBE_SCHEMA_ID) + ", actual=" + std::to_string(header->schemaId())));
    }
    return header;
}

/// Wrap a generated decoder at the message body (offset 8), after the header.
template <class Decoder>
[[nodiscard]] Decoder wrap_body(std::span<const std::uint8_t> message, const codecs::MessageHeader& header)
{
    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(message.data())), MESSAGE_HEADER_LENGTH,
                          header.blockLength(), header.version(), message.size());
    return decoder;
}

}  // namespace deepmsg::cluster::common
