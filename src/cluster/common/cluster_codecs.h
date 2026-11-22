#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <stdexcept>
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

/// Guard that the fixed SBE root block declared in `header` fits inside
/// `message`. A short body makes the generated decoder throw from
/// `sbeCheckPosition`, and the adapters are called from C frames, so the length
/// is judged before a decoder is wrapped.
[[nodiscard]] inline std::expected<void, axc::core::Error> check_body_length(
    std::span<const std::uint8_t> message, const codecs::MessageHeader& header) noexcept
{
    if (MESSAGE_HEADER_LENGTH + static_cast<std::size_t>(header.blockLength()) > message.size())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                     "a cluster message is shorter than its declared blockLength"));
    }
    return {};
}

/// The same for a field whose payload is a byte array.
template <class Decoder>
[[nodiscard]] std::expected<std::span<const std::uint8_t>, axc::core::Error> read_var_bytes(
    Decoder& decoder, std::span<const std::uint8_t> message) noexcept
{
    const std::uint64_t position = decoder.sbePosition();
    if (position + sizeof(std::uint32_t) > message.size())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                     "a cluster message ends inside a variable-length length field"));
    }

    std::uint32_t length = 0;
    std::memcpy(&length, message.data() + position, sizeof(length));
    if (position + sizeof(length) + length > message.size())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                                     "a variable-length field runs past the end of its message"));
    }

    decoder.sbePosition(position + sizeof(length) + length);
    return message.subspan(static_cast<std::size_t>(position) + sizeof(length), length);
}

/// The variable-length field at `decoder`'s position, read against the whole
/// message and advancing the decoder exactly as the generated accessor does.
///
/// The generated `getXxxAsStringView` / field-pointer accessors read the length
/// header from the buffer and return a view of it without checking that the
/// payload fits. A corrupt length therefore runs off the end, and the copy that
/// follows faults — a `catch` cannot see it. This is the checked read.
template <class Decoder>
[[nodiscard]] std::expected<std::string_view, axc::core::Error> read_var_string(
    Decoder& decoder, std::span<const std::uint8_t> message) noexcept
{
    auto bytes = read_var_bytes(decoder, message);
    if (!bytes)
    {
        return std::unexpected(bytes.error());
    }
    return std::string_view{reinterpret_cast<const char*>(bytes->data()), bytes->size()};
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
    if (auto body = check_body_length(message, *header); !body)
    {
        return std::unexpected(body.error());
    }
    return header;
}

/// Wrap a generated decoder at the message body (offset 8), after the header.
///
/// The generated fixed-field accessors read at their encoding offsets without a
/// bounds check (only the variable-length tail goes through `sbeCheckPosition`),
/// so a sender that declares a block length smaller than its template's would
/// make them read past the fragment. Such a frame is refused here; every caller
/// is inside the adapter's `catch (std::exception&)`, which turns it into the
/// pass-ending protocol error.
template <class Decoder>
[[nodiscard]] Decoder wrap_body(std::span<const std::uint8_t> message, const codecs::MessageHeader& header)
{
    if (static_cast<std::size_t>(header.blockLength()) < Decoder::SBE_BLOCK_LENGTH)
    {
        throw std::runtime_error("a cluster message's blockLength is shorter than its template's");
    }

    Decoder decoder;
    decoder.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(message.data())), MESSAGE_HEADER_LENGTH,
                          header.blockLength(), header.version(), message.size());
    return decoder;
}

}  // namespace deepmsg::cluster::common
