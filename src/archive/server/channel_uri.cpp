#include "archive/server/channel_uri.h"

#include <aeronc.h>

// The internal URI header does not wrap its declarations in `extern "C"`.
extern "C"
{
#include "uri/aeron_uri.h"
}

#include <cstddef>
#include <cstring>
#include <string>

#include "archive/server/archive_config.h"

namespace deepmsg::archive::server
{
namespace
{

constexpr std::string_view SPY_PREFIX{"aeron-spy:"};

[[nodiscard]] std::string_view without_spy_prefix(std::string_view channel) noexcept
{
    if (channel.size() > SPY_PREFIX.size() && channel.substr(0, SPY_PREFIX.size()) == SPY_PREFIX)
    {
        return channel.substr(SPY_PREFIX.size());
    }
    return channel;
}

/// `ChannelUri.parse`: the C client's own parser so the two agree on syntax.
[[nodiscard]] std::expected<aeron_uri_t, axc::core::Error> parse_channel(std::string_view channel)
{
    const std::string_view parsed_channel = without_spy_prefix(channel);
    aeron_uri_t uri{};
    if (0 != aeron_uri_parse(parsed_channel.size(), parsed_channel.data(), &uri))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument, aeron_errmsg()));
    }
    return uri;
}

[[nodiscard]] const char* udp_param(const aeron_uri_t& uri, std::string_view key) noexcept
{
    const aeron_udp_channel_params_t& params = uri.params.udp;
    if (key == "endpoint")
    {
        return params.endpoint;
    }
    if (key == "interface")
    {
        return params.bind_interface;
    }
    if (key == "control")
    {
        return params.control;
    }
    if (key == "control-mode")
    {
        return params.control_mode;
    }
    if (key == "ttl")
    {
        return params.ttl;
    }
    return nullptr;
}

[[nodiscard]] const char* param(const aeron_uri_t& uri, std::string_view key) noexcept
{
    if (AERON_URI_UDP == uri.type)
    {
        if (const char* value = udp_param(uri, key); nullptr != value)
        {
            return value;
        }
    }
    else if (key == "control-mode")
    {
        return uri.params.ipc.control_mode;
    }

    const aeron_uri_params_t& extra =
        AERON_URI_UDP == uri.type ? uri.params.udp.additional_params : uri.params.ipc.additional_params;
    for (std::size_t index = 0; index < extra.length; index++)
    {
        if (key == extra.array[index].key)
        {
            return extra.array[index].value;
        }
    }

    return nullptr;
}

/// `ChannelUri.get(TAGS_PARAM_NAME)`: the C parser splits `tags=channel,entity`
/// in place, so the original value is put back together.
[[nodiscard]] std::string tags_value(const aeron_uri_t& uri)
{
    const char* channel_tag = AERON_URI_UDP == uri.type ? uri.params.udp.channel_tag : uri.params.ipc.channel_tag;
    const char* entity_tag = AERON_URI_UDP == uri.type ? uri.params.udp.entity_tag : uri.params.ipc.entity_tag;

    std::string value;
    if (nullptr != channel_tag)
    {
        value = channel_tag;
    }
    if (nullptr != entity_tag)
    {
        value.push_back(',');
        value += entity_tag;
    }
    return value;
}

void append_param(std::string& out, std::string_view key, const char* value)
{
    if (nullptr == value || '\0' == *value)
    {
        return;
    }
    out.append(key);
    out.push_back('=');
    out.append(value);
    out.push_back('|');
}

void append_param(std::string& out, std::string_view key, const std::string& value)
{
    append_param(out, key, value.c_str());
}

void append_int(std::string& out, std::string_view key, std::int64_t value)
{
    append_param(out, key, std::to_string(value));
}

/// `ArchiveConductor.java:457`: a size parameter on the channel wins over the
/// context default; `parseSize` suffixes (`64k`) are honoured.
[[nodiscard]] std::int32_t uri_int32(const aeron_uri_t& uri, std::string_view key, std::int32_t fallback) noexcept
{
    const char* value = param(uri, key);
    if (nullptr == value)
    {
        return fallback;
    }
    const auto parsed = parse_size(value);
    return parsed.has_value() ? static_cast<std::int32_t>(*parsed) : fallback;
}

[[nodiscard]] bool uri_bool(const aeron_uri_t& uri, std::string_view key, bool fallback) noexcept
{
    const char* value = param(uri, key);
    if (nullptr == value)
    {
        return fallback;
    }
    return 0 == std::strcmp(value, "true");
}

/// `ChannelUriStringBuilder.build()` drops a trailing `|` or `?` (`:2555-2558`).
void trim_trailing_separator(std::string& out)
{
    if (!out.empty() && ('|' == out.back() || '?' == out.back()))
    {
        out.pop_back();
    }
}

/// Which of the forced parameters `build_channel` emits, and their values.
struct BuildOptions
{
    bool force_mtu{false};
    std::int32_t mtu{0};
    bool force_term_length{false};
    std::int32_t term_length{0};
    bool has_initial_position{false};
    std::int32_t initial_term_id{0};
    std::int32_t term_id{0};
    std::int32_t term_offset{0};
    /// Control path: `termLength` / `sparse` are always written.
    bool force_sparse{false};
    bool sparse{false};
    /// Replay path: `eos` / `sparse` are copied from the channel when present
    /// (`ChannelUriStringBuilder.eos(ChannelUri)` / `.sparse(ChannelUri)`).
    bool copy_sparse{false};
    bool copy_eos{false};
    bool has_linger{false};
    std::int64_t linger_ns{0};
    bool has_response_correlation_id{false};
    std::int64_t response_correlation_id{0};
};

/// `ChannelUriStringBuilder.build()` order (`ChannelUriStringBuilder.java:2514-2552`),
/// restricted to the parameters `strippedChannelBuilder` copies plus the ones
/// the control and replay paths force.
[[nodiscard]] std::string build_channel(const aeron_uri_t& uri, const BuildOptions& options)
{
    std::string out;
    out += "aeron:";
    out += AERON_URI_UDP == uri.type ? "udp" : "ipc";
    out.push_back('?');

    append_param(out, "tags", tags_value(uri));
    append_param(out, "endpoint", param(uri, "endpoint"));
    append_param(out, "interface", param(uri, "interface"));
    append_param(out, "control", param(uri, "control"));
    append_param(out, "control-mode", param(uri, "control-mode"));

    if (options.force_mtu)
    {
        append_int(out, "mtu", options.mtu);
    }
    if (options.force_term_length)
    {
        append_int(out, "term-length", options.term_length);
    }
    if (options.has_initial_position)
    {
        append_int(out, "init-term-id", options.initial_term_id);
        append_int(out, "term-id", options.term_id);
        append_int(out, "term-offset", options.term_offset);
    }
    append_param(out, "session-id", param(uri, "session-id"));
    append_param(out, "ttl", param(uri, "ttl"));
    if (options.has_linger)
    {
        append_param(out, "linger", format_duration_ns(options.linger_ns));
    }
    append_param(out, "alias", param(uri, "alias"));
    append_param(out, "cc", param(uri, "cc"));
    append_param(out, "fc", param(uri, "fc"));
    append_param(out, "gtag", param(uri, "gtag"));
    if (options.force_sparse)
    {
        append_param(out, "sparse", options.sparse ? "true" : "false");
    }
    else if (options.copy_sparse)
    {
        append_param(out, "sparse", param(uri, "sparse"));
    }
    if (options.copy_eos)
    {
        append_param(out, "eos", param(uri, "eos"));
    }
    append_param(out, "tether", param(uri, "tether"));
    append_param(out, "group", param(uri, "group"));
    append_param(out, "rejoin", param(uri, "rejoin"));
    append_param(out, "so-sndbuf", param(uri, "so-sndbuf"));
    append_param(out, "so-rcvbuf", param(uri, "so-rcvbuf"));
    append_param(out, "rcv-wnd", param(uri, "rcv-wnd"));
    append_param(out, "media-rcv-ts-offset", param(uri, "media-rcv-ts-offset"));
    append_param(out, "channel-rcv-ts-offset", param(uri, "channel-rcv-ts-offset"));
    append_param(out, "channel-snd-ts-offset", param(uri, "channel-snd-ts-offset"));
    append_param(out, "response-endpoint", param(uri, "response-endpoint"));
    if (options.has_response_correlation_id)
    {
        append_int(out, "response-correlation-id", options.response_correlation_id);
    }
    else
    {
        append_param(out, "response-correlation-id", param(uri, "response-correlation-id"));
    }
    append_param(out, "stream-id", param(uri, "stream-id"));

    trim_trailing_separator(out);
    return out;
}

}  // namespace

bool is_udp_channel(std::string_view channel) noexcept
{
    const std::string_view parsed_channel = without_spy_prefix(channel);
    return parsed_channel.size() >= 9 && parsed_channel.substr(0, 9) == "aeron:udp";
}

bool is_mds_response_channel(std::string_view channel) noexcept
{
    const std::string_view parsed_channel = without_spy_prefix(channel);
    return parsed_channel.find("control-mode=response") != std::string_view::npos;
}

std::expected<std::string, axc::core::Error> stripped_channel(std::string_view channel)
{
    auto uri = parse_channel(channel);
    if (!uri)
    {
        return std::unexpected(uri.error());
    }
    return build_channel(*uri, BuildOptions{});
}

std::expected<std::string, axc::core::Error> control_response_channel(std::string_view channel,
                                                                      const ChannelUriOptions& options)
{
    auto uri = parse_channel(channel);
    if (!uri)
    {
        return std::unexpected(uri.error());
    }

    // `ArchiveConductor.java:453-468`: a value on the channel wins over the
    // context default, and only the context default is used when it is absent.
    BuildOptions build;
    build.force_mtu = true;
    build.mtu = uri_int32(*uri, "mtu", options.mtu_length);
    build.force_term_length = true;
    build.term_length = uri_int32(*uri, "term-length", options.term_length);
    build.force_sparse = true;
    build.sparse = uri_bool(*uri, "sparse", options.sparse);
    build.has_response_correlation_id = options.has_response_correlation_id;
    build.response_correlation_id = options.response_correlation_id;
    return build_channel(*uri, build);
}

std::expected<std::string, axc::core::Error> replay_channel(std::string_view channel,
                                                            const ReplayChannelOptions& options)
{
    auto uri = parse_channel(channel);
    if (!uri)
    {
        return std::unexpected(uri.error());
    }

    // `ChannelUriStringBuilder.initialPosition` (`:1628-1648`): a negative
    // position, a position off the frame alignment or a non-power-of-two term
    // length is an error, the same checks the builder throws.
    constexpr std::int64_t FRAME_ALIGNMENT_MASK = 32 - 1;
    if (options.position < 0 || 0 != (options.position & FRAME_ALIGNMENT_MASK))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                     "invalid initial position=" + std::to_string(options.position)));
    }
    if (options.term_length <= 0 || 0 != (options.term_length & (options.term_length - 1)))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                     "invalid term length=" + std::to_string(options.term_length)));
    }

    const std::int32_t position_bits = __builtin_ctz(static_cast<unsigned>(options.term_length));

    BuildOptions build;
    build.force_mtu = true;
    build.mtu = options.mtu_length;
    build.force_term_length = true;
    build.term_length = options.term_length;
    build.has_initial_position = true;
    build.initial_term_id = options.initial_term_id;
    build.term_id = options.initial_term_id + static_cast<std::int32_t>(options.position >> position_bits);
    build.term_offset = static_cast<std::int32_t>(options.position & (options.term_length - 1));
    build.copy_sparse = true;
    build.copy_eos = true;

    // `ArchiveConductor.java:899-901`: the channel's `linger` wins, otherwise
    // the context's replay linger timeout.
    build.has_linger = true;
    if (const char* linger_value = param(*uri, "linger"); nullptr != linger_value)
    {
        auto parsed = parse_duration_ns(linger_value);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        build.linger_ns = *parsed;
    }
    else
    {
        build.linger_ns = options.linger_timeout_ns;
    }

    build.has_response_correlation_id = options.has_response_correlation_id;
    build.response_correlation_id = options.response_correlation_id;
    return build_channel(*uri, build);
}

std::expected<std::string, axc::core::Error> make_key(std::int32_t stream_id, std::string_view channel)
{
    auto uri = parse_channel(channel);
    if (!uri)
    {
        return std::unexpected(uri.error());
    }

    // `ArchiveConductor.makeKey` (`ArchiveConductor.java:1914-1947`).
    std::string out = std::to_string(stream_id);
    out.push_back(':');
    out += AERON_URI_UDP == uri->type ? "udp" : "ipc";
    out.push_back('?');

    append_param(out, "endpoint", param(*uri, "endpoint"));
    append_param(out, "interface", param(*uri, "interface"));
    append_param(out, "control", param(*uri, "control"));
    append_param(out, "session-id", param(*uri, "session-id"));
    append_param(out, "tags", tags_value(*uri));

    // `:1945`: chopped unconditionally, so a key with no parameters keeps the
    // `streamId:media` prefix and loses the `?`.
    out.pop_back();
    return out;
}

}  // namespace deepmsg::archive::server
