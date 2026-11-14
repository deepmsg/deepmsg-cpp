#include "core/media/dpdk/media.h"

#include <arpa/inet.h>
#include <tuple>

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

namespace axc::core::media::dpdk
{

namespace
{

constexpr std::string_view PROPERTY_PREFIX{"deepmsg.dpdk."};

constexpr std::string_view KNOWN_PROPERTIES[] = {
    "deepmsg.dpdk.enabled",         "deepmsg.dpdk.eal.args",       "deepmsg.dpdk.port",
    "deepmsg.dpdk.ipv4.address",    "deepmsg.dpdk.ipv4.gateway",   "deepmsg.dpdk.static.neighbours",
    "deepmsg.dpdk.rx.descriptors",  "deepmsg.dpdk.tx.descriptors", "deepmsg.dpdk.mempool.size",
    "deepmsg.dpdk.mempool.cache",   "deepmsg.dpdk.rx.burst",       "deepmsg.dpdk.sender.ring",
    "deepmsg.dpdk.ephemeral.ports", "deepmsg.dpdk.arp.timeout.ms", "deepmsg.dpdk.arp.retry.ms",
};

[[nodiscard]] Error config_error(std::string_view name, std::string_view reason) noexcept
{
    std::string message{name};
    message += ": ";
    message += reason;
    return make_error(ErrorCode::config_error, message);
}

[[nodiscard]] bool is_bool(std::string_view value) noexcept
{
    return value == "true" || value == "false";
}

[[nodiscard]] std::expected<std::uint64_t, Error> parse_uint(std::string_view name, std::string_view value,
                                                             std::uint64_t minimum, std::uint64_t maximum) noexcept
{
    if (value.empty())
    {
        return std::unexpected(config_error(name, "value is empty"));
    }

    std::uint64_t parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (std::errc{} != error || end != value.data() + value.size())
    {
        return std::unexpected(config_error(name, "not an unsigned integer"));
    }
    if (parsed < minimum || parsed > maximum)
    {
        return std::unexpected(config_error(name, "value is out of range"));
    }
    return parsed;
}

[[nodiscard]] std::expected<std::uint32_t, Error> parse_ipv4(std::string_view name, std::string_view value) noexcept
{
    struct in_addr address{};
    const std::string copy{value};
    if (1 != ::inet_pton(AF_INET, copy.c_str(), &address))
    {
        return std::unexpected(config_error(name, "not an IPv4 address"));
    }
    return ntohl(address.s_addr);
}

[[nodiscard]] std::expected<Ipv4Prefix, Error> parse_ipv4_prefix(std::string_view name, std::string_view value) noexcept
{
    const std::size_t slash = value.find('/');
    if (std::string_view::npos == slash)
    {
        return std::unexpected(config_error(name, "expected <address>/<prefix>"));
    }

    auto address = parse_ipv4(name, value.substr(0, slash));
    if (!address)
    {
        return std::unexpected(address.error());
    }
    auto prefix = parse_uint(name, value.substr(slash + 1), 0, 32);
    if (!prefix)
    {
        return std::unexpected(prefix.error());
    }
    return Ipv4Prefix{*address, static_cast<std::uint8_t>(*prefix)};
}

[[nodiscard]] std::expected<std::pair<std::uint16_t, std::uint16_t>, Error> parse_port_range(
    std::string_view name, std::string_view value) noexcept
{
    const std::size_t dash = value.find('-');
    if (std::string_view::npos == dash)
    {
        return std::unexpected(config_error(name, "expected <low>-<high>"));
    }
    auto low = parse_uint(name, value.substr(0, dash), 1, 65535);
    if (!low)
    {
        return std::unexpected(low.error());
    }
    auto high = parse_uint(name, value.substr(dash + 1), 1, 65535);
    if (!high)
    {
        return std::unexpected(high.error());
    }
    if (*low > *high)
    {
        return std::unexpected(config_error(name, "low port is above the high port"));
    }
    return std::pair{static_cast<std::uint16_t>(*low), static_cast<std::uint16_t>(*high)};
}

[[nodiscard]] bool is_power_of_two(std::uint64_t value) noexcept
{
    return 0 != value && 0 == (value & (value - 1));
}

[[nodiscard]] bool is_hex_digit(char c) noexcept
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

[[nodiscard]] std::expected<void, Error> validate_neighbours(std::string_view name, std::string_view value) noexcept
{
    std::size_t begin = 0;
    while (begin < value.size())
    {
        std::size_t end = value.find(',', begin);
        if (std::string_view::npos == end)
        {
            end = value.size();
        }
        const std::string_view entry = value.substr(begin, end - begin);
        begin = end + 1;

        if (entry.empty())
        {
            return std::unexpected(config_error(name, "empty neighbour entry"));
        }
        const std::size_t equals = entry.find('=');
        if (std::string_view::npos == equals)
        {
            return std::unexpected(config_error(name, "expected <address>=<mac>"));
        }
        auto address = parse_ipv4(name, entry.substr(0, equals));
        if (!address)
        {
            return std::unexpected(address.error());
        }
        const std::string_view mac = entry.substr(equals + 1);
        if (mac.size() != 17)
        {
            return std::unexpected(config_error(name, "MAC address is not xx:xx:xx:xx:xx:xx"));
        }
        for (std::size_t i = 0; i < mac.size(); ++i)
        {
            const bool separator = (i % 3) == 2;
            if ((separator && mac[i] != ':') || (!separator && !is_hex_digit(mac[i])))
            {
                return std::unexpected(config_error(name, "MAC address is not xx:xx:xx:xx:xx:xx"));
            }
        }
    }
    return {};
}

[[nodiscard]] std::optional<std::string_view> value_of(const PropertySource& properties, std::string_view name) noexcept
{
    return properties.get(name);
}

}  // namespace

std::expected<Settings, Error> parse_settings(const PropertySource& properties) noexcept
{
    // An unknown name is refused rather than ignored (the reference's own
    // configuration policy for the names it serves).
    std::optional<Error> unknown;
    properties.for_each(
        [&unknown](std::string_view name, std::string_view /*value*/)
        {
            if (unknown || !name.starts_with(PROPERTY_PREFIX))
            {
                return;
            }
            bool known = false;
            for (std::string_view candidate : KNOWN_PROPERTIES)
            {
                if (name == candidate)
                {
                    known = true;
                    break;
                }
            }
            if (!known)
            {
                unknown = config_error(name, "unknown DPDK property");
            }
        });
    if (unknown)
    {
        return std::unexpected(*unknown);
    }

    Settings settings;

    if (auto value = value_of(properties, "deepmsg.dpdk.enabled"))
    {
        if (!is_bool(*value))
        {
            return std::unexpected(config_error("deepmsg.dpdk.enabled", "expected true or false"));
        }
        settings.enabled = *value == "true";
    }

    if (auto value = value_of(properties, "deepmsg.dpdk.eal.args"))
    {
        settings.eal_args = std::string{*value};
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.port"))
    {
        if (value->empty())
        {
            return std::unexpected(config_error("deepmsg.dpdk.port", "value is empty"));
        }
        settings.port = std::string{*value};
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.ipv4.address"))
    {
        auto parsed = parse_ipv4_prefix("deepmsg.dpdk.ipv4.address", *value);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.ipv4_address = *parsed;
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.ipv4.gateway"))
    {
        auto parsed = parse_ipv4("deepmsg.dpdk.ipv4.gateway", *value);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.ipv4_gateway = *parsed;
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.static.neighbours"))
    {
        if (auto valid = validate_neighbours("deepmsg.dpdk.static.neighbours", *value); !valid)
        {
            return std::unexpected(valid.error());
        }
        settings.static_neighbours = std::string{*value};
    }

    if (auto value = value_of(properties, "deepmsg.dpdk.rx.descriptors"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.rx.descriptors", *value, 64, 4096);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.rx_descriptors = static_cast<std::uint16_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.tx.descriptors"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.tx.descriptors", *value, 64, 4096);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.tx_descriptors = static_cast<std::uint16_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.mempool.size"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.mempool.size", *value, 1, 16777216);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.mempool_size = static_cast<std::uint32_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.mempool.cache"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.mempool.cache", *value, 0, 512);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.mempool_cache = static_cast<std::uint32_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.rx.burst"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.rx.burst", *value, 1, 64);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.rx_burst = static_cast<std::uint16_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.sender.ring"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.sender.ring", *value, 64, 1048576);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        if (!is_power_of_two(*parsed))
        {
            return std::unexpected(config_error("deepmsg.dpdk.sender.ring", "must be a power of two"));
        }
        settings.sender_ring = static_cast<std::uint32_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.ephemeral.ports"))
    {
        auto parsed = parse_port_range("deepmsg.dpdk.ephemeral.ports", *value);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.ephemeral_low = parsed->first;
        settings.ephemeral_high = parsed->second;
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.arp.timeout.ms"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.arp.timeout.ms", *value, 1, 86400000);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.arp_timeout_ms = static_cast<std::uint32_t>(*parsed);
    }
    if (auto value = value_of(properties, "deepmsg.dpdk.arp.retry.ms"))
    {
        auto parsed = parse_uint("deepmsg.dpdk.arp.retry.ms", *value, 1, 86400000);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.arp_retry_ms = static_cast<std::uint32_t>(*parsed);
    }

    if (settings.enabled && !settings.ipv4_address)
    {
        return std::unexpected(config_error("deepmsg.dpdk.ipv4.address", "required when the DPDK media is enabled"));
    }

    return settings;
}

Media::~Media()
{
    std::ignore = close();
}

std::expected<void, Error> Media::close() noexcept
{
    if (closed_)
    {
        return {};
    }
    closed_ = true;
    // P1: nothing has been opened yet. P3 closes the port and the pool here.
    return {};
}

std::expected<Media, Error> install_if_configured(aeron_driver_context_t* context,
                                                  const PropertySource& properties) noexcept
{
    (void)context;

    auto settings = parse_settings(properties);
    if (!settings)
    {
        return std::unexpected(settings.error());
    }
    if (!settings->enabled)
    {
        return Media{};
    }

#if defined(AXC_WITH_DPDK)
    return std::unexpected(
        make_error(ErrorCode::config_error,
                   "deepmsg.dpdk.enabled=true: the DPDK media is not implemented in this build yet (P3)"));
#else
    return std::unexpected(make_error(ErrorCode::config_error,
                                      "deepmsg.dpdk.enabled=true but this build has no DPDK media; "
                                      "configure with -DAXC_WITH_DPDK=ON"));
#endif
}

}  // namespace axc::core::media::dpdk
