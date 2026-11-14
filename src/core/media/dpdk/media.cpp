#include "core/media/dpdk/media.h"

#include <arpa/inet.h>
#include <algorithm>
#include <tuple>

#include <charconv>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#if defined(AXC_WITH_DPDK)
#include "core/media/dpdk/dpdk_port.h"
#include "core/media/dpdk/eal.h"
#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/port.h"
#include "core/media/dpdk/stack/stack.h"
#endif

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

#if defined(AXC_WITH_DPDK)
[[nodiscard]] int hex_value(char value) noexcept
{
    if (value >= '0' && value <= '9')
    {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f')
    {
        return 10 + (value - 'a');
    }
    if (value >= 'A' && value <= 'F')
    {
        return 10 + (value - 'A');
    }
    return -1;
}

[[nodiscard]] std::vector<std::pair<std::uint32_t, stack::Mac>> parse_neighbours(std::string_view text) noexcept
{
    std::vector<std::pair<std::uint32_t, stack::Mac>> result;
    std::size_t begin = 0;
    while (begin < text.size())
    {
        std::size_t end = text.find(',', begin);
        if (std::string_view::npos == end)
        {
            end = text.size();
        }
        const std::string_view entry = text.substr(begin, end - begin);
        begin = end + 1;

        const std::size_t equals = entry.find('=');
        if (std::string_view::npos == equals)
        {
            continue;
        }
        const std::string address_text{entry.substr(0, equals)};
        struct in_addr address{};
        if (1 != ::inet_pton(AF_INET, address_text.c_str(), &address))
        {
            continue;
        }
        const std::string_view mac_text = entry.substr(equals + 1);
        if (17 != mac_text.size())
        {
            continue;
        }
        stack::Mac mac{};
        bool valid = true;
        for (std::size_t index = 0; index < mac.size(); ++index)
        {
            const int high = hex_value(mac_text[index * 3]);
            const int low = hex_value(mac_text[index * 3 + 1]);
            if (high < 0 || low < 0)
            {
                valid = false;
                break;
            }
            mac[index] = static_cast<std::uint8_t>((high << 4) | low);
        }
        if (valid)
        {
            result.emplace_back(ntohl(address.s_addr), mac);
        }
    }
    return result;
}
#endif

}  // namespace

struct Media::Impl
{
#if defined(AXC_WITH_DPDK)
    Eal eal;
    Mempool pool;
    Port port;
    std::unique_ptr<stack::Stack> stack;
#endif
};

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

Media::Media() noexcept = default;

Media::Media(Media&& other) noexcept
    : impl_{std::move(other.impl_)}, eal_started_{other.eal_started_}, closed_{other.closed_}
{
    other.impl_.reset();
    other.eal_started_ = false;
    other.closed_ = true;
}

Media& Media::operator=(Media&& other) noexcept
{
    if (this != &other)
    {
        std::ignore = close();
        impl_ = std::move(other.impl_);
        eal_started_ = other.eal_started_;
        closed_ = other.closed_;
        other.impl_.reset();
        other.eal_started_ = false;
        other.closed_ = true;
    }
    return *this;
}

Media::~Media()
{
    std::ignore = close();
}

stack::Stack* Media::stack() const noexcept
{
#if defined(AXC_WITH_DPDK)
    return impl_ ? impl_->stack.get() : nullptr;
#else
    return nullptr;
#endif
}

std::expected<void, Error> Media::close() noexcept
{
    if (closed_)
    {
        return {};
    }
    closed_ = true;
    impl_.reset();
#if defined(AXC_WITH_DPDK)
    if (eal_started_)
    {
        eal_started_ = false;
        return Eal::cleanup();
    }
#endif
    return {};
}

std::expected<Media, Error> open_media(const Settings& settings) noexcept
{
#if !defined(AXC_WITH_DPDK)
    (void)settings;
    return std::unexpected(
        make_error(ErrorCode::config_error, "this build has no DPDK media; configure with -DAXC_WITH_DPDK=ON"));
#else
    if (!settings.enabled)
    {
        return Media{};
    }
    if (!settings.ipv4_address)
    {
        return std::unexpected(config_error("deepmsg.dpdk.ipv4.address", "required when the DPDK media is enabled"));
    }

    std::vector<std::string> eal_args{"deepmsg-dpdk"};
    std::string_view remaining = settings.eal_args;
    while (!remaining.empty())
    {
        const std::size_t start = remaining.find_first_not_of(" \t");
        if (std::string_view::npos == start)
        {
            break;
        }
        remaining.remove_prefix(start);
        const std::size_t end = remaining.find_first_of(" \t");
        eal_args.emplace_back(remaining.substr(0, end));
        remaining = std::string_view::npos == end ? std::string_view{} : remaining.substr(end);
    }

    auto eal = Eal::init(eal_args);
    if (!eal)
    {
        return std::unexpected(eal.error());
    }

    auto pool =
        Mempool::create("deepmsg-dpdk-pool", settings.mempool_size.value_or(8191), settings.mempool_cache, 0, -1);
    if (!pool)
    {
        std::ignore = Eal::cleanup();
        return std::unexpected(pool.error());
    }
    auto pool_owner = std::make_unique<Mempool>(std::move(*pool));

    auto port = settings.port ? Port::open_by_name(*settings.port) : Port::open(0);
    if (!port)
    {
        std::ignore = Eal::cleanup();
        return std::unexpected(port.error());
    }
    auto port_owner = std::make_unique<Port>(std::move(*port));

    const auto fail = [&port_owner, &pool_owner](Error error) -> std::expected<Media, Error>
    {
        port_owner.reset();
        pool_owner.reset();
        std::ignore = Eal::cleanup();
        return std::unexpected(error);
    };

    const auto info = port_owner->info();
    if (!info)
    {
        return fail(info.error());
    }
    const auto mac = port_owner->mac();
    if (!mac)
    {
        return fail(mac.error());
    }
    const auto mtu = port_owner->mtu();
    if (!mtu)
    {
        return fail(mtu.error());
    }

    const std::uint64_t rx_bits = Port::rx_offload_ipv4_checksum() | Port::rx_offload_udp_checksum();
    const std::uint64_t tx_bits = Port::tx_offload_ipv4_checksum() | Port::tx_offload_udp_checksum();
    const bool rx_checksums = (info->rx_offloads & rx_bits) == rx_bits;
    const bool tx_checksums = (info->tx_offloads & tx_bits) == tx_bits;

    // DEDICATED wants one receive queue and two transmit queues. A virtual
    // device reports one of each; the single-queue fallback is for tests and
    // for a media that is driven by one thread.
    const std::uint16_t rx_queues = std::min<std::uint16_t>(1, info->max_rx_queues);
    const std::uint16_t tx_queues = std::min<std::uint16_t>(2, info->max_tx_queues);
    if (0 == rx_queues || 0 == tx_queues)
    {
        return fail(make_error(ErrorCode::config_error, "the DPDK port has no queue to open"));
    }

    if (auto configured =
            port_owner->configure(rx_queues, tx_queues, rx_checksums ? rx_bits : 0, tx_checksums ? tx_bits : 0);
        !configured)
    {
        return fail(configured.error());
    }
    if (auto setup = port_owner->setup_rx_queue(0, settings.rx_descriptors, *pool_owner); !setup)
    {
        return fail(setup.error());
    }
    if (auto setup = port_owner->setup_tx_queue(0, settings.tx_descriptors); !setup)
    {
        return fail(setup.error());
    }
    if (tx_queues > 1)
    {
        if (auto setup = port_owner->setup_tx_queue(1, settings.tx_descriptors); !setup)
        {
            return fail(setup.error());
        }
    }
    if (auto started = port_owner->start(); !started)
    {
        return fail(started.error());
    }

    stack::StackParams params;
    params.address = settings.ipv4_address->address;
    params.prefix_length = settings.ipv4_address->prefix_length;
    params.mac = *mac;
    params.gateway = settings.ipv4_gateway;
    params.mtu = *mtu;
    params.ephemeral_low = settings.ephemeral_low;
    params.ephemeral_high = settings.ephemeral_high;
    params.rx_burst = settings.rx_burst;
    params.arp_interval_ns = static_cast<std::int64_t>(settings.arp_retry_ms) * 1000 * 1000;
    params.arp_timeout_ns = static_cast<std::int64_t>(settings.arp_timeout_ms) * 1000 * 1000;
    params.static_neighbours = parse_neighbours(settings.static_neighbours);

    // The adapters hold raw pointers to the port and the pool, so they are
    // built from the Impl's own members, never from the locals that were moved
    // into it.
    auto impl = std::make_unique<Media::Impl>(
        Media::Impl{std::move(*eal), std::move(*pool_owner), std::move(*port_owner), nullptr});

    const unsigned receiver_queue = tx_queues > 1 ? 1 : 0;
    auto rx_adapter = std::make_unique<DpdkPort>(impl->port, impl->pool, 0, 0, tx_checksums);
    auto sender_adapter = std::make_unique<DpdkPort>(impl->port, impl->pool, 0, 0, tx_checksums);
    auto receiver_adapter = std::make_unique<DpdkPort>(impl->port, impl->pool, 0, receiver_queue, tx_checksums);
    impl->stack = std::make_unique<stack::Stack>(params, std::move(rx_adapter), std::move(sender_adapter),
                                                 std::move(receiver_adapter));

    Media media;
    media.impl_ = std::move(impl);
    media.eal_started_ = true;
    return media;
#endif
}

std::expected<Media, Error> open_media(const PropertySource& properties) noexcept
{
    auto settings = parse_settings(properties);
    if (!settings)
    {
        return std::unexpected(settings.error());
    }
    return open_media(*settings);
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
        make_error(ErrorCode::config_error, "deepmsg.dpdk.enabled=true: the DPDK bindings are not wired yet (P4b)"));
#else
    return std::unexpected(make_error(ErrorCode::config_error,
                                      "deepmsg.dpdk.enabled=true but this build has no DPDK media; "
                                      "configure with -DAXC_WITH_DPDK=ON"));
#endif
}

}  // namespace axc::core::media::dpdk
